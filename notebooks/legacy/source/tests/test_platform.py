from pathlib import Path
import json
import zipfile
import pandas as pd
import pytest
from filelock import FileLock, Timeout
from aircraft_core import Config, connect, initialize, reference_snapshot, sha256, locked
from aircraft_transform import COLUMNS, normalize, clock_minutes, utc_schedule
from aircraft_ingest import ingest_month, validate_zip, download_zip
from aircraft_models import build_models, metrics
from run_pipeline import run_pipeline


def row(**changes):
    r={k:"" for k in COLUMNS}
    r.update(FlightDate='2025-03-10',Reporting_Airline='AA',DOT_ID_Reporting_Airline='19805',Tail_Number='N123AA',
             Flight_Number_Reporting_Airline='101',OriginAirportID='10721',Origin='BOS',OriginCityName='Boston, MA',
             DestAirportID='12478',Dest='JFK',DestCityName='New York, NY',CRSDepTime='0800',CRSArrTime='0900',
             DepTime='0820',ArrTime='0920',DepDelay='20',ArrDelay='20',CRSElapsedTime='60',ActualElapsedTime='60',
             Cancelled='0',Diverted='0',Distance='187',TaxiOut='10',TaxiIn='5')
    r.update(changes)
    return r


def second(**changes):
    return row(Flight_Number_Reporting_Airline='102',OriginAirportID='12478',Origin='JFK',OriginCityName='New York, NY',
               DestAirportID='10721',Dest='BOS',DestCityName='Boston, MA',CRSDepTime='1000',CRSArrTime='1100',
               DepTime='1010',ArrTime='1110',DepDelay='10',ArrDelay='10',**changes)


def zip_rows(tmp_path,rows,name='source.zip'):
    path=tmp_path/name
    with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
        z.writestr('source.csv',pd.DataFrame(rows).to_csv(index=False))
    return path


@pytest.fixture
def cfg(tmp_path):
    c=Config(tmp_path/'runtime',months=('2025-03',),chunk_rows=2)
    initialize(c)
    return c


@pytest.fixture
def airports(cfg):
    return reference_snapshot(cfg)[0]


@pytest.mark.parametrize('hhmm,expected',[('0000',0),('2359',1439),('2400',1440),('1259',779),('1260',None),('2401',None),('-1',None),('1.5',None),('',None)])
def test_hhmm_contract(hhmm,expected):
    value=clock_minutes(pd.Series([hhmm])).iloc[0]
    assert pd.isna(value) if expected is None else value==expected


@pytest.mark.parametrize('date,clock,expected',[
    ('2025-03-08','1200','2025-03-08T17:00:00Z'),('2025-03-09','1200','2025-03-09T16:00:00Z'),
    ('2025-03-09','0230',None),('2025-11-02','0130',None),('2025-03-10','2400','2025-03-11T04:00:00Z')])
def test_timezone_dst_and_midnight(date,clock,expected):
    v=utc_schedule(pd.Series([date]),pd.Series([clock]),pd.Series(['America/New_York'])).iloc[0]
    assert pd.isna(v) if expected is None else v==pd.Timestamp(expected)


def test_real_clock_reconstruction(airports):
    good,bad=normalize(pd.DataFrame([row()]),'2025-03',airports)
    assert len(bad)==0 and bool(good.time_valid.iloc[0])
    assert good.actual_dep_utc.iloc[0]==pd.Timestamp('2025-03-10T12:20:00Z')


def test_cross_zone_overnight_and_multiday_delay(airports):
    r=row(Origin='LAX',Dest='JFK',CRSDepTime='2300',CRSArrTime='0700',CRSElapsedTime='300',
          DepTime='0100',ArrTime='0900',DepDelay='1560',ArrDelay='1560',ActualElapsedTime='300')
    d,_=normalize(pd.DataFrame([r]),'2025-03',airports)
    assert bool(d.time_valid.iloc[0])
    assert d.actual_arr_utc.iloc[0]==pd.Timestamp('2025-03-12T13:00:00Z')


@pytest.mark.parametrize('field,value',[('FlightDate','2025-02-28'),('Cancelled','2'),('Flight_Number_Reporting_Airline',''),('Distance','-10'),('DepDelay','oops'),('Tail_Number',None),('Origin','')])
def test_contract_rejection_and_nullable_tail(airports,field,value):
    d,bad=normalize(pd.DataFrame([row(**{field:value})]),'2025-03',airports)
    if field=='Tail_Number':assert len(d)==1 and pd.isna(d.tail_number.iloc[0])
    else:assert len(bad)==1 and len(d)==0


def test_unknown_timezone_retained_but_unlinked(airports):
    d,_=normalize(pd.DataFrame([row(Origin='ZZZ')]),'2025-03',airports)
    assert len(d)==1 and not bool(d.time_valid.iloc[0]) and d.time_status.iloc[0]=='missing_timezone'


def test_clock_mismatch_is_not_silently_corrected(airports):
    d,_=normalize(pd.DataFrame([row(ArrTime='0950')]),'2025-03',airports)
    assert not bool(d.time_valid.iloc[0])


def test_missing_delay_does_not_become_zero(airports):
    d,_=normalize(pd.DataFrame([row(DepDelay='')]),'2025-03',airports)
    assert pd.isna(d.actual_dep_utc.iloc[0]) and pd.isna(d.dep_delay_min.iloc[0])


def test_required_schema(airports):
    with pytest.raises(ValueError,match='schema'):
        normalize(pd.DataFrame([row()]).drop(columns='ArrDelay'),'2025-03',airports)


def test_idempotent_partition(cfg,tmp_path):
    p=zip_rows(tmp_path,[row(),second()])
    a=ingest_month(cfg,'2025-03','one',p);b=ingest_month(cfg,'2025-03','two',p)
    assert a['rows']==2 and b['status']=='unchanged'
    con=connect(cfg);assert con.execute('SELECT count(*) FROM flights').fetchone()[0]==2;con.close()


def test_duplicate_collapse_across_chunks(cfg,tmp_path):
    p=zip_rows(tmp_path,[row(),second(),row()])
    r=ingest_month(cfg,'2025-03','run',p)
    assert r['rows']==2 and r['duplicates']==1


def test_conflicting_key_fails_quality_gate(cfg,tmp_path):
    p=zip_rows(tmp_path,[row(),row(Tail_Number='N555AA')])
    with pytest.raises(ValueError,match='Quality gate'):
        ingest_month(cfg,'2025-03','run',p)
    con=connect(cfg);assert con.execute('SELECT count(*) FROM checkpoints').fetchone()[0]==0;con.close()


def test_revision_and_withdrawal_are_retained(cfg,tmp_path):
    first=zip_rows(tmp_path,[row(),second()],'first.zip');ingest_month(cfg,'2025-03','one',first)
    p=zip_rows(tmp_path,[row(Tail_Number='N999AA')],'revised.zip')
    r=ingest_month(cfg,'2025-03','two',p)
    assert r['updated']==1 and r['withdrawn']==1
    con=connect(cfg)
    assert con.execute('SELECT count(*) FROM partition_versions').fetchone()[0]==2
    assert con.execute('SELECT count(*) FROM flight_changes').fetchone()[0]==2
    assert len(list((cfg.root/'silver').rglob('*.parquet')))==2
    con.close()


def test_transaction_failure_preserves_previous_partition(cfg,tmp_path):
    p=zip_rows(tmp_path,[row(),second()]);a=ingest_month(cfg,'2025-03','one',p)
    p2=zip_rows(tmp_path,[row(Tail_Number='N999AA')],'change.zip')
    with pytest.raises(RuntimeError,match='Injected'):
        ingest_month(cfg,'2025-03','two',p2,fail_before_commit=True)
    con=connect(cfg)
    assert con.execute('SELECT count(*) FROM flights').fetchone()[0]==2
    assert con.execute('SELECT version_id FROM checkpoints').fetchone()[0]==a['version_id']
    assert con.execute('SELECT count(*) FROM flight_changes').fetchone()[0]==0
    con.close()
    assert ingest_month(cfg,'2025-03','retry',p2)['rows']==1


def test_source_checksum_tamper(cfg,tmp_path):
    p=zip_rows(tmp_path,[row()])
    with pytest.raises(ValueError,match='checksum'):
        ingest_month(cfg,'2025-03','run',p,{'sha256':'wrong'})


@pytest.mark.parametrize('name',['../bad.csv','/bad.csv','x\\bad.csv'])
def test_archive_paths(tmp_path,name):
    p=tmp_path/'bad.zip'
    with zipfile.ZipFile(p,'w') as z:z.writestr(name,'x')
    with pytest.raises(ValueError,match='Unsafe'):validate_zip(p)


def test_live_and_demo_cannot_mix(cfg):
    with pytest.raises(ValueError,match='separate'):
        initialize(Config(cfg.root,months=('2025-03',),mode='demo'))


def test_single_writer(cfg):
    with FileLock(str(cfg.root/'writer.lock')):
        with pytest.raises(Timeout):
            with locked(cfg):pass


def test_turnaround_identity_and_recovery(cfg,tmp_path):
    p=zip_rows(tmp_path,[row(),second()]);ingest_month(cfg,'2025-03','run',p);build_models(cfg)
    con=connect(cfg);v=con.execute('SELECT planned_turn_min,actual_turn_min,recovered_delay_min FROM fact_turnarounds').fetchone();con.close()
    assert v==(60.,50.,10.)


@pytest.mark.parametrize('barrier',['cancelled','diverted','discontinuity','carrier','overlap','long_gap'])
def test_broken_chain_is_excluded(cfg,tmp_path,barrier):
    r=second()
    if barrier=='cancelled':r.update(Cancelled='1')
    elif barrier=='diverted':r.update(Diverted='1')
    elif barrier=='discontinuity':r.update(Origin='BOS')
    elif barrier=='carrier':r.update(DOT_ID_Reporting_Airline='19790')
    elif barrier=='overlap':r.update(DepTime='0800',ArrTime='0900',DepDelay='-120',ArrDelay='-120')
    elif barrier=='long_gap':r.update(FlightDate='2025-03-11')
    p=zip_rows(tmp_path,[row(),r]);ingest_month(cfg,'2025-03','run',p);build_models(cfg)
    con=connect(cfg);assert con.execute('SELECT count(*) FROM fact_turnarounds').fetchone()[0]==0;con.close()


def test_cancelled_middle_leg_cannot_be_skipped(cfg,tmp_path):
    middle=row(Flight_Number_Reporting_Airline='999',CRSDepTime='0930',CRSArrTime='0945',Cancelled='1')
    p=zip_rows(tmp_path,[row(),middle,second()]);ingest_month(cfg,'2025-03','run',p);build_models(cfg)
    con=connect(cfg);assert con.execute('SELECT count(*) FROM fact_turnarounds').fetchone()[0]==0;con.close()


def test_failed_orchestration_has_audit(cfg,tmp_path):
    p=zip_rows(tmp_path,[row(Cancelled='9')])
    with pytest.raises(ValueError):run_pipeline(cfg,{'2025-03':{'path':p}},publish_output=False)
    con=connect(cfg);assert con.execute('SELECT status FROM pipeline_runs').fetchone()[0]=='failed';con.close()


def test_source_replay_reconstructs_same_business_facts(cfg,tmp_path):
    p=zip_rows(tmp_path,[row(),second()]);a=ingest_month(cfg,'2025-03','one',p);build_models(cfg)
    other=Config(tmp_path/'rebuild',months=('2025-03',));initialize(other);b=ingest_month(other,'2025-03','two',p);build_models(other)
    assert a['version_id']==b['version_id']
    assert metrics(cfg)==metrics(other)


class Response:
    def __init__(self,status,data=b'',headers=None):self.status_code=status;self.data=data;self.headers=headers or {}
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def raise_for_status(self):
        if self.status_code>=400:
            import requests
            raise requests.HTTPError(str(self.status_code))
    def iter_content(self,size):yield self.data


class Session:
    def __init__(self,items):self.items=iter(items);self.calls=0
    def get(self,*a,**k):self.calls+=1;v=next(self.items);return v


def test_retry_429_then_success(tmp_path):
    p=zip_rows(tmp_path,[row()]);s=Session([Response(429,headers={'Retry-After':'1'}),Response(200,p.read_bytes())]);wait=[]
    m=download_zip('https://example.test',tmp_path/'out.zip',session=s,sleeper=wait.append)
    assert s.calls==2 and wait==[1] and m['sha256']==sha256(p)


def test_404_not_retried(tmp_path):
    import requests
    s=Session([Response(404)])
    with pytest.raises(requests.HTTPError):download_zip('https://example.test',tmp_path/'out.zip',session=s,sleeper=lambda _:None)
    assert s.calls==1


def test_html_response_not_loaded(tmp_path):
    with pytest.raises(zipfile.BadZipFile):download_zip('https://example.test',tmp_path/'out.zip',session=Session([Response(200,b'<html>error</html>')]))
    assert not (tmp_path/'out.zip').exists()


def test_cross_month_aircraft_link(tmp_path):
    cfg=Config(tmp_path/'runtime',months=('2025-01','2025-02'));initialize(cfg)
    a=row(FlightDate='2025-01-31',CRSDepTime='2300',CRSArrTime='2400',DepTime='2320',ArrTime='0020')
    b=second();b.update(FlightDate='2025-02-01',CRSDepTime='0100',CRSArrTime='0200',DepTime='0110',ArrTime='0210')
    ingest_month(cfg,'2025-01','one',zip_rows(tmp_path,[a],'jan.zip'))
    ingest_month(cfg,'2025-02','two',zip_rows(tmp_path,[b],'feb.zip'))
    build_models(cfg)
    con=connect(cfg);assert con.execute('SELECT planned_turn_min,actual_turn_min FROM fact_turnarounds').fetchone()==(60.,50.);con.close()


def test_reactivation_has_its_own_audit(cfg,tmp_path):
    a=zip_rows(tmp_path,[row()],'a.zip');b=zip_rows(tmp_path,[row(Tail_Number='N999AA')],'b.zip')
    first=ingest_month(cfg,'2025-03','one',a);ingest_month(cfg,'2025-03','two',b)
    third=ingest_month(cfg,'2025-03','three',a)
    assert first['version_id']==third['version_id']
    con=connect(cfg);assert con.execute('SELECT count(*) FROM partition_activations').fetchone()[0]==3;con.close()


def test_checkpoint_roundtrip(cfg,tmp_path):
    from workspace_tools import make_checkpoint,safe_restore
    p=zip_rows(tmp_path,[row(),second()]);ingest_month(cfg,'2025-03','run',p);build_models(cfg)
    archive=tmp_path/'checkpoint.zip';make_checkpoint(cfg.root,archive)
    restored=tmp_path/'restored';safe_restore(archive,restored)
    assert sha256(cfg.db)==sha256(restored/'warehouse.duckdb')
    with pytest.raises(ValueError,match='empty'):safe_restore(archive,restored)


@pytest.mark.parametrize('names',[['../escape'],['/escape'],['a/../b','b'],['a\\b'],['C:bad']])
def test_checkpoint_path_safety(tmp_path,names):
    from workspace_tools import safe_restore
    archive=tmp_path/'bad.zip'
    with zipfile.ZipFile(archive,'w') as z:
        for name in names:z.writestr(name,'data')
    with pytest.raises(ValueError):safe_restore(archive,tmp_path/'target')


def test_no_eligible_pairs_is_null_not_zero(cfg,tmp_path):
    from aircraft_report import publish
    p=zip_rows(tmp_path,[row()]);ingest_month(cfg,'2025-03','run',p);build_models(cfg)
    m=metrics(cfg)
    assert m['eligible_turns']==0 and m['median_actual_turn_min'] is None
    assert m['conditional_outbound_late_pct'] is None
    output=publish(cfg)
    assert Path(output['report']).exists()
