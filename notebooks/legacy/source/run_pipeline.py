"""The same orchestration entry point is used by Colab and the command line."""
from pathlib import Path
import argparse
import json
import time
from aircraft_core import Config, connect, locked, log, now, run_id, write_json
from aircraft_ingest import ingest_month, event
from aircraft_models import build_models
from aircraft_report import publish


def run_pipeline(cfg, inputs=None, publish_output=True):
    inputs=inputs or {}
    with locked(cfg):
        rid=run_id(); start=time.monotonic()
        con=connect(cfg)
        con.execute("INSERT INTO pipeline_runs VALUES (?,?,NULL,'running',?,?,NULL)",[rid,now(),cfg.mode,json.dumps(cfg.months)])
        con.close()
        try:
            loaded=[]
            for month in cfg.months:
                item=inputs.get(month,{})
                result=ingest_month(cfg,month,rid,item.get("path"),item.get("metadata"))
                loaded.append(result)
                print(f"{month}: {result['status']} · {result['rows']:,} accepted flights",flush=True)
            checks=build_models(cfg)
            con=connect(cfg);event(con,rid,"all","models","succeeded",checks);con.close()
            result=publish(cfg) if publish_output else {}
            con=connect(cfg)
            event(con,rid,"all","publish","succeeded" if publish_output else "skipped",{"publication_id":result.get("publication_id")})
            con.execute("UPDATE pipeline_runs SET ended_at=?,status='succeeded' WHERE run_id=?",[now(),rid]);con.close()
            result.update({"run_id":rid,"partitions":loaded,"seconds":round(time.monotonic()-start,2)})
            write_json(cfg.root/"logs/latest_run.json",result)
            log(cfg,"pipeline_succeeded",run_id=rid,seconds=result["seconds"])
            return result
        except Exception as exc:
            con=connect(cfg)
            con.execute("UPDATE pipeline_runs SET ended_at=?,status='failed',error=? WHERE run_id=?",[now(),str(exc)[:2000],rid]);con.close()
            log(cfg,"pipeline_failed",run_id=rid,error=str(exc)[:2000])
            raise


def replay_inputs(raw_root, months):
    raw_root=Path(raw_root)
    result={}
    for month in months:
        pointer=raw_root/"bronze"/month/"latest.json"
        meta=json.loads(pointer.read_text())
        path=(raw_root/meta["raw_relative_path"]).resolve()
        if not path.is_relative_to(raw_root.resolve()):
            raise ValueError("Raw path escapes archive root")
        result[month]={"path":path,"metadata":meta}
    return result


def demo_inputs():
    p=Path(__file__).parent/"demo"
    meta=json.loads((p/"source.json").read_text())
    return {"2025-03":{"path":p/"bts_demo.zip","metadata":meta}}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description="Aircraft turnaround batch pipeline")
    parser.add_argument('--root',default='runtime_live')
    parser.add_argument('--months',nargs='+',default=['2025-01','2025-02','2025-03'])
    parser.add_argument('--mode',choices=['live','demo'],default='live')
    parser.add_argument('--refresh',action='store_true',help='Fetch source ZIPs again to detect revisions')
    parser.add_argument('--replay-root',help='Existing runtime containing immutable bronze sources')
    parser.add_argument('--max-turn-minutes',type=int,default=720)
    parser.add_argument('--skip-report',action='store_true')
    args=parser.parse_args()
    months=('2025-03',) if args.mode=='demo' else tuple(args.months)
    cfg=Config(args.root,months=months,mode=args.mode,refresh=args.refresh,max_turn_minutes=args.max_turn_minutes)
    inputs=replay_inputs(args.replay_root,months) if args.replay_root else (demo_inputs() if args.mode=='demo' else None)
    result=run_pipeline(cfg,inputs=inputs,publish_output=not args.skip_report)
    print(json.dumps({k:v for k,v in result.items() if k!='metrics'},indent=2,default=str))
