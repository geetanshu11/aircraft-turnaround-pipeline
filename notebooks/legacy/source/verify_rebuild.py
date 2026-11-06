"""Compare a fresh reconstruction with the source warehouse using SQL EXCEPT ALL."""
from pathlib import Path
import json
from aircraft_core import Config, connect, sql_path, write_json
from run_pipeline import run_pipeline, replay_inputs


def verify_rebuild(source_cfg, destination):
    destination=Path(destination)
    if destination.exists() and any(destination.iterdir()):raise ValueError('Rebuild destination must be empty')
    rebuilt=Config(destination,months=source_cfg.months,mode=source_cfg.mode,max_turn_minutes=source_cfg.max_turn_minutes)
    run_pipeline(rebuilt,replay_inputs(source_cfg.root,source_cfg.months),publish_output=False)
    con=connect(source_cfg)
    try:
        con.execute(f"ATTACH {sql_path(rebuilt.db)} AS rebuilt (READ_ONLY)")
        result={}
        for table in ['flights','fact_turnarounds','mart_daily_reliability','mart_airport_daily','mart_routes','mart_turnaround_airports','mart_turn_bands','mart_reported_causes','mart_link_coverage']:
            n=con.execute(f"SELECT count(*) FROM ((SELECT * FROM main.{table} EXCEPT ALL SELECT * FROM rebuilt.main.{table}) UNION ALL (SELECT * FROM rebuilt.main.{table} EXCEPT ALL SELECT * FROM main.{table}))").fetchone()[0]
            result[table]=n
        con.execute('DETACH rebuilt')
    finally:con.close()
    if any(result.values()):raise AssertionError(f'Rebuild differences: {result}')
    rerun=run_pipeline(rebuilt,replay_inputs(source_cfg.root,source_cfg.months),publish_output=False)
    if any(p['status']!='unchanged' for p in rerun['partitions']):raise AssertionError('Replay was not idempotent')
    result={'business_table_differences':result,'second_replay_all_unchanged':True}
    write_json(destination/'logs/rebuild_verification.json',result)
    return result
