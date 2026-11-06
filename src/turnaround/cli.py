"""Shared command line for Docker, PowerShell, CI and Airflow."""

import argparse
import json
from pathlib import Path
from .core import Config, month_range, read_json, write_json
from .pipeline import STAGES, abandon, create_job, execute_stage, resume, run_pipeline
from . import warehouse


def main(argv=None):
    parser = argparse.ArgumentParser(description="Aircraft Turnaround platform")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ["run", "plan"]:
        p = sub.add_parser(name)
        p.add_argument("--mode", choices=["demo", "live", "fixture"])
        p.add_argument("--root", type=Path)
        p.add_argument("--months", nargs="+")
        p.add_argument("--start")
        p.add_argument("--end")
        p.add_argument("--refresh", action="store_true")
        p.add_argument("--run-id")
        p.add_argument("--inputs-json", type=Path)
    p = sub.add_parser("stage")
    p.add_argument("stage", choices=STAGES)
    p.add_argument("--job", required=True)
    p = sub.add_parser("resume")
    p.add_argument("--job", required=True)
    for name in ["init", "health", "abandon", "replay-inputs"]:
        p = sub.add_parser(name)
        p.add_argument("--mode", choices=["demo", "live", "fixture"])
        p.add_argument("--root", type=Path)
        if name == "replay-inputs":
            p.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "stage":
        print(execute_stage(args.job, args.stage))
        return
    if args.command == "resume":
        result = resume(args.job)
    else:
        options = {k: getattr(args, k) for k in ["mode", "root"] if getattr(args, k, None) is not None}
        if args.command in {"run", "plan"}:
            options["refresh"] = args.refresh
        cfg = Config.from_env(**options)
        if args.command == "init":
            warehouse.initialize(cfg)
            result = {"status": "initialized", "mode": cfg.mode}
        elif args.command == "health":
            result = warehouse.health(cfg)
        elif args.command == "abandon":
            abandon(cfg)
            result = {"status": "lease_released"}
        elif args.command == "replay-inputs":
            with warehouse.connect(cfg) as con:
                rows = list(
                    con.execute(
                        "SELECT c.month,v.manifest FROM meta.checkpoints c JOIN meta.partition_versions v USING(version_id) ORDER BY c.month"
                    )
                )
            result = {
                r["month"]: {"path": str(cfg.root / r["manifest"]["raw_relative_path"]), "metadata": r["manifest"]}
                for r in rows
            }
            write_json(args.output, result)
        else:
            if bool(args.start) != bool(args.end) or (args.months and args.start):
                raise ValueError("Supply --months or both --start and --end")
            months = month_range(args.start, args.end) if args.start else args.months
            months = months or (["2025-03"] if cfg.mode == "demo" else ["2025-01", "2025-02", "2025-03"])
            inputs = read_json(args.inputs_json) if args.inputs_json else None
            if args.command == "plan":
                print(create_job(cfg, months, args.run_id, inputs))
                return
            result = run_pipeline(cfg, months, inputs, args.run_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
