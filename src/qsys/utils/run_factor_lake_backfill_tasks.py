from __future__ import annotations

import argparse
import json

from qsys.data.factor_lake.backfill_execute import execute_backfill_tasks
from qsys.data.factor_lake.backfill_tasks import filter_tasks, generate_tasks_from_default_backfill_plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Run/dry-run Raw Factor Lake backfill tasks")
    parser.add_argument("--output-root", default="outputs/factor_lake_backfill")
    parser.add_argument("--metastore-path", default="outputs/factor_lake_backfill/metastore.sqlite")
    parser.add_argument("--priority")
    parser.add_argument("--source-family")
    parser.add_argument("--dataset-name")
    parser.add_argument("--max-tasks", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--request-sleep", type=float, default=0.0)
    args = parser.parse_args()

    dry_run = True
    if args.execute:
        dry_run = False
    elif args.dry_run:
        dry_run = True

    if not dry_run and args.max_tasks is None:
        raise ValueError("For safety, --execute requires --max-tasks.")

    tasks = generate_tasks_from_default_backfill_plan()
    tasks = filter_tasks(tasks, priority=args.priority, source_family=args.source_family, dataset_name=args.dataset_name, max_tasks=args.max_tasks)
    result = execute_backfill_tasks(tasks, output_root=args.output_root, metastore_path=args.metastore_path, max_tasks=args.max_tasks, dry_run=dry_run, request_sleep=args.request_sleep)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
