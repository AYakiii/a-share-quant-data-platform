from __future__ import annotations

import argparse

from qsys.data.factor_lake.backfill_tasks import (
    export_backfill_tasks_csv,
    filter_tasks,
    generate_tasks_from_default_backfill_plan,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export raw backfill tasks and dry-run summary")
    parser.add_argument("--output-root", default="outputs/factor_lake_registry")
    parser.add_argument("--priority")
    parser.add_argument("--source-family")
    parser.add_argument("--dataset-name")
    parser.add_argument("--max-tasks", type=int)
    args = parser.parse_args()

    tasks = generate_tasks_from_default_backfill_plan()
    tasks = filter_tasks(
        tasks,
        priority=args.priority,
        source_family=args.source_family,
        dataset_name=args.dataset_name,
        max_tasks=args.max_tasks,
    )
    t_path, s_path = export_backfill_tasks_csv(args.output_root, tasks)
    print(t_path)
    print(s_path)


if __name__ == "__main__":
    main()
