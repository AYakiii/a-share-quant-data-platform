from __future__ import annotations

import argparse

import akshare as ak

from qsys.data.factor_lake.runner import run_probe


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local factor lake probe")
    parser.add_argument("--output-root", default="outputs/factor_lake_probe")
    parser.add_argument("--family")
    parser.add_argument("--api-name")
    parser.add_argument("--case-id")
    parser.add_argument("--max-cases", type=int)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-sleep", type=float, default=0.0)
    parser.add_argument("--enabled-only", action="store_true")
    parser.add_argument("--run-name")
    args = parser.parse_args()

    run_probe(
        ak_module=ak,
        output_root=args.output_root,
        family=args.family,
        api_name=args.api_name,
        case_id=args.case_id,
        max_cases=args.max_cases,
        enabled_only=args.enabled_only,
        timeout_seconds=args.request_timeout,
        request_sleep=args.request_sleep,
        run_name=args.run_name,
    )


if __name__ == "__main__":
    main()
