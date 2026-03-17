from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"


def _run_module(module: str) -> int:
    env = dict(**__import__("os").environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SRC_PATH}:{existing}" if existing else str(SRC_PATH)

    cmd = [sys.executable, "-m", module]
    print(f"\n=== Running: {module} ===")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env)
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run demo workflows for the A-share research-oriented trading system."
    )
    parser.add_argument(
        "--mode",
        choices=[
            "synthetic",
            "signal",
            "diagnostics",
            "backtest",
            "impact",
            "all",
        ],
        default="all",
        help="Which demo to run.",
    )
    args = parser.parse_args()

    module_map = {
        "synthetic": ["qsys.utils.generate_synthetic_feature_store"],
        "signal": ["qsys.utils.signal_engine_example"],
        "diagnostics": ["qsys.utils.research_diagnostics_example"],
        "backtest": ["qsys.utils.backtest_example"],
        "impact": ["qsys.utils.constraint_impact_example"],
        "all": [
            "qsys.utils.generate_synthetic_feature_store",
            "qsys.utils.signal_engine_example",
            "qsys.utils.research_diagnostics_example",
            "qsys.utils.backtest_example",
            "qsys.utils.constraint_impact_example",
        ],
    }

    failures: list[str] = []
    for module in module_map[args.mode]:
        code = _run_module(module)
        if code != 0:
            failures.append(module)

    if failures:
        print("\nDemo run finished with failures:")
        for m in failures:
            print(f" - {m}")
        raise SystemExit(1)

    print("\nAll requested demo steps completed successfully.")


if __name__ == "__main__":
    main()