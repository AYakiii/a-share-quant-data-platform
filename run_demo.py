from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"


def _run_module(module: str, module_args: list[str] | None = None) -> int:
    env = dict(**__import__("os").environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SRC_PATH}:{existing}" if existing else str(SRC_PATH)

    cmd = [sys.executable, "-m", module, *(module_args or [])]
    print(f"\n=== Running: {module} ===")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env)
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run synthetic demo workflows only (deprecated for real-data research)."
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
    print(
        "[DEPRECATED] run_demo.py is synthetic/demo-only. "
        "Use qsys.utils.build_real_feature_store and explicit --feature-root workflows for real data."
    )

    default_feature_root = "data/processed/feature_store/v1"
    module_map = {
        "synthetic": [("qsys.utils.generate_synthetic_feature_store", [])],
        "signal": [("qsys.utils.signal_engine_example", ["--feature-root", default_feature_root])],
        "diagnostics": [("qsys.utils.research_diagnostics_example", ["--feature-root", default_feature_root])],
        "backtest": [("qsys.utils.backtest_example", ["--feature-root", default_feature_root])],
        "impact": [("qsys.utils.constraint_impact_example", ["--feature-root", default_feature_root])],
        "all": [
            ("qsys.utils.generate_synthetic_feature_store", []),
            ("qsys.utils.signal_engine_example", ["--feature-root", default_feature_root]),
            ("qsys.utils.research_diagnostics_example", ["--feature-root", default_feature_root]),
            ("qsys.utils.backtest_example", ["--feature-root", default_feature_root]),
            ("qsys.utils.constraint_impact_example", ["--feature-root", default_feature_root]),
        ],
    }

    failures: list[str] = []
    for module, module_args in module_map[args.mode]:
        code = _run_module(module, module_args)
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
