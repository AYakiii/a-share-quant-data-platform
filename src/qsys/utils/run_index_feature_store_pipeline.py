"""Index-based real feature-store pipeline wrapper (Phase 18A-data-pipeline)."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from qsys.reporting.artifacts import write_warnings
from qsys.universe.csindex import build_universe_sample, fetch_index_components, normalize_component_codes
from qsys.utils.build_real_feature_store import build_real_feature_store
from qsys.utils.run_technical_liquidity_real_runner import run_technical_liquidity_real_runner


def _select_index_symbols(index_code: str, n_assets: int | None, all_assets: bool, seed: int) -> list[str]:
    if all_assets:
        comp = fetch_index_components(index_code)
        symbols = sorted(set(normalize_component_codes(comp).tolist()))
        if not symbols:
            raise ValueError(f"No constituents found for index code {index_code}")
        return symbols

    n = n_assets if n_assets is not None else 100
    symbols, _meta = build_universe_sample([index_code], n=n, seed=seed)
    if not symbols:
        raise ValueError(f"No sampled symbols found for index code {index_code}")
    return symbols


def run_index_feature_store_pipeline(
    *,
    index_code: str = "000905",
    n_assets: int | None = None,
    all_assets: bool = False,
    seed: int = 42,
    start_date: str,
    end_date: str,
    clean_run: bool = False,
    run_technical_liquidity: bool = False,
    feature_root: str | Path = "data/processed/feature_store/v1",
    output_dir: str | Path = "outputs/data_pipeline",
    factor_output_dir: str | Path = "outputs/factor_research",
    run_name: str | None = None,
    retries: int = 2,
    retry_wait: float = 1.0,
    request_sleep: float = 0.5,
    source_panel_version: str = "feature_store_v1",
) -> dict[str, Path]:
    """Build index-based feature store and optionally run technical/liquidity diagnostics."""

    if all_assets and n_assets is not None:
        raise ValueError("--all-assets and --n-assets cannot be used together")

    pipeline_name = run_name or f"index_{index_code}_{start_date}_{end_date}"
    run_root = Path(output_dir) / pipeline_name
    run_root.mkdir(parents=True, exist_ok=True)
    warnings: list[str] = []

    symbols = _select_index_symbols(index_code=index_code, n_assets=n_assets, all_assets=all_assets, seed=seed)

    symbols_fp = run_root / "symbols.txt"
    symbols_fp.write_text("\n".join(symbols) + "\n", encoding="utf-8")

    feature_root_path = Path(feature_root)
    if clean_run and feature_root_path.exists():
        shutil.rmtree(feature_root_path)

    built_feature_root = build_real_feature_store(
        feature_root=feature_root_path,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        retries=retries,
        retry_wait=retry_wait,
        request_sleep=request_sleep,
    )

    result: dict[str, Path] = {
        "symbols": symbols_fp,
        "feature_root": built_feature_root,
    }

    if run_technical_liquidity:
        factor_run_name = f"{pipeline_name}_technical_liquidity"
        factor_artifacts = run_technical_liquidity_real_runner(
            feature_root=built_feature_root,
            output_dir=factor_output_dir,
            start_date=start_date,
            end_date=end_date,
            run_name=factor_run_name,
            source_panel_version=source_panel_version,
            data_source_type="real",
        )
        result["factor_run_dir"] = Path(factor_output_dir) / factor_run_name
        result.update({f"factor_{k}": v for k, v in factor_artifacts.items()})

    assumptions = [
        "Index constituents are fetched from index component source and normalized to AkShare symbols.",
        "Feature store is built only for selected symbols (no full A-share fallback when index code is provided).",
        "Pipeline output is for data/factor diagnostics only and does not imply alpha or tradable strategy performance.",
    ]

    manifest = {
        "index_code": index_code,
        "n_assets": n_assets,
        "all_assets": all_assets,
        "seed": seed,
        "start_date": start_date,
        "end_date": end_date,
        "selected_symbols": symbols,
        "n_selected_symbols": len(symbols),
        "feature_root": str(built_feature_root),
        "output_dir": str(run_root),
        "factor_output_dir": str(factor_output_dir),
        "run_name": pipeline_name,
        "retries": retries,
        "retry_wait": retry_wait,
        "request_sleep": request_sleep,
        "clean_run": clean_run,
        "run_technical_liquidity": run_technical_liquidity,
        "source_panel_version": source_panel_version,
        "assumptions": assumptions,
        "note": "This is a data/factor diagnostics pipeline, not alpha evidence and not a tradable strategy.",
        "warnings": warnings,
    }

    manifest_fp = run_root / "pipeline_manifest.json"
    manifest_fp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    warnings_fp = write_warnings(run_root, warnings)

    result["pipeline_manifest"] = manifest_fp
    result["warnings"] = warnings_fp
    return result


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run index-based feature-store pipeline")
    p.add_argument("--index-code", default="000905")
    p.add_argument("--n-assets", type=int, default=None)
    p.add_argument("--all-assets", action="store_true")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--clean-run", action="store_true")
    p.add_argument("--run-technical-liquidity", action="store_true")
    p.add_argument("--feature-root", default="data/processed/feature_store/v1")
    p.add_argument("--output-dir", default="outputs/data_pipeline")
    p.add_argument("--factor-output-dir", default="outputs/factor_research")
    p.add_argument("--run-name", default=None)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--retry-wait", type=float, default=1.0)
    p.add_argument("--request-sleep", type=float, default=0.5)
    p.add_argument("--source-panel-version", default="feature_store_v1")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = run_index_feature_store_pipeline(
        index_code=args.index_code,
        n_assets=args.n_assets,
        all_assets=args.all_assets,
        seed=args.seed,
        start_date=args.start_date,
        end_date=args.end_date,
        clean_run=args.clean_run,
        run_technical_liquidity=args.run_technical_liquidity,
        feature_root=args.feature_root,
        output_dir=args.output_dir,
        factor_output_dir=args.factor_output_dir,
        run_name=args.run_name,
        retries=args.retries,
        retry_wait=args.retry_wait,
        request_sleep=args.request_sleep,
        source_panel_version=args.source_panel_version,
    )
    print({k: str(v) for k, v in out.items()})


if __name__ == "__main__":
    main()
