"""Signal quality diagnostics MVP for cross-sectional stock-selection research."""

from qsys.research.signal_quality.align import prepare_signal_quality_frame
from qsys.research.signal_quality.ic import compute_ic_by_date, summarize_ic
from qsys.research.signal_quality.quantile import (
    assign_quantiles_by_date,
    compute_quantile_forward_returns,
    compute_quantile_spread,
)

__all__ = [
    "prepare_signal_quality_frame",
    "compute_ic_by_date",
    "summarize_ic",
    "assign_quantiles_by_date",
    "compute_quantile_forward_returns",
    "compute_quantile_spread",
]
