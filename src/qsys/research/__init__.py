"""Research Diagnostics v1 interfaces."""

from qsys.research.constraint_impact import ConstraintImpactConfig, compare_constraint_impact
from qsys.research.correlation import pairwise_signal_correlation
from qsys.research.decay import decay_analysis
from qsys.research.exposure import (
    exposure_summary,
    group_exposure_daily,
    signal_feature_correlation_daily,
    size_exposure_daily,
)
from qsys.research.ic import daily_ic, daily_rank_ic, ic_summary
from qsys.research.quantiles import quantile_mean_forward_returns, quantile_spread
from qsys.research.turnover import signal_autocorrelation, top_n_turnover

__all__ = [
    "daily_ic",
    "daily_rank_ic",
    "ic_summary",
    "quantile_mean_forward_returns",
    "quantile_spread",
    "signal_autocorrelation",
    "top_n_turnover",
    "decay_analysis",
    "pairwise_signal_correlation",
    "ConstraintImpactConfig",
    "compare_constraint_impact",
    "size_exposure_daily",
    "group_exposure_daily",
    "signal_feature_correlation_daily",
    "exposure_summary",
]
