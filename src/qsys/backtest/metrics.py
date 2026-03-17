"""Performance metrics for Backtest MVP."""

from __future__ import annotations

import numpy as np
import pandas as pd


def cumulative_return(returns: pd.Series) -> float:
    """Compute cumulative return from periodic return series."""

    r = returns.fillna(0.0)
    return float((1.0 + r).prod() - 1.0)


def annual_return(returns: pd.Series, periods_per_year: int = 252) -> float:
    """Compute annualized geometric return."""

    r = returns.dropna()
    if len(r) == 0:
        return 0.0
    total = (1.0 + r).prod()
    return float(total ** (periods_per_year / len(r)) - 1.0)


def annual_vol(returns: pd.Series, periods_per_year: int = 252) -> float:
    """Compute annualized volatility."""

    r = returns.dropna()
    if len(r) == 0:
        return 0.0
    return float(r.std(ddof=0) * np.sqrt(periods_per_year))


def sharpe(returns: pd.Series, periods_per_year: int = 252, risk_free_rate: float = 0.0) -> float:
    """Compute annualized Sharpe ratio."""

    ar = annual_return(returns, periods_per_year=periods_per_year)
    av = annual_vol(returns, periods_per_year=periods_per_year)
    if av == 0:
        return 0.0
    return float((ar - risk_free_rate) / av)


def max_drawdown(returns: pd.Series) -> float:
    """Compute max drawdown from cumulative equity curve."""

    equity = (1.0 + returns.fillna(0.0)).cumprod()
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min()) if len(dd) else 0.0


def summarize_metrics(returns: pd.Series, turnover: pd.Series | None = None) -> dict[str, float]:
    """Return standard metric summary dictionary."""

    summary = {
        "cumulative_return": cumulative_return(returns),
        "annual_return": annual_return(returns),
        "annual_vol": annual_vol(returns),
        "sharpe": sharpe(returns),
        "max_drawdown": max_drawdown(returns),
    }
    summary["turnover"] = float(turnover.mean()) if turnover is not None and len(turnover) else 0.0
    return summary
