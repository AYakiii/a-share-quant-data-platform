"""Feature abstractions for research feature computation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class BaseFeature(ABC):
    """Base abstraction for a feature definition."""

    name: str
    required_columns: tuple[str, ...]

    @abstractmethod
    def compute(self, panel: pd.DataFrame) -> pd.Series | pd.DataFrame:
        """Compute feature values from a normalized panel indexed by [date, asset]."""
