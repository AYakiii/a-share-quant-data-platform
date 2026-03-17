"""Feature registry for lookup by feature name."""

from __future__ import annotations

from dataclasses import dataclass

from qsys.features.base import BaseFeature


@dataclass
class FeatureRegistry:
    """In-memory registry mapping feature names to feature objects."""

    _features: dict[str, BaseFeature]

    def __init__(self) -> None:
        self._features = {}

    def register(self, feature: BaseFeature) -> None:
        self._features[feature.name] = feature

    def get(self, name: str) -> BaseFeature:
        if name not in self._features:
            raise KeyError(f"Feature not registered: {name}")
        return self._features[name]

    def names(self) -> list[str]:
        return sorted(self._features.keys())

    def has(self, name: str) -> bool:
        return name in self._features
