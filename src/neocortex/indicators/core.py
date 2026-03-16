"""Indicator contracts shared by the registry and calculation engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, TypeVar

from neocortex.models.core import PriceSeries


ParamsT = TypeVar("ParamsT", bound="IndicatorParams")


@dataclass(frozen=True, slots=True)
class IndicatorSpec:
    """Metadata that describes one supported indicator."""

    key: str
    display_name: str
    category: str
    input_field: str
    formula: str = ""
    interpretation: str = ""


@dataclass(frozen=True, slots=True)
class IndicatorPoint:
    """One indicator value aligned to a market-data timestamp."""

    timestamp: datetime
    value: float | None


@dataclass(frozen=True, slots=True)
class IndicatorParams:
    """Base dataclass mixin for indicator-specific parameter objects."""

    @classmethod
    def from_dict(
        cls: type[ParamsT],
        payload: dict[str, object] | None,
    ) -> ParamsT:
        if payload is None:
            return cls()
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class IndicatorSeries:
    """Calculated indicator values and the spec used to produce them."""

    spec: IndicatorSpec
    parameters: IndicatorParams
    points: tuple[IndicatorPoint, ...] = ()


class Indicator(Protocol):
    """Behavior contract implemented by concrete indicator modules."""

    @property
    def spec(self) -> IndicatorSpec:
        """Return the static metadata for this indicator."""

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: object | None = None,
    ) -> IndicatorSeries:
        """Calculate an aligned series over normalized price bars."""
