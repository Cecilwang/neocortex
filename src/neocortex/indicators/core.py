"""Indicator contracts shared by the registry and calculation engine."""

from __future__ import annotations

from dataclasses import dataclass, fields
import logging
from typing import TypeVar

import pandas as pd

from neocortex.models.core import PRICE_BAR_TIMESTAMP, PriceSeries


ParamsT = TypeVar("ParamsT", bound="IndicatorParams")
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IndicatorSpec:
    """Metadata and calculation behavior for one supported indicator."""

    key: str
    display_name: str
    category: str
    formula: str = ""
    interpretation: str = ""

    def calculate(
        self,
        bars: PriceSeries,
        *,
        parameters: object | None = None,
    ) -> Indicator:
        """Calculate one aligned indicator result over normalized price bars."""

        raise NotImplementedError


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
        allowed_keys = {field.name for field in fields(cls)}
        extra_keys = tuple(sorted(set(payload) - allowed_keys))
        if extra_keys:
            raise ValueError(
                f"{cls.__name__} received unknown parameter keys: {', '.join(extra_keys)}."
            )
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class Indicator:
    """One indicator calculation result with metadata and tabular output."""

    spec: IndicatorSpec
    parameters: IndicatorParams
    data: pd.DataFrame

    @property
    def timestamp(self) -> pd.Series:
        return self.data[PRICE_BAR_TIMESTAMP]


def log_indicator_calculation(
    *,
    indicator_key: str,
    bars: PriceSeries,
    parameters: object | None,
) -> None:
    logger.info(
        f"Calculating {indicator_key} security={bars.security_id.ticker} start={bars.start_timestamp} end={bars.end_timestamp} parameters={parameters}"
    )
