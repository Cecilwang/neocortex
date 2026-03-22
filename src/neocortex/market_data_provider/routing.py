"""Shared routing helpers for market-data provider components."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import wraps
import inspect
import logging

from neocortex.connectors.base import BaseSourceConnector
from neocortex.models import Market
from neocortex.storage.market_store import MarketDataStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourceRoutingError(Exception):
    resource_type: str
    target: object
    failures: tuple[tuple[str, Exception], ...]

    def __str__(self) -> str:
        summary = ", ".join(
            f"{source_name}:{type(error).__name__}"
            for source_name, error in self.failures
        )
        return (
            f"All sources failed for resource={self.resource_type} "
            f"target={self.target}: {summary}"
        )


class SourceRoutedComponent:
    """Common source-priority helpers for DB and fetch subcomponents."""

    def __init__(
        self,
        *,
        store: MarketDataStore,
        source_connectors: Mapping[str, BaseSourceConnector],
        source_priority: Mapping[Market, Mapping[str, Sequence[str]]],
    ) -> None:
        self.store = store
        self.source_connectors = dict(source_connectors)
        self.source_priority = {
            market: {
                resource_type: tuple(source_names)
                for resource_type, source_names in priorities.items()
            }
            for market, priorities in source_priority.items()
        }

    def _priority(self, market: Market, resource_type: str) -> tuple[str, ...]:
        market_priority = self.source_priority.get(market)
        if market_priority is None or resource_type not in market_priority:
            raise ValueError(
                f"Missing source priority config for market={market.value} resource={resource_type}."
            )
        return market_priority[resource_type]

    def _source_connector(self, source_name: str) -> BaseSourceConnector:
        try:
            return self.source_connectors[source_name]
        except KeyError as exc:
            raise ValueError(
                f"Missing source connector implementation for {source_name}."
            ) from exc


def _resolve_route_context(signature, *args, **kwargs) -> tuple[Market, object]:
    bound = signature.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    market = bound.arguments.get("market")
    security_id = bound.arguments.get("security_id")
    if market is None:
        if security_id is None:
            raise ValueError("Source routing requires either market or security_id.")
        return security_id.market, security_id
    return market, security_id or market


def route_db_by_source(*, resource_type: str):
    """Try DB-backed per-source lookups until one hits, otherwise raise KeyError."""

    def decorator(method):
        signature = inspect.signature(method)

        @wraps(method)
        def wrapper(self: SourceRoutedComponent, *args, **kwargs):
            assert isinstance(self, SourceRoutedComponent), (
                "DB routing requires a SourceRoutedComponent."
            )
            market, target = _resolve_route_context(signature, self, *args, **kwargs)
            for source_name in self._priority(market, resource_type):
                logger.info(
                    "Trying DB route: resource=%s source=%s target=%s",
                    resource_type,
                    source_name,
                    target,
                )
                try:
                    result = method(self, *args, source_name=source_name, **kwargs)
                    logger.info(
                        "DB route succeeded: resource=%s source=%s target=%s",
                        resource_type,
                        source_name,
                        target,
                    )
                    return result
                except KeyError:
                    logger.info(
                        "DB route missed: resource=%s source=%s target=%s",
                        resource_type,
                        source_name,
                        target,
                    )
                    continue
            raise KeyError(target)

        return wrapper

    return decorator


def route_fetch_by_source(*, resource_type: str):
    """Try connector-backed per-source fetches until one succeeds."""

    def decorator(method):
        signature = inspect.signature(method)

        @wraps(method)
        def wrapper(self: SourceRoutedComponent, *args, **kwargs):
            assert isinstance(self, SourceRoutedComponent), (
                "DB routing requires a SourceRoutedComponent."
            )
            market, target = _resolve_route_context(signature, self, *args, **kwargs)
            failures: list[tuple[str, Exception]] = []
            for source_name in self._priority(market, resource_type):
                logger.info(
                    "Trying fetch route: resource=%s source=%s target=%s",
                    resource_type,
                    source_name,
                    target,
                )
                try:
                    result = method(self, *args, source_name=source_name, **kwargs)
                    logger.info(
                        "Fetch route succeeded: resource=%s source=%s target=%s",
                        resource_type,
                        source_name,
                        target,
                    )
                    return result
                except Exception as exc:
                    logger.info(
                        "Fetch route failed: resource=%s source=%s target=%s error=%s",
                        resource_type,
                        source_name,
                        target,
                        exc,
                    )
                    failures.append((source_name, exc))
            raise SourceRoutingError(
                resource_type=resource_type,
                target=target,
                failures=tuple(failures),
            )

        return wrapper

    return decorator


def route_read_through(*, db_method: str | None, fetch_method: str, resource_label: str):
    """Try DB reader first, then fall back to source fetcher on DB miss."""

    def decorator(method):
        signature = inspect.signature(method)

        @wraps(method)
        def wrapper(self, *args, **kwargs):
            bound = signature.bind_partial(self, *args, **kwargs)
            bound.apply_defaults()
            call_kwargs = {
                name: value
                for name, value in bound.arguments.items()
                if name != "self"
            }
            route_target = bound.arguments.get("security_id") or bound.arguments.get(
                "market"
            )
            target = f"{resource_label}:{route_target}" if route_target else resource_label
            if db_method is not None:
                try:
                    return getattr(self.db_reader, db_method)(**call_kwargs)
                except KeyError:
                    logger.info("%s DB miss: target=%s", resource_label, target)
            return getattr(self.source_fetcher, fetch_method)(**call_kwargs)

        return wrapper

    return decorator
