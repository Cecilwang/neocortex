"""Local market metadata registry."""

from __future__ import annotations

from neocortex.models.core import Market, MarketContext, TradingCalendar


_MARKET_CONTEXTS: dict[Market, MarketContext] = {
    Market.US: MarketContext(
        market=Market.US,
        region="North America",
        timezone="America/New_York",
        trading_currency="USD",
        benchmark_symbol="SPY",
        trading_calendar=TradingCalendar.XNYS,
    ),
    Market.JP: MarketContext(
        market=Market.JP,
        region="Japan",
        timezone="Asia/Tokyo",
        trading_currency="JPY",
        benchmark_symbol="TOPIX",
        trading_calendar=TradingCalendar.XTKS,
    ),
    Market.HK: MarketContext(
        market=Market.HK,
        region="Hong Kong",
        timezone="Asia/Hong_Kong",
        trading_currency="HKD",
        benchmark_symbol="HSI",
        trading_calendar=TradingCalendar.XHKG,
    ),
    Market.CN: MarketContext(
        market=Market.CN,
        region="China",
        timezone="Asia/Shanghai",
        trading_currency="CNY",
        benchmark_symbol="000300.SH",
        trading_calendar=TradingCalendar.XSHG,
    ),
}


def get_market_context(market: Market) -> MarketContext:
    """Return local market metadata for one supported market."""

    return _MARKET_CONTEXTS[market]
