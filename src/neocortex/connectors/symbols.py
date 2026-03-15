"""Provider ticker codecs for market-aware symbol conversion."""

from __future__ import annotations

from neocortex.models.core import DataProvider, Market, SecurityId


_MARKET_EXCHANGES: dict[Market, str] = {
    Market.US: "SMART",
    Market.JP: "TSE",
    Market.HK: "HKEX",
    Market.CN: "SSE",
}

_YAHOO_MARKET_SUFFIX: dict[Market, str] = {
    Market.JP: ".T",
    Market.HK: ".HK",
}

_YAHOO_CN_SUFFIX_TO_EXCHANGE: dict[str, str] = {
    ".SS": "SSE",
    ".SZ": "SZSE",
}

_CN_EXCHANGE_TO_YAHOO_SUFFIX: dict[str, str] = {
    "SSE": ".SS",
    "SH": ".SS",
    "SHA": ".SS",
    "SZSE": ".SZ",
    "SZ": ".SZ",
}

_CN_EXCHANGE_TO_PREFIX: dict[str, str] = {
    "SSE": "sh",
    "SH": "sh",
    "SHA": "sh",
    "SZSE": "sz",
    "SZ": "sz",
}

_PREFIX_TO_CN_EXCHANGE: dict[str, str] = {
    "sh": "SSE",
    "sz": "SZSE",
}


def to_provider_ticker(security_id: SecurityId, provider: DataProvider) -> str:
    """Convert a canonical security id into a provider-specific ticker string."""

    if provider is DataProvider.YAHOO_FINANCE:
        return _to_yahoo_ticker(security_id)
    if provider is DataProvider.AKSHARE:
        return _to_cn_prefixed_ticker(security_id)
    if provider is DataProvider.MANUAL:
        return security_id.ticker
    raise ValueError(f"Ticker conversion is not supported for provider {provider}.")


def from_provider_ticker(
    ticker: str,
    provider: DataProvider,
    *,
    market: Market | None = None,
    exchange: str | None = None,
) -> SecurityId:
    """Convert a provider ticker string into a canonical security id."""

    if provider is DataProvider.YAHOO_FINANCE:
        return _from_yahoo_ticker(ticker, market=market, exchange=exchange)
    if provider is DataProvider.AKSHARE:
        return _from_cn_prefixed_ticker(ticker, market=market)
    if provider is DataProvider.MANUAL:
        return _from_manual_ticker(ticker, exchange=exchange)
    raise ValueError(f"Ticker conversion is not supported for provider {provider}.")


def _to_yahoo_ticker(security_id: SecurityId) -> str:
    if security_id.market is Market.US:
        return security_id.symbol
    if security_id.market in _YAHOO_MARKET_SUFFIX:
        return f"{security_id.symbol}{_YAHOO_MARKET_SUFFIX[security_id.market]}"
    if security_id.market is Market.CN:
        suffix = _CN_EXCHANGE_TO_YAHOO_SUFFIX.get(security_id.exchange.upper())
        if suffix is None:
            raise ValueError(
                f"Yahoo Finance requires an SSE or SZSE exchange for CN symbols, got {security_id.exchange}."
            )
        return f"{security_id.symbol}{suffix}"
    raise ValueError(f"Yahoo Finance does not support market {security_id.market}.")


def _from_yahoo_ticker(
    ticker: str,
    *,
    market: Market | None,
    exchange: str | None,
) -> SecurityId:
    upper_ticker = ticker.upper()
    if upper_ticker.endswith(".T"):
        return SecurityId(symbol=upper_ticker[:-2], market=Market.JP, exchange="TSE")
    if upper_ticker.endswith(".HK"):
        return SecurityId(symbol=upper_ticker[:-3], market=Market.HK, exchange="HKEX")
    for suffix, cn_exchange in _YAHOO_CN_SUFFIX_TO_EXCHANGE.items():
        if upper_ticker.endswith(suffix):
            return SecurityId(
                symbol=upper_ticker[: -len(suffix)],
                market=Market.CN,
                exchange=cn_exchange,
            )

    resolved_market = market or Market.US
    resolved_exchange = exchange or _MARKET_EXCHANGES[resolved_market]
    return SecurityId(
        symbol=upper_ticker, market=resolved_market, exchange=resolved_exchange
    )


def _to_cn_prefixed_ticker(security_id: SecurityId) -> str:
    if security_id.market is not Market.CN:
        raise ValueError(
            f"{security_id.market} is not supported by the CN-prefixed ticker codec."
        )

    prefix = _CN_EXCHANGE_TO_PREFIX.get(security_id.exchange.upper())
    if prefix is None:
        raise ValueError(
            f"CN-prefixed tickers require an SSE or SZSE exchange, got {security_id.exchange}."
        )
    return f"{prefix}{security_id.symbol}"


def _from_cn_prefixed_ticker(ticker: str, *, market: Market | None) -> SecurityId:
    normalized_ticker = ticker.lower()
    prefix = normalized_ticker[:2]
    exchange = _PREFIX_TO_CN_EXCHANGE.get(prefix)
    if exchange is None:
        raise ValueError(f"Unsupported CN-prefixed ticker: {ticker}.")

    resolved_market = market or Market.CN
    return SecurityId(
        symbol=normalized_ticker[2:].upper(), market=resolved_market, exchange=exchange
    )


def _from_manual_ticker(ticker: str, *, exchange: str | None) -> SecurityId:
    market_value, symbol = ticker.split(":", maxsplit=1)
    market = Market(market_value)
    return SecurityId(
        symbol=symbol,
        market=market,
        exchange=exchange or _MARKET_EXCHANGES[market],
    )
