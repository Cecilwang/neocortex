from neocortex.markets import get_market_context
from neocortex.models import Market, TradingCalendar


def test_get_market_context_returns_local_market_metadata() -> None:
    us_context = get_market_context(Market.US)
    cn_context = get_market_context(Market.CN)

    assert us_context.trading_currency == "USD"
    assert us_context.trading_calendar is TradingCalendar.XNYS
    assert cn_context.timezone == "Asia/Shanghai"
    assert cn_context.benchmark_symbol == "000300.SH"
