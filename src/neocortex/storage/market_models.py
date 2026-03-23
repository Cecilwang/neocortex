"""SQLAlchemy models for the market data database."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class MarketDataBase(DeclarativeBase):
    """Base class for market data ORM models."""


class SecurityRow(MarketDataBase):
    """Canonical securities keyed by stable internal identity."""

    __tablename__ = "securities"
    __table_args__ = (
        CheckConstraint("length(trim(market)) > 0", name="ck_securities_market"),
        CheckConstraint("length(trim(exchange)) > 0", name="ck_securities_exchange"),
        CheckConstraint("length(trim(symbol)) > 0", name="ck_securities_symbol"),
        Index("idx_securities_market_exchange", "market", "exchange"),
    )

    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    last_seen_at: Mapped[str] = mapped_column(String, nullable=False)


class SecurityAliasRow(MarketDataBase):
    """Search aliases kept separate so names can evolve independently."""

    __tablename__ = "security_aliases"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
        CheckConstraint("length(trim(alias)) > 0", name="ck_alias_nonempty"),
        CheckConstraint("length(trim(alias_norm)) > 0", name="ck_alias_norm_nonempty"),
        CheckConstraint("length(trim(language)) > 0", name="ck_alias_lang_nonempty"),
        CheckConstraint("length(trim(source)) > 0", name="ck_alias_source_nonempty"),
        Index(
            "uq_security_aliases_identity",
            "market",
            "exchange",
            "symbol",
            "alias",
            "language",
            "source",
            unique=True,
        ),
        Index("idx_security_aliases_alias_norm", "alias_norm"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column(String, nullable=False)
    exchange: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    alias: Mapped[str] = mapped_column(String, nullable=False)
    alias_norm: Mapped[str] = mapped_column(String, nullable=False)
    language: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    updated_at: Mapped[str] = mapped_column(String, nullable=False)


class SecurityProfileRow(MarketDataBase):
    """Source-specific company metadata snapshot."""

    __tablename__ = "security_profiles"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
        CheckConstraint("length(trim(source)) > 0", name="ck_profile_source_nonempty"),
        CheckConstraint("primary_listing IN (0, 1)", name="ck_profile_primary_listing"),
        Index("idx_security_profiles_lookup", "market", "exchange", "symbol"),
    )

    source: Mapped[str] = mapped_column(String, primary_key=True)
    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    provider_company_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    sector: Mapped[str | None] = mapped_column(Text, nullable=True)
    industry: Mapped[str | None] = mapped_column(Text, nullable=True)
    country: Mapped[str | None] = mapped_column(String, nullable=True)
    currency: Mapped[str | None] = mapped_column(String, nullable=True)
    primary_listing: Mapped[int] = mapped_column(nullable=False, default=1)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)


class DailyPriceBarRow(MarketDataBase):
    """Source-specific daily OHLCV bars."""

    __tablename__ = "daily_price_bars"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
        CheckConstraint("length(trim(source)) > 0", name="ck_daily_source_nonempty"),
        CheckConstraint("open >= 0", name="ck_daily_open"),
        CheckConstraint("high >= 0", name="ck_daily_high"),
        CheckConstraint("low >= 0", name="ck_daily_low"),
        CheckConstraint("close >= 0", name="ck_daily_close"),
        CheckConstraint("volume IS NULL OR volume >= 0", name="ck_daily_volume"),
        CheckConstraint("amount IS NULL OR amount >= 0", name="ck_daily_amount"),
        CheckConstraint("high >= low", name="ck_daily_high_low"),
        Index(
            "idx_daily_price_bars_symbol_date",
            "market",
            "exchange",
            "symbol",
            "trade_date",
        ),
    )

    source: Mapped[str] = mapped_column(String, primary_key=True)
    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    trade_date: Mapped[str] = mapped_column(String, primary_key=True)
    open: Mapped[float] = mapped_column(nullable=False)
    high: Mapped[float] = mapped_column(nullable=False)
    low: Mapped[float] = mapped_column(nullable=False)
    close: Mapped[float] = mapped_column(nullable=False)
    volume: Mapped[float | None] = mapped_column(nullable=True)
    amount: Mapped[float | None] = mapped_column(nullable=True)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)


class IntradayPriceBarRow(MarketDataBase):
    """Source-specific intraday OHLCV bars."""

    __tablename__ = "intraday_price_bars"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
        CheckConstraint(
            "interval IN ('1m', '5m', '15m', '30m', '60m')",
            name="ck_intraday_interval",
        ),
        CheckConstraint("open >= 0", name="ck_intraday_open"),
        CheckConstraint("high >= 0", name="ck_intraday_high"),
        CheckConstraint("low >= 0", name="ck_intraday_low"),
        CheckConstraint("close >= 0", name="ck_intraday_close"),
        CheckConstraint(
            "volume IS NULL OR volume >= 0",
            name="ck_intraday_volume",
        ),
        CheckConstraint(
            "amount IS NULL OR amount >= 0",
            name="ck_intraday_amount",
        ),
        CheckConstraint("high >= low", name="ck_intraday_high_low"),
        Index(
            "idx_intraday_price_bars_symbol",
            "market",
            "exchange",
            "symbol",
            "interval",
            "bar_time",
        ),
    )

    source: Mapped[str] = mapped_column(String, primary_key=True)
    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    interval: Mapped[str] = mapped_column(String, primary_key=True)
    bar_time: Mapped[str] = mapped_column(String, primary_key=True)
    open: Mapped[float] = mapped_column(nullable=False)
    high: Mapped[float] = mapped_column(nullable=False)
    low: Mapped[float] = mapped_column(nullable=False)
    close: Mapped[float] = mapped_column(nullable=False)
    volume: Mapped[float | None] = mapped_column(nullable=True)
    amount: Mapped[float | None] = mapped_column(nullable=True)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)


class FundamentalSnapshotRow(MarketDataBase):
    """Source-specific fundamental snapshot."""

    __tablename__ = "fundamental_snapshots"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
        CheckConstraint(
            "length(trim(canonical_period_label)) > 0",
            name="ck_fundamental_period_label",
        ),
        CheckConstraint(
            "length(trim(statement_kind)) > 0",
            name="ck_fundamental_statement_kind",
        ),
        CheckConstraint("json_valid(raw_items_json)", name="ck_fundamental_raw_json"),
        CheckConstraint(
            "json_valid(derived_metrics_json)",
            name="ck_fundamental_derived_json",
        ),
        Index(
            "idx_fundamental_snapshots_series",
            "source",
            "market",
            "exchange",
            "symbol",
            "period_end_date",
        ),
    )

    source: Mapped[str] = mapped_column(String, primary_key=True)
    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    period_end_date: Mapped[str] = mapped_column(String, primary_key=True)
    canonical_period_label: Mapped[str] = mapped_column(String, primary_key=True)
    statement_kind: Mapped[str] = mapped_column(String, primary_key=True)
    provider_period_label: Mapped[str | None] = mapped_column(Text, nullable=True)
    report_date: Mapped[str | None] = mapped_column(String, nullable=True)
    currency: Mapped[str | None] = mapped_column(String, nullable=True)
    raw_items_json: Mapped[str] = mapped_column(Text, nullable=False)
    derived_metrics_json: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)


class DisclosureSectionRow(MarketDataBase):
    """Source-specific disclosure text sections used by qualitative prompts."""

    __tablename__ = "disclosure_sections"
    __table_args__ = (
        ForeignKeyConstraint(
            ["market", "exchange", "symbol"],
            ["securities.market", "securities.exchange", "securities.symbol"],
        ),
        CheckConstraint(
            "section_kind IN ('overview', 'risks', 'mda', 'governance')",
            name="ck_disclosure_section_kind",
        ),
        CheckConstraint("length(trim(content)) > 0", name="ck_disclosure_content"),
        Index(
            "idx_disclosure_sections_series",
            "source",
            "market",
            "exchange",
            "symbol",
            "report_date",
        ),
    )

    source: Mapped[str] = mapped_column(String, primary_key=True)
    market: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    report_date: Mapped[str] = mapped_column(String, primary_key=True)
    section_kind: Mapped[str] = mapped_column(String, primary_key=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)


class MacroPointRow(MarketDataBase):
    """Source-specific macroeconomic or market series point."""

    __tablename__ = "macro_points"
    __table_args__ = (
        CheckConstraint(
            "category IN ('macro', 'index', 'fx', 'commodity', 'rates', 'volatility')",
            name="ck_macro_category",
        ),
        CheckConstraint("length(trim(series_name)) > 0", name="ck_macro_series_name"),
        CheckConstraint("length(trim(unit)) > 0", name="ck_macro_unit"),
        CheckConstraint("length(trim(frequency)) > 0", name="ck_macro_frequency"),
        Index(
            "idx_macro_points_series",
            "source",
            "market",
            "series_key",
            "observed_at",
        ),
    )

    source: Mapped[str] = mapped_column(String, primary_key=True)
    market: Mapped[str] = mapped_column(String, primary_key=True)
    series_key: Mapped[str] = mapped_column(String, primary_key=True)
    observed_at: Mapped[str] = mapped_column(String, primary_key=True)
    series_name: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[float | None] = mapped_column(nullable=True)
    unit: Mapped[str] = mapped_column(String, nullable=False)
    frequency: Mapped[str] = mapped_column(String, nullable=False)
    category: Mapped[str] = mapped_column(String, nullable=False)
    change_pct: Mapped[float | None] = mapped_column(nullable=True)
    yoy_change_pct: Mapped[float | None] = mapped_column(nullable=True)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)


class TradingDateRow(MarketDataBase):
    """Source-specific market trading-date calendar rows."""

    __tablename__ = "trading_dates"
    __table_args__ = (
        CheckConstraint("length(trim(source)) > 0", name="ck_trading_dates_source"),
        CheckConstraint("length(trim(market)) > 0", name="ck_trading_dates_market"),
        CheckConstraint(
            "length(trim(calendar)) > 0",
            name="ck_trading_dates_calendar",
        ),
        CheckConstraint(
            "is_trading_day IN (0, 1)",
            name="ck_trading_dates_is_trading_day",
        ),
        Index(
            "idx_trading_dates_market_calendar_date",
            "market",
            "calendar",
            "trade_date",
        ),
        Index("idx_trading_dates_market_date", "market", "trade_date"),
    )

    source: Mapped[str] = mapped_column(String, primary_key=True)
    market: Mapped[str] = mapped_column(String, primary_key=True)
    calendar: Mapped[str] = mapped_column(String, primary_key=True)
    trade_date: Mapped[str] = mapped_column(String, primary_key=True)
    is_trading_day: Mapped[int] = mapped_column(nullable=False)
    fetched_at: Mapped[str] = mapped_column(String, nullable=False)
