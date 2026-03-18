import json
from datetime import date, datetime

from neocortex.models import (
    CompanyProfile,
    Exchange,
    Market,
    PriceBar,
    PriceSeries,
    PRICE_BAR_ADJUSTED_CLOSE,
    PRICE_BAR_CLOSE,
    PRICE_BAR_HIGH,
    PRICE_BAR_LOW,
    PRICE_BAR_OPEN,
    PRICE_BAR_TIMESTAMP,
    PRICE_BAR_VOLUME,
    SecurityId,
)


class FakeAkShareConnector:
    last_timeout: float | None = None
    last_profile_security_id: SecurityId | None = None
    last_bars_call: tuple[SecurityId, date, date, str | None] | None = None

    def __init__(self, *, timeout: float | None = None) -> None:
        self.last_timeout = timeout

    def get_company_profile(self, security_id: SecurityId) -> CompanyProfile:
        self.last_profile_security_id = security_id
        return CompanyProfile(
            security_id=security_id,
            company_name="贵州茅台",
            sector="酿酒行业",
            industry="酿酒行业",
            country="CN",
            currency="CNY",
        )

    def get_price_bars(
        self,
        security_id: SecurityId,
        *,
        start_date: date,
        end_date: date,
        interval: str = "1d",
        adjust: str | None = None,
    ) -> PriceSeries:
        assert interval == "1d"
        self.last_bars_call = (security_id, start_date, end_date, adjust)
        return PriceSeries(
            security_id=security_id,
            bars=(
                PriceBar(
                    security_id=security_id,
                    timestamp=datetime(2026, 3, 14, 15, 0),
                    open=1500.0,
                    high=1520.0,
                    low=1498.0,
                    close=1515.0,
                    volume=120000.0,
                ),
                PriceBar(
                    security_id=security_id,
                    timestamp=datetime(2026, 3, 15, 15, 0),
                    open=1510.0,
                    high=1533.0,
                    low=1505.0,
                    close=1528.0,
                    volume=110000.0,
                    adjusted_close=1528.0,
                ),
            ),
        )


def test_cli_profile_command_prints_normalized_company_profile(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli

    fake_connector = FakeAkShareConnector(timeout=3.0)
    monkeypatch.setattr(cli, "AkShareConnector", lambda timeout=None: fake_connector)

    exit_code = cli.main(
        [
            "akshare",
            "profile",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--timeout",
            "3",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_timeout == 3.0
    assert fake_connector.last_profile_security_id == SecurityId(
        symbol="600519",
        market=Market.CN,
        exchange=Exchange.XSHG,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "security_id": {
            "symbol": "600519",
            "market": "CN",
            "exchange": "XSHG",
        },
        "company_name": "贵州茅台",
        "sector": "酿酒行业",
        "industry": "酿酒行业",
        "country": "CN",
        "currency": "CNY",
        "primary_listing": True,
    }


def test_cli_bars_command_prints_normalized_price_bars(
    monkeypatch,
    capsys,
) -> None:
    from neocortex import cli

    fake_connector = FakeAkShareConnector()
    monkeypatch.setattr(cli, "AkShareConnector", lambda timeout=None: fake_connector)

    exit_code = cli.main(
        [
            "akshare",
            "bars",
            "--symbol",
            "600519",
            "--exchange",
            "XSHG",
            "--start-date",
            "2026-03-14",
            "--end-date",
            "2026-03-15",
            "--adjust",
            "qfq",
        ]
    )

    assert exit_code == 0
    assert fake_connector.last_bars_call == (
        SecurityId(symbol="600519", market=Market.CN, exchange=Exchange.XSHG),
        date(2026, 3, 14),
        date(2026, 3, 15),
        "qfq",
    )
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0].split() == [
        PRICE_BAR_TIMESTAMP,
        PRICE_BAR_OPEN,
        PRICE_BAR_HIGH,
        PRICE_BAR_LOW,
        PRICE_BAR_CLOSE,
        PRICE_BAR_VOLUME,
        PRICE_BAR_ADJUSTED_CLOSE,
    ]
    assert lines[1].split() == [
        "2026-03-14",
        "15:00:00",
        "1500.0",
        "1520.0",
        "1498.0",
        "1515.0",
        "120000.0",
        "NaN",
    ]
    assert lines[2].split() == [
        "2026-03-15",
        "15:00:00",
        "1510.0",
        "1533.0",
        "1505.0",
        "1528.0",
        "110000.0",
        "1528.0",
    ]


def test_cli_feishu_longconn_starts_runner(monkeypatch) -> None:
    from neocortex import cli

    fake_settings = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        cli,
        "load_dotenv",
        lambda path, override=False: captured.update(
            {"path": path, "override": override}
        ),
    )
    monkeypatch.setattr(cli.FeishuSettings, "from_env", lambda: fake_settings)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda level: captured.update({"log_level": level}),
    )

    class FakeRunner:
        def __init__(self, settings) -> None:
            captured["settings"] = settings

        def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(cli, "FeishuLongConnectionRunner", FakeRunner)

    exit_code = cli.main(
        ["feishu", "longconn", "--env-file", ".env.local", "--log-level", "DEBUG"]
    )

    assert exit_code == 0
    assert captured == {
        "path": ".env.local",
        "override": True,
        "log_level": "DEBUG",
        "settings": fake_settings,
        "started": True,
    }
