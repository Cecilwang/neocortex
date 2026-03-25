from datetime import date

from neocortex.feishu.cards import build_table_card
from neocortex.feishu.client import _render_card
from neocortex.feishu.models import FeishuCardResp, FeishuMessageTarget


def test_build_table_card_formats_cells_and_aligns_numeric_columns() -> None:
    card = build_table_card(
        title="demo",
        columns=("symbol", "close", "trade_date", "note"),
        rows=(("600519", 123.45, date(2024, 1, 2), None),),
    )

    table = card["body"]["elements"][0]
    assert table["page_size"] == 100
    assert table["row_height"] == "medium"
    assert table["freeze_first_column"] is True
    assert table["columns"][0]["horizontal_align"] == "left"
    assert table["columns"][1]["horizontal_align"] == "right"
    assert table["rows"][0]["col_1"] == "123.45"
    assert table["rows"][0]["col_2"] == "2024-01-02"
    assert table["rows"][0]["col_3"] == "-"


def test_render_card_prefixes_job_status_without_mutating_source_card() -> None:
    response = FeishuCardResp(
        target=FeishuMessageTarget(chat_id="oc_test"),
        job_id=3,
        card=build_table_card(
            title="demo async-table (1 rows)",
            columns=("symbol",),
            rows=(("600519",),),
        ),
    )

    rendered = _render_card(response)

    assert (
        rendered["header"]["title"]["content"]
        == "Job 3 succeeded: demo async-table (1 rows)"
    )
    assert (
        response.card["header"]["title"]["content"]
        == "demo async-table (1 rows)"
    )
