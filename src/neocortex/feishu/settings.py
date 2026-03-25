"""Environment-backed settings for the Feishu bot service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from neocortex.config import get_config


def _split_csv(value: str) -> frozenset[str]:
    return frozenset(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class FeishuSettings:
    """Static runtime settings for the Feishu integration."""

    app_id: str
    app_secret: str
    bot_open_id: str = ""
    base_url: str = "https://open.feishu.cn"
    db_path: Path = Path("data/feishu_bot.sqlite3")
    admin_open_ids: frozenset[str] = frozenset()
    max_reply_chars: int = 3500
    job_workers: int = 4

    @classmethod
    def from_env(cls) -> FeishuSettings:
        """Load required settings from environment variables."""

        app_config = get_config()
        app_id = os.environ["FEISHU_APP_ID"]
        app_secret = os.environ["FEISHU_APP_SECRET"]
        bot_open_id = os.environ.get("FEISHU_BOT_OPEN_ID", "")
        admin_open_ids = _split_csv(os.environ.get("FEISHU_ADMIN_OPEN_IDS", ""))
        max_reply_chars = int(os.environ.get("FEISHU_MAX_REPLY_CHARS", "3500"))
        job_workers = int(os.environ.get("FEISHU_JOB_WORKERS", "4"))
        base_url = os.environ.get("FEISHU_BASE_URL", "https://open.feishu.cn")
        return cls(
            app_id=app_id,
            app_secret=app_secret,
            bot_open_id=bot_open_id,
            base_url=base_url,
            db_path=app_config.storage.bot_db_path,
            admin_open_ids=admin_open_ids,
            max_reply_chars=max_reply_chars,
            job_workers=job_workers,
        )
