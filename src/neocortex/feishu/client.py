"""Thin Feishu REST client for token and message operations."""

from __future__ import annotations

import json
import logging
from time import monotonic
from uuid import uuid4

import httpx

from neocortex.feishu.settings import FeishuSettings


logger = logging.getLogger(__name__)
_TOKEN_REFRESH_BUFFER_SECONDS = 60


class FeishuClient:
    """Minimal Feishu API client for internal app bots."""

    def __init__(
        self,
        settings: FeishuSettings,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self.http_client = http_client or httpx.Client(
            base_url=settings.base_url, timeout=10.0
        )
        self._owns_http_client = http_client is None
        self._tenant_access_token: str | None = None
        self._token_deadline: float = 0.0
        self._bot_open_id: str | None = None
        logger.info(f"Initialized FeishuClient: base_url={settings.base_url}")

    def send_text(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
        reply_in_thread: bool = False,
    ) -> None:
        """Send one text message into the target chat."""

        payload = {
            "content": json.dumps({"text": text}, ensure_ascii=False),
            "msg_type": "text",
            "uuid": str(uuid4()),
        }
        if reply_to_message_id:
            logger.info(
                "Calling Feishu reply_message API for message_id=%s chat_id=%s reply_in_thread=%s",
                reply_to_message_id,
                chat_id,
                reply_in_thread,
            )
            payload["reply_in_thread"] = reply_in_thread
            self._request(
                "POST",
                f"/open-apis/im/v1/messages/{reply_to_message_id}/reply",
                json=payload,
            )
            return

        logger.info(f"Calling Feishu send_message API for chat_id={chat_id}")
        payload["receive_id"] = chat_id
        self._request(
            "POST",
            "/open-apis/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            json=payload,
        )

    def get_bot_open_id(self) -> str:
        """Resolve and cache the current bot open_id from Feishu."""

        if self._bot_open_id is not None:
            return self._bot_open_id

        document = self._request("GET", "/open-apis/bot/v3/info")
        candidates: list[dict[str, object]] = []
        data = document.get("data")
        if isinstance(data, dict):
            bot = data.get("bot")
            if isinstance(bot, dict):
                candidates.append(bot)
            candidates.append(data)
        bot = document.get("bot")
        if isinstance(bot, dict):
            candidates.append(bot)

        for candidate in candidates:
            open_id = candidate.get("open_id")
            if isinstance(open_id, str) and open_id:
                self._bot_open_id = open_id
                logger.info(f"Resolved Feishu bot open_id from bot info API: {open_id}")
                return open_id

        raise RuntimeError(
            f"Feishu bot info response did not include open_id: {document}"
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, object] | None = None,
    ) -> dict[str, object]:
        logger.info(
            f"Calling Feishu API: method={method} path={path} "
            f"has_params={params is not None} has_json={json is not None}"
        )
        headers = {"Authorization": f"Bearer {self._get_tenant_access_token()}"}
        response = self.http_client.request(
            method,
            path,
            params=params,
            json=json,
            headers=headers,
        )
        response.raise_for_status()
        document = response.json()
        if document.get("code") not in (None, 0):
            raise RuntimeError(f"Feishu API request failed: {document}")
        logger.debug(f"Feishu API request succeeded: path={path}")
        return document

    def _get_tenant_access_token(self) -> str:
        now = monotonic()
        if self._tenant_access_token is not None and now < self._token_deadline:
            logger.debug("Using cached Feishu tenant access token.")
            return self._tenant_access_token

        logger.debug("Refreshing Feishu tenant access token.")
        response = self.http_client.post(
            "/open-apis/auth/v3/tenant_access_token/internal",
            json={
                "app_id": self.settings.app_id,
                "app_secret": self.settings.app_secret,
            },
        )
        response.raise_for_status()
        document = response.json()
        if document.get("code") not in (None, 0):
            raise RuntimeError(f"Feishu token request failed: {document}")

        token = document["tenant_access_token"]
        expire = int(document.get("expire", 7200))
        self._tenant_access_token = token
        self._token_deadline = now + max(expire - _TOKEN_REFRESH_BUFFER_SECONDS, 1)
        logger.info(f"Refreshed Feishu tenant access token: expires_in={expire}")
        return token

    def close(self) -> None:
        if self._owns_http_client:
            logger.info("Closing Feishu HTTP client.")
            self.http_client.close()
