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
        self._tenant_access_token: str | None = None
        self._token_deadline: float = 0.0

    def send_text(self, *, chat_id: str, text: str) -> None:
        """Send one text message into the target chat."""

        logger.info("Calling Feishu send_message API for chat_id=%s", chat_id)
        payload = {
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
            "uuid": str(uuid4()),
        }
        self._request(
            "POST",
            "/open-apis/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            json=payload,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, object] | None = None,
    ) -> dict[str, object]:
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
        logger.debug("Feishu API request succeeded: path=%s", path)
        return document

    def _get_tenant_access_token(self) -> str:
        now = monotonic()
        if self._tenant_access_token is not None and now < self._token_deadline:
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
        return token
