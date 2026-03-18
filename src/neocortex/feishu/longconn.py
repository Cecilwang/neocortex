"""Feishu long-connection runner backed by the official SDK."""

from __future__ import annotations

import logging

import lark_oapi as lark
from lark_oapi.ws import Client as LongConnectionClient

from neocortex.feishu.service import FeishuBotService
from neocortex.feishu.settings import FeishuSettings


logger = logging.getLogger(__name__)


class FeishuLongConnectionRunner:
    """Run the Feishu bot over long connection instead of webhook callbacks."""

    def __init__(
        self,
        settings: FeishuSettings,
        *,
        service: FeishuBotService | None = None,
        ws_client_factory: type[LongConnectionClient] = LongConnectionClient,
    ) -> None:
        self.settings = settings
        self.service = service or FeishuBotService(settings)
        self.ws_client_factory = ws_client_factory

    def start(self) -> None:
        """Connect to Feishu and block while receiving events."""

        logger.info(
            "Starting Feishu long connection: app_id=%s domain=%s",
            self.settings.app_id,
            self.settings.base_url,
        )
        client = self.ws_client_factory(
            self.settings.app_id,
            self.settings.app_secret,
            domain=self.settings.base_url,
            event_handler=self._build_event_handler(),
            log_level=lark.LogLevel.INFO,
        )
        logger.info("Feishu long connection client initialized; waiting for events.")
        client.start()

    def _build_event_handler(self) -> lark.EventDispatcherHandler:
        return (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_customized_event(
                "im.message.receive_v1",
                self._handle_message_receive_event,
            )
            .build()
        )

    def _handle_message_receive_event(self, event: lark.CustomizedEvent) -> None:
        event_id = getattr(event.header, "event_id", None)
        logger.info("Received long-connection event: event_id=%s", event_id)
        payload = {
            "schema": "2.0",
            "header": {
                "event_id": event_id,
                "event_type": getattr(event.header, "event_type", None),
            },
            "event": event.event,
        }
        self.service.handle_event_payload(payload)
