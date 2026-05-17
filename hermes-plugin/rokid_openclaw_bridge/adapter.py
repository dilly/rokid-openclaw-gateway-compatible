"""Rokid OpenClaw Bridge platform adapter for Hermes Agent.

This plugin connects Hermes Agent's messaging gateway to the Rokid
OpenClaw-compatible WebSocket link service. Incoming Rokid frames are converted
to Hermes MessageEvent objects; Hermes replies are sent back using the existing
Rokid answer stream/done frame shape.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urlencode

try:
    import websockets
except Exception:  # pragma: no cover - handled by check_requirements()
    websockets = None  # type: ignore[assignment]

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME = "rokid_openclaw_bridge"
DEFAULT_WS_URL = "wss://rcs.rokid.com/claw/ws/link"
DEFAULT_AGENT_ID = "main"


class RokidOpenClawBridgeAdapter(BasePlatformAdapter):
    """Hermes gateway adapter for Rokid OpenClaw WebSocket bridge messages."""

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        extra = getattr(config, "extra", {}) or {}

        self.link_code = (
            os.getenv("ROKID_OPENCLAW_LINK_CODE")
            or extra.get("link_code")
            or extra.get("linkCode")
            or ""
        ).strip()
        self.link_secret = (
            os.getenv("ROKID_OPENCLAW_LINK_SECRET")
            or extra.get("link_secret")
            or extra.get("linkSecret")
            or ""
        ).strip()
        self.ws_url = (
            os.getenv("ROKID_OPENCLAW_WS_URL")
            or extra.get("ws_url")
            or extra.get("wsUrl")
            or DEFAULT_WS_URL
        ).strip()
        self.agent_id = (
            os.getenv("ROKID_OPENCLAW_AGENT_ID")
            or extra.get("agent_id")
            or extra.get("agentId")
            or DEFAULT_AGENT_ID
        ).strip()

        max_message_length = extra.get("max_message_length")
        self.max_message_length = int(max_message_length or 0)

        self._ws = None
        self._receive_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._request_ids_by_chat: Dict[str, str] = {}

    @property
    def name(self) -> str:
        return "Rokid OpenClaw Bridge"

    async def connect(self) -> bool:
        if websockets is None:
            self._set_fatal_error(
                "dependency_missing",
                "Python package 'websockets' is required by rokid_openclaw_bridge",
                retryable=False,
            )
            return False

        if not self.link_code or not self.link_secret:
            self._set_fatal_error(
                "config_missing",
                "ROKID_OPENCLAW_LINK_CODE and ROKID_OPENCLAW_LINK_SECRET are required",
                retryable=False,
            )
            return False

        self._stopping = False
        self._receive_task = asyncio.create_task(self._connect_loop())
        self._mark_connected()
        logger.info("Rokid bridge: adapter started for linkCode=%s", self.link_code)
        return True

    async def disconnect(self) -> None:
        self._stopping = True
        self._mark_disconnected()

        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        self._receive_task = None

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if self._ws is None:
            return SendResult(success=False, error="Rokid WebSocket is not connected")

        request_id = self._resolve_request_id(chat_id, reply_to, metadata)
        try:
            await self._send_frame(_message_frame(request_id, self.agent_id, content))
            await self._send_frame(_done_frame(request_id, self.agent_id))
            return SendResult(success=True, message_id=request_id)
        except Exception as exc:
            logger.warning("Rokid bridge: failed to send reply: %s", exc)
            return SendResult(success=False, error=str(exc))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm"}

    async def _connect_loop(self) -> None:
        attempt = 0
        while not self._stopping:
            try:
                full_url = _build_ws_url(self.ws_url, self.link_code, self.link_secret)
                logger.info("Rokid bridge: connecting to %s", _redact_secret(full_url))
                async with websockets.connect(full_url) as ws:
                    self._ws = ws
                    attempt = 0
                    await self._send_frame({"type": "status", "connected": True, "gatewayReachable": True})
                    async for raw in ws:
                        await self._handle_raw_message(str(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stopping:
                    break
                logger.warning("Rokid bridge: WebSocket connection failed: %s", exc)
                self._ws = None
                await asyncio.sleep(_backoff_delay(attempt))
                attempt += 1
            finally:
                if not self._stopping:
                    self._ws = None

    async def _handle_raw_message(self, raw: str) -> None:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Rokid bridge: ignoring invalid JSON: %s", raw[:200])
            return

        if not isinstance(parsed, dict):
            return

        if parsed.get("type") == "cancel" and isinstance(parsed.get("requestId"), str):
            await self._handle_cancel(parsed["requestId"])
            return

        payload = parsed.get("payload")
        request_id = parsed.get("requestId")
        if isinstance(payload, str) and isinstance(request_id, str):
            await self._dispatch_inbound(payload, request_id, parsed)
            return

        logger.warning("Rokid bridge: unrecognized message: %s", raw[:200])

    async def _dispatch_inbound(
        self,
        text: str,
        request_id: str,
        parsed: Dict[str, Any],
    ) -> None:
        if not self._message_handler:
            logger.warning("Rokid bridge: no Hermes message handler is registered")
            return

        session_key = parsed.get("sessionKey")
        chat_id = str(session_key or self.link_code)
        self._request_ids_by_chat[chat_id] = request_id

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_id,
            chat_type="dm",
            user_id=chat_id,
            user_name=chat_id,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=parsed,
            message_id=request_id,
            timestamp=datetime.now(),
        )
        await self.handle_message(event)

    async def _handle_cancel(self, request_id: str) -> None:
        if not self._message_handler:
            return
        chat_id = _find_chat_id_by_request(self._request_ids_by_chat, request_id) or self.link_code
        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_id,
            chat_type="dm",
            user_id=chat_id,
            user_name=chat_id,
        )
        event = MessageEvent(
            text="/stop",
            message_type=MessageType.TEXT,
            source=source,
            message_id=f"{request_id}:cancel",
            timestamp=datetime.now(),
            raw_message={"type": "cancel", "requestId": request_id},
        )
        await self.handle_message(event)

    async def _send_frame(self, frame: Dict[str, Any]) -> None:
        if self._ws is None:
            raise RuntimeError("WebSocket is not connected")
        await self._ws.send(json.dumps(frame, ensure_ascii=False))

    def _resolve_request_id(
        self,
        chat_id: str,
        reply_to: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> str:
        if metadata:
            for key in ("request_id", "requestId", "message_id", "messageId"):
                value = metadata.get(key)
                if isinstance(value, str) and value:
                    return value
        if isinstance(reply_to, str) and reply_to:
            return reply_to
        request_id = self._request_ids_by_chat.get(chat_id)
        if request_id:
            return request_id
        return f"hermes-{int(time.time() * 1000)}"


def check_requirements() -> bool:
    return websockets is not None and bool(
        os.getenv("ROKID_OPENCLAW_LINK_CODE") and os.getenv("ROKID_OPENCLAW_LINK_SECRET")
    )


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    link_code = (
        os.getenv("ROKID_OPENCLAW_LINK_CODE")
        or extra.get("link_code")
        or extra.get("linkCode")
        or ""
    )
    link_secret = (
        os.getenv("ROKID_OPENCLAW_LINK_SECRET")
        or extra.get("link_secret")
        or extra.get("linkSecret")
        or ""
    )
    return websockets is not None and bool(str(link_code).strip() and str(link_secret).strip())


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> Optional[Dict[str, Any]]:
    link_code = os.getenv("ROKID_OPENCLAW_LINK_CODE", "").strip()
    link_secret = os.getenv("ROKID_OPENCLAW_LINK_SECRET", "").strip()
    if not (link_code and link_secret):
        return None

    seed: Dict[str, Any] = {
        "link_code": link_code,
        "link_secret": link_secret,
        "home_channel": {
            "chat_id": link_code,
            "name": "Rokid OpenClaw Bridge",
        },
    }
    ws_url = os.getenv("ROKID_OPENCLAW_WS_URL", "").strip()
    if ws_url:
        seed["ws_url"] = ws_url
    agent_id = os.getenv("ROKID_OPENCLAW_AGENT_ID", "").strip()
    if agent_id:
        seed["agent_id"] = agent_id
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id=None,
    media_files=None,
    force_document: bool = False,
) -> Dict[str, Any]:
    return {
        "error": (
            "rokid_openclaw_bridge standalone send is not supported; "
            "run Hermes gateway with the live WebSocket adapter"
        )
    }


def register(ctx):
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Rokid OpenClaw Bridge",
        adapter_factory=lambda cfg: RokidOpenClawBridgeAdapter(cfg),
        check_fn=lambda: websockets is not None,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["ROKID_OPENCLAW_LINK_CODE", "ROKID_OPENCLAW_LINK_SECRET"],
        install_hint="Install Hermes with gateway dependencies including the 'websockets' Python package.",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ROKID_OPENCLAW_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ROKID_OPENCLAW_ALLOWED_USERS",
        allow_all_env="ROKID_OPENCLAW_ALLOW_ALL_USERS",
        max_message_length=0,
        emoji="",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting through Rokid glasses via the OpenClaw bridge. "
            "Keep replies concise and readable in a wearable display."
        ),
    )


def _build_ws_url(base_url: str, link_code: str, link_secret: str) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode({'linkCode': link_code, 'linkSecret': link_secret})}"


def _message_frame(request_id: str, agent_id: str, content: str) -> Dict[str, Any]:
    return {
        "event": "message",
        "data": {
            "role": "agent",
            "message_id": request_id,
            "agent_id": agent_id,
            "answer_stream": content,
            "is_finish": False,
            "type": "answer",
        },
    }


def _done_frame(request_id: str, agent_id: str) -> Dict[str, Any]:
    return {
        "event": "done",
        "data": {
            "role": "agent",
            "message_id": request_id,
            "agent_id": agent_id,
            "answer_stream": "",
            "is_finish": True,
            "type": "answer",
        },
    }


def _backoff_delay(attempt: int) -> float:
    return min(1.0 * (2 ** min(attempt, 5)), 30.0)


def _redact_secret(url: str) -> str:
    if "linkSecret=" not in url:
        return url
    prefix, _, suffix = url.partition("linkSecret=")
    secret, separator, rest = suffix.partition("&")
    redacted = "<redacted>" if secret else ""
    return f"{prefix}linkSecret={redacted}{separator}{rest}"


def _find_chat_id_by_request(mapping: Dict[str, str], request_id: str) -> Optional[str]:
    for chat_id, mapped_request_id in mapping.items():
        if mapped_request_id == request_id:
            return chat_id
    return None
