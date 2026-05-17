import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ADAPTER_PATH = ROOT / "hermes-plugin" / "rokid_openclaw_bridge" / "adapter.py"


class SendResult:
    def __init__(self, success, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error


class MessageType:
    TEXT = "text"


class MessageEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class BasePlatformAdapter:
    def __init__(self, config, platform):
        self.config = config
        self.platform = platform
        self._message_handler = None
        self.connected = False

    def _mark_connected(self):
        self.connected = True

    def _mark_disconnected(self):
        self.connected = False

    def _set_fatal_error(self, code, message, retryable):
        self.fatal_error = (code, message, retryable)

    def build_source(self, **kwargs):
        return types.SimpleNamespace(**kwargs)

    async def handle_message(self, event):
        await self._message_handler(event)


class Platform(str):
    pass


def load_adapter_module():
    gateway = types.ModuleType("gateway")
    gateway_config = types.ModuleType("gateway.config")
    gateway_config.Platform = Platform
    gateway_platforms = types.ModuleType("gateway.platforms")
    gateway_platforms_base = types.ModuleType("gateway.platforms.base")
    gateway_platforms_base.BasePlatformAdapter = BasePlatformAdapter
    gateway_platforms_base.MessageEvent = MessageEvent
    gateway_platforms_base.MessageType = MessageType
    gateway_platforms_base.SendResult = SendResult

    sys.modules.setdefault("gateway", gateway)
    sys.modules["gateway.config"] = gateway_config
    sys.modules.setdefault("gateway.platforms", gateway_platforms)
    sys.modules["gateway.platforms.base"] = gateway_platforms_base

    spec = importlib.util.spec_from_file_location("rokid_adapter_under_test", ADAPTER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeWebSocket:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)


class AdapterProtocolTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_adapter_module()

    async def test_send_uses_rokid_message_and_done_frames(self):
        adapter = self.module.RokidOpenClawBridgeAdapter(
            types.SimpleNamespace(extra={"link_code": "abc", "link_secret": "secret"})
        )
        adapter._ws = FakeWebSocket()
        adapter._request_ids_by_chat["abc"] = "req-1"

        result = await adapter.send("abc", "hello")

        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "req-1")
        self.assertEqual(len(adapter._ws.sent), 2)
        self.assertIn('"event": "message"', adapter._ws.sent[0])
        self.assertIn('"message_id": "req-1"', adapter._ws.sent[0])
        self.assertIn('"answer_stream": "hello"', adapter._ws.sent[0])
        self.assertIn('"event": "done"', adapter._ws.sent[1])
        self.assertIn('"is_finish": true', adapter._ws.sent[1])

    async def test_inbound_payload_dispatches_message_event(self):
        adapter = self.module.RokidOpenClawBridgeAdapter(
            types.SimpleNamespace(extra={"link_code": "abc", "link_secret": "secret"})
        )
        events = []

        async def capture(event):
            events.append(event)

        adapter._message_handler = capture
        await adapter._handle_raw_message(
            '{"payload":"hi","requestId":"req-2","sessionKey":"session-1"}'
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].text, "hi")
        self.assertEqual(events[0].message_id, "req-2")
        self.assertEqual(events[0].source.chat_id, "session-1")
        self.assertEqual(events[0].raw_message["requestId"], "req-2")

    async def test_missing_credentials_reject_connect(self):
        self.module.websockets = object()
        adapter = self.module.RokidOpenClawBridgeAdapter(types.SimpleNamespace(extra={}))

        ok = await adapter.connect()

        self.assertFalse(ok)
        self.assertEqual(adapter.fatal_error[0], "config_missing")


if __name__ == "__main__":
    unittest.main()
