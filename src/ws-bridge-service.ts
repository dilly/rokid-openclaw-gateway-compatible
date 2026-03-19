// ============================================================
// WebSocket Bridge 核心服务
// 管理 WebSocket 客户端连接、消息路由、并发请求、自动重连
// 支持双模式：channelRuntime SDK 模式 / Gateway WS RPC 降级模式
// ============================================================

import type { OpenClawConfig, PluginRuntime, ReplyPayload } from "openclaw/plugin-sdk";
import WebSocket from "ws";
import type { WsBridgeRequest } from "./protocol.js";
import {
  streamCompletion,
  setGatewayLogger,
  GatewayError,
  type GatewayConfig,
} from "./gateway-client.js";

export interface WsBridgeConfig {
  wsUrl: string;
  linkCode: string;
  linkSecret: string;
  reconnectMaxRetries: number;
  reconnectBaseDelayMs: number;
  openclaw: {
    cfg: OpenClawConfig;
    accountId?: string | null;
    channelId: string;
    /** 优先使用；不存在时降级为 Gateway WS RPC 模式 */
    channelRuntime?: PluginRuntime["channel"];
    /** 降级模式：Gateway 地址，默认 http://127.0.0.1:18789 */
    gatewayUrl?: string;
    /** 降级模式：Gateway auth token */
    gatewayToken?: string;
    /** 降级模式：目标 Agent ID，默认 "main" */
    agentId?: string;
  };
}

interface WsBridgeMessageFrame {
  event: "message";
  data: {
    role: "agent";
    message_id: string;
    agent_id: string;
    answer_stream: string;
    is_finish: false;
    type: "answer";
  };
}

interface WsBridgeDoneFrame {
  event: "done";
  data: {
    role: "agent";
    message_id: string;
    agent_id: string;
    answer_stream: "";
    is_finish: true;
    type: "answer";
  };
}

interface WsBridgeErrorFrame {
  type: "error";
  requestId: string;
  code: string;
  message: string;
}

interface WsBridgeStatusFrame {
  type: "status";
  connected: boolean;
  gatewayReachable: boolean;
}

type OutboundFrame =
  | WsBridgeMessageFrame
  | WsBridgeDoneFrame
  | WsBridgeErrorFrame
  | WsBridgeStatusFrame;

/**
 * 拼接完整的 WebSocket 连接地址
 */
function buildWsUrl(config: WsBridgeConfig): string {
  const url = new URL(config.wsUrl);
  url.searchParams.set("linkCode", config.linkCode);
  url.searchParams.set("linkSecret", config.linkSecret);
  return url.toString();
}

interface Logger {
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
}

function backoffDelay(attempt: number, baseMs: number, maxMs = 30000): number {
  const delay = baseMs * Math.pow(2, attempt);
  const jitter = Math.random() * baseMs;
  return Math.min(delay + jitter, maxMs);
}

function normalizeAccountId(accountId?: string | null, linkCode?: string): string {
  const trimmed = typeof accountId === "string" ? accountId.trim() : "";
  return trimmed || (linkCode ?? "");
}

function normalizeTextPayload(payload: ReplyPayload): string {
  return typeof payload.text === "string" ? payload.text : "";
}

export function createWsBridgeService(config: WsBridgeConfig, logger: Logger) {
  let ws: WebSocket | null = null;
  let reconnectAttempt = 0;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;

  const activeRequests = new Map<string, AbortController>();
  const accountId = normalizeAccountId(config.openclaw.accountId, config.linkCode);
  const rt = config.openclaw.channelRuntime;
  const cfg = config.openclaw.cfg;
  const channelId = config.openclaw.channelId;

  // 降级模式配置
  const gatewayConfig: GatewayConfig | null = rt
    ? null
    : {
        gatewayUrl: config.openclaw.gatewayUrl ?? "http://127.0.0.1:18789",
        gatewayToken: config.openclaw.gatewayToken ?? "",
        agentId: config.openclaw.agentId ?? "main",
      };

  if (gatewayConfig) {
    setGatewayLogger(logger);
    logger.info(
      `[rokid-openclaw-bridge] channelRuntime unavailable — falling back to Gateway WS RPC mode (${gatewayConfig.gatewayUrl}, agentId=${gatewayConfig.agentId})`
    );
  }

  function sendWs(msg: OutboundFrame) {
    if (ws?.readyState === WebSocket.OPEN) {
      const raw = JSON.stringify(msg);
      logger.info(`[rokid-openclaw-bridge] >>> SEND: ${raw}`);
      ws.send(raw);
    }
  }

  function sendStreamChunk(requestId: string, agentId: string, delta: string) {
    if (!delta) return;
    sendWs({
      event: "message",
      data: {
        role: "agent",
        message_id: requestId,
        agent_id: agentId,
        answer_stream: delta,
        is_finish: false,
        type: "answer",
      },
    });
  }

  function sendDone(requestId: string, agentId: string) {
    sendWs({
      event: "done",
      data: {
        role: "agent",
        message_id: requestId,
        agent_id: agentId,
        answer_stream: "",
        is_finish: true,
        type: "answer",
      },
    });
  }

  function handleMessage(raw: string) {
    let msg: unknown;
    try {
      msg = JSON.parse(raw);
    } catch {
      logger.warn(`[rokid-openclaw-bridge] Invalid JSON received: ${raw.slice(0, 200)}`);
      return;
    }

    if (!msg || typeof msg !== "object") return;
    const parsed = msg as Record<string, unknown>;

    if (parsed.type === "cancel" && typeof parsed.requestId === "string") {
      handleCancel(parsed.requestId);
      return;
    }

    if (typeof parsed.payload === "string" && typeof parsed.requestId === "string") {
      void handleChatRequest(parsed as unknown as WsBridgeRequest);
      return;
    }

    logger.warn(`[rokid-openclaw-bridge] Unrecognized message: ${raw.slice(0, 200)}`);
  }

  async function handleChatRequest(request: WsBridgeRequest) {
    const abortCtrl = new AbortController();
    activeRequests.set(request.requestId, abortCtrl);

    try {
      if (rt) {
        await handleChatRequestWithRuntime(request, abortCtrl);
      } else {
        await handleChatRequestWithGateway(request, abortCtrl);
      }
    } finally {
      activeRequests.delete(request.requestId);
    }
  }

  // ---- 模式一：channelRuntime SDK ----

  async function handleChatRequestWithRuntime(
    request: WsBridgeRequest,
    abortCtrl: AbortController
  ) {
    let lastPartialText = "";
    let partialStreamed = false;
    let finalFallbackText = "";

    try {
      const route = rt!.routing.resolveAgentRoute({
        cfg,
        channel: channelId,
        accountId,
        peer: { kind: "direct", id: accountId },
      });

      const sessionKey = request.sessionKey ?? route.sessionKey;
      const storePath = rt!.session.resolveStorePath(cfg.session?.store, {
        agentId: route.agentId,
      });
      const previousTimestamp = rt!.session.readSessionUpdatedAt({
        storePath,
        sessionKey,
      });
      const rawBody = request.payload;
      const body = rt!.reply.formatAgentEnvelope({
        channel: "WebSocket Bridge",
        from: accountId,
        timestamp: Date.now(),
        envelope: rt!.reply.resolveEnvelopeFormatOptions(cfg),
        body: rawBody,
        previousTimestamp,
      });

      const ctxPayload = rt!.reply.finalizeInboundContext({
        Body: body,
        BodyForAgent: rawBody,
        RawBody: rawBody,
        CommandBody: rawBody,
        From: `rokid-openclaw-bridge:${accountId}`,
        To: `rokid-openclaw-bridge:${accountId}`,
        SessionKey: sessionKey,
        AccountId: route.accountId,
        ChatType: "direct",
        ConversationLabel: accountId,
        SenderName: accountId,
        SenderId: accountId,
        Provider: channelId,
        Surface: channelId,
        MessageSid: request.requestId,
        Timestamp: Date.now(),
        OriginatingChannel: channelId,
        OriginatingTo: `rokid-openclaw-bridge:${accountId}`,
      });

      await rt!.session.recordInboundSession({
        storePath,
        sessionKey: ctxPayload.SessionKey ?? route.sessionKey,
        ctx: ctxPayload,
        updateLastRoute: {
          sessionKey: route.mainSessionKey,
          channel: channelId,
          to: `rokid-openclaw-bridge:${accountId}`,
          accountId: route.accountId,
        },
        onRecordError: (err: unknown) => {
          logger.warn(`[rokid-openclaw-bridge] Failed to record inbound session: ${String(err)}`);
        },
      });

      await rt!.reply.dispatchReplyWithBufferedBlockDispatcher({
        ctx: ctxPayload,
        cfg,
        dispatcherOptions: {
          responsePrefix: "",
          deliver: async (payload, info) => {
            if (abortCtrl.signal.aborted) return;
            if (info.kind === "tool") return;
            if (payload.isReasoning) return;
            if (partialStreamed) return;
            const text = normalizeTextPayload(payload);
            if (!text) return;
            finalFallbackText = text;
          },
          onError: (err) => {
            logger.error(`[rokid-openclaw-bridge] Dispatch delivery error: ${String(err)}`);
          },
        },
        replyOptions: {
          abortSignal: abortCtrl.signal,
          disableBlockStreaming: true,
          onPartialReply: async (payload) => {
            if (abortCtrl.signal.aborted) return;
            if (payload.isReasoning) return;
            const text = normalizeTextPayload(payload);
            if (!text) return;
            const delta = text.startsWith(lastPartialText) ? text.slice(lastPartialText.length) : text;
            lastPartialText = text;
            if (!delta) return;
            partialStreamed = true;
            sendStreamChunk(request.requestId, route.agentId, delta);
          },
        },
      });

      if (!abortCtrl.signal.aborted) {
        if (!partialStreamed && finalFallbackText) {
          sendStreamChunk(request.requestId, route.agentId, finalFallbackText);
        }
        sendDone(request.requestId, route.agentId);
      }
    } catch (err: unknown) {
      if (abortCtrl.signal.aborted) return;
      const message = err instanceof Error ? err.message : String(err);
      logger.error(`[rokid-openclaw-bridge] OpenClaw dispatch failed for ${request.requestId}: ${message}`);
      sendWs({
        type: "error",
        requestId: request.requestId,
        code: "GATEWAY_UNAVAILABLE",
        message,
      });
    }
  }

  // ---- 模式二：Gateway WS RPC 降级 ----

  async function handleChatRequestWithGateway(
    request: WsBridgeRequest,
    abortCtrl: AbortController
  ) {
    const gw = gatewayConfig!;
    try {
      for await (const event of streamCompletion(gw, request, abortCtrl.signal)) {
        if (event.type === "delta") {
          sendStreamChunk(request.requestId, gw.agentId, event.delta);
        } else if (event.type === "done") {
          sendDone(request.requestId, gw.agentId);
        }
      }
    } catch (err: unknown) {
      if (abortCtrl.signal.aborted) return;
      const code = err instanceof GatewayError ? err.code : "GATEWAY_UNAVAILABLE";
      const message = err instanceof Error ? err.message : String(err);
      logger.error(`[rokid-openclaw-bridge] Gateway dispatch failed for ${request.requestId}: ${message}`);
      sendWs({ type: "error", requestId: request.requestId, code, message });
    }
  }

  function handleCancel(requestId: string) {
    const ctrl = activeRequests.get(requestId);
    if (ctrl) {
      ctrl.abort();
      activeRequests.delete(requestId);
      logger.info(`[rokid-openclaw-bridge] Request ${requestId} cancelled`);
    }
  }

  function connect() {
    if (stopped) return;

    const fullWsUrl = buildWsUrl(config);
    logger.info(
      `[rokid-openclaw-bridge] Config: linkCode=${config.linkCode}, wsUrl=${config.wsUrl}`
    );
    logger.info(
      `[rokid-openclaw-bridge] Connecting to ${fullWsUrl} (attempt ${reconnectAttempt + 1})`
    );

    ws = new WebSocket(fullWsUrl);

    ws.on("open", () => {
      reconnectAttempt = 0;
      logger.info(`[rokid-openclaw-bridge] Connected to ${config.wsUrl}`);
      sendWs({ type: "status", connected: true, gatewayReachable: true });
    });

    ws.on("message", (data) => {
      const raw = data.toString();
      logger.info(`[rokid-openclaw-bridge] <<< RECV: ${raw.slice(0, 500)}`);
      handleMessage(raw);
    });

    ws.on("close", (code, reason) => {
      logger.warn(
        `[rokid-openclaw-bridge] Disconnected (code=${code}, reason=${reason.toString()})`
      );
      scheduleReconnect();
    });

    ws.on("error", (err) => {
      logger.error(`[rokid-openclaw-bridge] WebSocket error: ${err.message}`);
    });
  }

  function scheduleReconnect() {
    if (stopped) return;

    if (reconnectAttempt >= config.reconnectMaxRetries) {
      logger.error(
        `[rokid-openclaw-bridge] Max reconnect retries (${config.reconnectMaxRetries}) exhausted. Giving up.`
      );
      return;
    }

    const delay = backoffDelay(reconnectAttempt, config.reconnectBaseDelayMs);
    reconnectAttempt++;
    logger.info(`[rokid-openclaw-bridge] Reconnecting in ${Math.round(delay)}ms...`);

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, delay);
  }

  return {
    id: "rokid-openclaw-bridge",

    async start() {
      stopped = false;
      connect();
    },

    async stop() {
      stopped = true;

      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }

      for (const [id, ctrl] of activeRequests) {
        ctrl.abort();
        activeRequests.delete(id);
      }

      if (ws) {
        ws.removeAllListeners();
        if (ws.readyState === WebSocket.OPEN) {
          ws.close(1000, "Plugin stopping");
        }
        ws = null;
      }

      logger.info("[rokid-openclaw-bridge] Service stopped");
    },
  };
}
