// ============================================================
// Gateway WebSocket RPC 客户端
// 通过本地 OpenClaw Gateway 的 WebSocket RPC 协议发送 agent 请求
// ============================================================

import WebSocket from "ws";
import { randomUUID } from "node:crypto";
import type { WsBridgeRequest } from "./protocol.js";

export interface GatewayConfig {
  gatewayUrl: string;
  gatewayToken: string;
  agentId: string;
}

export type StreamEvent =
  | { type: "delta"; delta: string }
  | { type: "done"; model: string };

let _logger: { info(msg: string): void } | null = null;
export function setGatewayLogger(logger: { info(msg: string): void }) {
  _logger = logger;
}

/**
 * 将 HTTP Gateway URL 转为 WebSocket URL
 * e.g. http://127.0.0.1:18789 -> ws://127.0.0.1:18789
 */
function toWsUrl(gatewayUrl: string): string {
  return gatewayUrl.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
}

/**
 * 通过 Gateway WebSocket RPC 流式调用 agent
 * 返回 async generator，逐步 yield 文本片段和完成事件
 */
export async function* streamCompletion(
  config: GatewayConfig,
  request: WsBridgeRequest,
  signal: AbortSignal
): AsyncGenerator<StreamEvent> {
  const wsUrl = toWsUrl(config.gatewayUrl);
  const sessionKey = request.sessionKey ?? randomUUID();
  const idempotencyKey = randomUUID();

  const params = {
    message: request.payload,
    agentId: config.agentId,
    sessionKey,
    idempotencyKey,
    deliver: false,
  };

  _logger?.info(
    `[rokid-openclaw-bridge] [gateway] --> WS RPC agent ${wsUrl} params=${JSON.stringify(params).slice(0, 300)}`
  );

  const headers: Record<string, string> = {};
  if (config.gatewayToken) {
    headers["Authorization"] = `Bearer ${config.gatewayToken}`;
  }

  yield* await new Promise<AsyncGenerator<StreamEvent>>((resolve, reject) => {
    const ws = new WebSocket(wsUrl, { headers });
    let settled = false;
    let requestSent = false;
    const requestId = randomUUID();

    const queue: Array<StreamEvent | Error | null> = [];
    let waiter: ((v: void) => void) | null = null;

    function push(item: StreamEvent | Error | null) {
      queue.push(item);
      waiter?.();
      waiter = null;
    }

    async function* gen(): AsyncGenerator<StreamEvent> {
      while (true) {
        if (queue.length === 0) {
          await new Promise<void>((r) => { waiter = r; });
        }
        const item = queue.shift()!;
        if (item === null) return;
        if (item instanceof Error) throw item;
        yield item;
      }
    }

    signal.addEventListener("abort", () => {
      if (!settled) {
        settled = true;
        ws.close();
        push(null);
      }
    });

    ws.on("open", () => {
      _logger?.info(`[rokid-openclaw-bridge] [gateway] <-- WS connected to ${wsUrl}`);
    });

    ws.on("message", (data: { toString(): string }) => {
      const raw = data.toString();
      _logger?.info(`[rokid-openclaw-bridge] [gateway] SSE data: ${raw.slice(0, 300)}`);

      let parsed: any;
      try {
        parsed = JSON.parse(raw);
      } catch {
        return;
      }

      // Handle connect challenge — send connect frame
      if (parsed.event === "connect.challenge") {
        const connectFrame: any = {
          type: "req",
          id: randomUUID(),
          method: "connect",
          params: {
            minProtocol: 3,
            maxProtocol: 3,
            client: {
              id: "cli",
              version: "1.0.0",
              platform: (globalThis as any).process?.platform ?? "linux",
              mode: "cli",
            },
            caps: [],
            role: "operator",
            scopes: ["operator.admin"],
            ...(config.gatewayToken
              ? { auth: { token: config.gatewayToken } }
              : {}),
          },
        };
        ws.send(JSON.stringify(connectFrame));
        return;
      }

      // After connect ack, send the agent request
      if (!requestSent && parsed.id && parsed.ok !== undefined) {
        requestSent = true;
        if (!settled) {
          const agentFrame = {
            type: "req",
            id: requestId,
            method: "agent",
            params,
          };
          _logger?.info(
            `[rokid-openclaw-bridge] [gateway] --> sending agent frame: ${JSON.stringify(agentFrame).slice(0, 300)}`
          );
          ws.send(JSON.stringify(agentFrame));
          resolve(gen());
        }
        return;
      }

      // Handle streaming events
      if (parsed.event) {
        const payload = parsed.payload;
        if (!payload) return;

        if (parsed.event === "agent" && payload.stream === "assistant" && payload.data?.delta) {
          push({ type: "delta", delta: payload.data.delta });
          return;
        }

        if (parsed.event === "chat") {
          if (payload.state === "final") {
            push({ type: "done", model: config.agentId });
            if (!settled) {
              settled = true;
              push(null);
              ws.close();
            }
          } else if (payload.state === "error") {
            if (!settled) {
              settled = true;
              push(new GatewayError("GATEWAY_ERROR", payload.errorMessage ?? "Agent error"));
              push(null);
              ws.close();
            }
          } else if (payload.state === "aborted") {
            if (!settled) {
              settled = true;
              push(null);
              ws.close();
            }
          }
        }

        if (parsed.event === "agent" && payload.data?.phase === "end") {
          if (!settled) {
            settled = true;
            push({ type: "done", model: config.agentId });
            push(null);
            ws.close();
          }
        }
        return;
      }

      // Response frame for the agent request
      if (parsed.id === requestId) {
        if (!parsed.ok) {
          if (!settled) {
            settled = true;
            push(new GatewayError("GATEWAY_ERROR", parsed.error?.message ?? "RPC error"));
            push(null);
            ws.close();
          }
        }
        return;
      }
    });

    ws.on("error", (err: Error) => {
      _logger?.info(`[rokid-openclaw-bridge] [gateway] WS error: ${err.message}`);
      if (!settled) {
        settled = true;
        if (!requestSent) {
          reject(new GatewayError("GATEWAY_UNAVAILABLE", err.message));
        } else {
          push(new GatewayError("GATEWAY_UNAVAILABLE", err.message));
          push(null);
        }
      }
    });

    ws.on("close", (code: number, reason: { toString(): string }) => {
      _logger?.info(`[rokid-openclaw-bridge] [gateway] WS closed code=${code}`);
      if (!settled) {
        settled = true;
        const msg = `Gateway WS closed unexpectedly (code=${code}, reason=${reason.toString()})`;
        if (!requestSent) {
          reject(new GatewayError("GATEWAY_UNAVAILABLE", msg));
        } else {
          push(new GatewayError("STREAM_ERROR", msg));
          push(null);
        }
      }
    });
  });
}

export class GatewayError extends Error {
  constructor(
    public code: string,
    message: string
  ) {
    super(message);
    this.name = "GatewayError";
  }
}
