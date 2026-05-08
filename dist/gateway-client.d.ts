import type { WsBridgeRequest } from "./protocol.js";
export interface GatewayConfig {
    gatewayUrl: string;
    gatewayToken: string;
    agentId: string;
}
export type StreamEvent = {
    type: "delta";
    delta: string;
} | {
    type: "done";
    model: string;
};
export declare function setGatewayLogger(logger: {
    info(msg: string): void;
}): void;
/**
 * 通过 Gateway WebSocket RPC 流式调用 agent
 * 返回 async generator，逐步 yield 文本片段和完成事件
 */
export declare function streamCompletion(config: GatewayConfig, request: WsBridgeRequest, signal: AbortSignal): AsyncGenerator<StreamEvent>;
export declare class GatewayError extends Error {
    code: string;
    constructor(code: string, message: string);
}
