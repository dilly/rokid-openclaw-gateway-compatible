import type { OpenClawConfig, PluginRuntime } from "openclaw/plugin-sdk";
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
interface Logger {
    info(msg: string): void;
    warn(msg: string): void;
    error(msg: string): void;
}
export declare function createWsBridgeService(config: WsBridgeConfig, logger: Logger): {
    id: string;
    start(): Promise<void>;
    stop(): Promise<void>;
};
export {};
