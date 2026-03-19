// ============================================================
// WebSocket Bridge Protocol — 入站 JSON 消息类型定义
// ============================================================

// ------ 入站消息（用户服务 → 插件） ------

/** 用户服务推送的消息 */
export interface WsBridgeRequest {
  /** 消息内容 */
  payload: string;
  /** 唯一请求 ID，用于关联响应 */
  requestId: string;
  /** 可选：会话 key，用于多轮对话持久化 */
  sessionKey?: string;
  /** 可选：历史消息，由调用方维护（当前桥接层保持兼容但不消费） */
  history?: Array<{
    role: "user" | "assistant" | "system";
    content: string;
  }>;
}

/** 取消正在进行的请求 */
export interface WsBridgeCancel {
  type: "cancel";
  requestId: string;
}

export type InboundMessage = WsBridgeRequest | WsBridgeCancel;
