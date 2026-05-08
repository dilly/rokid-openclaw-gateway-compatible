// ============================================================
// OpenClaw WebSocket Bridge 插件入口
// ============================================================

import { createWsBridgeService, type WsBridgeConfig } from "./ws-bridge-service.js";

const CHANNEL_ID = "rokid-openclaw-bridge";

// 硬编码的内部配置常量
const INTERNAL_CONFIG = {
  wsUrl: "wss://rcs.rokid.com/claw/ws/link",
  reconnectMaxRetries: 10,
  reconnectBaseDelayMs: 1000,
  // 降级模式：Gateway WS RPC
  gatewayDefaultPort: 18789,
  agentId: "main",
} as const;

/**
 * OpenClaw 插件注册函数
 * Gateway 在加载插件时调用此函数
 */
export default function register(api: any) {
  const logger = api.logger ?? {
    info: (msg: string) => console.log(msg),
    warn: (msg: string) => console.warn(msg),
    error: (msg: string) => console.error(msg),
  };

  logger.info(`[rokid-openclaw-bridge] Plugin registered.`);
  logger.info(
    `[rokid-openclaw-bridge] api keys: ${Object.keys(api ?? {}).join(",")}`
  );

  if (typeof api.registerChannel === "function") {
    const channelPlugin = buildChannelPlugin(logger);
    api.registerChannel({ plugin: channelPlugin });
    logger.info(`[rokid-openclaw-bridge] Channel plugin registered for onboarding.`);
  } else {
    logger.warn(
      `[rokid-openclaw-bridge] api.registerChannel is NOT a function (typeof=${typeof api.registerChannel}); channel not registered.`
    );
  }
}

function buildChannelPlugin(logger: any) {
  return {
    id: CHANNEL_ID,

    meta: {
      id: CHANNEL_ID,
      label: "WebSocket Bridge",
      selectionLabel: "WebSocket Bridge (rokid-openclaw-bridge)",
      docsPath: "",
      blurb: "Connect OpenClaw to an external device via WebSocket link code.",
      order: 100,
    },

    capabilities: {
      chatTypes: ["dm"] as any,
    },

    config: {
      listAccountIds: (_cfg: any) => {
        const accounts = _cfg?.channels?.[CHANNEL_ID]?.accounts ?? {};
        const ids = Object.keys(accounts);
        logger.info(
          `[rokid-openclaw-bridge] listAccountIds called -> ids=${JSON.stringify(ids)}`
        );
        return ids;
      },
      resolveAccount: (_cfg: any, accountId?: string | null) => {
        const acc =
          accountId != null
            ? _cfg?.channels?.[CHANNEL_ID]?.accounts?.[accountId]
            : undefined;
        logger.info(
          `[rokid-openclaw-bridge] resolveAccount called -> accountId=${accountId ?? "(none)"} found=${Boolean(acc)}`
        );
        return { accountId, config: acc };
      },
    },

    onboarding: {
      channel: CHANNEL_ID,

      getStatus: async (ctx: any) => {
        const pluginCfg = ctx.cfg?.plugins?.entries?.[CHANNEL_ID]?.config ?? {};
        const linkCode: string = pluginCfg.linkCode ?? "";
        const linkSecret: string = pluginCfg.linkSecret ?? "";
        const configured = Boolean(linkCode && linkSecret);

        return {
          channel: CHANNEL_ID,
          configured,
          statusLines: configured
            ? [`Link code: ${linkCode}`]
            : ["Not configured — run setup to provide linkCode and linkSecret."],
          selectionHint: configured ? `linkCode=${linkCode}` : "not configured",
        };
      },

      configure: async (ctx: any) => {
        const { cfg, prompter } = ctx;

        await prompter.intro("WebSocket Bridge Setup");
        await prompter.note(
          "This plugin connects OpenClaw to an external device over WebSocket.\n" +
            "You only need the link code and link secret provided by your device.",
          "About rokid-openclaw-bridge"
        );

        const currentCfg = cfg?.plugins?.entries?.[CHANNEL_ID]?.config ?? {};

        const linkCode: string = await prompter.text({
          message: "Link code (provided by your device)",
          placeholder: "e.g. ABC123",
          initialValue: currentCfg.linkCode ?? "",
          validate: (v: string) => (v.trim() ? undefined : "Link code is required"),
        });

        const linkSecret: string = await prompter.text({
          message: "Link secret (provided by your device)",
          placeholder: "e.g. mysecret",
          initialValue: currentCfg.linkSecret ?? "",
          validate: (v: string) => (v.trim() ? undefined : "Link secret is required"),
        });

        const updatedCfg = deepSet(cfg, ["plugins", "entries", CHANNEL_ID, "config"], {
          linkCode: linkCode.trim(),
          linkSecret: linkSecret.trim(),
        });

        await prompter.outro(`WebSocket Bridge configured!\nlinkCode=${linkCode.trim()}`);

        return { cfg: updatedCfg, accountId: linkCode.trim() };
      },
    },

    gateway: {
      startAccount: async (ctx: any) => {
        logger.info(
          `[rokid-openclaw-bridge] startAccount INVOKED -> accountId=${ctx?.account?.accountId ?? "(none)"}`
        );
        const { account, cfg, abortSignal, log, channelRuntime } = ctx;
        const pluginConfig = cfg?.plugins?.entries?.[CHANNEL_ID]?.config ?? {};
        const accountConfig = account.config ?? {};

        const linkCode = String(accountConfig.linkCode || pluginConfig.linkCode || "").trim();
        const linkSecret = String(accountConfig.linkSecret || pluginConfig.linkSecret || "").trim();

        if (!linkCode || !linkSecret) {
          throw new Error("linkCode and linkSecret are required");
        }

        if (!channelRuntime) {
          log?.warn?.(`[${account.accountId}] channelRuntime not available — falling back to Gateway WS RPC mode`);
        }

        const gatewayToken = String(cfg?.gateway?.auth?.token ?? "").trim();
        const gatewayPort = cfg?.gateway?.port ?? INTERNAL_CONFIG.gatewayDefaultPort;
        const gatewayUrl = `http://127.0.0.1:${gatewayPort}`;

        const config: WsBridgeConfig = {
          wsUrl: INTERNAL_CONFIG.wsUrl,
          linkCode,
          linkSecret,
          reconnectMaxRetries: INTERNAL_CONFIG.reconnectMaxRetries,
          reconnectBaseDelayMs: INTERNAL_CONFIG.reconnectBaseDelayMs,
          openclaw: {
            cfg,
            accountId: account.accountId,
            channelId: CHANNEL_ID,
            channelRuntime,
            gatewayUrl,
            gatewayToken,
            agentId: INTERNAL_CONFIG.agentId,
          },
        };

        log?.info?.(`[${account.accountId}] Starting WebSocket Bridge service...`);
        log?.info?.(`[${account.accountId}] Config: linkCode=${config.linkCode}, wsUrl=${config.wsUrl}`);

        const service = createWsBridgeService(config, log ?? logger);
        await service.start();

        const stopPromise = new Promise<void>((resolve) => {
          abortSignal.addEventListener("abort", async () => {
            log?.info?.(`[${account.accountId}] Stopping WebSocket Bridge service...`);
            await service.stop();
            resolve();
          });
        });

        await stopPromise;

        return {
          stop: async () => {
            await service.stop();
          },
        };
      },
    },
  };
}

/** Immutably set a nested path in an object. */
function deepSet(obj: any, path: string[], value: any): any {
  if (path.length === 0) return value;
  const [head, ...tail] = path;
  return {
    ...obj,
    [head]: deepSet(obj?.[head] ?? {}, tail, value),
  };
}
