# rokid-openclaw-gateway-compatible

#### 介绍
Rokid glasses 网关桥接插件，保留 OpenClaw 插件入口，同时新增 Hermes Agent 平台插件兼容层。

#### 软件架构
- OpenClaw：TypeScript 插件入口在 `src/index.ts`，插件声明为 `openclaw.plugin.json`。
- Hermes Agent：Python 平台插件位于 `hermes-plugin/rokid_openclaw_bridge/`，按 Hermes gateway 第三方平台插件方式加载。
- 设备侧 WebSocket 协议保持一致：入站 `{ payload, requestId, sessionKey? }`，出站 `event: "message"` 与 `event: "done"`。


#### OpenClaw 安装教程

1. 克隆仓库并构建：
   ```bash
   cd /tmp
   git clone https://gitee.com/rokid-eco/rokid-openclaw-gateway-compatible.git
   cd rokid-openclaw-gateway-compatible
   npm install
   npm run build
   ```

2. 通过 openclaw CLI 注册插件（`--link` 表示链接本地路径，源码改动重启即可生效）：
   ```bash
   openclaw plugins install --link /tmp/rokid-openclaw-gateway-compatible
   ```

3. 编辑 `~/.openclaw/openclaw.json`，加入 `channels.rokid-openclaw-bridge.accounts.<accountId>` 账号配置（`accountId` 可任意，下例用设备的 linkCode 作为 id）：
   ```json
   {
     "plugins": {
       "allow": [
         "rokid-openclaw-bridge"
       ],
       "entries": {
         "rokid-openclaw-bridge": {
           "enabled": true
         }
       },
       "load": {
         "paths": [
           "/tmp/rokid-openclaw-gateway-compatible"
         ]
       }
     },
     "channels": {
       "rokid-openclaw-bridge": {
         "accounts": {
           "<your-link-code>": {
             "linkCode": "<your-link-code>",
             "linkSecret": "<your-link-secret>"
           }
         }
       }
     }
   }
   ```
   将 `<your-link-code>` 与 `<your-link-secret>` 替换为设备配对界面提供的值。

4. 重启 gateway 让配置生效：
   ```bash
   openclaw gateway restart
   ```

5. 验证：
   ```bash
   openclaw plugins list                       # 应能看到 rokid-openclaw-bridge enabled
   tail -f /tmp/openclaw/openclaw-*.log | grep rokid-openclaw-bridge
   ```
   日志中出现 `Plugin registered.` 与 `Starting WebSocket Bridge service...` 即表示连接成功。

#### 配置字段说明

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `channels.rokid-openclaw-bridge.accounts.<id>.linkCode` | 是 | 设备配对码（device link code） |
| `channels.rokid-openclaw-bridge.accounts.<id>.linkSecret` | 是 | 设备配对密钥（device link secret） |

#### Hermes Agent 安装教程

Hermes Agent 通过 `~/.hermes/plugins/<plugin-name>/plugin.yaml` 和 `__init__.py` 加载第三方平台插件。当前仓库提供的 Hermes 插件目录是 `hermes-plugin/rokid_openclaw_bridge`。

1. 安装或链接插件：
   ```bash
   mkdir -p ~/.hermes/plugins
   ln -s /tmp/rokid-openclaw-gateway-compatible/hermes-plugin/rokid_openclaw_bridge \
     ~/.hermes/plugins/rokid_openclaw_bridge
   ```

   也可以复制目录：
   ```bash
   cp -R /tmp/rokid-openclaw-gateway-compatible/hermes-plugin/rokid_openclaw_bridge \
     ~/.hermes/plugins/rokid_openclaw_bridge
   ```

2. 启用 Hermes 插件。编辑 `~/.hermes/config.yaml`：
   ```yaml
   plugins:
     enabled:
       - rokid_openclaw_bridge

   gateway:
     platforms:
       rokid_openclaw_bridge:
         enabled: true
         extra:
           link_code: "<your-link-code>"
           link_secret: "<your-link-secret>"
   ```

   或者使用环境变量配置：
   ```bash
   export ROKID_OPENCLAW_LINK_CODE="<your-link-code>"
   export ROKID_OPENCLAW_LINK_SECRET="<your-link-secret>"
   # 可选，默认是 wss://rcs.rokid.com/claw/ws/link
   export ROKID_OPENCLAW_WS_URL="wss://rcs.rokid.com/claw/ws/link"
   ```

3. 启动 Hermes gateway：
   ```bash
   hermes gateway start
   ```

4. 验证：
   ```bash
   hermes gateway status
   tail -f ~/.hermes/logs/gateway.log | grep rokid_openclaw_bridge
   ```

#### Hermes 配置字段说明

| 字段 / 环境变量 | 必填 | 说明 |
| --- | --- | --- |
| `gateway.platforms.rokid_openclaw_bridge.extra.link_code` / `ROKID_OPENCLAW_LINK_CODE` | 是 | 设备配对码 |
| `gateway.platforms.rokid_openclaw_bridge.extra.link_secret` / `ROKID_OPENCLAW_LINK_SECRET` | 是 | 设备配对密钥 |
| `gateway.platforms.rokid_openclaw_bridge.extra.ws_url` / `ROKID_OPENCLAW_WS_URL` | 否 | Rokid WebSocket 地址 |
| `ROKID_OPENCLAW_ALLOWED_USERS` | 否 | 允许访问的 session/chat id，逗号分隔 |
| `ROKID_OPENCLAW_ALLOW_ALL_USERS` | 否 | 是否允许所有 Rokid 会话访问 |

#### 开发验证

```bash
python -m py_compile hermes-plugin/rokid_openclaw_bridge/*.py
python -m unittest discover -s tests
npm install
npm run build
```
