# rokid-openclaw-gateway-compatible

#### 介绍
openclaw 接入 Rokid glasses 插件，兼容历史版本 openclaw。

#### 软件架构
软件架构说明


#### 安装教程

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