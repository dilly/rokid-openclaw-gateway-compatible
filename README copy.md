### 说明
##### 安装插件执行步骤
1、cd /tmp
2、下载 https://gitee.com/rokid-eco/rokid-openclaw-bridge.git
3、执行npm install 下载依赖
4、执行openclaw plugins install . --link 安装插件
5、配置插件:
"plugins": {
    "load": {
      "paths": [
        "/tmp/rokid-openclaw-bridge"
      ]
    },
    "entries": {
      "rokid-openclaw-bridge": {
        "enabled": true,
        "config": {
          "linkCode": "",
          "linkSecret": ""
        }
      }
    },
    "installs": {
      "rokid-openclaw-bridge": {
        "source": "path",
        "sourcePath": "/tmp/rokid-openclaw-bridge",
        "installPath": "/tmp/rokid-openclaw-bridge",
        "version": "1.0.0",
        "installedAt": "2026-03-16T06:34:16.825Z"
      }
    }
  }
linkCode、linkSecret为用户输入内容