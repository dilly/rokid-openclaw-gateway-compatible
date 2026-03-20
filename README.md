# rokid-openclaw-gateway-compatible

#### 介绍
openclaw 接入Rokid glasses 插件,兼容历史版本openclaw，安装手册：README.md

#### 软件架构
软件架构说明


#### 安装教程
1.  cd /tmp
2.  git clone https://gitee.com/rokid-eco/rokid-openclaw-gateway-compatible.git
3.  执行npm install 下载依赖
4.  执行openclaw plugins install . --link 安装插件
5.  检查插件配置：
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