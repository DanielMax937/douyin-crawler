# Douyin Crawler

## 目标

抖音视频爬取与处理管道系统。通过 Node.js 爬虫采集抖音热门视频元数据存入 PostgreSQL，由 Celery Worker 对每条视频在同一任务内完成 **下载 → WebGemini 概括 → 删除本地视频文件**，最终只在库中保留文本摘要。

## 使用场景

- **每日爬取**：定时从抖音抓取高评论量视频，筛选优质内容入库
- **视频下载**：调用 Douyin_TikTok_Download_API 将视频下载到本地
- **AI 摘要**：通过 Webgemini 将视频上传至 Gemini，获取「这段视频讲了什么」的文本概括
- **任务管理**：通过 CLI（status、trigger、process、reset）查看进度、触发处理、重置卡住任务

## 技术栈

- **爬虫**：Node.js + Patchright，连接 Chrome CDP 9222
- **Worker**：Python Celery（worker + beat），Redis 作为 broker
- **存储**：PostgreSQL（douyin 数据库）
- **依赖服务**：douyin-download（8000，local-service）、Webgemini（8200）

## 前置条件

- PostgreSQL、Redis 已启动
- Chrome 以 `--remote-debugging-port=9222` 启动（爬虫需要）
- Douyin_TikTok_Download_API 运行于 8000
- Webgemini 运行于 8200
- 环境变量：`PGHOST`、`PGPORT`、`PGDATABASE`、`PGUSER`、`PGPASSWORD`、`REDIS_URL` 等（见 `worker/.env.example`）

## 启动与停止

```bash
# 启动 Celery Worker + Beat
./start-bg.sh

# 停止
./stop-bg.sh
```

## 参考

- 项目 README：`README.md`
- Worker 说明：`worker/README.md`
- CLI 用法：`cd worker && uv run python cli.py --help`
