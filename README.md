# 发票/账本自动化OCR处理系统

基于 RapidOCR + Rust 高性能文件监听的发票OCR自动化处理系统。

## 架构确认

### 选型理由
| 组件 | 选型 | 协议 | 商用限制 |
|------|------|------|----------|
| OCR引擎 | RapidOCR | Apache-2.0 | 无限制 |
| 文件监听 | Rust (notify) | Apache-2.0 | 无限制 |
| 调度器 | APScheduler | MIT | 无限制 |
| Web框架 | FastAPI | MIT | 无限制 |

### Python vs Rust 职责划分
- **Python**: 业务编排、调度配置、数据库写入、HTTP服务
- **Rust**: 高性能文件监听、图像预处理、哈希去重

## 快速开始

```bash
docker-compose up -d
curl http://localhost:8080/health
```

## 目录结构

```
invoice-ocr-system/
├── main.py              # FastAPI + APScheduler
├── config.yaml          # 配置文件
├── src/lib.rs           # Rust PyO3绑定
├── output/              # CSV/SQLite输出
├── logs/                # 日志目录
└── licenses/            # 开源协议
```

## 致谢

- RapidOCR (Apache-2.0)
- APScheduler (MIT)
- FastAPI (MIT)
- notify (Apache-2.0)

MIT License
