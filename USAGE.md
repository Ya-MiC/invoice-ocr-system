# 使用说明

## 快速开始

### 方式一：Docker 部署（推荐）

```bash
# 克隆仓库
git clone https://github.com/Ya-MiC/invoice-ocr-system.git
cd invoice-ocr-system

# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f
```

### 方式二：本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 创建必要目录
mkdir -p input output logs processed failed

# 启动服务
python main.py
```

## API 端点

### 健康检查
```bash
curl http://localhost:8080/health
```

返回：
```json
{
  "status": "healthy",
  "cpu_percent": 12.5,
  "memory_percent": 45.2,
  "disk_percent": 60.0,
  "timestamp": "2026-04-19T15:30:00",
  "uptime": "0:05:30"
}
```

### 任务状态
```bash
curl http://localhost:8080/status
```

返回：
```json
{
  "total_tasks": 10,
  "pending": 2,
  "processing": 1,
  "completed": 6,
  "failed": 1,
  "last_task": "task_20260419153000_invoice.jpg"
}
```

### 手动触发处理
```bash
curl -X POST "http://localhost:8080/process?file_path=./input/invoice.jpg"
```

## 配置说明

编辑 `config.yaml` 文件：

```yaml
# OCR 配置
ocr:
  engine: "rapidocr"        # OCR引擎
  model_dir: "./models"     # 模型目录
  languages: ["ch", "en"]   # 支持语言

# 定时扫描间隔
scheduler:
  scan_interval: 30         # 扫描间隔(秒)

# 重试配置
retry:
  max_attempts: 3           # 最大重试次数
  backoff_factor: 2         # 退避因子

# 输出格式
output:
  format: "csv"             # csv 或 sqlite

# 资源监控阈值
monitoring:
  cpu_threshold: 80         # CPU告警阈值(%)
  memory_threshold: 80      # 内存告警阈值(%)
```

## 工作流程

1. 将发票图片放入 `input/` 目录
2. 系统自动检测并处理
3. 提取字段：发票代码、号码、日期、金额、购买方、销售方
4. 输出到 `output/invoices.csv` 或 SQLite 数据库
5. 处理完成的文件移动到 `processed/` 目录
6. 失败文件移动到 `failed/` 目录并记录日志

## 日志位置

- 系统日志: `logs/ocr_system.log`
- 任务日志: `logs/tasks.log`
- 错误归档: `logs/errors/`

## 协议检查

```bash
python check_licenses.py
```

扫描所有依赖的许可证，确保符合宽松协议要求。
