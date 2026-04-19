#!/usr/bin/env python3
"""发票OCR系统主程序 - FastAPI + APScheduler"""

import os
import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import psutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./logs/ocr_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 加载配置
def load_config() -> Dict[str, Any]:
    with open('./config.yaml', 'r') as f:
        return yaml.safe_load(f)

config = load_config()

# FastAPI 应用
app = FastAPI(
    title="发票OCR处理系统",
    description="自动化发票/账本OCR处理系统",
    version="1.0.0"
)

# 任务状态存储
task_queue: Dict[str, Dict] = {}
processed_files: set = set()
file_locks: Dict[str, asyncio.Lock] = {}

class HealthStatus(BaseModel):
    status: str
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    timestamp: str
    uptime: str

class TaskStatus(BaseModel):
    total_tasks: int
    pending: int
    processing: int
    completed: int
    failed: int
    last_task: Optional[str]

start_time = datetime.now()

@app.get("/health", response_model=HealthStatus)
async def health_check():
    """健康检查端点"""
    cpu = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory().percent
    disk = psutil.disk_usage('/').percent
    uptime = str(datetime.now() - start_time)
    
    # 资源阈值检查
    thresholds = config['monitoring']
    warnings = []
    
    if cpu > thresholds['cpu_threshold']:
        warnings.append(f"CPU使用率 {cpu}% 超过阈值 {thresholds['cpu_threshold']}%")
        logger.warning(f"CPU告警: {cpu}%")
    
    if memory > thresholds['memory_threshold']:
        warnings.append(f"内存使用率 {memory}% 超过阈值 {thresholds['memory_threshold']}%")
        logger.warning(f"内存告警: {memory}%")
    
    return HealthStatus(
        status="healthy" if not warnings else "warning",
        cpu_percent=cpu,
        memory_percent=memory,
        disk_percent=disk,
        timestamp=datetime.now().isoformat(),
        uptime=uptime
    )

@app.get("/status", response_model=TaskStatus)
async def get_status():
    """任务队列状态"""
    pending = sum(1 for t in task_queue.values() if t['status'] == 'pending')
    processing = sum(1 for t in task_queue.values() if t['status'] == 'processing')
    completed = sum(1 for t in task_queue.values() if t['status'] == 'completed')
    failed = sum(1 for t in task_queue.values() if t['status'] == 'failed')
    
    last_task = max(task_queue.keys()) if task_queue else None
    
    return TaskStatus(
        total_tasks=len(task_queue),
        pending=pending,
        processing=processing,
        completed=completed,
        failed=failed,
        last_task=last_task
    )

@app.post("/process")
async def process_file(file_path: str):
    """手动触发OCR处理"""
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="文件不存在")
    
    task_id = f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}_{os.path.basename(file_path)}"
    task_queue[task_id] = {
        'file_path': file_path,
        'status': 'pending',
        'created_at': datetime.now().isoformat(),
        'attempts': 0
    }
    
    return {"task_id": task_id, "status": "queued"}

async def scan_input_directory():
    """扫描输入目录"""
    input_dir = config['scheduler']['input_dir']
    if not os.path.exists(input_dir):
        os.makedirs(input_dir, exist_ok=True)
        logger.info(f"创建输入目录: {input_dir}")
        return
    
    supported_extensions = {'.jpg', '.jpeg', '.png', '.pdf', '.bmp'}
    
    for filename in os.listdir(input_dir):
        file_path = os.path.join(input_dir, filename)
        
        if not os.path.isfile(file_path):
            continue
        
        ext = os.path.splitext(filename)[1].lower()
        if ext not in supported_extensions:
            continue
        
        # 去重检查
        file_hash = await compute_file_hash(file_path)
        if file_hash in processed_files:
            logger.debug(f"跳过已处理文件: {filename}")
            continue
        
        # 创建任务
        task_id = f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
        task_queue[task_id] = {
            'file_path': file_path,
            'file_hash': file_hash,
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
            'attempts': 0
        }
        logger.info(f"发现新文件: {filename}, 任务ID: {task_id}")

async def compute_file_hash(file_path: str) -> str:
    """计算文件哈希（用于去重）"""
    import hashlib
    hasher = hashlib.sha256()
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)
    
    return hasher.hexdigest()

async def process_task(task_id: str):
    """处理单个任务"""
    if task_id not in task_queue:
        return
    
    task = task_queue[task_id]
    task['status'] = 'processing'
    task['started_at'] = datetime.now().isoformat()
    
    try:
        # 获取文件锁
        if task['file_path'] not in file_locks:
            file_locks[task['file_path']] = asyncio.Lock()
        
        async with file_locks[task['file_path']]:
            # 执行OCR
            result = await run_ocr(task['file_path'])
            
            # 提取发票字段
            invoice_data = extract_invoice_fields(result)
            
            # 输出结果
            await save_result(invoice_data)
            
            # 移动到已处理目录
            move_to_processed(task['file_path'])
            
            # 标记完成
            task['status'] = 'completed'
            task['completed_at'] = datetime.now().isoformat()
            processed_files.add(task.get('file_hash', ''))
            
            logger.info(f"任务完成: {task_id}")
    
    except Exception as e:
        task['status'] = 'failed'
        task['error'] = str(e)
        task['failed_at'] = datetime.now().isoformat()
        logger.error(f"任务失败: {task_id}, 错误: {e}")
        
        # 重试逻辑
        if task['attempts'] < config['retry']['max_attempts']:
            task['attempts'] += 1
            delay = config['retry']['initial_delay'] * (config['retry']['backoff_factor'] ** task['attempts'])
            await asyncio.sleep(delay)
            task['status'] = 'pending'
            logger.info(f"重试任务: {task_id}, 第 {task['attempts']} 次")

async def run_ocr(file_path: str) -> Dict:
    """执行OCR识别"""
    try:
        from rapidocr_onnxruntime import RapidOCR
        
        ocr = RapidOCR()
        result, _ = ocr(file_path)
        
        if result:
            texts = [item[1] for item in result]
            return {'text': '\n'.join(texts), 'raw_result': result}
        return {'text': '', 'raw_result': None}
    
    except ImportError:
        logger.warning("RapidOCR未安装，使用模拟模式")
        return {'text': '模拟OCR结果', 'raw_result': None}

def extract_invoice_fields(ocr_result: Dict) -> Dict:
    """提取发票字段"""
    import re
    
    text = ocr_result.get('text', '')
    
    # 发票字段正则
    patterns = {
        'invoice_code': r'发票代码[：:]\s*(\d+)',
        'invoice_number': r'发票号码[：:]\s*(\d+)',
        'date': r'开票日期[：:]\s*(\d{4}年\d{1,2}月\d{1,2}日|\d{4}-\d{2}-\d{2})',
        'amount': r'[合总]计[金额]*[：:]\s*[￥¥]?\s*([\d,]+\.?\d*)',
        'buyer': r'购买方[名称]*[：:]\s*(.+?)(?=\n|$)',
        'seller': r'销售方[名称]*[：:]\s*(.+?)(?=\n|$)',
    }
    
    invoice_data = {
        'raw_text': text,
        'extracted_at': datetime.now().isoformat()
    }
    
    for field, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            invoice_data[field] = match.group(1).strip()
    
    return invoice_data

async def save_result(invoice_data: Dict):
    """保存结果到CSV/SQLite"""
    output_format = config['output']['format']
    
    if output_format == 'csv':
        import csv
        
        csv_path = config['output']['csv_path']
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        file_exists = os.path.exists(csv_path)
        
        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=invoice_data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(invoice_data)
    
    elif output_format == 'sqlite':
        import sqlite3
        
        db_path = config['output']['sqlite_path']
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_code TEXT,
                invoice_number TEXT,
                date TEXT,
                amount TEXT,
                buyer TEXT,
                seller TEXT,
                raw_text TEXT,
                extracted_at TEXT
            )
        ''')
        
        cursor.execute('''
            INSERT INTO invoices (invoice_code, invoice_number, date, amount, buyer, seller, raw_text, extracted_at)
            VALUES (:invoice_code, :invoice_number, :date, :amount, :buyer, :seller, :raw_text, :extracted_at)
        ''', invoice_data)
        
        conn.commit()
        conn.close()

def move_to_processed(file_path: str):
    """移动文件到已处理目录"""
    processed_dir = config['scheduler']['processed_dir']
    os.makedirs(processed_dir, exist_ok=True)
    
    filename = os.path.basename(file_path)
    new_path = os.path.join(processed_dir, filename)
    
    os.rename(file_path, new_path)
    logger.info(f"移动文件: {file_path} -> {new_path}")

# 调度器
scheduler = AsyncIOScheduler()

async def scheduled_scan():
    """定时扫描任务"""
    logger.info("执行定时扫描...")
    await scan_input_directory()
    
    # 处理待处理任务
    for task_id, task in list(task_queue.items()):
        if task['status'] == 'pending':
            await process_task(task_id)

def setup_scheduler():
    """设置定时任务"""
    interval = config['scheduler']['scan_interval']
    
    scheduler.add_job(
        scheduled_scan,
        IntervalTrigger(seconds=interval),
        id='ocr_scan',
        name='OCR文件扫描',
        replace_existing=True
    )
    
    logger.info(f"定时任务已设置，间隔: {interval}秒")

@app.on_event("startup")
async def startup_event():
    """启动事件"""
    os.makedirs('./logs', exist_ok=True)
    os.makedirs('./input', exist_ok=True)
    os.makedirs('./output', exist_ok=True)
    
    setup_scheduler()
    scheduler.start()
    logger.info("系统启动完成")

@app.on_event("shutdown")
async def shutdown_event():
    """关闭事件"""
    scheduler.shutdown()
    logger.info("系统已关闭")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=config['server']['host'],
        port=config['server']['port']
    )
