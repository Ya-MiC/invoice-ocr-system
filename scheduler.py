#!/usr/bin/env python3
"""
Scheduler - Cron-based job scheduling for invoice OCR processing.
Supports interval-based and cron-expression scheduling.
"""

import asyncio
import logging
import threading
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Any
import signal
import sys

logger = logging.getLogger(__name__)


class ScheduledJob:
    """Represents a scheduled job with execution tracking."""
    
    def __init__(
        self,
        job_id: str,
        func: Callable,
        schedule_type: str = "interval",
        interval_seconds: int = 60,
        cron_expression: str = None,
        max_retries: int = 3,
        retry_delay: int = 5,
        enabled: bool = True
    ):
        self.job_id = job_id
        self.func = func
        self.schedule_type = schedule_type
        self.interval_seconds = interval_seconds
        self.cron_expression = cron_expression
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.enabled = enabled
        
        # Execution tracking
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.run_count: int = 0
        self.error_count: int = 0
        self.last_error: Optional[str] = None
        self.is_running: bool = False
        
        # Calculate next run
        self._calculate_next_run()
    
    def _calculate_next_run(self):
        """Calculate the next run time based on schedule type."""
        if self.schedule_type == "interval":
            if self.last_run:
                self.next_run = self.last_run + timedelta(seconds=self.interval_seconds)
            else:
                self.next_run = datetime.now() + timedelta(seconds=self.interval_seconds)
        elif self.schedule_type == "cron" and self.cron_expression:
            self.next_run = self._parse_cron(self.cron_expression)
    
    def _parse_cron(self, expression: str) -> datetime:
        """Parse cron expression and return next run time."""
        # Simple cron parser for common patterns
        # Format: minute hour day month weekday
        try:
            parts = expression.split()
            if len(parts) != 5:
                raise ValueError(f"Invalid cron expression: {expression}")
            
            minute, hour, day, month, weekday = parts
            
            now = datetime.now()
            next_time = now.replace(second=0, microsecond=0)
            
            # Handle minute
            if minute != "*":
                target_minute = int(minute)
                if next_time.minute >= target_minute:
                    next_time = next_time.replace(minute=target_minute) + timedelta(hours=1)
                else:
                    next_time = next_time.replace(minute=target_minute)
            else:
                next_time = next_time + timedelta(minutes=1)
            
            # Handle hour
            if hour != "*":
                target_hour = int(hour)
                if next_time.hour > target_hour or (next_time.hour == target_hour and next_time.minute > 0):
                    next_time = next_time.replace(hour=target_hour) + timedelta(days=1)
                else:
                    next_time = next_time.replace(hour=target_hour)
            
            return next_time
            
        except Exception as e:
            logger.error(f"Cron parsing error: {e}")
            return datetime.now() + timedelta(minutes=1)
    
    def should_run(self) -> bool:
        """Check if job should run now."""
        if not self.enabled or self.is_running:
            return False
        if self.next_run is None:
            return False
        return datetime.now() >= self.next_run
    
    def mark_started(self):
        """Mark job as started."""
        self.is_running = True
        self.last_run = datetime.now()
    
    def mark_completed(self, success: bool, error: str = None):
        """Mark job as completed."""
        self.is_running = False
        self.run_count += 1
        if not success:
            self.error_count += 1
            self.last_error = error
        self._calculate_next_run()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert job info to dictionary."""
        return {
            "job_id": self.job_id,
            "schedule_type": self.schedule_type,
            "interval_seconds": self.interval_seconds,
            "enabled": self.enabled,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "is_running": self.is_running
        }


class Scheduler:
    """
    Production-grade job scheduler.
    Supports interval and cron-based scheduling with retry logic.
    """
    
    def __init__(self, max_concurrent_jobs: int = 5):
        self.max_concurrent_jobs = max_concurrent_jobs
        self._jobs: Dict[str, ScheduledJob] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event = threading.Event()
        
    def add_job(
        self,
        job_id: str,
        func: Callable,
        schedule_type: str = "interval",
        interval_seconds: int = 60,
        cron_expression: str = None,
        max_retries: int = 3,
        retry_delay: int = 5,
        enabled: bool = True
    ) -> bool:
        """
        Add a new scheduled job.
        
        Args:
            job_id: Unique identifier for the job
            func: Function to execute (can be sync or async)
            schedule_type: 'interval' or 'cron'
            interval_seconds: Interval in seconds for interval scheduling
            cron_expression: Cron expression for cron scheduling
            max_retries: Maximum retry attempts on failure
            retry_delay: Delay between retries in seconds
            enabled: Whether job is enabled
            
        Returns:
            True if job was added successfully
        """
        if job_id in self._jobs:
            logger.warning(f"Job {job_id} already exists, replacing")
        
        job = ScheduledJob(
            job_id=job_id,
            func=func,
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            cron_expression=cron_expression,
            max_retries=max_retries,
            retry_delay=retry_delay,
            enabled=enabled
        )
        
        self._jobs[job_id] = job
        logger.info(f"Added job: {job_id}, next run: {job.next_run}")
        return True
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a scheduled job."""
        if job_id in self._jobs:
            del self._jobs[job_id]
            logger.info(f"Removed job: {job_id}")
            return True
        return False
    
    def enable_job(self, job_id: str) -> bool:
        """Enable a job."""
        if job_id in self._jobs:
            self._jobs[job_id].enabled = True
            self._jobs[job_id]._calculate_next_run()
            return True
        return False
    
    def disable_job(self, job_id: str) -> bool:
        """Disable a job."""
        if job_id in self._jobs:
            self._jobs[job_id].enabled = False
            return True
        return False
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job info by ID."""
        if job_id in self._jobs:
            return self._jobs[job_id].to_dict()
        return None
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """Get info for all jobs."""
        return [job.to_dict() for job in self._jobs.values()]
    
    async def _execute_job(self, job: ScheduledJob):
        """Execute a job with retry logic."""
        job.mark_started()
        
        for attempt in range(job.max_retries):
            try:
                # Handle both sync and async functions
                if asyncio.iscoroutinefunction(job.func):
                    await job.func()
                else:
                    await asyncio.to_thread(job.func)
                
                job.mark_completed(success=True)
                logger.info(f"Job {job.job_id} completed successfully")
                return
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Job {job.job_id} attempt {attempt + 1} failed: {error_msg}")
                
                if attempt < job.max_retries - 1:
                    await asyncio.sleep(job.retry_delay)
                else:
                    job.mark_completed(success=False, error=error_msg)
                    logger.error(f"Job {job.job_id} failed after {job.max_retries} attempts")
    
    async def _run_loop(self):
        """Main scheduler loop."""
        while not self._stop_event.is_set():
            try:
                # Check each job
                running_count = sum(1 for j in self._jobs.values() if j.is_running)
                
                for job in self._jobs.values():
                    if job.should_run() and running_count < self.max_concurrent_jobs:
                        asyncio.create_task(self._execute_job(job))
                        running_count += 1
                
                # Sleep before next check
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(5)
    
    def start(self):
        """Start the scheduler."""
        if self._running:
            logger.warning("Scheduler already running")
            return
        
        self._running = True
        self._stop_event.clear()
        
        def run_in_thread():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._run_loop())
        
        self._thread = threading.Thread(target=run_in_thread, daemon=True)
        self._thread.start()
        
        logger.info("Scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        if not self._running:
            return
        
        self._stop_event.set()
        self._running = False
        
        if self._loop and self._thread:
            # Give time for current jobs to complete
            self._thread.join(timeout=10)
        
        logger.info("Scheduler stopped")
    
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running
    
    def trigger_job(self, job_id: str) -> bool:
        """Manually trigger a job."""
        if job_id not in self._jobs:
            return False
        
        job = self._jobs[job_id]
        if job.is_running:
            logger.warning(f"Job {job_id} is already running")
            return False
        
        asyncio.create_task(self._execute_job(job))
        return True


class InvoiceScheduler(Scheduler):
    """
    Specialized scheduler for invoice OCR processing.
    Pre-configured with common invoice processing jobs.
    """
    
    def __init__(self, ocr_engine, file_processor, output_writer, config: dict = None):
        super().__init__(max_concurrent_jobs=config.get("max_concurrent_jobs", 3) if config else 3)
        self.ocr_engine = ocr_engine
        self.file_processor = file_processor
        self.output_writer = output_writer
        self.config = config or {}
        
    def setup_default_jobs(self):
        """Set up default invoice processing jobs."""
        
        # Batch processing job - runs every 5 minutes
        self.add_job(
            job_id="batch_process",
            func=self._batch_process,
            schedule_type="interval",
            interval_seconds=self.config.get("batch_interval", 300),
            max_retries=3
        )
        
        # Cleanup job - runs every hour
        self.add_job(
            job_id="cleanup",
            func=self._cleanup,
            schedule_type="interval",
            interval_seconds=3600,
            max_retries=1
        )
        
        # Stats logging - runs every 10 minutes
        self.add_job(
            job_id="stats_log",
            func=self._log_stats,
            schedule_type="interval",
            interval_seconds=600,
            max_retries=1
        )
        
        logger.info("Default jobs configured")
    
    def _batch_process(self):
        """Process pending invoice files."""
        if not self.file_processor or not self.ocr_engine:
            logger.warning("Components not initialized for batch processing")
            return
        
        pending_files = self.file_processor.get_pending_files()
        processed_count = 0
        
        for file_path in pending_files[:self.config.get("batch_size", 50)]:
            try:
                with open(file_path, "rb") as f:
                    content = f.read()
                
                result = self.ocr_engine.recognize(content, str(file_path))
                
                if result and result.get("text"):
                    from invoice_parser import InvoiceParser
                    parser = InvoiceParser()
                    invoice_data = parser.parse(result["text"])
                    invoice_data["source_file"] = str(file_path)
                    invoice_data["processed_at"] = datetime.now().isoformat()
                    
                    self.output_writer.write(invoice_data)
                    processed_count += 1
                
                self.file_processor.mark_processed(file_path)
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        logger.info(f"Batch processed {processed_count} files")
    
    def _cleanup(self):
        """Clean up old processed files and locks."""
        if not self.file_processor:
            return
        
        cleaned = self.file_processor.cleanup_old_files(
            max_age_hours=self.config.get("cleanup_age_hours", 24)
        )
        logger.info(f"Cleanup completed: {cleaned} files removed")
    
    def _log_stats(self):
        """Log processing statistics."""
        if self.output_writer:
            stats = self.output_writer.get_statistics()
            logger.info(f"Stats: {stats}")


# Signal handler for graceful shutdown
def setup_signal_handlers(scheduler: Scheduler):
    """Setup signal handlers for graceful shutdown."""
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        scheduler.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
