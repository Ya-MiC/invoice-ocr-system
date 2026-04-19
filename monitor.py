#!/usr/bin/env python3
"""
Resource Monitor - System resource monitoring and alerting.
Production-grade monitoring with configurable thresholds.
"""

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any

logger = logging.getLogger(__name__)


@dataclass
class ResourceStats:
    """System resource statistics."""
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_total_mb: float
    disk_percent: float
    disk_used_gb: float
    disk_total_gb: float
    process_count: int
    load_average: List[float]
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "memory_used_mb": self.memory_used_mb,
            "memory_total_mb": self.memory_total_mb,
            "disk_percent": self.disk_percent,
            "disk_used_gb": self.disk_used_gb,
            "disk_total_gb": self.disk_total_gb,
            "process_count": self.process_count,
            "load_average": self.load_average,
            "timestamp": self.timestamp
        }


class ResourceMonitor:
    """
    System resource monitor with configurable thresholds.
    Runs in background thread and provides alerts on threshold violations.
    """
    
    def __init__(
        self,
        enabled: bool = True,
        check_interval: int = 60,
        max_cpu_percent: float = 80.0,
        max_memory_percent: float = 80.0,
        max_disk_percent: float = 90.0,
        alert_callback: Callable[[str, Dict], None] = None
    ):
        self.enabled = enabled
        self.check_interval = check_interval
        self.max_cpu_percent = max_cpu_percent
        self.max_memory_percent = max_memory_percent
        self.max_disk_percent = max_disk_percent
        self.alert_callback = alert_callback
        
        # State
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._current_stats: Optional[ResourceStats] = None
        self._alert_history: List[Dict[str, Any]] = []
        
        # History for trending
        self._stats_history: List[ResourceStats] = []
        self._max_history_size = 1000
        
        # Initialize psutil
        self._psutil = None
        try:
            import psutil
            self._psutil = psutil
        except ImportError:
            logger.warning("psutil not installed - resource monitoring will be limited")
    
    def start(self):
        """Start the monitoring thread."""
        if not self.enabled or self._running:
            return
        
        self._running = True
        self._stop_event.clear()
        
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        
        logger.info(f"Resource monitor started: interval={self.check_interval}s")
    
    def stop(self):
        """Stop the monitoring thread."""
        if not self._running:
            return
        
        self._stop_event.set()
        self._running = False
        
        if self._thread:
            self._thread.join(timeout=5)
        
        logger.info("Resource monitor stopped")
    
    def is_running(self) -> bool:
        """Check if monitor is running."""
        return self._running
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        while not self._stop_event.is_set():
            try:
                # Collect stats
                stats = self._collect_stats()
                self._current_stats = stats
                
                # Store in history
                self._stats_history.append(stats)
                if len(self._stats_history) > self._max_history_size:
                    self._stats_history = self._stats_history[-self._max_history_size:]
                
                # Check thresholds
                self._check_thresholds(stats)
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
            
            # Wait for next check
            self._stop_event.wait(self.check_interval)
    
    def _collect_stats(self) -> ResourceStats:
        """Collect current system statistics."""
        timestamp = datetime.now().isoformat()
        
        if self._psutil:
            return self._collect_with_psutil(timestamp)
        else:
            return self._collect_basic(timestamp)
    
    def _collect_with_psutil(self, timestamp: str) -> ResourceStats:
        """Collect detailed stats using psutil."""
        psutil = self._psutil
        
        # CPU
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Memory
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_used_mb = memory.used / (1024 * 1024)
        memory_total_mb = memory.total / (1024 * 1024)
        
        # Disk
        disk = psutil.disk_usage("/")
        disk_percent = disk.percent
        disk_used_gb = disk.used / (1024 * 1024 * 1024)
        disk_total_gb = disk.total / (1024 * 1024 * 1024)
        
        # Process count
        process_count = len(psutil.pids())
        
        # Load average (Unix only)
        try:
            load_average = list(os.getloadavg()) if hasattr(os, "getloadavg") else [0.0, 0.0, 0.0]
        except:
            load_average = [0.0, 0.0, 0.0]
        
        return ResourceStats(
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_used_mb=memory_used_mb,
            memory_total_mb=memory_total_mb,
            disk_percent=disk_percent,
            disk_used_gb=disk_used_gb,
            disk_total_gb=disk_total_gb,
            process_count=process_count,
            load_average=load_average,
            timestamp=timestamp
        )
    
    def _collect_basic(self, timestamp: str) -> ResourceStats:
        """Collect basic stats without psutil."""
        import os
        
        # Basic info only
        cpu_percent = 0.0
        memory_percent = 0.0
        memory_used_mb = 0.0
        memory_total_mb = 0.0
        disk_percent = 0.0
        disk_used_gb = 0.0
        disk_total_gb = 0.0
        process_count = 1
        
        # Try to get load average
        load_average = [0.0, 0.0, 0.0]
        try:
            if hasattr(os, "getloadavg"):
                load_average = list(os.getloadavg())
        except:
            pass
        
        # Try to read from /proc (Linux only)
        try:
            with open("/proc/meminfo", "r") as f:
                meminfo = f.read()
                for line in meminfo.split("\n"):
                    if line.startswith("MemTotal:"):
                        memory_total_mb = int(line.split()[1]) / 1024
                    elif line.startswith("MemAvailable:"):
                        available = int(line.split()[1]) / 1024
                        memory_used_mb = memory_total_mb - available
                        memory_percent = (memory_used_mb / memory_total_mb) * 100 if memory_total_mb else 0
        except:
            pass
        
        return ResourceStats(
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_used_mb=memory_used_mb,
            memory_total_mb=memory_total_mb,
            disk_percent=disk_percent,
            disk_used_gb=disk_used_gb,
            disk_total_gb=disk_total_gb,
            process_count=process_count,
            load_average=load_average,
            timestamp=timestamp
        )
    
    def _check_thresholds(self, stats: ResourceStats):
        """Check if any thresholds are exceeded."""
        alerts = []
        
        if stats.cpu_percent > self.max_cpu_percent:
            alerts.append({
                "type": "cpu",
                "level": "warning" if stats.cpu_percent < 95 else "critical",
                "value": stats.cpu_percent,
                "threshold": self.max_cpu_percent,
                "message": f"CPU usage {stats.cpu_percent:.1f}% exceeds threshold {self.max_cpu_percent}%"
            })
        
        if stats.memory_percent > self.max_memory_percent:
            alerts.append({
                "type": "memory",
                "level": "warning" if stats.memory_percent < 95 else "critical",
                "value": stats.memory_percent,
                "threshold": self.max_memory_percent,
                "message": f"Memory usage {stats.memory_percent:.1f}% exceeds threshold {self.max_memory_percent}%"
            })
        
        if stats.disk_percent > self.max_disk_percent:
            alerts.append({
                "type": "disk",
                "level": "warning" if stats.disk_percent < 98 else "critical",
                "value": stats.disk_percent,
                "threshold": self.max_disk_percent,
                "message": f"Disk usage {stats.disk_percent:.1f}% exceeds threshold {self.max_disk_percent}%"
            })
        
        # Process alerts
        for alert in alerts:
            alert["timestamp"] = stats.timestamp
            self._alert_history.append(alert)
            logger.warning(alert["message"])
            
            if self.alert_callback:
                try:
                    self.alert_callback(alert["type"], alert)
                except Exception as e:
                    logger.error(f"Alert callback error: {e}")
        
        # Clean old alerts (keep last 100)
        if len(self._alert_history) > 100:
            self._alert_history = self._alert_history[-100:]
    
    def get_current_stats(self) -> Optional[Dict[str, Any]]:
        """Get current resource statistics."""
        if self._current_stats:
            return self._current_stats.to_dict()
        
        # Collect on-demand if not running
        stats = self._collect_stats()
        return stats.to_dict()
    
    def get_stats_history(self, count: int = 100) -> List[Dict[str, Any]]:
        """Get historical statistics."""
        return [s.to_dict() for s in self._stats_history[-count:]]
    
    def get_alert_history(self, count: int = 50) -> List[Dict[str, Any]]:
        """Get alert history."""
        return self._alert_history[-count:]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get monitoring summary."""
        current = self.get_current_stats() or {}
        
        # Calculate averages from history
        if self._stats_history:
            avg_cpu = sum(s.cpu_percent for s in self._stats_history) / len(self._stats_history)
            avg_memory = sum(s.memory_percent for s in self._stats_history) / len(self._stats_history)
        else:
            avg_cpu = current.get("cpu_percent", 0)
            avg_memory = current.get("memory_percent", 0)
        
        return {
            "current": current,
            "averages": {
                "cpu_percent": round(avg_cpu, 2),
                "memory_percent": round(avg_memory, 2)
            },
            "thresholds": {
                "cpu": self.max_cpu_percent,
                "memory": self.max_memory_percent,
                "disk": self.max_disk_percent
            },
            "alert_count": len(self._alert_history),
            "history_size": len(self._stats_history),
            "running": self._running,
            "enabled": self.enabled
        }
    
    def set_threshold(self, resource: str, value: float):
        """Update a threshold value."""
        if resource == "cpu":
            self.max_cpu_percent = value
        elif resource == "memory":
            self.max_memory_percent = value
        elif resource == "disk":
            self.max_disk_percent = value
        else:
            raise ValueError(f"Unknown resource: {resource}")
        
        logger.info(f"Threshold updated: {resource}={value}")


# Singleton monitor instance
_monitor_instance: Optional[ResourceMonitor] = None


def get_monitor(config: Dict = None) -> ResourceMonitor:
    """Get or create the global monitor instance."""
    global _monitor_instance
    
    if _monitor_instance is None:
        config = config or {}
        _monitor_instance = ResourceMonitor(
            enabled=config.get("enabled", True),
            check_interval=config.get("check_interval", 60),
            max_cpu_percent=config.get("max_cpu_percent", 80),
            max_memory_percent=config.get("max_memory_percent", 80),
            max_disk_percent=config.get("max_disk_percent", 90)
        )
    
    return _monitor_instance
