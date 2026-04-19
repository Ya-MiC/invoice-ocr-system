#!/usr/bin/env python3
"""
File Processor - Handles file watching, deduplication, and locking.
Production-grade file processing with hash-based deduplication.
"""

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import fcntl

logger = logging.getLogger(__name__)


class FileLock:
    """
    Cross-process file locking mechanism.
    Uses fcntl for POSIX systems and file-based locking for portability.
    """
    
    def __init__(self, lock_dir: Path, timeout: int = 300):
        self.lock_dir = lock_dir
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self._locks: Dict[str, Path] = {}
        self._lock_files: Dict[str, int] = {}
        
    def acquire(self, file_path: Path) -> bool:
        """Acquire a lock for the given file path."""
        lock_name = self._get_lock_name(file_path)
        lock_file_path = self.lock_dir / f"{lock_name}.lock"
        
        try:
            fd = os.open(str(lock_file_path), os.O_CREAT | os.O_RDWR)
            
            # Try to acquire lock with timeout
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._locks[str(file_path)] = lock_file_path
                    self._lock_files[str(file_path)] = fd
                    
                    # Write lock metadata
                    os.write(fd, json.dumps({
                        "file": str(file_path),
                        "locked_at": datetime.now().isoformat(),
                        "pid": os.getpid()
                    }).encode())
                    
                    return True
                except (IOError, OSError):
                    time.sleep(0.1)
            
            os.close(fd)
            return False
            
        except Exception as e:
            logger.error(f"Failed to acquire lock for {file_path}: {e}")
            return False
    
    def release(self, file_path: Path) -> bool:
        """Release the lock for the given file path."""
        file_str = str(file_path)
        
        if file_str not in self._lock_files:
            return True
        
        try:
            fd = self._lock_files[file_str]
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            
            lock_file_path = self._locks.get(file_str)
            if lock_file_path and lock_file_path.exists():
                lock_file_path.unlink()
            
            del self._lock_files[file_str]
            del self._locks[file_str]
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to release lock for {file_path}: {e}")
            return False
    
    def is_locked(self, file_path: Path) -> bool:
        """Check if a file is currently locked."""
        lock_name = self._get_lock_name(file_path)
        lock_file_path = self.lock_dir / f"{lock_name}.lock"
        
        if not lock_file_path.exists():
            return False
        
        # Check if lock is stale
        try:
            with open(lock_file_path, "r") as f:
                data = json.load(f)
                locked_at = datetime.fromisoformat(data.get("locked_at", ""))
                if datetime.now() - locked_at > timedelta(seconds=self.timeout):
                    self._force_release(lock_file_path)
                    return False
        except Exception:
            pass
        
        return True
    
    def _force_release(self, lock_file_path: Path):
        """Force release a stale lock."""
        try:
            lock_file_path.unlink()
        except Exception:
            pass
    
    def _get_lock_name(self, file_path: Path) -> str:
        """Generate a unique lock name for the file path."""
        return hashlib.md5(str(file_path).encode()).hexdigest()
    
    def cleanup_stale_locks(self):
        """Clean up all stale locks."""
        if not self.lock_dir.exists():
            return
        
        for lock_file in self.lock_dir.glob("*.lock"):
            try:
                with open(lock_file, "r") as f:
                    data = json.load(f)
                    locked_at = datetime.fromisoformat(data.get("locked_at", ""))
                    if datetime.now() - locked_at > timedelta(seconds=self.timeout):
                        lock_file.unlink()
                        logger.info(f"Cleaned up stale lock: {lock_file}")
            except Exception:
                pass


class FileDeduplicator:
    """
    Hash-based file deduplication.
    Prevents processing the same file multiple times.
    """
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        
    def _init_db(self):
        """Initialize the deduplication database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_hashes (
                file_hash TEXT PRIMARY KEY,
                file_path TEXT,
                first_seen TEXT,
                last_seen TEXT,
                process_count INTEGER DEFAULT 1
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_path ON file_hashes(file_path)
        """)
        
        conn.commit()
        conn.close()
        
    def compute_hash(self, file_path: Path) -> str:
        """Compute SHA-256 hash of file content."""
        sha256 = hashlib.sha256()
        
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        
        return sha256.hexdigest()
    
    def compute_hash_bytes(self, content: bytes) -> str:
        """Compute SHA-256 hash of byte content."""
        return hashlib.sha256(content).hexdigest()
    
    def is_duplicate(self, file_path: Path) -> bool:
        """Check if file has been processed before."""
        file_hash = self.compute_hash(file_path)
        return self.is_duplicate_hash(file_hash)
    
    def is_duplicate_hash(self, file_hash: str) -> bool:
        """Check if a hash has been seen before."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT 1 FROM file_hashes WHERE file_hash = ?",
            (file_hash,)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        return result is not None
    
    def record_file(self, file_path: Path) -> str:
        """Record a file in the deduplication database."""
        file_hash = self.compute_hash(file_path)
        self.record_hash(file_hash, str(file_path))
        return file_hash
    
    def record_hash(self, file_hash: str, file_path: str = None):
        """Record a hash in the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        cursor.execute(
            """INSERT INTO file_hashes (file_hash, file_path, first_seen, last_seen, process_count)
               VALUES (?, ?, ?, ?, 1)
               ON CONFLICT(file_hash) DO UPDATE SET 
                   last_seen = ?,
                   process_count = process_count + 1""",
            (file_hash, file_path, now, now, now)
        )
        
        conn.commit()
        conn.close()
    
    def get_stats(self) -> Dict:
        """Get deduplication statistics."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM file_hashes")
        total_hashes = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(process_count) FROM file_hashes")
        total_processed = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            "unique_files": total_hashes,
            "total_processed": total_processed,
            "duplicates_prevented": total_processed - total_hashes
        }


class FileProcessor:
    """
    Production-grade file processor.
    Handles file watching, locking, and deduplication.
    """
    
    SUPPORTED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".pdf", ".tiff", ".bmp", ".webp"}
    
    def __init__(
        self,
        watch_dir: Path,
        processed_dir: Path = None,
        lock_timeout: int = 300,
        max_file_size: int = 50 * 1024 * 1024  # 50MB
    ):
        self.watch_dir = Path(watch_dir)
        self.processed_dir = Path(processed_dir) if processed_dir else self.watch_dir / "processed"
        self.lock_timeout = lock_timeout
        self.max_file_size = max_file_size
        
        # Create directories
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.lock_dir = self.watch_dir / ".locks"
        self.lock_manager = FileLock(self.lock_dir, lock_timeout)
        
        self.db_path = self.watch_dir / ".dedup.db"
        self.deduplicator = FileDeduplicator(self.db_path)
        
        # Thread safety
        self._lock = threading.Lock()
        
        logger.info(f"FileProcessor initialized: watch={self.watch_dir}")
    
    def get_pending_files(self) -> List[Path]:
        """Get list of files waiting to be processed."""
        pending = []
        
        if not self.watch_dir.exists():
            return pending
        
        for file_path in self.watch_dir.iterdir():
            if not file_path.is_file():
                continue
            
            if file_path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
                continue
            
            # Skip hidden files
            if file_path.name.startswith("."):
                continue
            
            # Skip locked files
            if self.lock_manager.is_locked(file_path):
                continue
            
            # Check file size
            if file_path.stat().st_size > self.max_file_size:
                logger.warning(f"File too large, skipping: {file_path}")
                continue
            
            pending.append(file_path)
        
        # Sort by modification time (oldest first)
        pending.sort(key=lambda p: p.stat().st_mtime)
        
        return pending
    
    def acquire_for_processing(self, file_path: Path) -> bool:
        """Attempt to acquire a file for processing."""
        with self._lock:
            if self.lock_manager.is_locked(file_path):
                return False
            
            if not self.lock_manager.acquire(file_path):
                return False
            
            return True
    
    def release_from_processing(self, file_path: Path):
        """Release a file after processing."""
        self.lock_manager.release(file_path)
    
    def is_duplicate(self, file_path: Path) -> bool:
        """Check if file content has been processed before."""
        return self.deduplicator.is_duplicate(file_path)
    
    def mark_processed(self, file_path: Path, move_to_processed: bool = True):
        """
        Mark a file as processed.
        Records hash and optionally moves to processed directory.
        """
        # Record in deduplication database
        self.deduplicator.record_file(file_path)
        
        # Release lock
        self.lock_manager.release(file_path)
        
        # Move to processed directory
        if move_to_processed:
            try:
                dest_path = self.processed_dir / file_path.name
                
                # Handle name conflicts
                if dest_path.exists():
                    stem = dest_path.stem
                    suffix = dest_path.suffix
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    dest_path = self.processed_dir / f"{stem}_{timestamp}{suffix}"
                
                file_path.rename(dest_path)
                logger.debug(f"Moved to processed: {dest_path}")
                
            except Exception as e:
                logger.error(f"Failed to move processed file: {e}")
    
    def get_file_hash(self, file_path: Path) -> str:
        """Get the hash of a file."""
        return self.deduplicator.compute_hash(file_path)
    
    def cleanup_old_files(self, max_age_hours: int = 24) -> int:
        """Clean up old processed files and stale locks."""
        cleaned = 0
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        
        # Clean stale locks
        self.lock_manager.cleanup_stale_locks()
        
        # Clean old processed files
        if self.processed_dir.exists():
            for file_path in self.processed_dir.iterdir():
                if not file_path.is_file():
                    continue
                
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if mtime < cutoff:
                    try:
                        file_path.unlink()
                        cleaned += 1
                    except Exception as e:
                        logger.error(f"Failed to clean {file_path}: {e}")
        
        logger.info(f"Cleanup completed: {cleaned} files removed")
        return cleaned
    
    def get_statistics(self) -> Dict:
        """Get processing statistics."""
        pending = self.get_pending_files()
        dedup_stats = self.deduplicator.get_stats()
        
        processed_count = 0
        if self.processed_dir.exists():
            processed_count = len([
                f for f in self.processed_dir.iterdir()
                if f.is_file() and not f.name.startswith(".")
            ])
        
        return {
            "pending_files": len(pending),
            "processed_files": processed_count,
            "deduplication": dedup_stats,
            "watch_dir": str(self.watch_dir),
            "processed_dir": str(self.processed_dir)
        }
    
    def validate_file(self, file_path: Path) -> tuple:
        """Validate a file for processing."""
        errors = []
        
        if not file_path.exists():
            errors.append("File does not exist")
            return False, errors
        
        if not file_path.is_file():
            errors.append("Not a file")
            return False, errors
        
        if file_path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            errors.append(f"Unsupported format: {file_path.suffix}")
        
        if file_path.stat().st_size == 0:
            errors.append("File is empty")
        
        if file_path.stat().st_size > self.max_file_size:
            errors.append(f"File too large (max {self.max_file_size // (1024*1024)}MB)")
        
        return len(errors) == 0, errors
