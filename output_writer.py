#!/usr/bin/env python3
"""
Output Writer - Handles output to CSV and SQLite formats.
Production-grade output handling with buffering and error recovery.
"""

import csv
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class OutputWriter:
    """
    Unified output writer supporting multiple formats.
    Thread-safe with buffering for high-throughput scenarios.
    """
    
    def __init__(
        self,
        output_dir: Path,
        format: str = "csv",
        batch_size: int = 100,
        filename_prefix: str = "invoices"
    ):
        self.output_dir = Path(output_dir)
        self.format = format.lower()
        self.batch_size = batch_size
        self.filename_prefix = filename_prefix
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Buffer for batch writes
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        
        # Statistics
        self._write_count = 0
        self._error_count = 0
        self._start_time = datetime.now()
        
        # Initialize output files/database
        self._init_output()
        
        logger.info(f"OutputWriter initialized: format={format}, dir={self.output_dir}")
    
    def _init_output(self):
        """Initialize output destination based on format."""
        if self.format == "csv":
            self._init_csv()
        elif self.format == "sqlite":
            self._init_sqlite()
        elif self.format == "json":
            self._init_json()
        else:
            raise ValueError(f"Unsupported output format: {self.format}")
    
    def _init_csv(self):
        """Initialize CSV output file."""
        self.csv_path = self._get_output_path(".csv")
        
        # Define CSV columns
        self.csv_columns = [
            "invoice_type", "invoice_code", "invoice_number", "issue_date",
            "seller_name", "seller_tax_id", "buyer_name", "buyer_tax_id",
            "subtotal", "tax_total", "total", "currency",
            "payee", "reviewer", "drawer", "remarks",
            "source_file", "processed_at", "confidence"
        ]
        
        # Write header if file doesn't exist or is empty
        if not self.csv_path.exists() or self.csv_path.stat().st_size == 0:
            with open(self.csv_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_columns, extrasaction="ignore")
                writer.writeheader()
    
    def _init_sqlite(self):
        """Initialize SQLite database."""
        self.db_path = self._get_output_path(".db")
        
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_type TEXT,
                invoice_code TEXT,
                invoice_number TEXT UNIQUE,
                issue_date TEXT,
                seller_name TEXT,
                seller_tax_id TEXT,
                buyer_name TEXT,
                buyer_tax_id TEXT,
                subtotal REAL,
                tax_total REAL,
                total REAL,
                currency TEXT,
                payee TEXT,
                reviewer TEXT,
                drawer TEXT,
                remarks TEXT,
                source_file TEXT,
                processed_at TEXT,
                confidence REAL,
                raw_data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for common queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_invoice_number ON invoices(invoice_number)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_seller_tax_id ON invoices(seller_tax_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_buyer_tax_id ON invoices(buyer_tax_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_issue_date ON invoices(issue_date)")
        
        conn.commit()
        conn.close()
    
    def _init_json(self):
        """Initialize JSON output file."""
        self.json_path = self._get_output_path(".json")
        
        # Create empty array if file doesn't exist
        if not self.json_path.exists():
            with open(self.json_path, "w", encoding="utf-8") as f:
                f.write("[]")
    
    def _get_output_path(self, suffix: str) -> Path:
        """Generate output file path with date suffix."""
        date_str = datetime.now().strftime("%Y%m%d")
        return self.output_dir / f"{self.filename_prefix}_{date_str}{suffix}"
    
    def write(self, data: Dict[str, Any]) -> bool:
        """
        Write a single record to output.
        
        Args:
            data: Invoice data dictionary
            
        Returns:
            True if write was successful
        """
        with self._lock:
            try:
                # Add to buffer
                self._buffer.append(data)
                
                # Flush if buffer is full
                if len(self._buffer) >= self.batch_size:
                    return self._flush()
                
                return True
                
            except Exception as e:
                logger.error(f"Write error: {e}")
                self._error_count += 1
                return False
    
    def write_batch(self, records: List[Dict[str, Any]]) -> bool:
        """Write multiple records at once."""
        with self._lock:
            try:
                self._buffer.extend(records)
                
                if len(self._buffer) >= self.batch_size:
                    return self._flush()
                
                return True
                
            except Exception as e:
                logger.error(f"Batch write error: {e}")
                self._error_count += 1
                return False
    
    def flush(self) -> bool:
        """Public method to flush the buffer."""
        with self._lock:
            return self._flush()
    
    def _flush(self) -> bool:
        """Internal flush implementation."""
        if not self._buffer:
            return True
        
        try:
            if self.format == "csv":
                self._flush_csv()
            elif self.format == "sqlite":
                self._flush_sqlite()
            elif self.format == "json":
                self._flush_json()
            
            count = len(self._buffer)
            self._write_count += count
            self._buffer.clear()
            
            logger.debug(f"Flushed {count} records to {self.format}")
            return True
            
        except Exception as e:
            logger.error(f"Flush error: {e}")
            self._error_count += 1
            return False
    
    def _flush_csv(self):
        """Flush buffer to CSV file."""
        with open(self.csv_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=self.csv_columns, extrasaction="ignore")
            
            for record in self._buffer:
                # Flatten nested structures
                flat_record = self._flatten_record(record)
                writer.writerow(flat_record)
    
    def _flush_sqlite(self):
        """Flush buffer to SQLite database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        for record in self._buffer:
            flat_record = self._flatten_record(record)
            
            # Store raw JSON as well
            raw_json = json.dumps(record, ensure_ascii=False, default=str)
            flat_record["raw_data"] = raw_json
            
            columns = ", ".join(flat_record.keys())
            placeholders = ", ".join(["?" for _ in flat_record])
            
            # Use INSERT OR IGNORE to handle duplicates
            sql = f"""
                INSERT OR IGNORE INTO invoices ({columns})
                VALUES ({placeholders})
            """
            
            cursor.execute(sql, list(flat_record.values()))
        
        conn.commit()
        conn.close()
    
    def _flush_json(self):
        """Flush buffer to JSON file."""
        # Read existing data
        with open(self.json_path, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
        
        # Append new records
        existing.extend(self._buffer)
        
        # Write back
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2, default=str)
    
    def _flatten_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested record for CSV/SQL output."""
        flat = {}
        
        for key, value in record.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    flat[f"{key}_{sub_key}"] = str(sub_value) if sub_value is not None else ""
            elif isinstance(value, list):
                # Store list as JSON string
                flat[key] = json.dumps(value, ensure_ascii=False)
            else:
                flat[key] = str(value) if value is not None else ""
        
        # Ensure all required columns exist
        for col in self.csv_columns:
            if col not in flat:
                flat[col] = ""
        
        return flat
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get output statistics."""
        with self._lock:
            stats = {
                "format": self.format,
                "output_dir": str(self.output_dir),
                "write_count": self._write_count,
                "error_count": self._error_count,
                "buffer_size": len(self._buffer),
                "uptime_seconds": (datetime.now() - self._start_time).total_seconds()
            }
            
            # Add format-specific stats
            if self.format == "csv" and hasattr(self, "csv_path"):
                if self.csv_path.exists():
                    stats["file_size"] = self.csv_path.stat().st_size
                    # Count lines (approximate)
                    with open(self.csv_path, "r") as f:
                        stats["record_count"] = sum(1 for _ in f) - 1  # Subtract header
            
            elif self.format == "sqlite" and hasattr(self, "db_path"):
                if self.db_path.exists():
                    conn = sqlite3.connect(str(self.db_path))
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM invoices")
                    stats["record_count"] = cursor.fetchone()[0]
                    conn.close()
                    stats["file_size"] = self.db_path.stat().st_size
            
            elif self.format == "json" and hasattr(self, "json_path"):
                if self.json_path.exists():
                    stats["file_size"] = self.json_path.stat().st_size
                    with open(self.json_path, "r") as f:
                        data = json.load(f)
                        stats["record_count"] = len(data)
            
            return stats
    
    def query(self, filters: Dict[str, Any] = None, limit: int = 100) -> List[Dict]:
        """
        Query records from output (SQLite only).
        
        Args:
            filters: Dictionary of field: value pairs to filter
            limit: Maximum records to return
            
        Returns:
            List of matching records
        """
        if self.format != "sqlite":
            logger.warning("Query only supported for SQLite format")
            return []
        
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        sql = "SELECT * FROM invoices WHERE 1=1"
        params = []
        
        if filters:
            for key, value in filters.items():
                sql += f" AND {key} = ?"
                params.append(value)
        
        sql += f" ORDER BY created_at DESC LIMIT {limit}"
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def close(self):
        """Flush buffer and close output."""
        self._flush()
        logger.info(f"OutputWriter closed: {self._write_count} records written")
    
    @contextmanager
    def transaction(self):
        """Context manager for batch transaction (SQLite only)."""
        if self.format != "sqlite":
            yield
            return
        
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise
        finally:
            conn.close()
