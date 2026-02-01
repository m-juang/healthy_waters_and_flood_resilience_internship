"""
Database module for tracking retrieved data.

Provides SQLite-based storage to track which dates have been retrieved
for both rain gauge and radar data. Prevents duplicate API calls and
allows users to check data availability.

Usage:
    from moata_pipeline.common.database import RetrievalDatabase
    
    db = RetrievalDatabase()
    
    # Check if data exists
    if db.data_exists("gauge", "20250509-20250510"):
        print("Data already exists!")
    
    # Record a retrieval
    db.record_retrieval("gauge", "20250509-20250510", gauge_count=264)

Author: Auckland Council Internship Team (COMPSCI 778)
Created: 2026-02-01
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager


class RetrievalDatabase:
    """
    SQLite database for tracking data retrievals.
    
    Tracks which date ranges have been retrieved for gauge and radar data,
    along with metadata about each retrieval.
    """
    
    # Default database location
    DEFAULT_DB_PATH = Path("data/retrieval_history.db")
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the database.
        
        Args:
            db_path: Path to SQLite database file. Defaults to data/retrieval_history.db
        """
        self.db_path = db_path or self.DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_database(self) -> None:
        """Initialize database schema if not exists."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Main retrieval history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS retrievals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_type TEXT NOT NULL,          -- 'gauge' or 'radar'
                    date_range TEXT NOT NULL,         -- 'YYYYMMDD-YYYYMMDD'
                    start_date TEXT NOT NULL,         -- 'YYYY-MM-DD'
                    end_date TEXT NOT NULL,           -- 'YYYY-MM-DD'
                    retrieved_at TEXT NOT NULL,       -- ISO timestamp
                    item_count INTEGER,               -- Number of gauges/catchments
                    file_count INTEGER,               -- Number of files created
                    total_size_mb REAL,               -- Total size in MB
                    status TEXT DEFAULT 'completed',  -- 'completed', 'partial', 'failed'
                    notes TEXT,                       -- Optional notes
                    UNIQUE(data_type, date_range)
                )
            """)
            
            # Index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_retrievals_type_date 
                ON retrievals(data_type, date_range)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_retrievals_start_date 
                ON retrievals(start_date)
            """)
    
    def data_exists(self, data_type: str, date_range: str) -> bool:
        """
        Check if data for a specific date range already exists.
        
        Args:
            data_type: 'gauge' or 'radar'
            date_range: Date range string like '20250509-20250510'
            
        Returns:
            True if data exists, False otherwise
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM retrievals 
                WHERE data_type = ? AND date_range = ? AND status = 'completed'
            """, (data_type, date_range))
            return cursor.fetchone() is not None
    
    def get_retrieval_info(self, data_type: str, date_range: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a previous retrieval.
        
        Args:
            data_type: 'gauge' or 'radar'
            date_range: Date range string like '20250509-20250510'
            
        Returns:
            Dictionary with retrieval info, or None if not found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM retrievals 
                WHERE data_type = ? AND date_range = ?
            """, (data_type, date_range))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
    
    def record_retrieval(
        self,
        data_type: str,
        date_range: str,
        start_date: str,
        end_date: str,
        item_count: Optional[int] = None,
        file_count: Optional[int] = None,
        total_size_mb: Optional[float] = None,
        status: str = "completed",
        notes: Optional[str] = None
    ) -> int:
        """
        Record a new data retrieval.
        
        Args:
            data_type: 'gauge' or 'radar'
            date_range: Date range string like '20250509-20250510'
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            item_count: Number of gauges or catchments retrieved
            file_count: Number of files created
            total_size_mb: Total size of data in MB
            status: 'completed', 'partial', or 'failed'
            notes: Optional notes about the retrieval
            
        Returns:
            ID of the inserted record
        """
        retrieved_at = datetime.now().isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO retrievals 
                (data_type, date_range, start_date, end_date, retrieved_at, 
                 item_count, file_count, total_size_mb, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data_type, date_range, start_date, end_date, retrieved_at,
                item_count, file_count, total_size_mb, status, notes
            ))
            return cursor.lastrowid
    
    def update_retrieval(
        self,
        data_type: str,
        date_range: str,
        **kwargs
    ) -> bool:
        """
        Update an existing retrieval record.
        
        Args:
            data_type: 'gauge' or 'radar'
            date_range: Date range string
            **kwargs: Fields to update (item_count, file_count, total_size_mb, status, notes)
            
        Returns:
            True if record was updated, False if not found
        """
        allowed_fields = {'item_count', 'file_count', 'total_size_mb', 'status', 'notes'}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not updates:
            return False
        
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [data_type, date_range]
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE retrievals 
                SET {set_clause}
                WHERE data_type = ? AND date_range = ?
            """, values)
            return cursor.rowcount > 0
    
    def delete_retrieval(self, data_type: str, date_range: str) -> bool:
        """
        Delete a retrieval record.
        
        Args:
            data_type: 'gauge' or 'radar'
            date_range: Date range string
            
        Returns:
            True if record was deleted, False if not found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM retrievals 
                WHERE data_type = ? AND date_range = ?
            """, (data_type, date_range))
            return cursor.rowcount > 0
    
    def list_retrievals(
        self,
        data_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List retrieval records.
        
        Args:
            data_type: Filter by 'gauge' or 'radar' (optional)
            status: Filter by status (optional)
            limit: Maximum number of records to return
            
        Returns:
            List of retrieval records as dictionaries
        """
        query = "SELECT * FROM retrievals WHERE 1=1"
        params = []
        
        if data_type:
            query += " AND data_type = ?"
            params.append(data_type)
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
        query += " ORDER BY retrieved_at DESC LIMIT ?"
        params.append(limit)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get overall statistics about retrievals.
        
        Returns:
            Dictionary with stats (total counts, size, etc.)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Total counts by type
            cursor.execute("""
                SELECT data_type, COUNT(*) as count, SUM(total_size_mb) as total_size
                FROM retrievals 
                WHERE status = 'completed'
                GROUP BY data_type
            """)
            for row in cursor.fetchall():
                stats[row['data_type']] = {
                    'count': row['count'],
                    'total_size_mb': row['total_size'] or 0
                }
            
            # Date range covered
            cursor.execute("""
                SELECT MIN(start_date) as earliest, MAX(end_date) as latest
                FROM retrievals 
                WHERE status = 'completed'
            """)
            row = cursor.fetchone()
            if row:
                stats['date_range'] = {
                    'earliest': row['earliest'],
                    'latest': row['latest']
                }
            
            return stats


def check_and_prompt_existing_data(
    data_type: str,
    date_range: str,
    logger,
    force: bool = False
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Check if data exists and prompt user if needed.
    
    Args:
        data_type: 'gauge' or 'radar'
        date_range: Date range string like '20250509-20250510'
        logger: Logger instance
        force: If True, skip prompt and return proceed=True
        
    Returns:
        Tuple of (should_proceed, existing_info)
    """
    import sys
    
    db = RetrievalDatabase()
    
    if db.data_exists(data_type, date_range):
        info = db.get_retrieval_info(data_type, date_range)
        
        logger.warning("")
        logger.warning("=" * 80)
        logger.warning("[!] DATA ALREADY EXISTS")
        logger.warning("=" * 80)
        logger.warning(f"Data type: {data_type.upper()}")
        logger.warning(f"Date range: {date_range}")
        if info:
            logger.warning(f"Previously retrieved: {info.get('retrieved_at', 'Unknown')}")
            if info.get('item_count'):
                item_name = 'gauges' if data_type == 'gauge' else 'catchments'
                logger.warning(f"Item count: {info['item_count']} {item_name}")
            if info.get('total_size_mb'):
                logger.warning(f"Data size: {info['total_size_mb']:.1f} MB")
        logger.warning("")
        
        if force:
            logger.warning("--force flag set, proceeding with re-retrieval...")
            return True, info
        
        # Check if running in interactive mode
        # Non-interactive: stdin is not a TTY (e.g., piped, GUI subprocess)
        is_interactive = sys.stdin.isatty() if hasattr(sys.stdin, 'isatty') else False
        
        if not is_interactive:
            # Non-interactive mode (GUI, piped input) - skip retrieval
            logger.warning("Non-interactive mode detected. Skipping retrieval.")
            logger.warning("Use --force flag to re-retrieve in non-interactive mode.")
            return False, info
        
        # Interactive mode - ask user
        logger.warning("Options:")
        logger.warning("  [y] Yes, re-retrieve and overwrite existing data")
        logger.warning("  [n] No, skip and use existing data")
        logger.warning("  [Ctrl+C] Cancel operation")
        logger.warning("")
        
        try:
            response = input("Do you want to re-retrieve this data? [y/N]: ").strip().lower()
            if response == 'y':
                logger.info("User chose to re-retrieve data")
                return True, info
            else:
                logger.info("User chose to skip retrieval, using existing data")
                return False, info
        except (EOFError, KeyboardInterrupt):
            logger.info("User cancelled operation")
            return False, info
    
    return True, None


def format_retrieval_summary(retrievals: List[Dict[str, Any]]) -> str:
    """
    Format a list of retrievals as a readable summary table.
    
    Args:
        retrievals: List of retrieval records
        
    Returns:
        Formatted string table
    """
    if not retrievals:
        return "No retrievals found."
    
    lines = []
    lines.append("=" * 90)
    lines.append(f"{'Type':<8} {'Date Range':<20} {'Retrieved At':<20} {'Items':<8} {'Size (MB)':<10} {'Status':<10}")
    lines.append("-" * 90)
    
    for r in retrievals:
        item_count = str(r.get('item_count', '-')) if r.get('item_count') else '-'
        size = f"{r.get('total_size_mb', 0):.1f}" if r.get('total_size_mb') else '-'
        retrieved_at = r.get('retrieved_at', '')[:19]  # Truncate to datetime
        
        lines.append(
            f"{r['data_type']:<8} "
            f"{r['date_range']:<20} "
            f"{retrieved_at:<20} "
            f"{item_count:<8} "
            f"{size:<10} "
            f"{r.get('status', 'unknown'):<10}"
        )
    
    lines.append("=" * 90)
    return "\n".join(lines)
