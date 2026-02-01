#!/usr/bin/env python3
"""
Sync Existing Outputs to Database

Scans existing output folders and adds them to the retrieval database.
Run this once after implementing the database to sync historical data.

Usage:
    python sync_outputs_to_db.py

Author: Auckland Council Internship Team (COMPSCI 778)
Created: 2026-02-01
"""

import sys
from pathlib import Path
import re
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from moata_pipeline.common.database import RetrievalDatabase


def parse_date_range_folder(folder_name: str) -> tuple:
    """
    Parse folder name like '20260121-20260122' to dates.
    
    Returns:
        Tuple of (date_range, start_date, end_date) or None if invalid
    """
    pattern = r'^(\d{8})-(\d{8})$'
    match = re.match(pattern, folder_name)
    if not match:
        return None
    
    start_str, end_str = match.groups()
    
    try:
        start_date = datetime.strptime(start_str, "%Y%m%d")
        end_date = datetime.strptime(end_str, "%Y%m%d")
        return (
            folder_name,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
    except ValueError:
        return None


def count_files_in_folder(folder: Path) -> int:
    """Count files in folder recursively."""
    return sum(1 for _ in folder.rglob("*") if _.is_file())


def get_folder_size_mb(folder: Path) -> float:
    """Get folder size in MB."""
    total = sum(f.stat().st_size for f in folder.rglob("*") if f.is_file())
    return total / (1024 * 1024)


def sync_outputs_to_database():
    """Scan outputs folders and add to database."""
    db = RetrievalDatabase()
    
    outputs_base = Path("outputs")
    
    # Scan rain_gauges
    gauge_dir = outputs_base / "rain_gauges"
    if gauge_dir.exists():
        print(f"\nScanning {gauge_dir}...")
        for folder in gauge_dir.iterdir():
            if not folder.is_dir():
                continue
            
            parsed = parse_date_range_folder(folder.name)
            if not parsed:
                continue
            
            date_range, start_date, end_date = parsed
            
            # Check if already in database
            if db.data_exists("gauge", date_range):
                print(f"  [SKIP] gauge {date_range} - already in database")
                continue
            
            # Count files and size
            raw_dir = folder / "raw"
            if raw_dir.exists():
                file_count = count_files_in_folder(raw_dir)
                size_mb = get_folder_size_mb(folder)
            else:
                file_count = count_files_in_folder(folder)
                size_mb = get_folder_size_mb(folder)
            
            # Add to database
            db.record_retrieval(
                data_type="gauge",
                date_range=date_range,
                start_date=start_date,
                end_date=end_date,
                file_count=file_count,
                total_size_mb=size_mb,
                status="completed",
                notes="Synced from existing outputs folder"
            )
            print(f"  [ADD] gauge {date_range} - {file_count} files, {size_mb:.1f} MB")
    
    # Scan rain_radar
    radar_dir = outputs_base / "rain_radar"
    if radar_dir.exists():
        print(f"\nScanning {radar_dir}...")
        for folder in radar_dir.iterdir():
            if not folder.is_dir():
                continue
            
            parsed = parse_date_range_folder(folder.name)
            if not parsed:
                continue
            
            date_range, start_date, end_date = parsed
            
            # Check if already in database
            if db.data_exists("radar", date_range):
                print(f"  [SKIP] radar {date_range} - already in database")
                continue
            
            # Count files and size
            raw_dir = folder / "raw"
            if raw_dir.exists():
                file_count = count_files_in_folder(raw_dir)
                size_mb = get_folder_size_mb(folder)
            else:
                file_count = count_files_in_folder(folder)
                size_mb = get_folder_size_mb(folder)
            
            # Add to database
            db.record_retrieval(
                data_type="radar",
                date_range=date_range,
                start_date=start_date,
                end_date=end_date,
                file_count=file_count,
                total_size_mb=size_mb,
                status="completed",
                notes="Synced from existing outputs folder"
            )
            print(f"  [ADD] radar {date_range} - {file_count} files, {size_mb:.1f} MB")
    
    print("\n" + "=" * 60)
    print("Sync complete!")
    print("=" * 60)
    
    # Show final stats
    stats = db.get_stats()
    if 'gauge' in stats:
        print(f"\nGauge: {stats['gauge']['count']} retrievals, {stats['gauge']['total_size_mb']:.1f} MB")
    if 'radar' in stats:
        print(f"Radar: {stats['radar']['count']} retrievals, {stats['radar']['total_size_mb']:.1f} MB")


if __name__ == "__main__":
    sync_outputs_to_database()
