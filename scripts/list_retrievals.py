#!/usr/bin/env python3
"""
Data Retrieval History Viewer

View and manage the history of data retrievals from the Moata API.
Shows which dates have been retrieved for gauge and radar data.

Usage:
    # List all retrievals
    python list_retrievals.py
    
    # Filter by type
    python list_retrievals.py --type gauge
    python list_retrievals.py --type radar
    
    # Show statistics
    python list_retrievals.py --stats
    
    # Check specific date
    python list_retrievals.py --check gauge 20250509-20250510

Author: Auckland Council Internship Team (COMPSCI 778)
Created: 2026-02-01
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import argparse
from moata_pipeline.common.database import (
    RetrievalDatabase, 
    format_retrieval_summary
)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="View data retrieval history",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              # List all retrievals
  %(prog)s --type gauge                 # Filter by gauge data
  %(prog)s --type radar                 # Filter by radar data
  %(prog)s --stats                      # Show statistics
  %(prog)s --check gauge 20250509-20250510  # Check if specific date exists
  %(prog)s --delete gauge 20250509-20250510 # Delete record (not files)
        """
    )
    
    parser.add_argument(
        "--type", "-t",
        choices=["gauge", "radar"],
        help="Filter by data type"
    )
    
    parser.add_argument(
        "--stats", "-s",
        action="store_true",
        help="Show retrieval statistics"
    )
    
    parser.add_argument(
        "--check", "-c",
        nargs=2,
        metavar=("TYPE", "DATE_RANGE"),
        help="Check if specific data exists. Example: --check gauge 20250509-20250510"
    )
    
    parser.add_argument(
        "--delete", "-d",
        nargs=2,
        metavar=("TYPE", "DATE_RANGE"),
        help="Delete a retrieval record (does not delete actual files)"
    )
    
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=50,
        help="Maximum number of records to show (default: 50)"
    )
    
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()
    db = RetrievalDatabase()
    
    # Check specific date
    if args.check:
        data_type, date_range = args.check
        exists = db.data_exists(data_type, date_range)
        
        if exists:
            info = db.get_retrieval_info(data_type, date_range)
            print(f"✅ Data EXISTS: {data_type.upper()} {date_range}")
            if info:
                print(f"   Retrieved: {info.get('retrieved_at', 'Unknown')}")
                if info.get('item_count'):
                    print(f"   Items: {info['item_count']}")
                if info.get('status'):
                    print(f"   Status: {info['status']}")
        else:
            print(f"❌ Data NOT FOUND: {data_type.upper()} {date_range}")
        
        return 0
    
    # Delete record
    if args.delete:
        data_type, date_range = args.delete
        
        if db.data_exists(data_type, date_range):
            confirm = input(f"Delete record for {data_type.upper()} {date_range}? [y/N]: ").strip().lower()
            if confirm == 'y':
                db.delete_retrieval(data_type, date_range)
                print(f"✅ Record deleted (files not affected)")
            else:
                print("Cancelled")
        else:
            print(f"❌ Record not found: {data_type.upper()} {date_range}")
        
        return 0
    
    # Show statistics
    if args.stats:
        stats = db.get_stats()
        
        print("=" * 60)
        print("📊 RETRIEVAL STATISTICS")
        print("=" * 60)
        
        if 'gauge' in stats:
            print(f"\n🌧️  Rain Gauge:")
            print(f"   Total retrievals: {stats['gauge']['count']}")
            print(f"   Total size: {stats['gauge']['total_size_mb']:.1f} MB")
        else:
            print(f"\n🌧️  Rain Gauge: No data")
        
        if 'radar' in stats:
            print(f"\n📡 Rain Radar:")
            print(f"   Total retrievals: {stats['radar']['count']}")
            print(f"   Total size: {stats['radar']['total_size_mb']:.1f} MB")
        else:
            print(f"\n📡 Rain Radar: No data")
        
        if 'date_range' in stats:
            print(f"\n📅 Date Range Coverage:")
            print(f"   Earliest: {stats['date_range']['earliest']}")
            print(f"   Latest: {stats['date_range']['latest']}")
        
        print("\n" + "=" * 60)
        return 0
    
    # List retrievals
    retrievals = db.list_retrievals(
        data_type=args.type,
        limit=args.limit
    )
    
    if not retrievals:
        print("No retrievals found.")
        print("\nRun retrieve scripts to collect data:")
        print("  python scripts/gauge/retrieve.py --date 2025-05-09")
        print("  python scripts/radar/retrieve.py --date 2025-05-09")
        return 0
    
    print(f"\n📋 RETRIEVAL HISTORY (showing {len(retrievals)} records)")
    print(format_retrieval_summary(retrievals))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
