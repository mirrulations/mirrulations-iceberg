#!/usr/bin/env python3
"""
Script to explore the data directory structure
"""

from pathlib import Path
import sys

def explore_structure(data_path):
    """Explore the data directory structure"""
    print(f"Exploring: {data_path}")
    
    if not Path(data_path).exists():
        print(f"Path does not exist: {data_path}")
        return
    
    # List top-level contents
    data_dir = Path(data_path)
    print(f"\nTop-level contents:")
    for item in data_dir.iterdir():
        if item.is_dir():
            print(f"  DIR: {item.name}")
        else:
            print(f"  FILE: {item.name}")
    
    # Look for raw-data
    raw_data_path = data_dir / "raw-data"
    if raw_data_path.exists():
        print(f"\nFound raw-data directory:")
        for agency_dir in raw_data_path.iterdir():
            if agency_dir.is_dir():
                print(f"  Agency: {agency_dir.name}")
                # List first few dockets
                dockets = list(agency_dir.iterdir())
                for docket in dockets[:3]:
                    if docket.is_dir():
                        print(f"    Docket: {docket.name}")
                        # Check for subdirectories
                        subdirs = [d.name for d in docket.iterdir() if d.is_dir()]
                        print(f"      Subdirs: {subdirs}")
                if len(dockets) > 3:
                    print(f"    ... and {len(dockets) - 3} more")
    else:
        print(f"\nNo raw-data directory found")
        
        # Look for direct docket structure
        print(f"\nLooking for direct docket structure:")
        docket_dirs = []
        for item in data_dir.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                docket_dirs.append(item)
        
        print(f"Found {len(docket_dirs)} potential docket directories")
        for docket in docket_dirs[:5]:
            print(f"  {docket.name}")
            if docket.exists():
                subdirs = [d.name for d in docket.iterdir() if d.is_dir()]
                print(f"    Subdirs: {subdirs}")

if __name__ == "__main__":
    data_path = sys.argv[1] if len(sys.argv) > 1 else "/data/data"
    explore_structure(data_path) 