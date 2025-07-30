#!/usr/bin/env python3
"""
Simple test script to run on the server
"""

import logging
from pathlib import Path
from convert_to_iceberg import IcebergConverter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print("Testing conversion on server...")
    
    # Create converter
    converter = IcebergConverter(
        data_path="/data/data",
        output_path="."
    )
    
    # Get dockets
    dockets = converter.get_docket_directories()
    print(f"Found {len(dockets)} dockets")
    
    if len(dockets) == 0:
        print("No dockets found!")
        return
    
    # Test first docket
    first_docket = dockets[0]
    print(f"\nTesting first docket: {first_docket}")
    
    # Process docket
    docket_data = converter.process_docket(first_docket)
    
    print(f"Agency: {docket_data['agency']}")
    print(f"Docket ID: {docket_data['docket_id']}")
    print(f"Has docket info: {docket_data['docket_info'] is not None}")
    print(f"Documents: {len(docket_data['documents'])}")
    print(f"Comments: {len(docket_data['comments'])}")
    
    # Try to save
    if any([docket_data['docket_info'], docket_data['documents'], docket_data['comments']]):
        print("Attempting to save...")
        success = converter.save_docket_dataset(docket_data)
        print(f"Save success: {success}")
        
        if success:
            print("Checking if files were created...")
            # Check if derived-data directory was created
            derived_data_path = Path("derived-data")
            if derived_data_path.exists():
                print(f"✓ derived-data directory created")
                # List contents
                for item in derived_data_path.iterdir():
                    if item.is_dir():
                        print(f"  {item.name}/")
                        for subitem in item.iterdir():
                            if subitem.is_dir():
                                print(f"    {subitem.name}/")
            else:
                print("✗ derived-data directory not created")
    else:
        print("No data found in docket")

if __name__ == "__main__":
    main() 