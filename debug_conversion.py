#!/usr/bin/env python3
"""
Debug script to test conversion with verbose logging
"""

import logging
from convert_to_iceberg import IcebergConverter

# Set up verbose logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_conversion():
    """Test conversion with debug output"""
    print("Testing conversion with debug output...")
    
    # Test with a small subset
    converter = IcebergConverter(
        data_path="/data/data",
        output_path="."
    )
    
    # Get docket directories
    dockets = converter.get_docket_directories()
    print(f"Found {len(dockets)} dockets")
    
    # Test first few dockets
    for i, docket_path in enumerate(dockets[:5]):
        print(f"\n--- Testing docket {i+1}: {docket_path.name} ---")
        
        # Process docket
        docket_data = converter.process_docket(docket_path)
        
        print(f"  Agency: {docket_data['agency']}")
        print(f"  Docket ID: {docket_data['docket_id']}")
        print(f"  Docket info: {docket_data['docket_info'] is not None}")
        print(f"  Documents: {len(docket_data['documents'])}")
        print(f"  Comments: {len(docket_data['comments'])}")
        
        # Try to save
        if any([docket_data['docket_info'], docket_data['documents'], docket_data['comments']]):
            success = converter.save_docket_dataset(docket_data)
            print(f"  Save success: {success}")
        else:
            print(f"  No data to save")

if __name__ == "__main__":
    test_conversion() 