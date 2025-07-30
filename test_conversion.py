#!/usr/bin/env python3
"""
Test the Iceberg conversion with a small subset of dockets

This script tests the conversion process with a few dockets to ensure everything works correctly.
"""

import sys
from pathlib import Path
from convert_to_iceberg import IcebergConverter


def test_conversion():
    """Test the conversion with a small subset"""
    print("Testing Iceberg conversion with small subset...")
    
    # Use the existing results directory as test data
    data_path = "results"
    
    if not Path(data_path).exists():
        print(f"Error: Test data path does not exist: {data_path}")
        return False
    
    # Create converter for testing
    converter = IcebergConverter(
        data_path=data_path
        # No output_path specified - will use derived-data in same directory as data_path
    )
    
    # Get docket directories
    docket_dirs = converter.get_docket_directories()
    
    if not docket_dirs:
        print("No dockets found for testing")
        return False
    
    print(f"Found {len(docket_dirs)} dockets for testing")
    
    # Test with first 3 dockets
    test_dockets = docket_dirs[:3]
    
    print(f"Testing with {len(test_dockets)} dockets:")
    for docket in test_dockets:
        print(f"  - {docket.parent.name}/{docket.name}")
    
    # Process test dockets
    for docket_dir in test_dockets:
        try:
            print(f"\nProcessing: {docket_dir.parent.name}/{docket_dir.name}")
            
            # Process docket
            docket_data = converter.process_docket(docket_dir)
            
            # Print summary
            print(f"  Docket info: {'✓' if docket_data['docket_info'] else '✗'}")
            print(f"  Documents: {len(docket_data['documents'])}")
            print(f"  Comments: {len(docket_data['comments'])}")
            
            # Save dataset
            if converter.save_docket_dataset(docket_data):
                print(f"  ✓ Saved successfully")
            else:
                print(f"  ✗ Failed to save")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    print(f"\nTest complete! Check 'derived-data' directory for results.")
    return True


if __name__ == "__main__":
    success = test_conversion()
    sys.exit(0 if success else 1) 