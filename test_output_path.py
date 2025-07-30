#!/usr/bin/env python3
"""
Test script to verify output path behavior
"""

import sys
from pathlib import Path
from convert_to_iceberg import IcebergConverter


def test_output_paths():
    """Test different output path scenarios"""
    print("Testing output path behavior...")
    
    # Test data path
    data_path = "results"
    
    print(f"\n1. Testing default behavior (no output path):")
    converter1 = IcebergConverter(data_path=data_path)
    print(f"   Data path: {converter1.data_path}")
    print(f"   Output path: {converter1.output_path}")
    print(f"   Expected: {Path(data_path) / 'derived-data'}")
    print(f"   Correct: {converter1.output_path == Path(data_path) / 'derived-data'}")
    
    print(f"\n2. Testing --output-path .:")
    converter2 = IcebergConverter(data_path=data_path, output_path=".")
    print(f"   Data path: {converter2.data_path}")
    print(f"   Output path: {converter2.output_path}")
    print(f"   Expected: {Path('.').resolve()}")
    print(f"   Correct: {converter2.output_path == Path('.').resolve()}")
    
    print(f"\n3. Testing --output-path /absolute/path:")
    converter3 = IcebergConverter(data_path=data_path, output_path="/tmp/test-output")
    print(f"   Data path: {converter3.data_path}")
    print(f"   Output path: {converter3.output_path}")
    print(f"   Expected: {Path('/tmp/test-output').resolve()}")
    print(f"   Correct: {converter3.output_path == Path('/tmp/test-output').resolve()}")
    
    print(f"\n4. Testing --output-path relative/path:")
    converter4 = IcebergConverter(data_path=data_path, output_path="test-output")
    print(f"   Data path: {converter4.data_path}")
    print(f"   Output path: {converter4.output_path}")
    print(f"   Expected: {Path('test-output').resolve()}")
    print(f"   Correct: {converter4.output_path == Path('test-output').resolve()}")


if __name__ == "__main__":
    test_output_paths() 