#!/usr/bin/env python3
"""
Test script to verify S3 support in convert_to_iceberg.py
"""

import sys
import os
from pathlib import Path

# Add the current directory to the path so we can import convert_to_iceberg
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from convert_to_iceberg import PathHandler, IcebergConverter

def test_path_handler():
    """Test the PathHandler class"""
    print("Testing PathHandler...")
    
    # Test S3 path detection
    assert PathHandler.is_s3_path("s3://bucket/path") == True
    assert PathHandler.is_s3_path("/local/path") == False
    assert PathHandler.is_s3_path("s3://bucket") == True
    
    # Test S3 path parsing
    bucket, key = PathHandler.parse_s3_path("s3://my-bucket/folder/file")
    assert bucket == "my-bucket"
    assert key == "folder/file"
    
    bucket, key = PathHandler.parse_s3_path("s3://my-bucket")
    assert bucket == "my-bucket"
    assert key == ""
    
    # Test path joining
    assert PathHandler.join_s3_path("bucket", "key") == "s3://bucket/key"
    assert PathHandler.join_s3_path("bucket", "") == "s3://bucket"
    
    # Test name extraction
    assert PathHandler.get_name("s3://bucket/folder/file") == "file"
    assert PathHandler.get_name("s3://bucket") == "bucket"
    assert PathHandler.get_name("/local/path/file") == "file"
    
    print("✓ PathHandler tests passed")

def test_converter_initialization():
    """Test converter initialization with different path types"""
    print("Testing converter initialization...")
    
    # Test local path
    converter = IcebergConverter("/tmp/test", debug=True)
    assert converter.is_s3_source == False
    assert converter.is_s3_target == False
    assert converter.output_path == "/tmp/test/derived-data"
    
    # Test S3 path
    converter = IcebergConverter("s3://bucket/data", debug=True)
    assert converter.is_s3_source == True
    assert converter.is_s3_target == False
    assert converter.output_path == "s3://bucket/data/derived-data"
    
    # Test with explicit output path
    converter = IcebergConverter("/tmp/test", output_path="s3://bucket/output", debug=True)
    assert converter.is_s3_source == False
    assert converter.is_s3_target == True
    assert converter.output_path == "s3://bucket/output"
    
    print("✓ Converter initialization tests passed")

def test_path_operations():
    """Test path operations"""
    print("Testing path operations...")
    
    converter = IcebergConverter("/tmp/test", debug=True)
    
    # Test path joining
    assert converter.join_paths("/base", "part1", "part2") == "/base/part1/part2"
    assert converter.join_paths("s3://bucket", "folder", "file") == "s3://bucket/folder/file"
    
    # Test path existence (should work for local paths that exist)
    assert converter.path_exists("/tmp") == True
    assert converter.path_exists("/nonexistent/path") == False
    
    print("✓ Path operations tests passed")

if __name__ == "__main__":
    print("Running S3 support tests...")
    
    try:
        test_path_handler()
        test_converter_initialization()
        test_path_operations()
        
        print("\n🎉 All tests passed! S3 support is working correctly.")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 