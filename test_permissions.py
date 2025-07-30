#!/usr/bin/env python3
"""
Test script to verify permission error handling
"""

import sys
import tempfile
import os
from pathlib import Path
from convert_to_iceberg import IcebergConverter


def test_permission_handling():
    """Test permission error handling"""
    print("Testing permission error handling...")
    
    # Test 1: Non-existent data directory
    print("\n1. Testing non-existent data directory:")
    try:
        converter = IcebergConverter(data_path="/non/existent/path")
        converter.convert_all()
    except Exception as e:
        print(f"   ✓ Correctly caught error: {e}")
    
    # Test 2: Read-only data directory (simulate)
    print("\n2. Testing read-only data directory:")
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a read-only directory
        read_only_dir = Path(temp_dir) / "readonly"
        read_only_dir.mkdir()
        os.chmod(read_only_dir, 0o444)  # Read-only
        
        try:
            converter = IcebergConverter(data_path=str(read_only_dir))
            converter.convert_all()
        except Exception as e:
            print(f"   ✓ Correctly caught permission error: {e}")
        finally:
            # Restore permissions for cleanup
            os.chmod(read_only_dir, 0o755)
    
    # Test 3: Write-protected output directory
    print("\n3. Testing write-protected output directory:")
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a write-protected directory
        write_protected_dir = Path(temp_dir) / "writeprotected"
        write_protected_dir.mkdir()
        os.chmod(write_protected_dir, 0o444)  # Read-only
        
        try:
            converter = IcebergConverter(
                data_path="results",  # Use existing data
                output_path=str(write_protected_dir)
            )
            converter.convert_all()
        except Exception as e:
            print(f"   ✓ Correctly caught write permission error: {e}")
        finally:
            # Restore permissions for cleanup
            os.chmod(write_protected_dir, 0o755)
    
    print("\nPermission error handling tests completed!")


if __name__ == "__main__":
    test_permission_handling() 