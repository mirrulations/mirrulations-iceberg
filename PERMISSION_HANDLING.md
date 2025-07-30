# Permission Handling Improvements

## Overview

The Iceberg conversion script now includes comprehensive permission checking and error handling to prevent silent failures and provide clear error messages.

## Key Features

### 1. **Pre-flight Permission Checks**

The script now checks permissions before starting any conversion:

- **Data directory existence** - Ensures the input directory exists
- **Read permissions** - Verifies read access to data directory
- **Directory listing** - Tests ability to list directory contents
- **Output directory write permissions** - Checks write access to output location
- **Directory creation** - Tests ability to create output directories

### 2. **Clear Error Messages**

When permission issues are detected, the script provides specific error messages:

```
2025-07-30 18:55:19,708 - ERROR - Permission check failed: No write permission for output directory: /path/to/output
2025-07-30 18:55:19,708 - ERROR - Please ensure you have read access to the data directory and write access to the output directory.
```

### 3. **Graceful Termination**

- **Early termination** on permission errors to prevent wasted processing time
- **Clear exit codes** for different error conditions
- **Detailed logging** of all permission-related issues

## Error Scenarios Handled

### 1. **Non-existent Data Directory**
```
ERROR - Data directory does not exist: /path/to/data
```

### 2. **Read-only Data Directory**
```
ERROR - No read permission for data directory: /path/to/data
```

### 3. **Cannot List Directory Contents**
```
ERROR - Cannot list contents of data directory /path/to/data: Permission denied
```

### 4. **Write-protected Output Directory**
```
ERROR - No write permission for output directory: /path/to/output
```

### 5. **Cannot Create Output Directory**
```
ERROR - Cannot create or write to output directory /path/to/output: Permission denied
```

## Solutions for Common Issues

### Read-Only Mounted Drives

If your data is on a read-only filesystem:

```bash
# Specify a writable output location
python convert_to_iceberg.py /readonly/data --output-path /writable/output

# Use current directory (if writable)
python convert_to_iceberg.py /readonly/data --output-path .

# Use S3 for output
python convert_to_iceberg.py /readonly/data --s3-bucket my-bucket
```

### Insufficient Permissions

```bash
# Check current permissions
ls -la /path/to/data
ls -la /path/to/output

# Fix permissions if needed
chmod -R 755 /path/to/data
chmod -R 755 /path/to/output

# Or run with appropriate user
sudo python convert_to_iceberg.py /path/to/data
```

### SELinux/AppArmor Issues

```bash
# Check if SELinux is blocking access
getenforce
ls -Z /path/to/data

# Temporarily disable SELinux (if appropriate)
sudo setenforce 0

# Or add appropriate SELinux context
sudo chcon -R -t user_home_t /path/to/output
```

## Testing Permission Handling

Use the test script to verify permission handling:

```bash
python test_permissions.py
```

This will test:
- Non-existent directories
- Read-only data directories
- Write-protected output directories

## Logging

All permission-related errors are logged to:
- **Console output** - Immediate feedback
- **iceberg_conversion.log** - Detailed log file

## Exit Codes

- **0** - Success
- **1** - General error (including permission errors)
- **1** - Keyboard interrupt

## Best Practices

1. **Test permissions first** - Run a small test before processing large datasets
2. **Use appropriate output paths** - Ensure output directory is on a writable filesystem
3. **Check disk space** - Ensure sufficient space for output files
4. **Monitor logs** - Check `iceberg_conversion.log` for detailed error information
5. **Use S3 for read-only environments** - Consider S3 output for completely read-only systems

## Example Workflow for Read-Only Environments

```bash
# 1. Check available space on writable filesystem
df -h /writable/path

# 2. Test with small subset
python convert_to_iceberg.py /readonly/data --output-path /writable/test --compression snappy

# 3. If successful, run full conversion
python convert_to_iceberg.py /readonly/data --output-path /writable/output --compression brotli

# 4. Or use S3 for output
python convert_to_iceberg.py /readonly/data --s3-bucket my-bucket --compression brotli
```

The permission handling ensures that the script fails fast and provides clear guidance on how to resolve permission issues. 