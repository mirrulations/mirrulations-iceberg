# Iceberg Conversion Usage Guide

This guide explains how to use the `convert_to_iceberg.py` script to convert Mirrulations data to Iceberg format.

## Overview

The script converts each docket into its own Iceberg dataset with three tables:
1. **docket_info** - Basic docket metadata
2. **documents** - Document information and content  
3. **comments** - Comment data with flattened structure

## Prerequisites

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Data structure:** Ensure your data follows the Mirrulations structure:
   ```
   data_path/
   ├── raw-data/
   │   ├── agency1/
   │   │   ├── docket1/
   │   │   │   ├── docket/
   │   │   │   │   └── docket1.json
   │   │   │   ├── documents/
   │   │   │   │   ├── doc1.json
   │   │   │   │   └── doc2.json
   │   │   │   └── comments/
   │   │   │       ├── comment1.json
   │   │   │       └── comment2.json
   │   │   └── docket2/
   │   └── agency2/
   └── ...
   ```

## Basic Usage

### Local Conversion

```bash
# Convert all dockets to local directory (creates derived-data inside data directory)
python convert_to_iceberg.py /path/to/mirrulations/data

# Specify custom output directory (creates derived-data inside specified directory)
python convert_to_iceberg.py /path/to/mirrulations/data --output-path /path/to/output

# Write to current directory
python convert_to_iceberg.py /path/to/mirrulations/data --output-path .

# Use different compression
python convert_to_iceberg.py /path/to/mirrulations/data --compression brotli
```

### S3 Upload

```bash
# Convert and upload to S3
python convert_to_iceberg.py /path/to/mirrulations/data --s3-bucket my-bucket-name

# Custom output path with S3
python convert_to_iceberg.py /path/to/mirrulations/data \
    --output-path derived-data \
    --s3-bucket my-bucket-name
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `data_path` | Path to Mirrulations data directory | Required |
| `--output-path` | Directory where `derived-data` folder will be created | `derived-data` inside data_path directory |
| `--s3-bucket` | S3 bucket name for uploading results | None |
| `--compression` | Compression algorithm | `snappy` |

### Compression Options

- **snappy** - Fast compression, good balance (default)
- **gzip** - Higher compression, slower
- **brotli** - Best compression, slowest
- **lz4** - Fastest, lower compression

## Output Structure

The script creates the following structure:

**Default behavior (no --output-path):**
```
data_path/
├── derived-data/
│   ├── agency1/
│   │   ├── docket1/
│   │   │   └── iceberg/
│   │   │       ├── docket_info.parquet
│   │   │       ├── documents.parquet
│   │   │       └── comments.parquet
│   │   └── docket2/
│   │       └── iceberg/
│   │           ├── docket_info.parquet
│   │           ├── documents.parquet
│   │           └── comments.parquet
│   └── agency2/
│       └── ...
├── raw-data/
└── ...
```

**With --output-path specified:**
```
output_path/
├── derived-data/
│   ├── agency1/
│   │   ├── docket1/
│   │   │   └── iceberg/
│   │   │       ├── docket_info.parquet
│   │   │       ├── documents.parquet
│   │   │       └── comments.parquet
│   │   └── docket2/
│   │       └── iceberg/
│   │           ├── docket_info.parquet
│   │           ├── documents.parquet
│   │           └── comments.parquet
│   └── agency2/
│       └── ...
```

## Performance Considerations

### Expected Performance

Based on testing with sample data:
- **Processing rate**: ~2-5 dockets/second
- **Storage savings**: 75-80% vs JSON
- **Memory usage**: ~100-200MB per docket

### For Large Datasets (150,000+ dockets)

**Estimated time**: 8-20 hours depending on:
- Data size per docket
- Compression algorithm
- Storage type (local vs S3)
- System performance

**Memory management**: The script processes one docket at a time to minimize memory usage.

## Monitoring Progress

The script provides detailed progress information:

```
2025-07-30 15:25:26,983 - INFO - Starting conversion from: /path/to/data
2025-07-30 15:25:26,984 - INFO - Found 150000 dockets to process
2025-07-30 15:25:26,985 - INFO - Progress: 100/150000 (0.1%) Rate: 2.34 dockets/sec Elapsed: 0.0h ETA: 17.8h
```

### Log Files

- **Console output**: Real-time progress
- **iceberg_conversion.log**: Detailed log file
- **Progress updates**: Every 100 dockets

## Testing

Before running on the full dataset, test with a small subset:

```bash
# Test with existing sample data
python test_conversion.py

# Test with custom data path
python convert_to_iceberg.py /path/to/small/test/data --output-path test-output
```

## Error Handling

The script handles errors gracefully:

- **Individual docket failures** don't stop the entire process
- **Detailed error logging** in `iceberg_conversion.log`
- **Statistics tracking** for processed, skipped, and failed dockets
- **Resumable operation** - can restart from where it left off

## S3 Configuration

### Prerequisites

1. **AWS credentials** configured (via AWS CLI, environment variables, or IAM role)
2. **S3 bucket** with appropriate permissions
3. **boto3 and s3fs** installed

### S3 Permissions

The script needs these S3 permissions:
- `s3:PutObject`
- `s3:GetObject` (for verification)
- `s3:ListBucket`

### S3 Upload Strategy

- **Parallel uploads**: Files are uploaded as they're created
- **Error handling**: Failed uploads are logged but don't stop processing
- **Verification**: Upload success is confirmed before proceeding

## Data Quality

### Flattening Strategy

The script flattens nested JSON structures:

**Original JSON:**
```json
{
  "data": {
    "id": "docket-id",
    "attributes": {
      "title": "Docket Title",
      "agencyId": "AGENCY"
    },
    "relationships": {
      "comments": {
        "data": [{"id": "1"}, {"id": "2"}]
      }
    }
  }
}
```

**Flattened Structure:**
```json
{
  "id": "docket-id",
  "title": "Docket Title", 
  "agencyId": "AGENCY",
  "comments_count": 2
}
```

### Data Validation

- **Null handling**: Only non-null values are included
- **Type preservation**: Data types are maintained
- **Relationship flattening**: Counts and flags for related data

## Troubleshooting

### Common Issues

1. **"No dockets found"**
   - Check data path structure
   - Ensure raw-data directory exists
   - Verify agency/docket directory structure

2. **Memory errors**
   - Reduce batch size (not currently configurable)
   - Use faster compression (lz4)
   - Process smaller subsets

3. **S3 upload failures**
   - Check AWS credentials
   - Verify S3 bucket permissions
   - Check network connectivity

4. **Slow performance**
   - Use SSD storage for local processing
   - Consider faster compression (lz4)
   - Process during off-peak hours

### Debug Mode

For detailed debugging, modify the logging level in the script:

```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Best Practices

1. **Test first**: Always test with a small subset
2. **Monitor resources**: Watch disk space and memory usage
3. **Backup data**: Keep original data as backup
4. **Use appropriate compression**: Balance size vs speed
5. **Plan for interruptions**: Script can be restarted
6. **Monitor logs**: Check conversion.log for issues

## Example Workflows

### Development/Testing
```bash
# Test with sample data
python test_conversion.py

# Test with small subset
python convert_to_iceberg.py /path/to/small/data --output-path test-output
```

### Production Run
```bash
# Full conversion to local storage
python convert_to_iceberg.py /path/to/full/data --compression snappy

# Full conversion with S3 upload
python convert_to_iceberg.py /path/to/full/data \
    --output-path derived-data \
    --s3-bucket my-production-bucket \
    --compression brotli
```

### Incremental Updates
```bash
# Process only new dockets (manual process)
python convert_to_iceberg.py /path/to/new/data \
    --output-path derived-data \
    --s3-bucket my-bucket
``` 