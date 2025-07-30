# Apache Iceberg Exploration for Mirrulations Comment Data

This project explores using Apache Iceberg to store and manage comment data from government regulations, with a focus on storage efficiency, incremental updates, and optimization strategies.

## Project Overview

The goal is to understand how Apache Iceberg can improve upon JSON storage for comment data by providing:

1. **Better compression** - Columnar storage with efficient compression
2. **Incremental updates** - Ability to add new comments without full rewrites
3. **Optimization capabilities** - Compact fragmented tables for better performance
4. **S3 compatibility** - Store data in S3 for public access

## Data Structure

The input data consists of JSON files with this structure:
```json
{
  "data": {
    "id": "CMS-2025-0020-0002",
    "type": "comments",
    "links": {"self": "https://api.regulations.gov/v4/comments/CMS-2025-0020-0002"},
    "attributes": {
      "commentOn": "09000064869e3367",
      "agencyId": "CMS",
      "comment": "I recommend medicare for all...",
      "firstName": "Anonymous",
      "lastName": "Anonymous",
      // ... many more fields
    },
    "relationships": {
      "attachments": {"data": [], "links": {...}}
    }
  }
}
```

The exploration flattens this into a tabular structure:
- `data.id` → `id`
- `data.links.self` → `link`
- `data.attributes.*` → individual columns
- Attachment metadata → `has_attachments`, `attachment_count`

## Technology Stack

- **pyiceberg** - Core Iceberg functionality
- **pandas** - Data manipulation and analysis
- **duckdb** - Fast SQL queries and analytics
- **pyarrow** - Efficient data serialization
- **fsspec** - File system abstraction (for S3 compatibility)

## Setup Instructions

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the exploration:**
   ```bash
   python simple_iceberg_exploration.py
   ```

## What the Exploration Covers

### 1. Storage Efficiency Analysis
- Compare JSON vs Parquet file sizes
- Test different compression algorithms (Snappy, Gzip, Brotli, LZ4)
- Measure space savings and compression ratios

### 2. Delta Update Simulation
- Simulate adding new comments incrementally
- Measure storage overhead of delta files
- Analyze file size distribution

### 3. Table Optimization
- Demonstrate fragmentation vs optimization
- Show how to compact many small files into fewer large files
- Measure read efficiency improvements

### 4. Query Performance
- Test common query patterns
- Measure query execution times
- Analyze data characteristics for optimization

### 5. Data Characteristics
- Column null percentages
- Unique value counts
- Data type distribution
- Sample value analysis

## Expected Results

### Storage Efficiency
- **JSON**: ~25,000+ individual files, uncompressed
- **Parquet**: 1-3 files with 2-5x compression
- **Space savings**: 60-80% reduction in storage

### Delta Updates
- **Initial table**: Single optimized file
- **Delta files**: Small incremental files (5-10% of original size each)
- **Overhead**: 20-50% storage overhead for delta approach
- **Trade-off**: Storage efficiency vs update performance

### Optimization Benefits
- **File count reduction**: 20x fewer files after optimization
- **Read performance**: Significantly faster queries
- **Storage efficiency**: Minimal size change, better compression

## Key Insights for S3 Deployment

### Delta Strategy
1. **Write deltas as small Parquet files** - Efficient for incremental updates
2. **Periodic compaction** - Merge deltas into optimized base files
3. **Metadata tracking** - Use Iceberg metadata to track file versions

### S3 Considerations
- **Object store limitations** - No partial updates, must rewrite entire objects
- **Delta approach** - Add new files rather than modify existing ones
- **Compaction strategy** - Background process to optimize storage

### Performance Trade-offs
- **Write performance**: Deltas are fast to write
- **Read performance**: Multiple files require more I/O
- **Storage efficiency**: Deltas have some overhead
- **Optimization**: Periodic compaction balances these factors

## Next Steps

1. **Run the exploration** to see actual metrics for your data
2. **Analyze results** to understand storage and performance characteristics
3. **Design S3 strategy** based on update patterns and access requirements
4. **Implement production solution** with proper Iceberg catalog (AWS Glue, Nessie)

## Files

- `simple_iceberg_exploration.py` - Main exploration script
- `iceberg_exploration.py` - Full Iceberg implementation (requires catalog setup)
- `requirements.txt` - Python dependencies
- `README.md` - This documentation

## Output

The exploration creates an `exploration_output/` directory with:
- Parquet files for each docket
- Delta simulation files
- Fragmented and optimized table examples
- Detailed analysis reports

## Questions This Exploration Answers

1. **How much smaller is Iceberg than JSON?** - Measure compression ratios
2. **How do delta updates work?** - Simulate incremental updates
3. **What's the storage overhead?** - Compare delta vs optimized approaches
4. **How to optimize for S3?** - Understand compaction strategies
5. **What's the query performance impact?** - Test different table layouts 