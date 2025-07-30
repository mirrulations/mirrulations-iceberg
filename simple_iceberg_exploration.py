#!/usr/bin/env python3
"""
Simplified Apache Iceberg Exploration for Mirrulations Comment Data

This script explores:
1. Data flattening and schema creation
2. Storage efficiency comparison (JSON vs Parquet)
3. Delta update strategies
4. Table optimization techniques

Uses simplified approach without full Iceberg catalog for easier exploration.
"""

import json
import pandas as pd
import duckdb
from pathlib import Path
from typing import Dict, List, Any
import time
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class StorageMetrics:
    """Track storage metrics for comparison"""
    format: str
    total_size_bytes: int
    file_count: int
    compression_ratio: float = 1.0
    
    @property
    def total_size_mb(self) -> float:
        return self.total_size_bytes / (1024 * 1024)
    
    @property
    def effective_size_mb(self) -> float:
        return self.total_size_mb / self.compression_ratio


class CommentDataExplorer:
    """Explore comment data with Iceberg-like concepts"""
    
    def __init__(self, results_dir: str = "results"):
        self.results_dir = Path(results_dir)
        self.output_dir = Path("exploration_output")
        self.output_dir.mkdir(exist_ok=True)
        
    def flatten_comment_data(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten the nested JSON structure"""
        data = json_data.get("data", {})
        
        flattened = {
            "id": data.get("id"),
            "link": data.get("links", {}).get("self"),
            "type": data.get("type"),
        }
        
        # Flatten attributes
        attributes = data.get("attributes", {})
        for key, value in attributes.items():
            if value is not None:  # Only include non-null values
                flattened[key] = value
                
        # Handle relationships (simplified)
        relationships = data.get("relationships", {})
        attachments = relationships.get("attachments", {})
        flattened["has_attachments"] = len(attachments.get("data", [])) > 0
        flattened["attachment_count"] = len(attachments.get("data", []))
        
        # Handle included data (attachments)
        included = json_data.get("included", [])
        if included:
            flattened["has_included_attachments"] = True
            flattened["included_attachment_count"] = len(included)
        else:
            flattened["has_included_attachments"] = False
            flattened["included_attachment_count"] = 0
            
        return flattened
    
    def load_comments_from_docket(self, docket_id: str) -> pd.DataFrame:
        """Load all comments from a specific docket"""
        comments_dir = self.results_dir / docket_id / "raw-data" / "comments"
        
        if not comments_dir.exists():
            print(f"Comments directory not found: {comments_dir}")
            return pd.DataFrame()
        
        comments = []
        json_files = list(comments_dir.glob("*.json"))
        
        print(f"Loading {len(json_files)} comment files from {docket_id}...")
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    flattened = self.flatten_comment_data(json_data)
                    comments.append(flattened)
            except Exception as e:
                print(f"Error processing {json_file}: {e}")
                
        df = pd.DataFrame(comments)
        print(f"Loaded {len(df)} comments from {docket_id}")
        
        # Show sample data
        if not df.empty:
            print(f"Sample columns: {list(df.columns[:10])}...")
            print(f"Data types: {df.dtypes.value_counts().to_dict()}")
        
        return df
    
    def measure_storage_efficiency(self, docket_id: str) -> Dict[str, StorageMetrics]:
        """Compare storage efficiency between JSON and Parquet"""
        print(f"\n=== Storage Efficiency Analysis for {docket_id} ===")
        
        # Load data
        df = self.load_comments_from_docket(docket_id)
        if df.empty:
            return {}
        
        # Measure JSON storage
        json_dir = self.results_dir / docket_id / "raw-data" / "comments"
        json_files = list(json_dir.glob("*.json"))
        
        json_size = sum(f.stat().st_size for f in json_files)
        json_metrics = StorageMetrics(
            format="JSON",
            total_size_bytes=json_size,
            file_count=len(json_files)
        )
        
        # Create Parquet file and measure
        parquet_path = self.output_dir / f"{docket_id}_comments.parquet"
        df.to_parquet(parquet_path, index=False, compression='snappy')
        
        parquet_size = parquet_path.stat().st_size
        parquet_metrics = StorageMetrics(
            format="Parquet (Snappy)",
            total_size_bytes=parquet_size,
            file_count=1,
            compression_ratio=json_size / parquet_size if parquet_size > 0 else 1.0
        )
        
        # Try different compression options
        compression_tests = {
            'gzip': 'gzip',
            'brotli': 'brotli',
            'lz4': 'lz4'
        }
        
        compression_results = {}
        for name, compression in compression_tests.items():
            try:
                test_path = self.output_dir / f"{docket_id}_comments_{name}.parquet"
                df.to_parquet(test_path, index=False, compression=compression)
                test_size = test_path.stat().st_size
                compression_results[name] = StorageMetrics(
                    format=f"Parquet ({name})",
                    total_size_bytes=test_size,
                    file_count=1,
                    compression_ratio=json_size / test_size if test_size > 0 else 1.0
                )
                test_path.unlink()  # Clean up test file
            except Exception as e:
                print(f"Could not test {name} compression: {e}")
        
        # Print results
        print(f"\nStorage Comparison:")
        print(f"JSON: {json_metrics.total_size_mb:.2f} MB ({json_metrics.file_count} files)")
        print(f"Parquet (Snappy): {parquet_metrics.total_size_mb:.2f} MB ({parquet_metrics.file_count} files)")
        print(f"Compression ratio: {parquet_metrics.compression_ratio:.2f}x")
        print(f"Space savings: {((json_size - parquet_size) / json_size * 100):.1f}%")
        
        if compression_results:
            print(f"\nCompression Comparison:")
            for name, metrics in compression_results.items():
                print(f"  {name}: {metrics.total_size_mb:.2f} MB ({metrics.compression_ratio:.2f}x)")
        
        return {
            "json": json_metrics,
            "parquet": parquet_metrics,
            "compression_tests": compression_results
        }
    
    def simulate_delta_updates(self, docket_id: str, num_updates: int = 10):
        """Simulate incremental updates and measure efficiency"""
        print(f"\n=== Delta Update Simulation for {docket_id} ===")
        
        # Load existing data
        df = self.load_comments_from_docket(docket_id)
        if df.empty:
            return
        
        # Create initial table
        base_table_path = self.output_dir / f"{docket_id}_delta_experiment"
        base_table_path.mkdir(exist_ok=True)
        
        # Write initial data
        initial_path = base_table_path / "initial.parquet"
        df.to_parquet(initial_path, index=False, compression='snappy')
        initial_size = initial_path.stat().st_size
        
        print(f"Initial table size: {initial_size / (1024*1024):.2f} MB")
        
        # Simulate incremental updates
        delta_sizes = []
        total_delta_size = 0
        
        for i in range(num_updates):
            # Create a small delta (simulating new comments)
            delta_df = df.sample(min(5, len(df)))  # Simulate 5 new comments
            delta_df['id'] = delta_df['id'] + f"_update_{i}"  # Make IDs unique
            
            delta_path = base_table_path / f"delta_{i:03d}.parquet"
            delta_df.to_parquet(delta_path, index=False, compression='snappy')
            
            delta_size = delta_path.stat().st_size
            delta_sizes.append(delta_size)
            total_delta_size += delta_size
            
            print(f"Delta {i+1}: {delta_size / (1024*1024):.2f} MB")
        
        # Calculate efficiency metrics
        total_files = 1 + num_updates  # initial + deltas
        total_size = initial_size + total_delta_size
        
        print(f"\nDelta Update Results:")
        print(f"Total files: {total_files}")
        print(f"Total size: {total_size / (1024*1024):.2f} MB")
        print(f"Average delta size: {total_delta_size / num_updates / (1024*1024):.2f} MB")
        print(f"Overhead vs single file: {((total_size - initial_size) / initial_size * 100):.1f}%")
        
        # Show file size distribution
        print(f"\nFile size distribution:")
        print(f"  Initial: {initial_size / (1024*1024):.2f} MB")
        print(f"  Deltas: {min(delta_sizes) / (1024*1024):.2f} - {max(delta_sizes) / (1024*1024):.2f} MB")
        
        return {
            "initial_size": initial_size,
            "total_delta_size": total_delta_size,
            "total_size": total_size,
            "file_count": total_files,
            "delta_sizes": delta_sizes
        }
    
    def demonstrate_optimization(self, docket_id: str):
        """Demonstrate table optimization by compacting deltas"""
        print(f"\n=== Table Optimization for {docket_id} ===")
        
        # Load data
        df = self.load_comments_from_docket(docket_id)
        if df.empty:
            return
        
        fragmented_path = self.output_dir / f"{docket_id}_fragmented"
        fragmented_path.mkdir(exist_ok=True)
        
        # Create fragmented table (many small files)
        total_rows = len(df)
        chunk_size = max(1, total_rows // 20)  # Split into ~20 chunks
        
        fragmented_files = []
        for i in range(0, total_rows, chunk_size):
            chunk_df = df.iloc[i:i+chunk_size]
            chunk_path = fragmented_path / f"chunk_{i//chunk_size:03d}.parquet"
            chunk_df.to_parquet(chunk_path, index=False, compression='snappy')
            fragmented_files.append(chunk_path)
        
        fragmented_size = sum(f.stat().st_size for f in fragmented_files)
        print(f"Fragmented table: {len(fragmented_files)} files, {fragmented_size / (1024*1024):.2f} MB")
        
        # Optimize by compacting into fewer, larger files
        optimized_path = self.output_dir / f"{docket_id}_optimized"
        optimized_path.mkdir(exist_ok=True)
        
        # Compact into 2-3 larger files
        compact_chunk_size = total_rows // 3
        optimized_files = []
        
        for i in range(0, total_rows, compact_chunk_size):
            chunk_df = df.iloc[i:i+compact_chunk_size]
            chunk_path = optimized_path / f"compact_{i//compact_chunk_size:02d}.parquet"
            chunk_df.to_parquet(chunk_path, index=False, compression='snappy')
            optimized_files.append(chunk_path)
        
        optimized_size = sum(f.stat().st_size for f in optimized_files)
        
        print(f"Optimized table: {len(optimized_files)} files, {optimized_size / (1024*1024):.2f} MB")
        print(f"File count reduction: {len(fragmented_files)} → {len(optimized_files)} ({len(optimized_files)/len(fragmented_files)*100:.1f}%)")
        print(f"Size change: {fragmented_size / (1024*1024):.2f} → {optimized_size / (1024*1024):.2f} MB")
        
        # Calculate read efficiency (simplified)
        print(f"\nRead Efficiency Analysis:")
        print(f"  Fragmented: Need to read {len(fragmented_files)} files")
        print(f"  Optimized: Need to read {len(optimized_files)} files")
        print(f"  File count reduction: {len(fragmented_files)/len(optimized_files):.1f}x fewer files")
        
        return {
            "fragmented": {"files": len(fragmented_files), "size": fragmented_size},
            "optimized": {"files": len(optimized_files), "size": optimized_size}
        }
    
    def analyze_query_performance(self, docket_id: str):
        """Analyze query performance differences"""
        print(f"\n=== Query Performance Analysis for {docket_id} ===")
        
        df = self.load_comments_from_docket(docket_id)
        if df.empty:
            return
        
        # Create DuckDB connection for analysis
        con = duckdb.connect(':memory:')
        
        # Register DataFrame
        con.register('comments', df)
        
        # Test queries
        queries = [
            "SELECT COUNT(*) FROM comments",
            "SELECT agencyId, COUNT(*) FROM comments GROUP BY agencyId",
            "SELECT * FROM comments WHERE comment LIKE '%health%' LIMIT 10",
            "SELECT firstName, lastName, comment FROM comments WHERE firstName IS NOT NULL LIMIT 10",
            "SELECT docketId, COUNT(*) FROM comments GROUP BY docketId",
            "SELECT * FROM comments WHERE has_attachments = true LIMIT 5"
        ]
        
        print("Query Performance Results:")
        for i, query in enumerate(queries, 1):
            start_time = time.time()
            result = con.execute(query).fetchall()
            end_time = time.time()
            
            print(f"Query {i}: {end_time - start_time:.4f}s")
            print(f"  {query}")
            if result:
                print(f"  Result: {result[:3]}...")  # Show first 3 results
            print()
        
        con.close()
    
    def analyze_data_characteristics(self, docket_id: str):
        """Analyze data characteristics for optimization insights"""
        print(f"\n=== Data Characteristics for {docket_id} ===")
        
        df = self.load_comments_from_docket(docket_id)
        if df.empty:
            return
        
        print(f"Total rows: {len(df):,}")
        print(f"Total columns: {len(df.columns)}")
        
        # Column analysis
        print(f"\nColumn Analysis:")
        for col in df.columns:
            non_null_count = df[col].notna().sum()
            null_percentage = (len(df) - non_null_count) / len(df) * 100
            unique_count = df[col].nunique()
            
            print(f"  {col}:")
            print(f"    Non-null: {non_null_count:,} ({100-null_percentage:.1f}%)")
            print(f"    Unique values: {unique_count:,}")
            
            # Show sample values for string columns
            if df[col].dtype == 'object' and unique_count < 10:
                sample_values = df[col].dropna().unique()[:5]
                print(f"    Sample values: {sample_values}")
            print()
        
        # Size analysis by column type
        print(f"Size Analysis by Column Type:")
        for dtype in df.dtypes.unique():
            cols_of_type = df.select_dtypes(include=[dtype]).columns
            print(f"  {dtype}: {len(cols_of_type)} columns")
        
        return df


def main():
    """Main exploration function"""
    explorer = CommentDataExplorer()
    
    # Get available dockets
    dockets = [d.name for d in explorer.results_dir.iterdir() if d.is_dir()]
    print(f"Found dockets: {dockets}")
    
    # Analyze each docket
    for docket_id in dockets[:3]:  # Limit to first 3 for exploration
        print(f"\n{'='*60}")
        print(f"Analyzing docket: {docket_id}")
        print(f"{'='*60}")
        
        try:
            # Data characteristics
            df = explorer.analyze_data_characteristics(docket_id)
            
            # Storage efficiency
            storage_metrics = explorer.measure_storage_efficiency(docket_id)
            
            # Delta updates
            delta_metrics = explorer.simulate_delta_updates(docket_id, num_updates=5)
            
            # Optimization
            optimization_metrics = explorer.demonstrate_optimization(docket_id)
            
            # Query performance
            explorer.analyze_query_performance(docket_id)
            
        except Exception as e:
            print(f"Error analyzing {docket_id}: {e}")
            continue
    
    print(f"\n{'='*60}")
    print("Exploration complete!")
    print(f"Output files saved to: {explorer.output_dir}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main() 