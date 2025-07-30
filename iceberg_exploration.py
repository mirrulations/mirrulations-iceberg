#!/usr/bin/env python3
"""
Apache Iceberg Exploration for Mirrulations Comment Data

This script explores:
1. Data flattening and schema creation
2. Storage efficiency comparison (JSON vs Iceberg)
3. Delta update strategies
4. Table optimization techniques
"""

import json
import os
import glob
import pandas as pd
import duckdb
from pathlib import Path
from typing import Dict, List, Any, Optional
import time
import shutil
from dataclasses import dataclass
from datetime import datetime

# Iceberg imports
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, BooleanType, 
    TimestampType, ListType, StructType, MapType
)
from pyiceberg.table import Table
from pyiceberg.expressions import (
    And, Or, Not, AlwaysTrue, AlwaysFalse,
    IsNull, NotNull, Equal, NotEqual, LessThan, LessThanOrEqual,
    GreaterThan, GreaterThanOrEqual, In, NotIn, StartsWith, NotStartsWith
)
from pyiceberg.io import fsspec


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


class CommentDataProcessor:
    """Process and analyze comment data with Iceberg"""
    
    def __init__(self, results_dir: str = "results"):
        self.results_dir = Path(results_dir)
        self.catalog = None
        self.setup_catalog()
        
    def setup_catalog(self):
        """Initialize Iceberg catalog for local development"""
        # Use local file system for exploration
        catalog_properties = {
            "warehouse": "iceberg_warehouse",
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key": "admin",
            "s3.secret-key": "password",
        }
        
        # For local exploration, we'll use a simpler approach
        # In production, you'd use a proper catalog like AWS Glue or Nessie
        self.warehouse_path = Path("iceberg_warehouse")
        self.warehouse_path.mkdir(exist_ok=True)
        
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
            # For now, just track if there are included attachments
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
        return df
    
    def get_schema_from_dataframe(self, df: pd.DataFrame) -> Schema:
        """Create Iceberg schema from pandas DataFrame"""
        fields = []
        
        for column, dtype in df.dtypes.items():
            if dtype == 'object':
                # Check if it's a string or mixed type
                if df[column].apply(lambda x: isinstance(x, str) if pd.notna(x) else True).all():
                    fields.append(NestedField.required(column, StringType()))
                else:
                    # For mixed types, store as string
                    fields.append(NestedField.required(column, StringType()))
            elif dtype == 'int64':
                fields.append(NestedField.required(column, IntegerType()))
            elif dtype == 'bool':
                fields.append(NestedField.required(column, BooleanType()))
            elif dtype == 'datetime64[ns]':
                fields.append(NestedField.required(column, TimestampType()))
            else:
                # Default to string for unknown types
                fields.append(NestedField.required(column, StringType()))
        
        return Schema(*fields)
    
    def create_iceberg_table(self, df: pd.DataFrame, table_name: str) -> Table:
        """Create an Iceberg table from DataFrame"""
        schema = self.get_schema_from_dataframe(df)
        
        # Create table location
        table_location = self.warehouse_path / table_name
        table_location.mkdir(exist_ok=True)
        
        # For this exploration, we'll use a simplified approach
        # In production, you'd use a proper catalog
        print(f"Creating Iceberg table: {table_name}")
        print(f"Schema: {schema}")
        
        # Convert DataFrame to Arrow table for efficient writing
        arrow_table = df.to_arrow()
        
        # Write to Parquet format (Iceberg-compatible)
        parquet_path = table_location / "data.parquet"
        arrow_table.to_pandas().to_parquet(parquet_path, index=False)
        
        return table_location
    
    def measure_storage_efficiency(self, docket_id: str) -> Dict[str, StorageMetrics]:
        """Compare storage efficiency between JSON and Iceberg"""
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
        
        # Create Iceberg table and measure
        table_name = f"comments_{docket_id.lower().replace('-', '_')}"
        iceberg_location = self.create_iceberg_table(df, table_name)
        
        # Measure Iceberg storage
        iceberg_files = list(iceberg_location.rglob("*.parquet"))
        iceberg_size = sum(f.stat().st_size for f in iceberg_files)
        
        iceberg_metrics = StorageMetrics(
            format="Iceberg (Parquet)",
            total_size_bytes=iceberg_size,
            file_count=len(iceberg_files),
            compression_ratio=json_size / iceberg_size if iceberg_size > 0 else 1.0
        )
        
        # Print results
        print(f"\nStorage Comparison:")
        print(f"JSON: {json_metrics.total_size_mb:.2f} MB ({json_metrics.file_count} files)")
        print(f"Iceberg: {iceberg_metrics.total_size_mb:.2f} MB ({iceberg_metrics.file_count} files)")
        print(f"Compression ratio: {iceberg_metrics.compression_ratio:.2f}x")
        print(f"Space savings: {((json_size - iceberg_size) / json_size * 100):.1f}%")
        
        return {
            "json": json_metrics,
            "iceberg": iceberg_metrics
        }
    
    def simulate_delta_updates(self, docket_id: str, num_updates: int = 10):
        """Simulate incremental updates and measure efficiency"""
        print(f"\n=== Delta Update Simulation for {docket_id} ===")
        
        # Load existing data
        df = self.load_comments_from_docket(docket_id)
        if df.empty:
            return
        
        # Create initial table
        table_name = f"comments_{docket_id.lower().replace('-', '_')}"
        base_table_path = self.warehouse_path / table_name
        base_table_path.mkdir(exist_ok=True)
        
        # Write initial data
        initial_path = base_table_path / "initial.parquet"
        df.to_parquet(initial_path, index=False)
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
            delta_df.to_parquet(delta_path, index=False)
            
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
        
        # Simulate fragmented table
        df = self.load_comments_from_docket(docket_id)
        if df.empty:
            return
        
        table_name = f"comments_{docket_id.lower().replace('-', '_')}"
        fragmented_path = self.warehouse_path / f"{table_name}_fragmented"
        fragmented_path.mkdir(exist_ok=True)
        
        # Create fragmented table (many small files)
        total_rows = len(df)
        chunk_size = max(1, total_rows // 20)  # Split into ~20 chunks
        
        fragmented_files = []
        for i in range(0, total_rows, chunk_size):
            chunk_df = df.iloc[i:i+chunk_size]
            chunk_path = fragmented_path / f"chunk_{i//chunk_size:03d}.parquet"
            chunk_df.to_parquet(chunk_path, index=False)
            fragmented_files.append(chunk_path)
        
        fragmented_size = sum(f.stat().st_size for f in fragmented_files)
        print(f"Fragmented table: {len(fragmented_files)} files, {fragmented_size / (1024*1024):.2f} MB")
        
        # Optimize by compacting into fewer, larger files
        optimized_path = self.warehouse_path / f"{table_name}_optimized"
        optimized_path.mkdir(exist_ok=True)
        
        # Compact into 2-3 larger files
        compact_chunk_size = total_rows // 3
        optimized_files = []
        
        for i in range(0, total_rows, compact_chunk_size):
            chunk_df = df.iloc[i:i+compact_chunk_size]
            chunk_path = optimized_path / f"compact_{i//compact_chunk_size:02d}.parquet"
            chunk_df.to_parquet(chunk_path, index=False)
            optimized_files.append(chunk_path)
        
        optimized_size = sum(f.stat().st_size for f in optimized_files)
        
        print(f"Optimized table: {len(optimized_files)} files, {optimized_size / (1024*1024):.2f} MB")
        print(f"File count reduction: {len(fragmented_files)} → {len(optimized_files)} ({len(optimized_files)/len(fragmented_files)*100:.1f}%)")
        print(f"Size change: {fragmented_size / (1024*1024):.2f} → {optimized_size / (1024*1024):.2f} MB")
        
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
            "SELECT firstName, lastName, comment FROM comments WHERE firstName IS NOT NULL LIMIT 10"
        ]
        
        print("Query Performance Results:")
        for i, query in enumerate(queries, 1):
            start_time = time.time()
            result = con.execute(query).fetchall()
            end_time = time.time()
            
            print(f"Query {i}: {end_time - start_time:.4f}s")
            print(f"  {query}")
            print(f"  Result: {result[:3]}...")  # Show first 3 results
            print()
        
        con.close()


def main():
    """Main exploration function"""
    processor = CommentDataProcessor()
    
    # Get available dockets
    dockets = [d.name for d in processor.results_dir.iterdir() if d.is_dir()]
    print(f"Found dockets: {dockets}")
    
    # Analyze each docket
    for docket_id in dockets[:3]:  # Limit to first 3 for exploration
        print(f"\n{'='*60}")
        print(f"Analyzing docket: {docket_id}")
        print(f"{'='*60}")
        
        try:
            # Storage efficiency
            storage_metrics = processor.measure_storage_efficiency(docket_id)
            
            # Delta updates
            delta_metrics = processor.simulate_delta_updates(docket_id, num_updates=5)
            
            # Optimization
            optimization_metrics = processor.demonstrate_optimization(docket_id)
            
            # Query performance
            processor.analyze_query_performance(docket_id)
            
        except Exception as e:
            print(f"Error analyzing {docket_id}: {e}")
            continue
    
    print(f"\n{'='*60}")
    print("Exploration complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main() 