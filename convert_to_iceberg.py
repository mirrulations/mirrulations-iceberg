#!/usr/bin/env python3
"""
Convert Mirrulations Data to Iceberg Format

This script converts each docket into its own Iceberg dataset with three tables:
1. docket_info - Basic docket metadata
2. documents - Document information and content
3. comments - Comment data with flattened structure

Usage:
    python convert_to_iceberg.py /path/to/mirrulations/data [--s3-bucket bucket-name]
"""

import json
import os
import sys
import argparse
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import logging
from datetime import datetime

# S3 imports
import boto3
from botocore.exceptions import ClientError
import s3fs


class IcebergConverter:
    """Convert Mirrulations data to Iceberg format"""
    
    def __init__(self, data_path: str, output_path: str = None, s3_bucket: str = None):
        self.data_path = Path(data_path)
        # If no output path specified, create derived-data inside the data_path directory
        if output_path:
            self.output_path = Path(output_path).resolve()
            self.use_derived_data_subdir = True  # Need to create derived-data/ subdirectory
        else:
            self.output_path = self.data_path / "derived-data"
            self.use_derived_data_subdir = False  # Already pointing to derived-data directory
        self.s3_bucket = s3_bucket
        self.s3_client = None
        self.s3_fs = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('iceberg_conversion.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup S3 if specified
        if s3_bucket:
            self.setup_s3()
        
        # Statistics
        self.stats = {
            'dockets_processed': 0,
            'dockets_skipped': 0,
            'errors': 0,
            'start_time': time.time()
        }
    
    def setup_s3(self):
        """Setup S3 client and filesystem"""
        try:
            self.s3_client = boto3.client('s3')
            self.s3_fs = s3fs.S3FileSystem()
            self.logger.info(f"S3 setup complete for bucket: {self.s3_bucket}")
        except Exception as e:
            self.logger.error(f"Failed to setup S3: {e}")
            raise
    
    def flatten_docket_data(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten docket JSON structure"""
        data = json_data.get("data", {})
        
        flattened = {
            "id": data.get("id"),
            "type": data.get("type"),
            "link": data.get("links", {}).get("self"),
        }
        
        # Flatten attributes
        attributes = data.get("attributes", {})
        for key, value in attributes.items():
            if value is not None:
                flattened[key] = value
        
        # Handle relationships
        relationships = data.get("relationships", {})
        for rel_name, rel_data in relationships.items():
            if isinstance(rel_data, dict) and "data" in rel_data:
                flattened[f"{rel_name}_count"] = len(rel_data["data"])
        
        return flattened
    
    def flatten_document_data(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten document JSON structure"""
        data = json_data.get("data", {})
        
        flattened = {
            "id": data.get("id"),
            "type": data.get("type"),
            "link": data.get("links", {}).get("self"),
        }
        
        # Flatten attributes
        attributes = data.get("attributes", {})
        for key, value in attributes.items():
            if value is not None:
                flattened[key] = value
        
        # Handle relationships
        relationships = data.get("relationships", {})
        for rel_name, rel_data in relationships.items():
            if isinstance(rel_data, dict) and "data" in rel_data:
                flattened[f"{rel_name}_count"] = len(rel_data["data"])
        
        return flattened
    
    def flatten_comment_data(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten comment JSON structure (similar to exploration)"""
        data = json_data.get("data", {})
        
        flattened = {
            "id": data.get("id"),
            "link": data.get("links", {}).get("self"),
            "type": data.get("type"),
        }
        
        # Flatten attributes
        attributes = data.get("attributes", {})
        for key, value in attributes.items():
            if value is not None:
                flattened[key] = value
        
        # Handle relationships
        relationships = data.get("relationships", {})
        attachments = relationships.get("attachments", {})
        flattened["has_attachments"] = len(attachments.get("data", [])) > 0
        flattened["attachment_count"] = len(attachments.get("data", []))
        
        # Handle included data
        included = json_data.get("included", [])
        if included:
            flattened["has_included_attachments"] = True
            flattened["included_attachment_count"] = len(included)
        else:
            flattened["has_included_attachments"] = False
            flattened["included_attachment_count"] = 0
        
        return flattened
    
    def load_json_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Load and parse JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"Failed to load {file_path}: {e}")
            return None
    
    def process_docket(self, docket_path: Path) -> Dict[str, Any]:
        """Process a single docket directory"""
        docket_id = docket_path.name
        
        # Determine agency from docket ID (e.g., "DEA-2016-0015" -> "DEA")
        agency = docket_id.split('-')[0] if '-' in docket_id else "UNKNOWN"
        
        self.logger.info(f"Processing docket: {agency}/{docket_id}")
        
        result = {
            'docket_info': None,
            'documents': [],
            'comments': [],
            'agency': agency,
            'docket_id': docket_id
        }
        
        # Look for data in raw-data subdirectory
        raw_data_path = docket_path / "raw-data"
        if raw_data_path.exists():
            # Process docket info
            docket_file = raw_data_path / "docket" / f"{docket_id}.json"
            if docket_file.exists():
                docket_data = self.load_json_file(docket_file)
                if docket_data:
                    result['docket_info'] = self.flatten_docket_data(docket_data)
            else:
                # Try alternative docket file paths
                alt_paths = [
                    raw_data_path / "docket.json",
                    raw_data_path / f"{docket_id}.json"
                ]
                for alt_path in alt_paths:
                    if alt_path.exists():
                        docket_data = self.load_json_file(alt_path)
                        if docket_data:
                            result['docket_info'] = self.flatten_docket_data(docket_data)
                            break
            
            # Process documents
            documents_dir = raw_data_path / "documents"
            if documents_dir.exists():
                for doc_file in documents_dir.glob("*.json"):
                    doc_data = self.load_json_file(doc_file)
                    if doc_data:
                        flattened_doc = self.flatten_document_data(doc_data)
                        result['documents'].append(flattened_doc)
            
            # Process comments
            comments_dir = raw_data_path / "comments"
            if comments_dir.exists():
                for comment_file in comments_dir.glob("*.json"):
                    comment_data = self.load_json_file(comment_file)
                    if comment_data:
                        flattened_comment = self.flatten_comment_data(comment_data)
                        result['comments'].append(flattened_comment)
        
        # Also check for direct structure (fallback)
        else:
            # Process docket info
            docket_file = docket_path / "docket" / f"{docket_id}.json"
            if docket_file.exists():
                docket_data = self.load_json_file(docket_file)
                if docket_data:
                    result['docket_info'] = self.flatten_docket_data(docket_data)
            else:
                # Try alternative docket file paths
                alt_paths = [
                    docket_path / "docket.json",
                    docket_path / f"{docket_id}.json"
                ]
                for alt_path in alt_paths:
                    if alt_path.exists():
                        docket_data = self.load_json_file(alt_path)
                        if docket_data:
                            result['docket_info'] = self.flatten_docket_data(docket_data)
                            break
            
            # Process documents
            documents_dir = docket_path / "documents"
            if documents_dir.exists():
                for doc_file in documents_dir.glob("*.json"):
                    doc_data = self.load_json_file(doc_file)
                    if doc_data:
                        flattened_doc = self.flatten_document_data(doc_data)
                        result['documents'].append(flattened_doc)
            
            # Process comments
            comments_dir = docket_path / "comments"
            if comments_dir.exists():
                for comment_file in comments_dir.glob("*.json"):
                    comment_data = self.load_json_file(comment_file)
                    if comment_data:
                        flattened_comment = self.flatten_comment_data(comment_data)
                        result['comments'].append(flattened_comment)
        
        return result
    
    def save_to_parquet(self, data: List[Dict[str, Any]], table_name: str, 
                       output_dir: Path, compression: str = 'snappy') -> bool:
        """Save data to Parquet format"""
        if not data:
            return False
        
        try:
            df = pd.DataFrame(data)
            parquet_path = output_dir / f"{table_name}.parquet"
            
            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save to Parquet
            df.to_parquet(parquet_path, index=False, compression=compression)
            
            # Upload to S3 if configured
            if self.s3_bucket and self.s3_fs:
                s3_path = f"s3://{self.s3_bucket}/{parquet_path}"
                self.s3_fs.put(str(parquet_path), s3_path)
                self.logger.info(f"Uploaded to S3: {s3_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save {table_name}: {e}")
            return False
    
    def save_docket_dataset(self, docket_data: Dict[str, Any]) -> bool:
        """Save all tables for a docket"""
        agency = docket_data['agency']
        docket_id = docket_data['docket_id']
        
        # Create output directory - handle both cases
        if self.use_derived_data_subdir:
            # When output_path is specified, create derived-data/agency/docket_id/iceberg
            output_dir = self.output_path / "derived-data" / agency / docket_id / "iceberg"
        else:
            # When using default, already pointing to derived-data, so just agency/docket_id/iceberg
            output_dir = self.output_path / agency / docket_id / "iceberg"
        
        success = True
        
        # Save docket info
        if docket_data['docket_info']:
            success &= self.save_to_parquet([docket_data['docket_info']], 'docket_info', output_dir)
        
        # Save documents
        if docket_data['documents']:
            success &= self.save_to_parquet(docket_data['documents'], 'documents', output_dir)
        
        # Save comments
        if docket_data['comments']:
            success &= self.save_to_parquet(docket_data['comments'], 'comments', output_dir)
        
        return success
    
    def get_docket_directories(self) -> List[Path]:
        """Get all docket directories from the data path"""
        dockets = []
        
        # Look for raw-data structure (Mirrulations format)
        raw_data_path = self.data_path / "raw-data"
        if raw_data_path.exists():
            for agency_dir in raw_data_path.iterdir():
                if agency_dir.is_dir():
                    for docket_dir in agency_dir.iterdir():
                        if docket_dir.is_dir():
                            dockets.append(docket_dir)
        
        # Look for direct docket structure (like in results/)
        else:
            for docket_dir in self.data_path.iterdir():
                if docket_dir.is_dir() and not docket_dir.name.startswith('.'):
                    # Check if this looks like a docket directory
                    if (docket_dir / "raw-data").exists() or (docket_dir / "docket").exists():
                        dockets.append(docket_dir)
        
        # Filter out non-docket directories and sort
        filtered_dockets = []
        for docket in dockets:
            # Skip derived-data and other non-docket directories
            if docket.name not in ['derived-data', 'raw-data']:
                filtered_dockets.append(docket)
        
        return sorted(filtered_dockets)
    
    def print_progress(self, current: int, total: int, start_time: float):
        """Print progress information"""
        elapsed = time.time() - start_time
        if current > 0:
            rate = current / elapsed
            eta = (total - current) / rate if rate > 0 else 0
            
            self.logger.info(
                f"Progress: {current}/{total} ({current/total*100:.1f}%) "
                f"Rate: {rate:.2f} dockets/sec "
                f"Elapsed: {elapsed/3600:.1f}h "
                f"ETA: {eta/3600:.1f}h"
            )
    
    def convert_all(self):
        """Convert all dockets to Iceberg format"""
        self.logger.info(f"Starting conversion from: {self.data_path}")
        self.logger.info(f"Output path: {self.output_path}")
        if self.s3_bucket:
            self.logger.info(f"S3 bucket: {self.s3_bucket}")
        
        # Get all docket directories
        docket_dirs = self.get_docket_directories()
        total_dockets = len(docket_dirs)
        
        self.logger.info(f"Found {total_dockets} dockets to process")
        
        if total_dockets == 0:
            self.logger.error("No dockets found. Check the data path structure.")
            return
        
        start_time = time.time()
        
        # Process each docket
        for i, docket_dir in enumerate(tqdm(docket_dirs, desc="Converting dockets")):
            try:
                # Process docket
                docket_data = self.process_docket(docket_dir)
                
                # Save dataset
                if self.save_docket_dataset(docket_data):
                    self.stats['dockets_processed'] += 1
                else:
                    self.stats['dockets_skipped'] += 1
                
                # Print progress every 100 dockets
                if (i + 1) % 100 == 0:
                    self.print_progress(i + 1, total_dockets, start_time)
                
            except Exception as e:
                self.logger.error(f"Error processing {docket_dir}: {e}")
                self.stats['errors'] += 1
                continue
        
        # Final statistics
        total_time = time.time() - start_time
        self.logger.info(f"\nConversion complete!")
        self.logger.info(f"Total time: {total_time/3600:.2f} hours")
        self.logger.info(f"Dockets processed: {self.stats['dockets_processed']}")
        self.logger.info(f"Dockets skipped: {self.stats['dockets_skipped']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info(f"Average rate: {self.stats['dockets_processed']/total_time:.2f} dockets/sec")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Convert Mirrulations data to Iceberg format")
    parser.add_argument("data_path", help="Path to Mirrulations data directory")
    parser.add_argument("--output-path", help="Output directory for Iceberg data")
    parser.add_argument("--s3-bucket", help="S3 bucket name for uploading results")
    parser.add_argument("--compression", default="snappy", 
                       choices=["snappy", "gzip", "brotli", "lz4"],
                       help="Compression algorithm for Parquet files")
    
    args = parser.parse_args()
    
    # Validate data path
    if not Path(args.data_path).exists():
        print(f"Error: Data path does not exist: {args.data_path}")
        sys.exit(1)
    
    # Create converter
    converter = IcebergConverter(
        data_path=args.data_path,
        output_path=args.output_path,
        s3_bucket=args.s3_bucket
    )
    
    # Run conversion
    try:
        converter.convert_all()
    except KeyboardInterrupt:
        print("\nConversion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Conversion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
