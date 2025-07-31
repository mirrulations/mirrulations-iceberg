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
    
    def __init__(self, data_path: str, output_path: str = None, s3_bucket: str = None, debug: bool = False):
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
        log_level = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(
            level=log_level,
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
        except PermissionError as e:
            self.logger.error(f"Permission denied reading {file_path}: {e}")
            raise
        except FileNotFoundError as e:
            self.logger.warning(f"File not found: {file_path}")
            return None
        except Exception as e:
            self.logger.warning(f"Failed to load {file_path}: {e}")
            return None
    
    def process_docket(self, docket_path: Path) -> Dict[str, Any]:
        """Process a single docket directory"""
        docket_id = docket_path.name
        
        # Determine agency from docket ID (e.g., "DEA-2016-0015" -> "DEA")
        # Handle cases like "ACF/ACF-2024-0005" -> "ACF"
        if '/' in docket_id:
            agency = docket_id.split('/')[0]
        elif '-' in docket_id:
            agency = docket_id.split('-')[0]
        else:
            agency = "UNKNOWN"
        
        self.logger.info(f"Processing docket: {agency}/{docket_id}")
        
        result = {
            'docket_info': None,
            'documents': [],
            'comments': [],
            'agency': agency,
            'docket_id': docket_id
        }
        
        # Debug: Check what directories exist
        self.logger.debug(f"  Docket path: {docket_path}")
        if docket_path.exists():
            contents = [d.name for d in docket_path.iterdir() if d.is_dir()]
            self.logger.debug(f"  Contents: {contents}")
            # Also log if we find text-* directories
            text_dirs = [d.name for d in docket_path.iterdir() if d.is_dir() and d.name.startswith('text-')]
            if text_dirs:
                self.logger.debug(f"  Found text-* directories: {text_dirs}")
        else:
            self.logger.warning(f"  Docket path does not exist: {docket_path}")
            return result
        
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
                
                # NEW: Try the text-* subdirectory structure
                if result['docket_info'] is None:
                    text_subdirs = [d for d in raw_data_path.iterdir() if d.is_dir() and d.name.startswith('text-')]
                    if text_subdirs:
                        self.logger.debug(f"  Trying text-* subdirectories: {[d.name for d in text_subdirs]}")
                    for text_subdir in text_subdirs:
                        docket_file = text_subdir / "docket" / f"{docket_id}.json"
                        self.logger.debug(f"  Checking for docket file: {docket_file}")
                        if docket_file.exists():
                            self.logger.debug(f"  Found docket file: {docket_file}")
                            docket_data = self.load_json_file(docket_file)
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
            
            # NEW: Try the text-* subdirectory structure for documents
            if not result['documents']:
                text_subdirs = [d for d in raw_data_path.iterdir() if d.is_dir() and d.name.startswith('text-')]
                if text_subdirs:
                    self.logger.debug(f"  Trying text-* subdirectories for documents: {[d.name for d in text_subdirs]}")
                for text_subdir in text_subdirs:
                    documents_dir = text_subdir / "documents"
                    if documents_dir.exists():
                        self.logger.debug(f"  Found documents directory: {documents_dir}")
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
            
            # NEW: Try the text-* subdirectory structure for comments
            if not result['comments']:
                text_subdirs = [d for d in raw_data_path.iterdir() if d.is_dir() and d.name.startswith('text-')]
                if text_subdirs:
                    self.logger.debug(f"  Trying text-* subdirectories for comments: {[d.name for d in text_subdirs]}")
                for text_subdir in text_subdirs:
                    comments_dir = text_subdir / "comments"
                    if comments_dir.exists():
                        self.logger.debug(f"  Found comments directory: {comments_dir}")
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
                
                # NEW: Try the text-* subdirectory structure
                if result['docket_info'] is None:
                    text_subdirs = [d for d in docket_path.iterdir() if d.is_dir() and d.name.startswith('text-')]
                    for text_subdir in text_subdirs:
                        docket_file = text_subdir / "docket" / f"{docket_id}.json"
                        if docket_file.exists():
                            docket_data = self.load_json_file(docket_file)
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
            
            # NEW: Try the text-* subdirectory structure for documents
            if not result['documents']:
                text_subdirs = [d for d in docket_path.iterdir() if d.is_dir() and d.name.startswith('text-')]
                for text_subdir in text_subdirs:
                    documents_dir = text_subdir / "documents"
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
            
            # NEW: Try the text-* subdirectory structure for comments
            if not result['comments']:
                text_subdirs = [d for d in docket_path.iterdir() if d.is_dir() and d.name.startswith('text-')]
                for text_subdir in text_subdirs:
                    comments_dir = text_subdir / "comments"
                    if comments_dir.exists():
                        for comment_file in comments_dir.glob("*.json"):
                            comment_data = self.load_json_file(comment_file)
                            if comment_data:
                                flattened_comment = self.flatten_comment_data(comment_data)
                                result['comments'].append(flattened_comment)
        
        # Debug: Log what data was found
        self.logger.debug(f"  Found: docket_info={result['docket_info'] is not None}, "
                         f"documents={len(result['documents'])}, comments={len(result['comments'])}")
        
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
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except PermissionError as e:
                self.logger.error(f"Permission denied creating directory {output_dir}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Failed to create directory {output_dir}: {e}")
                raise
            
            # Save to Parquet
            try:
                df.to_parquet(parquet_path, index=False, compression=compression)
            except PermissionError as e:
                self.logger.error(f"Permission denied writing to {parquet_path}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Failed to save {table_name} to {parquet_path}: {e}")
                raise
            
            # Upload to S3 if configured
            if self.s3_bucket and self.s3_fs:
                try:
                    s3_path = f"s3://{self.s3_bucket}/{parquet_path}"
                    self.s3_fs.put(str(parquet_path), s3_path)
                    self.logger.info(f"Uploaded to S3: {s3_path}")
                except Exception as e:
                    self.logger.error(f"Failed to upload to S3: {e}")
                    # Don't raise here - S3 upload failure shouldn't stop the process
            
            return True
            
        except (PermissionError, OSError) as e:
            self.logger.error(f"Failed to save {table_name}: {e}")
            raise
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
        files_created = 0
        
        # Save docket info
        if docket_data['docket_info']:
            if self.save_to_parquet([docket_data['docket_info']], 'docket_info', output_dir):
                files_created += 1
            else:
                success = False
        
        # Save documents
        if docket_data['documents']:
            if self.save_to_parquet(docket_data['documents'], 'documents', output_dir):
                files_created += 1
            else:
                success = False
        
        # Save comments
        if docket_data['comments']:
            if self.save_to_parquet(docket_data['comments'], 'comments', output_dir):
                files_created += 1
            else:
                success = False
        
        # Log if no files were created
        if files_created == 0:
            self.logger.warning(f"No data found for docket {docket_id} - no files created")
            return False
        
        return success
    
    def get_docket_directories(self) -> List[Path]:
        """Get all docket directories from the data path"""
        dockets = []
        
        # Look for raw-data structure (Mirrulations format)
        raw_data_path = self.data_path / "raw-data"
        if raw_data_path.exists():
            self.logger.debug(f"Found raw-data structure at {raw_data_path}")
            for agency_dir in raw_data_path.iterdir():
                if agency_dir.is_dir():
                    self.logger.debug(f"  Agency: {agency_dir.name}")
                    for docket_dir in agency_dir.iterdir():
                        if docket_dir.is_dir():
                            dockets.append(docket_dir)
                            self.logger.debug(f"    Docket: {docket_dir.name}")
        
        # Look for direct docket structure (like in results/)
        else:
            self.logger.debug(f"No raw-data structure, looking for direct docket structure")
            for docket_dir in self.data_path.iterdir():
                if docket_dir.is_dir() and not docket_dir.name.startswith('.'):
                    # Check if this looks like a docket directory
                    if (docket_dir / "raw-data").exists() or (docket_dir / "docket").exists():
                        dockets.append(docket_dir)
                        self.logger.debug(f"  Found docket: {docket_dir.name}")
        
        # Filter out non-docket directories and sort
        filtered_dockets = []
        for docket in dockets:
            # Skip derived-data and other non-docket directories
            if docket.name not in ['derived-data', 'raw-data']:
                filtered_dockets.append(docket)
        
        self.logger.info(f"Found {len(filtered_dockets)} docket directories")
        return sorted(filtered_dockets)
    
    def check_permissions(self):
        """Check read/write permissions before starting conversion"""
        self.logger.info("Checking permissions...")
        
        # Check if data directory exists and is readable
        if not self.data_path.exists():
            raise FileNotFoundError(f"Data directory does not exist: {self.data_path}")
        
        if not os.access(self.data_path, os.R_OK):
            raise PermissionError(f"No read permission for data directory: {self.data_path}")
        
        # Check if we can list the contents
        try:
            list(self.data_path.iterdir())
        except PermissionError as e:
            raise PermissionError(f"Cannot list contents of data directory {self.data_path}: {e}")
        
        # Check output directory permissions
        if self.output_path.exists():
            if not os.access(self.output_path, os.W_OK):
                raise PermissionError(f"No write permission for output directory: {self.output_path}")
        else:
            # Try to create the output directory
            try:
                self.output_path.mkdir(parents=True, exist_ok=True)
                # Test write permission by creating a temporary file
                test_file = self.output_path / ".test_write_permission"
                test_file.write_text("test")
                test_file.unlink()
            except (PermissionError, OSError) as e:
                raise PermissionError(f"Cannot create or write to output directory {self.output_path}: {e}")
        
        self.logger.info("Permission check passed.")
    
    def __init__(self, data_path: str, output_path: str = None, s3_bucket: str = None, debug: bool = False, verbose: bool = False):
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
        self.verbose = verbose
        
        # Setup logging
        log_level = logging.DEBUG if debug else (logging.INFO if verbose else logging.WARNING)
        logging.basicConfig(
            level=log_level,
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
    

    
    def convert_all(self):
        """Convert all dockets to Iceberg format"""
        if self.verbose:
            self.logger.info(f"Starting conversion from: {self.data_path}")
            self.logger.info(f"Output path: {self.output_path}")
            if self.s3_bucket:
                self.logger.info(f"S3 bucket: {self.s3_bucket}")
        
        # Check permissions before starting
        try:
            self.check_permissions()
        except (PermissionError, OSError) as e:
            self.logger.error(f"Permission check failed: {e}")
            self.logger.error("Please ensure you have read access to the data directory and write access to the output directory.")
            return
        
        # Get all docket directories
        docket_dirs = self.get_docket_directories()
        total_dockets = len(docket_dirs)
        
        if self.verbose:
            self.logger.info(f"Found {total_dockets} dockets to process")
        
        if total_dockets == 0:
            self.logger.error("No dockets found. Check the data path structure.")
            return
        
        start_time = time.time()
        
        # Process each docket with tqdm progress bar
        with tqdm(docket_dirs, desc="Converting dockets", unit="docket") as pbar:
            for docket_dir in pbar:
                try:
                    # Update progress bar description with current docket
                    docket_name = docket_dir.name
                    pbar.set_description(f"Converting {docket_name}")
                    
                    # Process docket
                    docket_data = self.process_docket(docket_dir)
                    
                    # Save dataset
                    if self.save_docket_dataset(docket_data):
                        self.stats['dockets_processed'] += 1
                    else:
                        self.stats['dockets_skipped'] += 1
                    
                except (PermissionError, OSError) as e:
                    self.logger.error(f"Permission error processing {docket_dir}: {e}")
                    self.logger.error("Stopping conversion due to permission issues.")
                    self.stats['errors'] += 1
                    break
                except Exception as e:
                    self.logger.error(f"Error processing {docket_dir}: {e}")
                    self.stats['errors'] += 1
                    continue
        
        # Final statistics
        total_time = time.time() - start_time
        print(f"\nConversion complete!")
        print(f"Total time: {total_time/3600:.2f} hours")
        print(f"Dockets processed: {self.stats['dockets_processed']}")
        print(f"Dockets skipped: {self.stats['dockets_skipped']}")
        print(f"Errors: {self.stats['errors']}")
        if self.stats['dockets_processed'] > 0:
            print(f"Average rate: {self.stats['dockets_processed']/total_time:.2f} dockets/sec")
        
        # Check if we had permission errors
        if self.stats['errors'] > 0:
            self.logger.error("Conversion completed with errors. Check the log for details.")
            return False
        else:
            print("Conversion completed successfully!")
            return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Convert Mirrulations data to Iceberg format")
    parser.add_argument("data_path", help="Path to Mirrulations data directory")
    parser.add_argument("--output-path", help="Output directory for Iceberg data")
    parser.add_argument("--s3-bucket", help="S3 bucket name for uploading results")
    parser.add_argument("--compression", default="snappy", 
                       choices=["snappy", "gzip", "brotli", "lz4"],
                       help="Compression algorithm for Parquet files")
    parser.add_argument("--debug", action="store_true", 
                       help="Enable debug logging for troubleshooting")
    parser.add_argument("--verbose", action="store_true", 
                       help="Enable verbose INFO output")
    
    args = parser.parse_args()
    
    # Validate data path
    if not Path(args.data_path).exists():
        print(f"Error: Data path does not exist: {args.data_path}")
        sys.exit(1)
    
    # Create converter
    converter = IcebergConverter(
        data_path=args.data_path,
        output_path=args.output_path,
        s3_bucket=args.s3_bucket,
        debug=args.debug,
        verbose=args.verbose
    )
    
    # Run conversion
    try:
        success = converter.convert_all()
        if not success:
            print("Conversion completed with errors. Check the log for details.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nConversion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Conversion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
