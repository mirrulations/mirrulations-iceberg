#!/usr/bin/env python3
"""
Convert Mirrulations Data to Iceberg Format

This script converts each docket into its own Iceberg dataset with three tables:
1. docket_info - Basic docket metadata
2. documents - Document information and content
3. comments - Comment data with flattened structure

Usage:
    python convert_to_iceberg.py /path/to/mirrulations/data [--output-path /path/to/output]
    python convert_to_iceberg.py s3://bucket/mirrulations [--output-path s3://bucket/output]
"""

import json
import os
import sys
import argparse
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import logging
from datetime import datetime
import re

# S3 imports
import boto3
from botocore.exceptions import ClientError
import s3fs


class PathHandler:
    """Handle both local and S3 paths"""
    
    @staticmethod
    def is_s3_path(path: str) -> bool:
        """Check if a path is an S3 path"""
        return path.startswith('s3://')
    
    @staticmethod
    def parse_s3_path(s3_path: str) -> tuple[str, str]:
        """Parse S3 path into bucket and key"""
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Not an S3 path: {s3_path}")
        
        # Remove s3:// prefix
        path_without_prefix = s3_path[5:]
        
        # Split into bucket and key
        if '/' in path_without_prefix:
            bucket = path_without_prefix.split('/')[0]
            key = '/'.join(path_without_prefix.split('/')[1:])
        else:
            bucket = path_without_prefix
            key = ""
        
        return bucket, key
    
    @staticmethod
    def join_s3_path(bucket: str, key: str) -> str:
        """Join bucket and key into S3 path"""
        if key:
            return f"s3://{bucket}/{key}"
        else:
            return f"s3://{bucket}"
    
    @staticmethod
    def get_parent_path(path: str) -> str:
        """Get parent path for both local and S3 paths"""
        if PathHandler.is_s3_path(path):
            bucket, key = PathHandler.parse_s3_path(path)
            if '/' in key:
                parent_key = '/'.join(key.split('/')[:-1])
                return PathHandler.join_s3_path(bucket, parent_key)
            else:
                return f"s3://{bucket}"
        else:
            return str(Path(path).parent)
    
    @staticmethod
    def get_name(path: str) -> str:
        """Get name component for both local and S3 paths"""
        if PathHandler.is_s3_path(path):
            bucket, key = PathHandler.parse_s3_path(path)
            if key:
                return key.split('/')[-1]
            else:
                return bucket
        else:
            return Path(path).name


class IcebergConverter:
    """Convert Mirrulations data to Iceberg format"""
    
    def __init__(self, data_path: str, output_path: str = None, debug: bool = False, verbose: bool = False, comment_threshold: int = 10):
        self.data_path = data_path
        self.is_s3_source = PathHandler.is_s3_path(data_path)
        
        # If no output path specified, create derived-data inside the data_path directory
        if output_path:
            self.output_path = output_path
            self.use_derived_data_subdir = True  # Need to create derived-data/ subdirectory
        else:
            if self.is_s3_source:
                # For S3, append derived-data to the path
                self.output_path = f"{data_path}/derived-data"
            else:
                # For local, use Path to handle the join
                self.output_path = str(Path(data_path) / "derived-data")
            self.use_derived_data_subdir = False  # Already pointing to derived-data directory
        
        self.is_s3_target = PathHandler.is_s3_path(self.output_path)
        self.verbose = verbose
        self.comment_threshold = comment_threshold
        
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
        
        # Setup S3 filesystem if needed
        self.s3_fs = None
        if self.is_s3_source or self.is_s3_target:
            self.setup_s3()
        
        # Statistics
        self.stats = {
            'dockets_processed': 0,
            'dockets_skipped': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Filters for processing specific agencies or dockets
        self.agency_filter = None
        self.docket_pattern_filter = None
    
    def setup_s3(self):
        """Setup S3 filesystem"""
        try:
            self.s3_fs = s3fs.S3FileSystem()
            self.logger.info("S3 filesystem setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup S3 filesystem: {e}")
            raise
    
    def list_directory(self, path: str) -> List[str]:
        """List directory contents for both local and S3 paths"""
        if PathHandler.is_s3_path(path):
            if not self.s3_fs:
                raise RuntimeError("S3 filesystem not initialized")
            
            try:
                # Ensure path ends with / for directory listing
                if not path.endswith('/'):
                    path += '/'
                
                # List contents
                contents = []
                for item in self.s3_fs.ls(path):
                    # For S3, the items returned are full paths, so we need to extract the relative part
                    # Remove the path prefix to get relative names
                    if item.startswith(path):
                        relative_name = item[len(path):].rstrip('/')
                        if relative_name and '/' not in relative_name:  # Only immediate children
                            contents.append(relative_name)
                    else:
                        # If the item doesn't start with our path, it might be a different format
                        # Try to extract just the filename
                        item_name = PathHandler.get_name(item)
                        if item_name and item_name not in contents:
                            contents.append(item_name)
                
                return contents
            except Exception as e:
                self.logger.error(f"Failed to list S3 directory {path}: {e}")
                return []
        else:
            try:
                return [item.name for item in Path(path).iterdir()]
            except Exception as e:
                self.logger.error(f"Failed to list local directory {path}: {e}")
                return []
    
    def path_exists(self, path: str) -> bool:
        """Check if path exists for both local and S3 paths"""
        if PathHandler.is_s3_path(path):
            if not self.s3_fs:
                return False
            
            try:
                return self.s3_fs.exists(path)
            except Exception as e:
                self.logger.error(f"Failed to check S3 path {path}: {e}")
                return False
        else:
            return Path(path).exists()
    
    def is_directory(self, path: str) -> bool:
        """Check if path is a directory for both local and S3 paths"""
        if PathHandler.is_s3_path(path):
            if not self.s3_fs:
                return False
            
            try:
                # For S3, check if it's a directory by listing it
                return self.s3_fs.isdir(path)
            except Exception as e:
                self.logger.error(f"Failed to check if S3 path is directory {path}: {e}")
                return False
        else:
            return Path(path).is_dir()
    
    def join_paths(self, base: str, *parts: str) -> str:
        """Join paths for both local and S3 paths"""
        if PathHandler.is_s3_path(base):
            # For S3, manually join with /
            result = base.rstrip('/')
            for part in parts:
                result += '/' + part.lstrip('/')
            return result
        else:
            # For local, use Path
            return str(Path(base).joinpath(*parts))
    
    def load_json_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Load and parse JSON file from both local and S3 paths"""
        try:
            if PathHandler.is_s3_path(file_path):
                if not self.s3_fs:
                    raise RuntimeError("S3 filesystem not initialized")
                
                self.logger.debug(f"Reading S3 file: {file_path}")
                with self.s3_fs.open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    self.logger.debug(f"Read {len(content)} characters from {file_path}")
                    return json.loads(content)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except PermissionError as e:
            self.logger.error(f"Permission denied reading {file_path}: {e}")
            raise
        except FileNotFoundError as e:
            self.logger.warning(f"File not found: {file_path}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error reading {file_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to load {file_path}: {e}")
            return None
    
    def glob_files(self, directory: str, pattern: str) -> List[str]:
        """Glob files for both local and S3 paths"""
        if PathHandler.is_s3_path(directory):
            if not self.s3_fs:
                return []
            
            try:
                # Convert glob pattern to regex for S3
                regex_pattern = pattern.replace('*', '.*')
                files = []
                
                self.logger.debug(f"Globbing S3 directory: {directory} with pattern: {pattern}")
                directory_contents = self.s3_fs.ls(directory)
                self.logger.debug(f"Found {len(directory_contents)} items in {directory}")
                
                for item in directory_contents:
                    item_name = PathHandler.get_name(item)
                    self.logger.debug(f"  Checking item: {item_name} against pattern: {regex_pattern}")
                    if re.match(regex_pattern, item_name):
                        files.append(item)
                        self.logger.debug(f"    ✓ Matched: {item}")
                    else:
                        self.logger.debug(f"    ✗ No match: {item}")
                
                self.logger.debug(f"Glob result: {len(files)} files matched pattern")
                return files
            except Exception as e:
                self.logger.error(f"Failed to glob S3 files {directory}/{pattern}: {e}")
                return []
        else:
            try:
                return [str(f) for f in Path(directory).glob(pattern)]
            except Exception as e:
                self.logger.error(f"Failed to glob local files {directory}/{pattern}: {e}")
                return []
    
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
    
    def process_docket(self, docket_path: str, outer_pbar=None) -> Dict[str, Any]:
        """Process a single docket directory"""
        docket_id = PathHandler.get_name(docket_path)
        
        # Determine agency from docket ID (e.g., "DEA-2016-0015" -> "DEA")
        # Handle cases like "ACF/ACF-2024-0005" -> "ACF"
        if '/' in docket_id:
            agency = docket_id.split('/')[0]
        elif '-' in docket_id:
            agency = docket_id.split('-')[0]
        else:
            agency = "UNKNOWN"
        
        if self.verbose:
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
        if self.path_exists(docket_path):
            contents = self.list_directory(docket_path)
            self.logger.debug(f"  Contents: {contents}")
            # Also log if we find text-* directories
            text_dirs = [d for d in contents if d.startswith('text-')]
            if text_dirs:
                self.logger.debug(f"  Found text-* directories: {text_dirs}")
        else:
            self.logger.warning(f"  Docket path does not exist: {docket_path}")
            return result
        
        # Look for data in raw-data subdirectory
        raw_data_path = self.join_paths(docket_path, "raw-data")
        if self.path_exists(raw_data_path):
            # Process docket info
            docket_file = self.join_paths(raw_data_path, "docket", f"{docket_id}.json")
            if self.path_exists(docket_file):
                docket_data = self.load_json_file(docket_file)
                if docket_data:
                    result['docket_info'] = self.flatten_docket_data(docket_data)
            else:
                # Try alternative docket file paths
                alt_paths = [
                    self.join_paths(raw_data_path, "docket.json"),
                    self.join_paths(raw_data_path, f"{docket_id}.json")
                ]
                for alt_path in alt_paths:
                    if self.path_exists(alt_path):
                        docket_data = self.load_json_file(alt_path)
                        if docket_data:
                            result['docket_info'] = self.flatten_docket_data(docket_data)
                            break
                
                # NEW: Try the text-* subdirectory structure
                if result['docket_info'] is None:
                    text_subdirs = [d for d in self.list_directory(raw_data_path) if d.startswith('text-')]
                    if text_subdirs:
                        self.logger.debug(f"  Trying text-* subdirectories: {[d for d in text_subdirs]}")
                    for text_subdir in text_subdirs:
                        docket_file = self.join_paths(self.join_paths(raw_data_path, text_subdir), "docket", f"{docket_id}.json")
                        self.logger.debug(f"  Checking for docket file: {docket_file}")
                        if self.path_exists(docket_file):
                            self.logger.debug(f"  Found docket file: {docket_file}")
                            docket_data = self.load_json_file(docket_file)
                            if docket_data:
                                result['docket_info'] = self.flatten_docket_data(docket_data)
                                break
            
            # Process documents
            documents_dir = self.join_paths(raw_data_path, "documents")
            if self.path_exists(documents_dir):
                self.logger.debug(f"  Found documents directory: {documents_dir}")
                doc_files = self.glob_files(documents_dir, "*.json")
                self.logger.debug(f"  Found {len(doc_files)} document files")
                for doc_file in doc_files:
                    self.logger.debug(f"    Processing document: {doc_file}")
                    doc_data = self.load_json_file(doc_file)
                    if doc_data:
                        flattened_doc = self.flatten_document_data(doc_data)
                        result['documents'].append(flattened_doc)
                        self.logger.debug(f"    Successfully processed document: {PathHandler.get_name(doc_file)}")
                    else:
                        self.logger.warning(f"    Failed to load document: {doc_file}")
            
            # NEW: Try the text-* subdirectory structure for documents
            if not result['documents']:
                text_subdirs = [d for d in self.list_directory(raw_data_path) if d.startswith('text-')]
                if text_subdirs:
                    self.logger.debug(f"  Trying text-* subdirectories for documents: {[d for d in text_subdirs]}")
                for text_subdir in text_subdirs:
                    documents_dir = self.join_paths(raw_data_path, text_subdir, "documents")
                    if self.path_exists(documents_dir):
                        self.logger.debug(f"  Found documents directory: {documents_dir}")
                        doc_files = self.glob_files(documents_dir, "*.json")
                        self.logger.debug(f"  Found {len(doc_files)} document files in {text_subdir}")
                        for doc_file in doc_files:
                            self.logger.debug(f"    Processing document: {doc_file}")
                            doc_data = self.load_json_file(doc_file)
                            if doc_data:
                                flattened_doc = self.flatten_document_data(doc_data)
                                result['documents'].append(flattened_doc)
                                self.logger.debug(f"    Successfully processed document: {PathHandler.get_name(doc_file)}")
                            else:
                                self.logger.warning(f"    Failed to load document: {doc_file}")
            
            # Process comments
            comments_dir = self.join_paths(raw_data_path, "comments")
            if self.path_exists(comments_dir):
                comment_files = self.glob_files(comments_dir, "*.json")
                if len(comment_files) > self.comment_threshold and outer_pbar:  # Only show nested bar for dockets with many comments
                    with tqdm(comment_files, desc=f"  Processing {len(comment_files)} comments", 
                             leave=False, position=1) as comment_pbar:
                        for comment_file in comment_pbar:
                            comment_data = self.load_json_file(comment_file)
                            if comment_data:
                                flattened_comment = self.flatten_comment_data(comment_data)
                                result['comments'].append(flattened_comment)
                else:
                    for comment_file in comment_files:
                        comment_data = self.load_json_file(comment_file)
                        if comment_data:
                            flattened_comment = self.flatten_comment_data(comment_data)
                            result['comments'].append(flattened_comment)
            
            # NEW: Try the text-* subdirectory structure for comments
            if not result['comments']:
                text_subdirs = [d for d in self.list_directory(raw_data_path) if d.startswith('text-')]
                if text_subdirs:
                    self.logger.debug(f"  Trying text-* subdirectories for comments: {[d for d in text_subdirs]}")
                for text_subdir in text_subdirs:
                    comments_dir = self.join_paths(raw_data_path, text_subdir, "comments")
                    if self.path_exists(comments_dir):
                        self.logger.debug(f"  Found comments directory: {comments_dir}")
                        comment_files = self.glob_files(comments_dir, "*.json")
                        if len(comment_files) > self.comment_threshold and outer_pbar:  # Only show nested bar for dockets with many comments
                            with tqdm(comment_files, desc=f"  Processing {len(comment_files)} comments", 
                                     leave=False, position=1) as comment_pbar:
                                for comment_file in comment_pbar:
                                    comment_data = self.load_json_file(comment_file)
                                    if comment_data:
                                        flattened_comment = self.flatten_comment_data(comment_data)
                                        result['comments'].append(flattened_comment)
                        else:
                            for comment_file in comment_files:
                                comment_data = self.load_json_file(comment_file)
                                if comment_data:
                                    flattened_comment = self.flatten_comment_data(comment_data)
                                    result['comments'].append(flattened_comment)
        
        # Also check for direct structure (fallback)
        else:
            # Process docket info
            docket_file = self.join_paths(docket_path, "docket", f"{docket_id}.json")
            if self.path_exists(docket_file):
                docket_data = self.load_json_file(docket_file)
                if docket_data:
                    result['docket_info'] = self.flatten_docket_data(docket_data)
            else:
                # Try alternative docket file paths
                alt_paths = [
                    self.join_paths(docket_path, "docket.json"),
                    self.join_paths(docket_path, f"{docket_id}.json")
                ]
                for alt_path in alt_paths:
                    if self.path_exists(alt_path):
                        docket_data = self.load_json_file(alt_path)
                        if docket_data:
                            result['docket_info'] = self.flatten_docket_data(docket_data)
                            break
                
                # NEW: Try the text-* subdirectory structure
                if result['docket_info'] is None:
                    text_subdirs = [d for d in self.list_directory(docket_path) if d.startswith('text-')]
                    for text_subdir in text_subdirs:
                        docket_file = self.join_paths(self.join_paths(docket_path, text_subdir), "docket", f"{docket_id}.json")
                        if self.path_exists(docket_file):
                            docket_data = self.load_json_file(docket_file)
                            if docket_data:
                                result['docket_info'] = self.flatten_docket_data(docket_data)
                                break
            
            # Process documents
            documents_dir = self.join_paths(docket_path, "documents")
            if self.path_exists(documents_dir):
                for doc_file in self.glob_files(documents_dir, "*.json"):
                    doc_data = self.load_json_file(doc_file)
                    if doc_data:
                        flattened_doc = self.flatten_document_data(doc_data)
                        result['documents'].append(flattened_doc)
            
            # NEW: Try the text-* subdirectory structure for documents
            if not result['documents']:
                text_subdirs = [d for d in self.list_directory(docket_path) if d.startswith('text-')]
                for text_subdir in text_subdirs:
                    documents_dir = self.join_paths(docket_path, text_subdir, "documents")
                    if self.path_exists(documents_dir):
                        for doc_file in self.glob_files(documents_dir, "*.json"):
                            doc_data = self.load_json_file(doc_file)
                            if doc_data:
                                flattened_doc = self.flatten_document_data(doc_data)
                                result['documents'].append(flattened_doc)
            
            # Process comments
            comments_dir = self.join_paths(docket_path, "comments")
            if self.path_exists(comments_dir):
                comment_files = self.glob_files(comments_dir, "*.json")
                if len(comment_files) > self.comment_threshold and outer_pbar:  # Only show nested bar for dockets with many comments
                    with tqdm(comment_files, desc=f"  Processing {len(comment_files)} comments", 
                             leave=False, position=1) as comment_pbar:
                        for comment_file in comment_pbar:
                            comment_data = self.load_json_file(comment_file)
                            if comment_data:
                                flattened_comment = self.flatten_comment_data(comment_data)
                                result['comments'].append(flattened_comment)
                else:
                    for comment_file in comment_files:
                        comment_data = self.load_json_file(comment_file)
                        if comment_data:
                            flattened_comment = self.flatten_comment_data(comment_data)
                            result['comments'].append(flattened_comment)
            
            # NEW: Try the text-* subdirectory structure for comments
            if not result['comments']:
                text_subdirs = [d for d in self.list_directory(docket_path) if d.startswith('text-')]
                for text_subdir in text_subdirs:
                    comments_dir = self.join_paths(docket_path, text_subdir, "comments")
                    if self.path_exists(comments_dir):
                        comment_files = self.glob_files(comments_dir, "*.json")
                        if len(comment_files) > self.comment_threshold and outer_pbar:  # Only show nested bar for dockets with many comments
                            with tqdm(comment_files, desc=f"  Processing {len(comment_files)} comments", 
                                     leave=False, position=1) as comment_pbar:
                                for comment_file in comment_pbar:
                                    comment_data = self.load_json_file(comment_file)
                                    if comment_data:
                                        flattened_comment = self.flatten_comment_data(comment_data)
                                        result['comments'].append(flattened_comment)
                        else:
                            for comment_file in comment_files:
                                comment_data = self.load_json_file(comment_file)
                                if comment_data:
                                    flattened_comment = self.flatten_comment_data(comment_data)
                                    result['comments'].append(flattened_comment)
        
        # Debug: Log what data was found
        self.logger.info(f"  Found: docket_info={result['docket_info'] is not None}, "
                         f"documents={len(result['documents'])}, comments={len(result['comments'])}")
        
        # If we found no data, log the paths we checked
        if not result['docket_info'] and not result['documents'] and not result['comments']:
            self.logger.warning(f"  No data found for docket {docket_id}. Checked paths:")
            # Log the paths we checked for debugging
            raw_data_path = self.join_paths(docket_path, "raw-data")
            if self.path_exists(raw_data_path):
                self.logger.warning(f"    Raw data path exists: {raw_data_path}")
                # List what's in the raw-data directory
                try:
                    raw_contents = self.list_directory(raw_data_path)
                    self.logger.warning(f"    Raw data contents: {raw_contents[:10]}...")
                except Exception as e:
                    self.logger.warning(f"    Error listing raw data contents: {e}")
            else:
                self.logger.warning(f"    Raw data path does not exist: {raw_data_path}")
        
        return result
    
    def save_to_parquet(self, data: List[Dict[str, Any]], table_name: str, 
                        output_dir: str, compression: str = 'snappy') -> bool:
        """Save data to Parquet format"""
        if not data:
            return False
        
        try:
            df = pd.DataFrame(data)
            
            if self.is_s3_target:
                # For S3, save directly to S3
                parquet_path = self.join_paths(output_dir, f"{table_name}.parquet")
                try:
                    # Convert DataFrame to bytes and upload to S3
                    buffer = df.to_parquet(index=False, compression=compression)
                    with self.s3_fs.open(parquet_path, 'wb') as f:
                        f.write(buffer)
                    self.logger.info(f"Saved to S3: {parquet_path}")
                except Exception as e:
                    self.logger.error(f"Failed to save {table_name} to S3 {parquet_path}: {e}")
                    return False
            else:
                # For local, save to local filesystem
                parquet_path = self.join_paths(output_dir, f"{table_name}.parquet")
                
                # Create output directory
                try:
                    output_dir_path = Path(output_dir)
                    output_dir_path.mkdir(parents=True, exist_ok=True)
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
            output_dir = self.join_paths(self.output_path, "derived-data", agency, docket_id, "iceberg")
        else:
            # When using default, already pointing to derived-data, so just agency/docket_id/iceberg
            output_dir = self.join_paths(self.output_path, agency, docket_id, "iceberg")
        
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
    
    def get_docket_directories(self) -> List[str]:
        """Get all docket directories from the data path using known Mirrulations structure"""
        dockets = []
        
        # Look for raw-data structure (Mirrulations format)
        raw_data_path = self.join_paths(self.data_path, "raw-data")
        self.logger.info(f"Checking for raw-data structure at: {raw_data_path}")
        
        if self.path_exists(raw_data_path):
            self.logger.info(f"Found raw-data structure at {raw_data_path}")
            
            # For S3, we'll use a more efficient approach based on known structure
            if self.is_s3_source:
                self.logger.info("Using optimized S3 scanning based on known Mirrulations structure...")
                dockets = self._get_dockets_from_s3_optimized(raw_data_path)
            else:
                # For local paths, use the original directory scanning
                self.logger.info("Scanning agency directories...")
                agency_dirs = self.list_directory(raw_data_path)
                self.logger.info(f"Found {len(agency_dirs)} agency directories")
                
                # Show progress for agency scanning
                with tqdm(agency_dirs, desc="Scanning agencies", unit="agency") as pbar:
                    for agency_dir in pbar:
                        pbar.set_description(f"Scanning {agency_dir}")
                        agency_path = self.join_paths(raw_data_path, agency_dir)
                        if self.is_directory(agency_path):
                            self.logger.debug(f"  Agency: {agency_dir}")
                            docket_dirs = self.list_directory(agency_path)
                            self.logger.debug(f"    Found {len(docket_dirs)} dockets in {agency_dir}")
                            for docket_dir in docket_dirs:
                                docket_path = self.join_paths(agency_path, docket_dir)
                                if self.is_directory(docket_path):
                                    dockets.append(docket_path)
                                    self.logger.debug(f"      Docket: {docket_dir}")
        else:
            self.logger.info(f"No raw-data structure found at {raw_data_path}")
            # Look for direct docket structure (like in results/)
            self.logger.info(f"Looking for direct docket structure in {self.data_path}")
            self.logger.info("Scanning data directory...")
            
            data_contents = self.list_directory(self.data_path)
            self.logger.info(f"Found {len(data_contents)} items in data path")
            
            # Show progress for direct structure scanning
            with tqdm(data_contents, desc="Scanning dockets", unit="item") as pbar:
                for docket_dir in pbar:
                    pbar.set_description(f"Checking {docket_dir}")
                    docket_path = self.join_paths(self.data_path, docket_dir)
                    if self.is_directory(docket_path) and not PathHandler.get_name(docket_path).startswith('.'):
                        # Check if this looks like a docket directory
                        if self.path_exists(self.join_paths(docket_path, "raw-data")) or self.path_exists(self.join_paths(docket_path, "docket")):
                            dockets.append(docket_path)
                            self.logger.debug(f"  Found docket: {PathHandler.get_name(docket_path)}")
        
        # Filter out non-docket directories and sort
        self.logger.info("Filtering docket directories...")
        filtered_dockets = []
        for docket in dockets:
            # Skip derived-data and other non-docket directories
            if PathHandler.get_name(docket) not in ['derived-data', 'raw-data']:
                filtered_dockets.append(docket)
        
        self.logger.info(f"Found {len(filtered_dockets)} docket directories after filtering")
        return sorted(filtered_dockets)
    
    def _get_dockets_from_s3_optimized(self, raw_data_path: str) -> List[str]:
        """Get docket directories from S3 using optimized approach based on known structure"""
        dockets = []
        
        # Known agency prefixes from Mirrulations structure
        # This is a subset - we can expand this list as needed
        known_agencies = [
            'ABMC', 'ACF', 'ACFR', 'ACHP', 'ACL', 'ACUS', 'ADF', 'AFRH', 'AHRQ', 'AID',
            'AMS', 'APHIS', 'ATF', 'BIA', 'BLM', 'BLS', 'BOP', 'BSEE', 'BTS', 'CBP',
            'CDC', 'CFPB', 'CMS', 'CNCS', 'CPSC', 'CRS', 'DARS', 'DEA', 'DHS', 'DOC',
            'DOD', 'DOE', 'DOI', 'DOJ', 'DOL', 'DOS', 'DOT', 'ED', 'EPA', 'FAA',
            'FBI', 'FCC', 'FDA', 'FDIC', 'FHFA', 'FISC', 'FLRA', 'FMCSA', 'FRA', 'FRB',
            'FTC', 'GAO', 'GSA', 'HHS', 'HUD', 'ICE', 'IRS', 'ITC', 'NASA', 'NEA',
            'NFA', 'NGA', 'NIGC', 'NIH', 'NIST', 'NOAA', 'NPS', 'NRC', 'NSA', 'NSF',
            'NTSB', 'OCC', 'ODNI', 'OGE', 'OMB', 'ONRR', 'OPM', 'OSHA', 'PBGC', 'PTO',
            'SEC', 'SBA', 'SSA', 'SSS', 'TREAS', 'TSA', 'USCIS', 'USDA', 'USGS', 'VA'
        ]
        
        # Apply agency filter if specified
        if self.agency_filter:
            if self.agency_filter in known_agencies:
                known_agencies = [self.agency_filter]
                self.logger.info(f"Filtering to single agency: {self.agency_filter}")
            else:
                self.logger.warning(f"Agency filter '{self.agency_filter}' not in known agencies list")
                # If agency filter is not in known list, just try it anyway
                known_agencies = [self.agency_filter]
        
        self.logger.info(f"Using optimized scanning with {len(known_agencies)} agencies")
        
        # Test each known agency to see if it exists
        with tqdm(known_agencies, desc="Testing agencies", unit="agency") as pbar:
            for agency in pbar:
                pbar.set_description(f"Testing {agency}")
                agency_path = self.join_paths(raw_data_path, agency)
                
                # Quick check if agency directory exists
                if self.path_exists(agency_path):
                    self.logger.debug(f"Found agency: {agency}")
                    
                    # For agencies that exist, we can either:
                    # 1. List all dockets (if the agency is small)
                    # 2. Use a pattern-based approach for large agencies
                    
                    # If we have a specific docket pattern, we can be more efficient
                    if self.docket_pattern_filter:
                        self.logger.debug(f"  Using pattern-based search for docket: {self.docket_pattern_filter}")
                        # For pattern-based search, we can use S3 prefix filtering
                        try:
                            # Extract the docket ID from the pattern (e.g., "FAA-2000-7032" from pattern)
                            docket_id = self.docket_pattern_filter
                            docket_path = self.join_paths(agency_path, docket_id)
                            
                            if self.path_exists(docket_path) and self.is_directory(docket_path):
                                if self._should_process_docket(docket_path):
                                    dockets.append(docket_path)
                                    self.logger.debug(f"    Found specific docket: {docket_id}")
                                else:
                                    self.logger.debug(f"    Skipping docket: {docket_id} (filtered out)")
                            else:
                                self.logger.debug(f"    Docket {docket_id} not found in {agency}")
                        except Exception as e:
                            self.logger.warning(f"Error checking specific docket {self.docket_pattern_filter} in agency {agency}: {e}")
                    else:
                        # List all dockets in the agency
                        try:
                            docket_dirs = self.list_directory(agency_path)
                            self.logger.debug(f"  Found {len(docket_dirs)} dockets in {agency}")
                            
                            for docket_dir in docket_dirs:
                                docket_path = self.join_paths(agency_path, docket_dir)
                                if self.is_directory(docket_path):
                                    # Apply filters
                                    if self._should_process_docket(docket_path):
                                        dockets.append(docket_path)
                                        self.logger.debug(f"    Docket: {docket_dir}")
                                    else:
                                        self.logger.debug(f"    Skipping docket: {docket_dir} (filtered out)")
                        except Exception as e:
                            self.logger.warning(f"Error listing dockets for agency {agency}: {e}")
                            # Continue with other agencies
                            continue
        
        return dockets
    
    def _convert_s3_dockets_streaming(self) -> bool:
        """Convert S3 dockets using streaming approach - process as we find them"""
        raw_data_path = self.join_paths(self.data_path, "raw-data")
        
        # Known agency prefixes from Mirrulations structure
        known_agencies = [
            'ABMC', 'ACF', 'ACFR', 'ACHP', 'ACL', 'ACUS', 'ADF', 'AFRH', 'AHRQ', 'AID',
            'AMS', 'APHIS', 'ATF', 'BIA', 'BLM', 'BLS', 'BOP', 'BSEE', 'BTS', 'CBP',
            'CDC', 'CFPB', 'CMS', 'CNCS', 'CPSC', 'CRS', 'DARS', 'DEA', 'DHS', 'DOC',
            'DOD', 'DOE', 'DOI', 'DOJ', 'DOL', 'DOS', 'DOT', 'ED', 'EPA', 'FAA',
            'FBI', 'FCC', 'FDA', 'FDIC', 'FHFA', 'FISC', 'FLRA', 'FMCSA', 'FRA', 'FRB',
            'FTC', 'GAO', 'GSA', 'HHS', 'HUD', 'ICE', 'IRS', 'ITC', 'NASA', 'NEA',
            'NFA', 'NGA', 'NIGC', 'NIH', 'NIST', 'NOAA', 'NPS', 'NRC', 'NSA', 'NSF',
            'NTSB', 'OCC', 'ODNI', 'OGE', 'OMB', 'ONRR', 'OPM', 'OSHA', 'PBGC', 'PTO',
            'SEC', 'SBA', 'SSA', 'SSS', 'TREAS', 'TSA', 'USCIS', 'USDA', 'USGS', 'VA'
        ]
        
        # Apply agency filter if specified
        if self.agency_filter:
            if self.agency_filter in known_agencies:
                known_agencies = [self.agency_filter]
                self.logger.info(f"Filtering to single agency: {self.agency_filter}")
            else:
                self.logger.warning(f"Agency filter '{self.agency_filter}' not in known agencies list")
                # If agency filter is not in known list, just try it anyway
                known_agencies = [self.agency_filter]
        
        self.logger.info(f"Starting streaming conversion with {len(known_agencies)} agencies")
        
        # Process each agency
        for agency in known_agencies:
            agency_path = self.join_paths(raw_data_path, agency)
            
            # Quick check if agency directory exists
            if not self.path_exists(agency_path):
                continue
                
            self.logger.info(f"Processing agency: {agency}")
            
            # If we have a specific docket pattern, check just that docket
            if self.docket_pattern_filter:
                docket_id = self.docket_pattern_filter
                docket_path = self.join_paths(agency_path, docket_id)
                
                if self.path_exists(docket_path) and self.is_directory(docket_path):
                    if self._should_process_docket(docket_path):
                        self._process_single_docket(docket_path)
                    else:
                        self.logger.info(f"Skipping docket {docket_id} (filtered out)")
                else:
                    self.logger.info(f"Docket {docket_id} not found in {agency}")
            else:
                # Stream through all dockets in the agency
                try:
                    docket_dirs = self.list_directory(agency_path)
                    self.logger.info(f"Found {len(docket_dirs)} dockets in {agency}")
                    
                    for docket_dir in docket_dirs:
                        docket_path = self.join_paths(agency_path, docket_dir)
                        if self.is_directory(docket_path):
                            if self._should_process_docket(docket_path):
                                self._process_single_docket(docket_path)
                            else:
                                self.logger.debug(f"Skipping docket: {docket_dir} (filtered out)")
                except Exception as e:
                    self.logger.warning(f"Error listing dockets for agency {agency}: {e}")
                    continue
        
        return True
    
    def _convert_local_dockets_streaming(self) -> bool:
        """Convert local dockets using streaming approach"""
        raw_data_path = self.join_paths(self.data_path, "raw-data")
        
        if not self.path_exists(raw_data_path):
            self.logger.error(f"Raw data path does not exist: {raw_data_path}")
            return False
        
        # Get agency directories
        agency_dirs = self.list_directory(raw_data_path)
        
        for agency_dir in agency_dirs:
            agency_path = self.join_paths(raw_data_path, agency_dir)
            
            if not self.is_directory(agency_path):
                continue
                
            # Apply agency filter if specified
            if self.agency_filter and agency_dir != self.agency_filter:
                continue
                
            self.logger.info(f"Processing agency: {agency_dir}")
            
            # Get docket directories
            try:
                docket_dirs = self.list_directory(agency_path)
                
                for docket_dir in docket_dirs:
                    docket_path = self.join_paths(agency_path, docket_dir)
                    if self.is_directory(docket_path):
                        if self._should_process_docket(docket_path):
                            self._process_single_docket(docket_path)
                        else:
                            self.logger.debug(f"Skipping docket: {docket_dir} (filtered out)")
            except Exception as e:
                self.logger.warning(f"Error listing dockets for agency {agency_dir}: {e}")
                continue
        
        return True
    
    def _process_single_docket(self, docket_path: str):
        """Process a single docket"""
        try:
            docket_name = PathHandler.get_name(docket_path)
            print(f"🔄 Converting {docket_name}...")
            
            # Process docket
            docket_data = self.process_docket(docket_path)
            
            # Save dataset
            if self.save_docket_dataset(docket_data):
                self.stats['dockets_processed'] += 1
                print(f"✅ Completed {docket_name}")
            else:
                self.stats['dockets_skipped'] += 1
                print(f"⏭️  Skipped {docket_name}")
                
        except (PermissionError, OSError) as e:
            self.logger.error(f"Permission error processing {docket_path}: {e}")
            self.logger.error("Stopping conversion due to permission issues.")
            self.stats['errors'] += 1
            raise
        except Exception as e:
            self.logger.error(f"Error processing {docket_path}: {e}")
            self.stats['errors'] += 1
    
    def add_filters(self, agency: str = None, docket_pattern: str = None):
        """Add filters to process only specific agencies or dockets"""
        if agency:
            self.agency_filter = agency.upper()
            self.logger.info(f"Added agency filter: {self.agency_filter}")
        
        if docket_pattern:
            self.docket_pattern_filter = docket_pattern
            self.logger.info(f"Added docket pattern filter: {self.docket_pattern_filter}")
    
    def _should_process_docket(self, docket_path: str) -> bool:
        """Check if a docket should be processed based on filters"""
        docket_name = PathHandler.get_name(docket_path)
        
        # Check agency filter
        if self.agency_filter:
            # Extract agency from docket name (e.g., "CMS-2025-0020" -> "CMS")
            if '/' in docket_name:
                agency = docket_name.split('/')[0]
            elif '-' in docket_name:
                agency = docket_name.split('-')[0]
            else:
                agency = "UNKNOWN"
            
            if agency != self.agency_filter:
                return False
        
        # Check docket pattern filter
        if self.docket_pattern_filter:
            import fnmatch
            if not fnmatch.fnmatch(docket_name, self.docket_pattern_filter):
                return False
        
        return True
    
    def check_permissions(self):
        """Check read/write permissions before starting conversion"""
        self.logger.info("Checking permissions...")
        
        # Check if data directory exists and is readable
        if not self.path_exists(self.data_path):
            raise FileNotFoundError(f"Data directory does not exist: {self.data_path}")
        
        # For local paths, check file system permissions
        if not PathHandler.is_s3_path(self.data_path):
            if not os.access(self.data_path, os.R_OK):
                raise PermissionError(f"No read permission for data directory: {self.data_path}")
        
        # Check if we can list the contents
        try:
            self.list_directory(self.data_path)
        except PermissionError as e:
            raise PermissionError(f"Cannot list contents of data directory {self.data_path}: {e}")
        
        # Check output directory permissions
        if self.path_exists(self.output_path):
            if not PathHandler.is_s3_path(self.output_path) and not os.access(self.output_path, os.W_OK):
                raise PermissionError(f"No write permission for output directory: {self.output_path}")
        else:
            # Try to create the output directory
            if PathHandler.is_s3_path(self.output_path):
                # For S3, we'll test write permission by trying to create a test file
                try:
                    test_path = self.join_paths(self.output_path, ".test_write_permission")
                    with self.s3_fs.open(test_path, 'w') as f:
                        f.write("test")
                    self.s3_fs.delete(test_path)
                except Exception as e:
                    raise PermissionError(f"Cannot write to S3 output directory {self.output_path}: {e}")
            else:
                try:
                    output_dir_path = Path(self.output_path)
                    output_dir_path.mkdir(parents=True, exist_ok=True)
                    # Test write permission by creating a temporary file
                    test_file = output_dir_path / ".test_write_permission"
                    test_file.write_text("test")
                    test_file.unlink()
                except (PermissionError, OSError) as e:
                    raise PermissionError(f"Cannot create or write to output directory {self.output_path}: {e}")
        
        self.logger.info("Permission check passed.")
    
    def convert_all(self):
        """Convert all dockets to Iceberg format"""
        print("🚀 Initializing Mirrulations to Iceberg conversion...")
        
        if self.verbose:
            self.logger.info(f"Starting conversion from: {self.data_path}")
            self.logger.info(f"Output path: {self.output_path}")
            if self.is_s3_source:
                self.logger.info(f"Source: S3 bucket: {self.data_path}")
            if self.is_s3_target:
                self.logger.info(f"Target: S3 bucket: {self.output_path}")
        
        print("🔍 Checking permissions and connectivity...")
        # Check permissions before starting
        try:
            self.check_permissions()
            print("✅ Permissions check passed")
        except (PermissionError, OSError) as e:
            self.logger.error(f"Permission check failed: {e}")
            self.logger.error("Please ensure you have read access to the data directory and write access to the output directory.")
            return False
        
        print("🔄 Starting conversion process...")
        start_time = time.time()
        
        # Process dockets as we find them (no upfront scanning)
        if self.is_s3_source:
            success = self._convert_s3_dockets_streaming()
        else:
            success = self._convert_local_dockets_streaming()
        
        # Final statistics
        total_time = time.time() - start_time
        print(f"\n🎉 Conversion complete!")
        print(f"⏱️  Total time: {total_time/3600:.2f} hours")
        print(f"📁 Dockets processed: {self.stats['dockets_processed']}")
        print(f"⏭️  Dockets skipped: {self.stats['dockets_skipped']}")
        print(f"❌ Errors: {self.stats['errors']}")
        if self.stats['dockets_processed'] > 0:
            print(f"🚀 Average rate: {self.stats['dockets_processed']/total_time:.2f} dockets/sec")
        
        # Check if we had permission errors
        if self.stats['errors'] > 0:
            self.logger.error("Conversion completed with errors. Check the log for details.")
            return False
        else:
            print("✅ Conversion completed successfully!")
            return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Convert Mirrulations data to Iceberg format")
    parser.add_argument("data_path", help="Path to Mirrulations data directory or S3 bucket path")
    parser.add_argument("--output-path", help="Output directory for Iceberg data or S3 bucket path")
    parser.add_argument("--agency", help="Process only a specific agency (e.g., 'CMS', 'DEA')")
    parser.add_argument("--docket-pattern", help="Process only dockets matching a pattern (e.g., 'CMS-2025-*')")
    parser.add_argument("--compression", default="snappy", 
                       choices=["snappy", "gzip", "brotli", "lz4"],
                       help="Compression algorithm for Parquet files")
    parser.add_argument("--debug", action="store_true", 
                       help="Enable debug logging for troubleshooting")
    parser.add_argument("--verbose", action="store_true", 
                       help="Enable verbose INFO output")
    parser.add_argument("--comment-threshold", type=int, default=10,
                       help="Number of comments above which to show nested progress bar (default: 10)")
    
    args = parser.parse_args()
    
    # Validate data path
    if not PathHandler.is_s3_path(args.data_path) and not Path(args.data_path).exists():
        print(f"Error: Data path does not exist: {args.data_path}")
        sys.exit(1)
    
    # Create converter
    converter = IcebergConverter(
        data_path=args.data_path,
        output_path=args.output_path,
        debug=args.debug,
        verbose=args.verbose,
        comment_threshold=args.comment_threshold
    )
    
    # Apply filters if specified
    if args.agency or args.docket_pattern:
        converter.add_filters(agency=args.agency, docket_pattern=args.docket_pattern)
    
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
