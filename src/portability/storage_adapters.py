"""
Werfen Storage Adapters - Storage Adapters
==========================================

Unified adapters for different storage types.
AWS Equivalent: S3 + EFS + FSx abstraction layer

Author: Werfen Data Team
Date: 2024
"""

import os
import shutil
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union, BinaryIO
from pathlib import Path
from dataclasses import dataclass
import tempfile
import zipfile
import gzip

# Import logging from our centralized system
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.logging.structured_logger import setup_structured_logging

@dataclass
class StorageConfig:
    """
    Unified storage configuration.
    AWS Equivalent: S3 bucket configuration + IAM policies
    """
    storage_type: str
    base_path: Optional[str] = None
    bucket_name: Optional[str] = None
    region: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    encryption_enabled: bool = True
    compression_enabled: bool = False
    backup_enabled: bool = True
    extra_config: Optional[Dict[str, Any]] = None

@dataclass
class StorageMetadata:
    """
    Stored object metadata.
    AWS Equivalent: S3 object metadata
    """
    path: str
    size_bytes: int
    created_at: str
    modified_at: str
    content_type: Optional[str] = None
    etag: Optional[str] = None
    storage_class: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None

class WerfenStorageAdapter(ABC):
    """
    Abstract base class for storage adapters.
    AWS Equivalent: S3 Client interface abstraction
    """
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = setup_structured_logging(service_name=f"werfen.storage.{self.__class__.__name__}")
        self._client = None
        
    @abstractmethod
    def connect(self) -> bool:
        """Connect to storage system"""
        pass
        
    @abstractmethod
    def disconnect(self) -> bool:
        """Disconnect from storage system"""
        pass
        
    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str, **kwargs) -> bool:
        """Upload file to storage"""
        pass
        
    @abstractmethod
    def download_file(self, remote_path: str, local_path: str, **kwargs) -> bool:
        """Download file from storage"""
        pass
        
    @abstractmethod
    def delete_file(self, remote_path: str) -> bool:
        """Delete file from storage"""
        pass
        
    @abstractmethod
    def list_files(self, prefix: str = "", **kwargs) -> List[str]:
        """List files in storage"""
        pass
        
    @abstractmethod
    def get_file_metadata(self, remote_path: str) -> Optional[StorageMetadata]:
        """Get file metadata"""
        pass
        
    @abstractmethod
    def file_exists(self, remote_path: str) -> bool:
        """Check if file exists"""
        pass
        
    def upload_data(self, data: Union[str, bytes], remote_path: str, **kwargs) -> bool:
        """
        Upload data directly (without local file).
        AWS Equivalent: s3.put_object() with direct data
        """
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                if isinstance(data, str):
                    temp_file.write(data.encode('utf-8'))
                else:
                    temp_file.write(data)
                temp_file.flush()
                
                success = self.upload_file(temp_file.name, remote_path, **kwargs)
                
            # Clean temporary file
            os.unlink(temp_file.name)
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to upload data: {str(e)}")
            return False
            
    def download_data(self, remote_path: str, **kwargs) -> Optional[bytes]:
        """
        Download data directly (without local file).
        AWS Equivalent: s3.get_object() with direct data return
        """
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                success = self.download_file(remote_path, temp_file.name, **kwargs)
                if success:
                    with open(temp_file.name, 'rb') as f:
                        data = f.read()
                    os.unlink(temp_file.name)
                    return data
                else:
                    os.unlink(temp_file.name)
                    return None
                    
        except Exception as e:
            self.logger.error(f"Failed to download data: {str(e)}")
            return None

class LocalStorageAdapter(WerfenStorageAdapter):
    """
    Local storage adapter.
    AWS Equivalent: EFS or FSx for on-premises simulation
    """
    
    def connect(self) -> bool:
        """Verify that base directory exists"""
        try:
            base_path = Path(self.config.base_path or ".")
            base_path.mkdir(parents=True, exist_ok=True)
            self._client = base_path
            self.logger.info(f"Connected to local storage: {base_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to local storage: {str(e)}")
            return False
            
    def disconnect(self) -> bool:
        """Disconnect (not necessary for local storage)"""
        self._client = None
        return True
        
    def upload_file(self, local_path: str, remote_path: str, **kwargs) -> bool:
        """
        Copy file to local storage.
        AWS Equivalent: Upload to EFS mount point
        """
        try:
            local_file = Path(local_path)
            remote_file = Path(self.config.base_path) / remote_path
            
            if not local_file.exists():
                raise FileNotFoundError(f"Local file not found: {local_path}")
                
            # Create parent directories if they don't exist
            remote_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            if self.config.compression_enabled and remote_path.endswith('.gz'):
                # Compress during copy
                with open(local_file, 'rb') as f_in:
                    with gzip.open(remote_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy2(local_file, remote_file)
                
            self.logger.info(f"Uploaded {local_path} to {remote_file}")
            
            # Create backup if enabled
            if self.config.backup_enabled:
                backup_path = remote_file.with_suffix(remote_file.suffix + '.backup')
                shutil.copy2(remote_file, backup_path)
                
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload file: {str(e)}")
            return False
            
    def download_file(self, remote_path: str, local_path: str, **kwargs) -> bool:
        """
        Copy file from local storage.
        AWS Equivalent: Download from EFS mount point
        """
        try:
            remote_file = Path(self.config.base_path) / remote_path
            local_file = Path(local_path)
            
            if not remote_file.exists():
                raise FileNotFoundError(f"Remote file not found: {remote_path}")
                
            local_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Handle decompression if needed
            if remote_path.endswith('.gz') and not local_path.endswith('.gz'):
                with gzip.open(remote_file, 'rb') as f_in:
                    with open(local_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy2(remote_file, local_file)
                
            self.logger.info(f"Downloaded {remote_file} to {local_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download file: {str(e)}")
            return False
            
    def delete_file(self, remote_path: str) -> bool:
        """Delete file from local storage"""
        try:
            remote_file = Path(self.config.base_path) / remote_path
            
            if remote_file.exists():
                remote_file.unlink()
                self.logger.info(f"Deleted file: {remote_file}")
                return True
            else:
                self.logger.warning(f"File not found for deletion: {remote_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to delete file: {str(e)}")
            return False
            
    def list_files(self, prefix: str = "", **kwargs) -> List[str]:
        """List files in local storage"""
        try:
            base_path = Path(self.config.base_path)
            prefix_path = base_path / prefix if prefix else base_path
            
            files = []
            search_path = prefix_path if prefix_path.is_dir() else base_path
            
            for file_path in search_path.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(base_path)
                    if not prefix or str(relative_path).startswith(prefix):
                        files.append(str(relative_path))
                        
            return sorted(files)
            
        except Exception as e:
            self.logger.error(f"Failed to list files: {str(e)}")
            return []
            
    def get_file_metadata(self, remote_path: str) -> Optional[StorageMetadata]:
        """Get local file metadata"""
        try:
            remote_file = Path(self.config.base_path) / remote_path
            
            if not remote_file.exists():
                return None
                
            stat = remote_file.stat()
            return StorageMetadata(
                path=remote_path,
                size_bytes=stat.st_size,
                created_at=str(stat.st_ctime),
                modified_at=str(stat.st_mtime),
                content_type=self._get_content_type(remote_file)
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get file metadata: {str(e)}")
            return None
            
    def file_exists(self, remote_path: str) -> bool:
        """Check if file exists in local storage"""
        try:
            remote_file = Path(self.config.base_path) / remote_path
            return remote_file.exists()
        except Exception:
            return False
            
    def _get_content_type(self, file_path: Path) -> str:
        """Determine content type based on extension"""
        suffix = file_path.suffix.lower()
        content_types = {
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.parquet': 'application/octet-stream',
            '.txt': 'text/plain',
            '.xml': 'application/xml',
            '.yaml': 'application/x-yaml',
            '.yml': 'application/x-yaml'
        }
        return content_types.get(suffix, 'application/octet-stream')

class CloudStorageAdapter(WerfenStorageAdapter):
    """
    Cloud storage adapter (simulates S3 for development).
    AWS Equivalent: boto3 S3 client implementation
    """
    
    def connect(self) -> bool:
        """Simulate connection to cloud storage"""
        try:
            # Create local simulation directory
            self._local_sim_path = Path(f"./cloud_sim/{self.config.bucket_name}")
            self._local_sim_path.mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"Connected to cloud storage (simulated): s3://{self.config.bucket_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to cloud storage: {str(e)}")
            return False
            
    def disconnect(self) -> bool:
        """Disconnect from cloud storage"""
        self._local_sim_path = None
        return True
        
    def upload_file(self, local_path: str, remote_path: str, **kwargs) -> bool:
        """
        Upload file to cloud storage.
        AWS Equivalent: s3.upload_file()
        """
        try:
            local_file = Path(local_path)
            remote_file = self._local_sim_path / remote_path
            
            if not local_file.exists():
                raise FileNotFoundError(f"Local file not found: {local_path}")
                
            remote_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Simulate encryption if enabled
            if self.config.encryption_enabled:
                self.logger.info(f"Encrypting file during upload: {remote_path}")
            
            # Simulate compression if enabled
            if self.config.compression_enabled:
                self.logger.info(f"Compressing file during upload: {remote_path}")
                
            shutil.copy2(local_file, remote_file)
            
            # Save metadata
            metadata = {
                'size': local_file.stat().st_size,
                'content_type': self._get_content_type(local_file),
                'storage_class': kwargs.get('storage_class', 'STANDARD'),
                'encryption': 'AES256' if self.config.encryption_enabled else 'None'
            }
            
            metadata_file = remote_file.with_suffix(remote_file.suffix + '.metadata')
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f)
            
            self.logger.info(f"Uploaded {local_path} to s3://{self.config.bucket_name}/{remote_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload file to cloud: {str(e)}")
            return False
            
    def download_file(self, remote_path: str, local_path: str, **kwargs) -> bool:
        """
        Download file from cloud storage.
        AWS Equivalent: s3.download_file()
        """
        try:
            remote_file = self._local_sim_path / remote_path
            local_file = Path(local_path)
            
            if not remote_file.exists():
                raise FileNotFoundError(f"Remote file not found: {remote_path}")
                
            local_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Simulate decryption if enabled
            if self.config.encryption_enabled:
                self.logger.info(f"Decrypting file during download: {remote_path}")
                
            shutil.copy2(remote_file, local_file)
            
            self.logger.info(f"Downloaded s3://{self.config.bucket_name}/{remote_path} to {local_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download file from cloud: {str(e)}")
            return False
            
    def delete_file(self, remote_path: str) -> bool:
        """
        Delete file from cloud storage.
        AWS Equivalent: s3.delete_object()
        """
        try:
            remote_file = self._local_sim_path / remote_path
            metadata_file = remote_file.with_suffix(remote_file.suffix + '.metadata')
            
            deleted = False
            if remote_file.exists():
                remote_file.unlink()
                deleted = True
                
            if metadata_file.exists():
                metadata_file.unlink()
                
            if deleted:
                self.logger.info(f"Deleted s3://{self.config.bucket_name}/{remote_path}")
                return True
            else:
                self.logger.warning(f"File not found for deletion: {remote_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to delete file from cloud: {str(e)}")
            return False
            
    def list_files(self, prefix: str = "", **kwargs) -> List[str]:
        """
        List files in cloud storage.
        AWS Equivalent: s3.list_objects_v2()
        """
        try:
            prefix_path = self._local_sim_path / prefix if prefix else self._local_sim_path
            
            files = []
            for file_path in self._local_sim_path.rglob("*"):
                if file_path.is_file() and not file_path.name.endswith('.metadata'):
                    if not prefix or str(file_path).startswith(str(prefix_path)):
                        relative_path = file_path.relative_to(self._local_sim_path)
                        files.append(str(relative_path))
                        
            return sorted(files)
            
        except Exception as e:
            self.logger.error(f"Failed to list cloud files: {str(e)}")
            return []
            
    def get_file_metadata(self, remote_path: str) -> Optional[StorageMetadata]:
        """
        Get cloud file metadata.
        AWS Equivalent: s3.head_object()
        """
        try:
            remote_file = self._local_sim_path / remote_path
            metadata_file = remote_file.with_suffix(remote_file.suffix + '.metadata')
            
            if not remote_file.exists():
                return None
                
            # Read simulated metadata
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    
                return StorageMetadata(
                    path=remote_path,
                    size_bytes=metadata.get('size', 0),
                    created_at=str(remote_file.stat().st_ctime),
                    modified_at=str(remote_file.stat().st_mtime),
                    content_type=metadata.get('content_type'),
                    storage_class=metadata.get('storage_class', 'STANDARD')
                )
            else:
                # Fallback to basic metadata
                stat = remote_file.stat()
                return StorageMetadata(
                    path=remote_path,
                    size_bytes=stat.st_size,
                    created_at=str(stat.st_ctime),
                    modified_at=str(stat.st_mtime),
                    content_type=self._get_content_type(remote_file),
                    storage_class='STANDARD'
                )
                
        except Exception as e:
            self.logger.error(f"Failed to get cloud file metadata: {str(e)}")
            return None
            
    def file_exists(self, remote_path: str) -> bool:
        """Check if file exists in cloud storage"""
        try:
            remote_file = self._local_sim_path / remote_path
            return remote_file.exists()
        except Exception:
            return False
            
    def _get_content_type(self, file_path: Path) -> str:
        """Determine content type based on extension"""
        suffix = file_path.suffix.lower()
        content_types = {
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.parquet': 'application/octet-stream',
            '.txt': 'text/plain',
            '.xml': 'application/xml',
            '.yaml': 'application/x-yaml',
            '.yml': 'application/x-yaml'
        }
        return content_types.get(suffix, 'application/octet-stream')

class StorageAdapterFactory:
    """
    Factory for creating storage adapters.
    AWS Equivalent: AWS SDK client factory with configuration management
    """
    
    @staticmethod
    def create_adapter(storage_config: StorageConfig) -> WerfenStorageAdapter:
        """
        Create adapter based on configuration.
        
        Args:
            storage_config: Storage configuration
            
        Returns:
            WerfenStorageAdapter: Appropriate adapter
        """
        adapter_map = {
            'local': LocalStorageAdapter,
            'cloud': CloudStorageAdapter,
            's3': CloudStorageAdapter  # Alias for cloud
        }
        
        adapter_class = adapter_map.get(storage_config.storage_type)
        if not adapter_class:
            raise ValueError(f"Unsupported storage type: {storage_config.storage_type}")
            
        return adapter_class(storage_config)
        
    @staticmethod 
    def create_local_adapter(base_path: str = "./data", **kwargs) -> LocalStorageAdapter:
        """
        Create local adapter with simplified configuration.
        
        Args:
            base_path: Base directory for storage
            **kwargs: Additional configuration
            
        Returns:
            LocalStorageAdapter: Configured local adapter
        """
        config = StorageConfig(
            storage_type='local',
            base_path=base_path,
            **kwargs
        )
        return LocalStorageAdapter(config)
        
    @staticmethod
    def create_cloud_adapter(bucket_name: str, region: str = "us-east-1", **kwargs) -> CloudStorageAdapter:
        """
        Create cloud adapter with simplified configuration.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
            **kwargs: Additional configuration
            
        Returns:
            CloudStorageAdapter: Configured cloud adapter
        """
        config = StorageConfig(
            storage_type='cloud',
            bucket_name=bucket_name,
            region=region,
            **kwargs
        )
        return CloudStorageAdapter(config) 