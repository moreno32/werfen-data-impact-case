"""
Werfen Data Connectors - Abstract Connectors System
==================================================

Unified connectors for different data sources.
AWS Equivalent: AWS Glue Connectors + Data Catalog

Author: Werfen Data Team
Date: 2024
"""

import os
import json
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from dataclasses import dataclass
from urllib.parse import urlparse

# Import logging from our centralized system
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.logging.structured_logger import setup_structured_logging

@dataclass
class ConnectionConfig:
    """
    Unified connection configuration.
    AWS Equivalent: AWS Glue Connection properties
    """
    connection_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    path: Optional[str] = None
    bucket: Optional[str] = None
    region: Optional[str] = None
    extra_params: Optional[Dict[str, Any]] = None

class WerfenDataConnector(ABC):
    """
    Abstract base class for data connectors.
    AWS Equivalent: AWS Glue Connector interface
    """
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.logger = setup_structured_logging(service_name=f"werfen.connector.{self.__class__.__name__}")
        self._connection = None
        
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to data source"""
        pass
        
    @abstractmethod
    def disconnect(self) -> bool:
        """Close connection"""
        pass
        
    @abstractmethod
    def read_data(self, query_or_path: str, **kwargs) -> pd.DataFrame:
        """Read data from source"""
        pass
        
    @abstractmethod
    def write_data(self, data: pd.DataFrame, destination: str, **kwargs) -> bool:
        """Write data to source"""
        pass
        
    @abstractmethod
    def list_resources(self) -> List[str]:
        """List available resources (tables, files, etc.)"""
        pass
        
    def test_connection(self) -> Dict[str, Any]:
        """
        Test connection and return status information.
        AWS Equivalent: AWS Glue Connection test
        """
        try:
            success = self.connect()
            if success:
                resources = self.list_resources()[:5]  # Limit to 5 for test
                self.disconnect()
                return {
                    'status': 'success',
                    'connection_type': self.config.connection_type,
                    'sample_resources': resources,
                    'timestamp': pd.Timestamp.now().isoformat()
                }
            else:
                return {
                    'status': 'failed',
                    'connection_type': self.config.connection_type,
                    'error': 'Could not establish connection',
                    'timestamp': pd.Timestamp.now().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return {
                'status': 'error',
                'connection_type': self.config.connection_type,
                'error': str(e),
                'timestamp': pd.Timestamp.now().isoformat()
            }

class LocalFileConnector(WerfenDataConnector):
    """
    Connector for local files.
    AWS Equivalent: Direct S3 file access
    """
    
    def connect(self) -> bool:
        """Verify that base directory exists"""
        try:
            base_path = Path(self.config.path or ".")
            if base_path.exists():
                self._connection = base_path
                self.logger.info(f"Connected to local path: {base_path}")
                return True
            else:
                self.logger.error(f"Path does not exist: {base_path}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to connect to local path: {str(e)}")
            return False
            
    def disconnect(self) -> bool:
        """Disconnect (not necessary for local files)"""
        self._connection = None
        return True
        
    def read_data(self, query_or_path: str, **kwargs) -> pd.DataFrame:
        """
        Read local file.
        AWS Equivalent: s3.get_object()
        """
        try:
            full_path = Path(self.config.path) / query_or_path if self.config.path else Path(query_or_path)
            
            if not full_path.exists():
                raise FileNotFoundError(f"File not found: {full_path}")
                
            # Auto-detect format by extension
            suffix = full_path.suffix.lower()
            
            if suffix == '.csv':
                df = pd.read_csv(full_path, **kwargs)
            elif suffix in ['.json', '.jsonl']:
                df = pd.read_json(full_path, **kwargs)
            elif suffix in ['.parquet', '.pqt']:
                df = pd.read_parquet(full_path, **kwargs)
            elif suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(full_path, **kwargs)
            else:
                # Default to CSV
                df = pd.read_csv(full_path, **kwargs)
                
            self.logger.info(f"Read {len(df)} rows from {full_path}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read data from {query_or_path}: {str(e)}")
            raise
            
    def write_data(self, data: pd.DataFrame, destination: str, **kwargs) -> bool:
        """
        Write local file.
        AWS Equivalent: s3.put_object()
        """
        try:
            full_path = Path(self.config.path) / destination if self.config.path else Path(destination)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            suffix = full_path.suffix.lower()
            
            if suffix == '.csv':
                data.to_csv(full_path, index=False, **kwargs)
            elif suffix == '.json':
                data.to_json(full_path, **kwargs)
            elif suffix in ['.parquet', '.pqt']:
                data.to_parquet(full_path, **kwargs)
            elif suffix in ['.xlsx']:
                data.to_excel(full_path, index=False, **kwargs)
            else:
                # Default to CSV
                data.to_csv(full_path, index=False, **kwargs)
                
            self.logger.info(f"Wrote {len(data)} rows to {full_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write data to {destination}: {str(e)}")
            return False
            
    def list_resources(self) -> List[str]:
        """
        List files in directory.
        AWS Equivalent: s3.list_objects_v2()
        """
        try:
            base_path = Path(self.config.path or ".")
            if not base_path.is_dir():
                return []
                
            resources = []
            for file_path in base_path.rglob("*"):
                if file_path.is_file() and not file_path.name.startswith('.'):
                    relative_path = file_path.relative_to(base_path)
                    resources.append(str(relative_path))
                    
            return sorted(resources)
            
        except Exception as e:
            self.logger.error(f"Failed to list resources: {str(e)}")
            return []

class DatabaseConnector(WerfenDataConnector):
    """
    Database connector.
    AWS Equivalent: Amazon RDS + Aurora connections
    """
    
    def connect(self) -> bool:
        """Connect to database"""
        try:
            # For POC, we simulate connection
            # In production, we would use sqlalchemy, psycopg2, etc.
            self.logger.info(f"Connected to database: {self.config.database}")
            self._connection = f"mock_db_connection_{self.config.database}"
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            return False
            
    def disconnect(self) -> bool:
        """Disconnect from database"""
        if self._connection:
            self._connection = None
            self.logger.info("Disconnected from database")
        return True
        
    def read_data(self, query_or_path: str, **kwargs) -> pd.DataFrame:
        """
        Execute SQL query.
        AWS Equivalent: Amazon Athena query execution
        """
        try:
            # For POC, return mock data
            self.logger.info(f"Executing query: {query_or_path}")
            
            # Simulate query result
            mock_data = {
                'id': range(1, 101),
                'name': [f'Record_{i}' for i in range(1, 101)],
                'value': [i * 10 for i in range(1, 101)],
                'category': [f'Category_{i%5}' for i in range(1, 101)]
            }
            
            df = pd.DataFrame(mock_data)
            self.logger.info(f"Query returned {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
            
    def write_data(self, data: pd.DataFrame, destination: str, **kwargs) -> bool:
        """
        Insert data into table.
        AWS Equivalent: Copy to Amazon Redshift
        """
        try:
            self.logger.info(f"Inserting {len(data)} rows into {destination}")
            # In production, we would use to_sql() or bulk insert
            return True
        except Exception as e:
            self.logger.error(f"Data insertion failed: {str(e)}")
            return False
            
    def list_resources(self) -> List[str]:
        """
        List available tables.
        AWS Equivalent: AWS Glue Data Catalog table listing
        """
        try:
            # For POC, return mock tables
            tables = [
                'customers', 'sales', 'products', 'orders', 
                'inventory', 'suppliers', 'categories'
            ]
            return tables
        except Exception as e:
            self.logger.error(f"Failed to list tables: {str(e)}")
            return []

class CloudStorageConnector(WerfenDataConnector):
    """
    Cloud storage connector (simulated).
    AWS Equivalent: Native S3 boto3 operations
    """
    
    def connect(self) -> bool:
        """Connect to cloud storage"""
        try:
            self.logger.info(f"Connected to cloud storage bucket: {self.config.bucket}")
            self._connection = f"mock_cloud_connection_{self.config.bucket}"
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to cloud storage: {str(e)}")
            return False
            
    def disconnect(self) -> bool:
        """Disconnect from storage"""
        if self._connection:
            self._connection = None
            self.logger.info("Disconnected from cloud storage")
        return True
        
    def read_data(self, query_or_path: str, **kwargs) -> pd.DataFrame:
        """
        Read object from storage.
        AWS Equivalent: s3.get_object() with pandas
        """
        try:
            self.logger.info(f"Reading cloud object: {query_or_path}")
            
            # For POC, simulate S3 read
            mock_data = {
                'transaction_id': range(1, 1001),
                'amount': [100 + (i * 5) for i in range(1, 1001)],
                'currency': ['USD'] * 500 + ['EUR'] * 500,
                'timestamp': pd.date_range('2024-01-01', periods=1000, freq='H')
            }
            
            df = pd.DataFrame(mock_data)
            self.logger.info(f"Read {len(df)} rows from cloud storage")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read from cloud storage: {str(e)}")
            raise
            
    def write_data(self, data: pd.DataFrame, destination: str, **kwargs) -> bool:
        """
        Write object to storage.
        AWS Equivalent: s3.put_object() with pandas integration
        """
        try:
            self.logger.info(f"Writing {len(data)} rows to cloud path: {destination}")
            # In production, we would use boto3 to write to S3
            return True
        except Exception as e:
            self.logger.error(f"Failed to write to cloud storage: {str(e)}")
            return False
            
    def list_resources(self) -> List[str]:
        """
        List objects in bucket.
        AWS Equivalent: s3.list_objects_v2()
        """
        try:
            # Simulate S3 object listing
            objects = [
                'data/raw/customers.csv',
                'data/raw/sales.parquet', 
                'data/processed/monthly_reports.json',
                'data/archive/historical_data.csv',
                'analytics/dashboard_data.parquet'
            ]
            return objects
        except Exception as e:
            self.logger.error(f"Failed to list cloud objects: {str(e)}")
            return []

class DataConnectorFactory:
    """
    Factory for creating data connectors.
    AWS Equivalent: AWS Glue Connector catalog + instantiation
    """
    
    @staticmethod
    def create_connector(connection_config: ConnectionConfig) -> WerfenDataConnector:
        """
        Create connector based on configuration.
        
        Args:
            connection_config: Connection configuration
            
        Returns:
            WerfenDataConnector: Appropriate connector
        """
        connector_map = {
            'local_file': LocalFileConnector,
            'database': DatabaseConnector,
            'cloud_storage': CloudStorageConnector
        }
        
        connector_class = connector_map.get(connection_config.connection_type)
        if not connector_class:
            raise ValueError(f"Unsupported connection type: {connection_config.connection_type}")
            
        return connector_class(connection_config)
        
    @staticmethod
    def create_from_url(url: str, **kwargs) -> WerfenDataConnector:
        """
        Create connector from URL.
        AWS Equivalent: AWS Glue Connection from connection string
        
        Args:
            url: Connection URL (file://, s3://, postgresql://, etc.)
            **kwargs: Additional parameters
            
        Returns:
            WerfenDataConnector: Appropriate connector
        """
        parsed = urlparse(url)
        
        if parsed.scheme in ['file', '']:
            config = ConnectionConfig(
                connection_type='local_file',
                path=parsed.path or parsed.netloc
            )
        elif parsed.scheme == 's3':
            config = ConnectionConfig(
                connection_type='cloud_storage',
                bucket=parsed.netloc,
                path=parsed.path.lstrip('/') if parsed.path else None
            )
        elif parsed.scheme in ['postgresql', 'mysql', 'sqlite']:
            config = ConnectionConfig(
                connection_type='database',
                host=parsed.hostname,
                port=parsed.port,
                database=parsed.path.lstrip('/') if parsed.path else None,
                username=parsed.username,
                password=parsed.password
            )
        else:
            raise ValueError(f"Unsupported URL scheme: {parsed.scheme}")
            
        # Add additional parameters
        if kwargs:
            config.extra_params = kwargs
            
        return DataConnectorFactory.create_connector(config) 