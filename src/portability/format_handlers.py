"""
Werfen Format Handlers - Manejadores de Formatos de Datos
=========================================================

Manejadores unificados para diferentes formatos de datos.
AWS Equivalent: AWS Glue Data Format Library + Schema Registry

Author: Werfen Data Team
Date: 2024
"""

import os
import json
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from dataclasses import dataclass
import io
import tempfile

# Import logging from our centralized system
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.logging.structured_logger import setup_structured_logging

@dataclass
class FormatConfig:
    """
    Configuración de formato unificada.
    AWS Equivalent: AWS Glue Schema configuration
    """
    format_type: str
    encoding: str = 'utf-8'
    delimiter: Optional[str] = None
    quote_char: Optional[str] = None
    escape_char: Optional[str] = None
    has_header: bool = True
    date_format: Optional[str] = None
    compression: Optional[str] = None
    schema: Optional[Dict[str, str]] = None
    extra_params: Optional[Dict[str, Any]] = None

@dataclass
class DataSchema:
    """
    Esquema de datos unificado.
    AWS Equivalent: AWS Glue Table Schema
    """
    columns: Dict[str, str]  # column_name -> data_type
    primary_key: Optional[List[str]] = None
    nullable_columns: Optional[List[str]] = None
    constraints: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

class WerfenFormatHandler(ABC):
    """
    Clase base abstracta para manejadores de formato.
    AWS Equivalent: AWS Glue Format interface
    """
    
    def __init__(self, config: FormatConfig):
        self.config = config
        self.logger = setup_structured_logging(service_name=f"werfen.format.{self.__class__.__name__}")
        
    @abstractmethod
    def read_data(self, source: Union[str, io.IOBase], **kwargs) -> pd.DataFrame:
        """Leer datos desde fuente"""
        pass
        
    @abstractmethod
    def write_data(self, data: pd.DataFrame, destination: Union[str, io.IOBase], **kwargs) -> bool:
        """Escribir datos a destino"""
        pass
        
    @abstractmethod
    def infer_schema(self, source: Union[str, io.IOBase], sample_size: int = 1000) -> DataSchema:
        """Inferir esquema de datos"""
        pass
        
    @abstractmethod
    def validate_data(self, data: pd.DataFrame, schema: Optional[DataSchema] = None) -> Dict[str, Any]:
        """Validar datos contra esquema"""
        pass
        
    def get_supported_compressions(self) -> List[str]:
        """Obtener tipos de compresión soportados"""
        return ['gzip', 'bz2', 'zip', 'xz']
        
    def detect_encoding(self, file_path: str, sample_size: int = 8192) -> str:
        """
        Detectar encoding de archivo.
        AWS Equivalent: AWS Glue automatic encoding detection
        """
        try:
            import chardet
            
            with open(file_path, 'rb') as f:
                raw_data = f.read(sample_size)
                result = chardet.detect(raw_data)
                return result.get('encoding', 'utf-8')
                
        except ImportError:
            self.logger.warning("chardet not available, using utf-8 encoding")
            return 'utf-8'
        except Exception as e:
            self.logger.error(f"Error detecting encoding: {str(e)}")
            return 'utf-8'

class CSVFormatHandler(WerfenFormatHandler):
    """
    Manejador para archivos CSV.
    AWS Equivalent: AWS Glue CSV SerDe
    """
    
    def read_data(self, source: Union[str, io.IOBase], **kwargs) -> pd.DataFrame:
        """
        Leer datos CSV.
        AWS Equivalent: AWS Glue CSV reader with auto-detection
        """
        try:
            # Preparar parámetros de lectura
            read_params = {
                'encoding': self.config.encoding,
                'delimiter': self.config.delimiter or ',',
                'quotechar': self.config.quote_char or '"',
                'escapechar': self.config.escape_char,
                'header': 0 if self.config.has_header else None,
                'compression': self.config.compression
            }
            
            # Agregar parámetros extras
            if self.config.extra_params:
                read_params.update(self.config.extra_params)
                
            # Agregar parámetros de llamada
            read_params.update(kwargs)
            
            # Leer datos
            if isinstance(source, str):
                df = pd.read_csv(source, **read_params)
            else:
                df = pd.read_csv(source, **read_params)
                
            # Aplicar schema si está disponible
            if self.config.schema:
                df = self._apply_schema(df, self.config.schema)
                
            self.logger.info(f"Read {len(df)} rows, {len(df.columns)} columns from CSV")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read CSV data: {str(e)}")
            raise
            
    def write_data(self, data: pd.DataFrame, destination: Union[str, io.IOBase], **kwargs) -> bool:
        """
        Escribir datos CSV.
        AWS Equivalent: AWS Glue CSV writer with compression
        """
        try:
            # Preparar parámetros de escritura
            write_params = {
                'encoding': self.config.encoding,
                'sep': self.config.delimiter or ',',
                'quotechar': self.config.quote_char or '"',
                'escapechar': self.config.escape_char,
                'header': self.config.has_header,
                'index': False,
                'compression': self.config.compression
            }
            
            # Agregar parámetros extras
            if self.config.extra_params:
                write_params.update(self.config.extra_params)
                
            # Agregar parámetros de llamada
            write_params.update(kwargs)
            
            # Escribir datos
            if isinstance(destination, str):
                data.to_csv(destination, **write_params)
            else:
                data.to_csv(destination, **write_params)
                
            self.logger.info(f"Wrote {len(data)} rows, {len(data.columns)} columns to CSV")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write CSV data: {str(e)}")
            return False
            
    def infer_schema(self, source: Union[str, io.IOBase], sample_size: int = 1000) -> DataSchema:
        """
        Inferir esquema CSV.
        AWS Equivalent: AWS Glue Crawler schema inference
        """
        try:
            # Leer muestra de datos
            sample_params = {
                'encoding': self.config.encoding,
                'delimiter': self.config.delimiter or ',',
                'quotechar': self.config.quote_char or '"',
                'header': 0 if self.config.has_header else None,
                'nrows': sample_size
            }
            
            if isinstance(source, str):
                sample_df = pd.read_csv(source, **sample_params)
            else:
                sample_df = pd.read_csv(source, **sample_params)
                
            # Inferir tipos de datos
            schema_columns = {}
            nullable_columns = []
            
            for column in sample_df.columns:
                dtype = sample_df[column].dtype
                has_nulls = sample_df[column].isnull().any()
                
                if has_nulls:
                    nullable_columns.append(column)
                    
                # Mapear tipos pandas a tipos estándar
                if pd.api.types.is_integer_dtype(dtype):
                    schema_columns[column] = 'INTEGER'
                elif pd.api.types.is_float_dtype(dtype):
                    schema_columns[column] = 'FLOAT'
                elif pd.api.types.is_bool_dtype(dtype):
                    schema_columns[column] = 'BOOLEAN'
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    schema_columns[column] = 'TIMESTAMP'
                else:
                    schema_columns[column] = 'STRING'
                    
            return DataSchema(
                columns=schema_columns,
                nullable_columns=nullable_columns,
                metadata={
                    'sample_size': len(sample_df),
                    'inferred_delimiter': self.config.delimiter or ',',
                    'has_header': self.config.has_header
                }
            )
            
        except Exception as e:
            self.logger.error(f"Failed to infer CSV schema: {str(e)}")
            raise
            
    def validate_data(self, data: pd.DataFrame, schema: Optional[DataSchema] = None) -> Dict[str, Any]:
        """
        Validar datos CSV.
        AWS Equivalent: AWS Glue data quality checks
        """
        try:
            validation_results = {
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'statistics': {
                    'total_rows': len(data),
                    'total_columns': len(data.columns),
                    'null_counts': data.isnull().sum().to_dict(),
                    'duplicate_rows': data.duplicated().sum()
                }
            }
            
            # Validar contra schema si está disponible
            if schema:
                # Verificar columnas
                missing_columns = set(schema.columns.keys()) - set(data.columns)
                extra_columns = set(data.columns) - set(schema.columns.keys())
                
                if missing_columns:
                    validation_results['errors'].append(f"Missing columns: {missing_columns}")
                    validation_results['is_valid'] = False
                    
                if extra_columns:
                    validation_results['warnings'].append(f"Extra columns: {extra_columns}")
                    
                # Verificar tipos de datos
                for column, expected_type in schema.columns.items():
                    if column in data.columns:
                        actual_dtype = data[column].dtype
                        
                        # Validación básica de tipos
                        if expected_type == 'INTEGER' and not pd.api.types.is_integer_dtype(actual_dtype):
                            validation_results['warnings'].append(f"Column {column} expected INTEGER, got {actual_dtype}")
                        elif expected_type == 'FLOAT' and not pd.api.types.is_numeric_dtype(actual_dtype):
                            validation_results['warnings'].append(f"Column {column} expected FLOAT, got {actual_dtype}")
                            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Failed to validate CSV data: {str(e)}")
            return {
                'is_valid': False,
                'errors': [str(e)],
                'warnings': [],
                'statistics': {}
            }
            
    def _apply_schema(self, df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
        """Aplicar schema a DataFrame"""
        try:
            for column, data_type in schema.items():
                if column in df.columns:
                    if data_type == 'INTEGER':
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    elif data_type == 'FLOAT':
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif data_type == 'BOOLEAN':
                        df[column] = df[column].astype('boolean')
                    elif data_type == 'TIMESTAMP':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                        
            return df
        except Exception as e:
            self.logger.warning(f"Failed to apply schema: {str(e)}")
            return df

class ParquetFormatHandler(WerfenFormatHandler):
    """
    Manejador para archivos Parquet.
    AWS Equivalent: AWS Glue Parquet SerDe
    """
    
    def read_data(self, source: Union[str, io.IOBase], **kwargs) -> pd.DataFrame:
        """
        Leer datos Parquet.
        AWS Equivalent: AWS Glue Parquet reader with columnar optimization
        """
        try:
            read_params = {}
            
            # Agregar parámetros extras
            if self.config.extra_params:
                read_params.update(self.config.extra_params)
                
            read_params.update(kwargs)
            
            # Leer datos
            if isinstance(source, str):
                df = pd.read_parquet(source, **read_params)
            else:
                df = pd.read_parquet(source, **read_params)
                
            self.logger.info(f"Read {len(df)} rows, {len(df.columns)} columns from Parquet")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read Parquet data: {str(e)}")
            raise
            
    def write_data(self, data: pd.DataFrame, destination: Union[str, io.IOBase], **kwargs) -> bool:
        """
        Escribir datos Parquet.
        AWS Equivalent: AWS Glue Parquet writer with compression
        """
        try:
            write_params = {
                'compression': self.config.compression or 'snappy',
                'index': False
            }
            
            if self.config.extra_params:
                write_params.update(self.config.extra_params)
                
            write_params.update(kwargs)
            
            # Escribir datos
            if isinstance(destination, str):
                data.to_parquet(destination, **write_params)
            else:
                data.to_parquet(destination, **write_params)
                
            self.logger.info(f"Wrote {len(data)} rows, {len(data.columns)} columns to Parquet")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write Parquet data: {str(e)}")
            return False
            
    def infer_schema(self, source: Union[str, io.IOBase], sample_size: int = 1000) -> DataSchema:
        """
        Inferir esquema Parquet.
        AWS Equivalent: Parquet metadata schema extraction
        """
        try:
            # Para Parquet, podemos usar metadatos del archivo
            if isinstance(source, str):
                import pyarrow.parquet as pq
                parquet_file = pq.ParquetFile(source)
                arrow_schema = parquet_file.schema_arrow
                
                schema_columns = {}
                for field in arrow_schema:
                    if field.type == "int64":
                        schema_columns[field.name] = 'INTEGER'
                    elif field.type == "double":
                        schema_columns[field.name] = 'FLOAT'
                    elif field.type == "bool":
                        schema_columns[field.name] = 'BOOLEAN'
                    elif "timestamp" in str(field.type):
                        schema_columns[field.name] = 'TIMESTAMP'
                    else:
                        schema_columns[field.name] = 'STRING'
                        
                return DataSchema(
                    columns=schema_columns,
                    metadata={
                        'parquet_version': parquet_file.metadata.format_version,
                        'num_row_groups': parquet_file.num_row_groups
                    }
                )
            else:
                # Fallback a lectura de muestra
                sample_df = pd.read_parquet(source, nrows=sample_size if hasattr(source, 'read') else None)
                return self._infer_schema_from_dataframe(sample_df)
                
        except ImportError:
            self.logger.warning("pyarrow not available, falling back to sample reading")
            sample_df = self.read_data(source)
            if len(sample_df) > sample_size:
                sample_df = sample_df.head(sample_size)
            return self._infer_schema_from_dataframe(sample_df)
        except Exception as e:
            self.logger.error(f"Failed to infer Parquet schema: {str(e)}")
            raise
            
    def validate_data(self, data: pd.DataFrame, schema: Optional[DataSchema] = None) -> Dict[str, Any]:
        """
        Validar datos Parquet.
        AWS Equivalent: Parquet file validation
        """
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'statistics': {
                'total_rows': len(data),
                'total_columns': len(data.columns),
                'memory_usage_mb': data.memory_usage(deep=True).sum() / 1024 / 1024,
                'dtypes': data.dtypes.to_dict()
            }
        }
        
        # Validaciones específicas de Parquet
        if data.index.name is not None:
            validation_results['warnings'].append("Index will be lost in Parquet format")
            
        return validation_results
        
    def _infer_schema_from_dataframe(self, df: pd.DataFrame) -> DataSchema:
        """Inferir schema desde DataFrame"""
        schema_columns = {}
        
        for column in df.columns:
            dtype = df[column].dtype
            
            if pd.api.types.is_integer_dtype(dtype):
                schema_columns[column] = 'INTEGER'
            elif pd.api.types.is_float_dtype(dtype):
                schema_columns[column] = 'FLOAT'
            elif pd.api.types.is_bool_dtype(dtype):
                schema_columns[column] = 'BOOLEAN'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                schema_columns[column] = 'TIMESTAMP'
            else:
                schema_columns[column] = 'STRING'
                
        return DataSchema(
            columns=schema_columns,
            metadata={'sample_size': len(df)}
        )

class JSONFormatHandler(WerfenFormatHandler):
    """
    Manejador para archivos JSON.
    AWS Equivalent: AWS Glue JSON SerDe
    """
    
    def read_data(self, source: Union[str, io.IOBase], **kwargs) -> pd.DataFrame:
        """
        Leer datos JSON.
        AWS Equivalent: AWS Glue JSON reader with nested data support
        """
        try:
            read_params = {
                'encoding': self.config.encoding,
                'compression': self.config.compression
            }
            
            if self.config.extra_params:
                read_params.update(self.config.extra_params)
                
            read_params.update(kwargs)
            
            # Leer datos
            if isinstance(source, str):
                df = pd.read_json(source, **read_params)
            else:
                df = pd.read_json(source, **read_params)
                
            self.logger.info(f"Read {len(df)} rows, {len(df.columns)} columns from JSON")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read JSON data: {str(e)}")
            raise
            
    def write_data(self, data: pd.DataFrame, destination: Union[str, io.IOBase], **kwargs) -> bool:
        """
        Escribir datos JSON.
        AWS Equivalent: AWS Glue JSON writer
        """
        try:
            write_params = {
                'orient': 'records',
                'compression': self.config.compression
            }
            
            if self.config.extra_params:
                write_params.update(self.config.extra_params)
                
            write_params.update(kwargs)
            
            # Escribir datos
            if isinstance(destination, str):
                data.to_json(destination, **write_params)
            else:
                data.to_json(destination, **write_params)
                
            self.logger.info(f"Wrote {len(data)} rows, {len(data.columns)} columns to JSON")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write JSON data: {str(e)}")
            return False
            
    def infer_schema(self, source: Union[str, io.IOBase], sample_size: int = 1000) -> DataSchema:
        """
        Inferir esquema JSON.
        AWS Equivalent: JSON schema inference with nested structure support
        """
        try:
            sample_df = self.read_data(source)
            if len(sample_df) > sample_size:
                sample_df = sample_df.head(sample_size)
                
            return self._infer_schema_from_dataframe(sample_df)
            
        except Exception as e:
            self.logger.error(f"Failed to infer JSON schema: {str(e)}")
            raise
            
    def validate_data(self, data: pd.DataFrame, schema: Optional[DataSchema] = None) -> Dict[str, Any]:
        """
        Validar datos JSON.
        AWS Equivalent: JSON data validation
        """
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'statistics': {
                'total_rows': len(data),
                'total_columns': len(data.columns),
                'nested_columns': [],
                'array_columns': []
            }
        }
        
        # Detectar columnas anidadas
        for column in data.columns:
            if data[column].dtype == 'object':
                sample_value = data[column].dropna().iloc[0] if not data[column].dropna().empty else None
                if isinstance(sample_value, (dict, list)):
                    if isinstance(sample_value, dict):
                        validation_results['statistics']['nested_columns'].append(column)
                    else:
                        validation_results['statistics']['array_columns'].append(column)
                        
        return validation_results
        
    def _infer_schema_from_dataframe(self, df: pd.DataFrame) -> DataSchema:
        """Inferir schema desde DataFrame JSON"""
        schema_columns = {}
        
        for column in df.columns:
            dtype = df[column].dtype
            
            if pd.api.types.is_integer_dtype(dtype):
                schema_columns[column] = 'INTEGER'
            elif pd.api.types.is_float_dtype(dtype):
                schema_columns[column] = 'FLOAT'
            elif pd.api.types.is_bool_dtype(dtype):
                schema_columns[column] = 'BOOLEAN'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                schema_columns[column] = 'TIMESTAMP'
            elif dtype == 'object':
                # Verificar si es estructura anidada
                sample_value = df[column].dropna().iloc[0] if not df[column].dropna().empty else None
                if isinstance(sample_value, dict):
                    schema_columns[column] = 'OBJECT'
                elif isinstance(sample_value, list):
                    schema_columns[column] = 'ARRAY'
                else:
                    schema_columns[column] = 'STRING'
            else:
                schema_columns[column] = 'STRING'
                
        return DataSchema(
            columns=schema_columns,
            metadata={'sample_size': len(df)}
        )

class FormatHandlerFactory:
    """
    Factory para crear manejadores de formato.
    AWS Equivalent: AWS Glue Format library with auto-detection
    """
    
    @staticmethod
    def create_handler(format_config: FormatConfig) -> WerfenFormatHandler:
        """
        Crear manejador basado en configuración.
        
        Args:
            format_config: Configuración de formato
            
        Returns:
            WerfenFormatHandler: Manejador apropiado
        """
        handler_map = {
            'csv': CSVFormatHandler,
            'parquet': ParquetFormatHandler,
            'json': JSONFormatHandler
        }
        
        handler_class = handler_map.get(format_config.format_type.lower())
        if not handler_class:
            raise ValueError(f"Unsupported format type: {format_config.format_type}")
            
        return handler_class(format_config)
        
    @staticmethod
    def detect_format(file_path: str) -> str:
        """
        Detectar formato de archivo basado en extensión.
        AWS Equivalent: AWS Glue automatic format detection
        
        Args:
            file_path: Ruta del archivo
            
        Returns:
            str: Tipo de formato detectado
        """
        suffix = Path(file_path).suffix.lower()
        
        format_map = {
            '.csv': 'csv',
            '.tsv': 'csv',
            '.txt': 'csv',
            '.parquet': 'parquet',
            '.pqt': 'parquet',
            '.json': 'json',
            '.jsonl': 'json',
            '.ndjson': 'json'
        }
        
        return format_map.get(suffix, 'csv')  # Default to CSV
        
    @staticmethod
    def create_from_file(file_path: str, **kwargs) -> WerfenFormatHandler:
        """
        Crear manejador desde archivo con detección automática.
        
        Args:
            file_path: Ruta del archivo
            **kwargs: Configuración adicional
            
        Returns:
            WerfenFormatHandler: Manejador apropiado
        """
        format_type = FormatHandlerFactory.detect_format(file_path)
        
        config = FormatConfig(
            format_type=format_type,
            **kwargs
        )
        
        return FormatHandlerFactory.create_handler(config) 