"""
Optimizaciones Pandas - Werfen Data Pipeline
============================================

Optimizaciones específicas para pandas que implementan mejores prácticas
de gestión de memoria, preparadas para migración a AWS Glue.

Autor: Lead Software Architect - Werfen Data Team
Fecha: Enero 2025
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterator, Union
from dataclasses import dataclass

# Importar componentes existentes
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.logging.structured_logger import get_pipeline_logger

@dataclass
class PandasOptimizationConfig:
    """Configuración de optimización para pandas"""
    chunk_size: int = 10000
    enable_dtype_optimization: bool = True
    enable_memory_mapping: bool = True
    use_categorical_strings: bool = True
    parallel_processing: bool = True
    
class WerfenPandasOptimizer:
    """
    Optimizador de rendimiento para operaciones pandas
    
    Equivalencias AWS:
    - Chunking → AWS Glue dynamic frame processing
    - Dtype optimization → Glue schema optimization
    - Memory mapping → S3 streaming with Glue
    - Categorical data → Parquet dictionary encoding
    """
    
    def __init__(self, config: Optional[PandasOptimizationConfig] = None):
        self.config = config or PandasOptimizationConfig()
        self.logger = get_pipeline_logger("pandas_optimizer")
        
        # Configuración AWS equivalente
        self.aws_equivalents = {
            'chunking': 'AWS Glue: Dynamic frame processing',
            'dtype_optimization': 'Glue: Schema optimization',
            'memory_mapping': 'S3: Streaming with Glue',
            'categorical_data': 'Parquet: Dictionary encoding'
        }
        
        self.logger.logger.info(
            "Pandas optimizer initialized",
            component="pandas_optimizer",
            chunk_size=self.config.chunk_size,
            dtype_optimization=self.config.enable_dtype_optimization,
            event_type="optimizer_init"
        )
    
    def optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimizar tipos de datos del DataFrame"""
        original_memory = df.memory_usage(deep=True).sum()
        optimized_df = df.copy()
        
        optimization_log = []
        
        for column in optimized_df.columns:
            col_type = optimized_df[column].dtype
            
            # Optimizar enteros
            if pd.api.types.is_integer_dtype(col_type):
                min_val = optimized_df[column].min()
                max_val = optimized_df[column].max()
                
                if min_val >= 0:  # Unsigned
                    if max_val < 255:
                        optimized_df[column] = optimized_df[column].astype('uint8')
                        optimization_log.append(f"{column}: {col_type} -> uint8")
                    elif max_val < 65535:
                        optimized_df[column] = optimized_df[column].astype('uint16')
                        optimization_log.append(f"{column}: {col_type} -> uint16")
                    elif max_val < 4294967295:
                        optimized_df[column] = optimized_df[column].astype('uint32')
                        optimization_log.append(f"{column}: {col_type} -> uint32")
                else:  # Signed
                    if min_val > -128 and max_val < 127:
                        optimized_df[column] = optimized_df[column].astype('int8')
                        optimization_log.append(f"{column}: {col_type} -> int8")
                    elif min_val > -32768 and max_val < 32767:
                        optimized_df[column] = optimized_df[column].astype('int16')
                        optimization_log.append(f"{column}: {col_type} -> int16")
                    elif min_val > -2147483648 and max_val < 2147483647:
                        optimized_df[column] = optimized_df[column].astype('int32')
                        optimization_log.append(f"{column}: {col_type} -> int32")
            
            # Optimizar flotantes
            elif pd.api.types.is_float_dtype(col_type):
                if optimized_df[column].min() >= np.finfo(np.float32).min and \
                   optimized_df[column].max() <= np.finfo(np.float32).max:
                    optimized_df[column] = optimized_df[column].astype('float32')
                    optimization_log.append(f"{column}: {col_type} -> float32")
            
            # Optimizar strings a categorical
            elif pd.api.types.is_object_dtype(col_type) and self.config.use_categorical_strings:
                unique_ratio = optimized_df[column].nunique() / len(optimized_df[column])
                if unique_ratio < 0.5:  # Si menos del 50% son valores únicos
                    optimized_df[column] = optimized_df[column].astype('category')
                    optimization_log.append(f"{column}: object -> category")
        
        optimized_memory = optimized_df.memory_usage(deep=True).sum()
        memory_reduction = (original_memory - optimized_memory) / original_memory * 100
        
        self.logger.logger.info(
            "DataFrame dtypes optimized",
            component="pandas_optimizer",
            original_memory_mb=original_memory / (1024 * 1024),
            optimized_memory_mb=optimized_memory / (1024 * 1024),
            memory_reduction_percent=memory_reduction,
            optimizations=optimization_log,
            event_type="dtypes_optimized"
        )
        
        return optimized_df
    
    def chunked_processing(self, file_path: str, 
                          processing_func: callable) -> Iterator[pd.DataFrame]:
        """Procesamiento por chunks para archivos grandes"""
        chunk_count = 0
        
        try:
            for chunk in pd.read_csv(file_path, chunksize=self.config.chunk_size):
                # Optimizar tipos de datos del chunk
                if self.config.enable_dtype_optimization:
                    chunk = self.optimize_dtypes(chunk)
                
                # Aplicar función de procesamiento
                processed_chunk = processing_func(chunk)
                
                chunk_count += 1
                
                self.logger.logger.debug(
                    "Chunk processed",
                    component="pandas_optimizer",
                    chunk_number=chunk_count,
                    chunk_size=len(chunk),
                    event_type="chunk_processed"
                )
                
                yield processed_chunk
                
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="chunked_processing",
                file_path=file_path
            )
    
    def memory_efficient_merge(self, left_df: pd.DataFrame, right_df: pd.DataFrame,
                              on: Union[str, List[str]], how: str = 'inner') -> pd.DataFrame:
        """Merge eficiente en memoria usando chunking si es necesario"""
        # Estimar memoria necesaria
        left_memory = left_df.memory_usage(deep=True).sum()
        right_memory = right_df.memory_usage(deep=True).sum()
        estimated_result_memory = left_memory + right_memory
        
        # Si la memoria estimada es muy alta, usar chunking
        memory_threshold = 500 * 1024 * 1024  # 500 MB
        
        if estimated_result_memory > memory_threshold:
            return self._chunked_merge(left_df, right_df, on, how)
        else:
            # Merge normal
            result = pd.merge(left_df, right_df, on=on, how=how)
            
            self.logger.logger.info(
                "Memory efficient merge completed",
                component="pandas_optimizer",
                left_rows=len(left_df),
                right_rows=len(right_df),
                result_rows=len(result),
                merge_type="standard",
                event_type="merge_completed"
            )
            
            return result
    
    def _chunked_merge(self, left_df: pd.DataFrame, right_df: pd.DataFrame,
                      on: Union[str, List[str]], how: str) -> pd.DataFrame:
        """Merge por chunks para DataFrames grandes"""
        results = []
        
        # Dividir el DataFrame más grande en chunks
        if len(left_df) > len(right_df):
            chunk_df = left_df
            other_df = right_df
        else:
            chunk_df = right_df
            other_df = left_df
            # Invertir orden para el merge
            left_df, right_df = right_df, left_df
        
        chunk_size = self.config.chunk_size
        total_chunks = len(chunk_df) // chunk_size + 1
        
        for i in range(0, len(chunk_df), chunk_size):
            chunk = chunk_df.iloc[i:i + chunk_size]
            
            if len(left_df) > len(right_df):
                merged_chunk = pd.merge(chunk, other_df, on=on, how=how)
            else:
                merged_chunk = pd.merge(other_df, chunk, on=on, how=how)
            
            results.append(merged_chunk)
            
            self.logger.logger.debug(
                "Chunk merged",
                component="pandas_optimizer",
                chunk_number=(i // chunk_size) + 1,
                total_chunks=total_chunks,
                event_type="chunk_merged"
            )
        
        # Concatenar todos los resultados
        final_result = pd.concat(results, ignore_index=True)
        
        self.logger.logger.info(
            "Chunked merge completed",
            component="pandas_optimizer",
            total_chunks=len(results),
            result_rows=len(final_result),
            merge_type="chunked",
            event_type="chunked_merge_completed"
        )
        
        return final_result
    
    def optimize_aggregations(self, df: pd.DataFrame, 
                            group_by: List[str],
                            agg_functions: Dict[str, str]) -> pd.DataFrame:
        """Optimizar operaciones de agregación"""
        # Preparar DataFrame para agregación
        optimized_df = df.copy()
        
        # Optimizar tipos de datos antes de agrupar
        if self.config.enable_dtype_optimization:
            optimized_df = self.optimize_dtypes(optimized_df)
        
        # Realizar agregación
        try:
            result = optimized_df.groupby(group_by).agg(agg_functions)
            
            # Resetear índice si es necesario
            if isinstance(result.index, pd.MultiIndex):
                result = result.reset_index()
            
            self.logger.logger.info(
                "Aggregation optimized",
                component="pandas_optimizer",
                input_rows=len(df),
                output_rows=len(result),
                group_by_columns=group_by,
                aggregation_functions=list(agg_functions.keys()),
                event_type="aggregation_completed"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="optimize_aggregations",
                group_by=group_by
            )
            raise
    
    def get_memory_usage_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generar reporte detallado de uso de memoria"""
        memory_usage = df.memory_usage(deep=True)
        total_memory = memory_usage.sum()
        
        # Análisis por columna
        column_analysis = []
        for column in df.columns:
            col_memory = memory_usage[column]
            col_type = str(df[column].dtype)
            unique_values = df[column].nunique()
            null_count = df[column].isnull().sum()
            
            column_analysis.append({
                "column": column,
                "dtype": col_type,
                "memory_bytes": col_memory,
                "memory_mb": col_memory / (1024 * 1024),
                "memory_percent": (col_memory / total_memory) * 100,
                "unique_values": unique_values,
                "null_count": null_count,
                "null_percent": (null_count / len(df)) * 100
            })
        
        # Ordenar por uso de memoria
        column_analysis.sort(key=lambda x: x["memory_bytes"], reverse=True)
        
        report = {
            "total_memory_mb": total_memory / (1024 * 1024),
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "memory_per_row_bytes": total_memory / len(df) if len(df) > 0 else 0,
            "column_analysis": column_analysis,
            "optimization_suggestions": self._generate_memory_suggestions(df, column_analysis)
        }
        
        return report
    
    def _generate_memory_suggestions(self, df: pd.DataFrame, 
                                   column_analysis: List[Dict]) -> List[str]:
        """Generar sugerencias de optimización de memoria"""
        suggestions = []
        
        # Analizar columnas que consumen más memoria
        high_memory_columns = [col for col in column_analysis 
                              if col["memory_percent"] > 10]
        
        for col_info in high_memory_columns:
            if col_info["dtype"] == "object":
                if col_info["null_percent"] > 0:
                    suggestions.append(f"Column '{col_info['column']}': Consider filling nulls to enable categorical conversion")
                
                unique_ratio = col_info["unique_values"] / len(df)
                if unique_ratio < 0.5:
                    suggestions.append(f"Column '{col_info['column']}': Convert to categorical (unique ratio: {unique_ratio:.2%})")
            
            elif "int64" in col_info["dtype"]:
                suggestions.append(f"Column '{col_info['column']}': Consider using smaller integer type")
            
            elif "float64" in col_info["dtype"]:
                suggestions.append(f"Column '{col_info['column']}': Consider using float32 if precision allows")
        
        # Sugerencias AWS
        suggestions.extend([
            "AWS: Use Parquet format for better compression and columnar storage",
            "AWS: Consider partitioning large datasets in S3",
            "AWS: Use AWS Glue dynamic frames for schema evolution",
            "AWS: Implement data lifecycle policies for cost optimization"
        ])
        
        return suggestions
    
    def create_optimized_copy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crear copia optimizada del DataFrame"""
        optimized_df = df.copy()
        
        # Aplicar todas las optimizaciones
        if self.config.enable_dtype_optimization:
            optimized_df = self.optimize_dtypes(optimized_df)
        
        # Eliminar duplicados si existen
        initial_rows = len(optimized_df)
        optimized_df = optimized_df.drop_duplicates()
        duplicates_removed = initial_rows - len(optimized_df)
        
        # Resetear índice para liberar memoria
        optimized_df = optimized_df.reset_index(drop=True)
        
        self.logger.logger.info(
            "DataFrame optimized copy created",
            component="pandas_optimizer",
            original_rows=initial_rows,
            optimized_rows=len(optimized_df),
            duplicates_removed=duplicates_removed,
            event_type="optimized_copy_created"
        )
        
        return optimized_df

def get_pandas_optimizer(config: Optional[PandasOptimizationConfig] = None) -> WerfenPandasOptimizer:
    """Factory function para obtener el optimizador pandas"""
    return WerfenPandasOptimizer(config) 