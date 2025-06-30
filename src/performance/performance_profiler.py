"""
Sistema de Profiling y Optimización de Rendimiento - Werfen Data Pipeline
=========================================================================

Sistema de análisis y optimización de rendimiento que implementa mejores prácticas
de performance engineering, preparado para equivalencias con AWS CloudWatch y X-Ray.

Autor: Lead Software Architect - Werfen Data Team
Fecha: Enero 2025
"""

import time
import psutil
import pandas as pd
import duckdb
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import json
import tracemalloc
import cProfile
import pstats
import io
from contextlib import contextmanager

# Importar componentes existentes
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.logging.structured_logger import get_pipeline_logger

@dataclass
class PerformanceMetrics:
    """Métricas de rendimiento detalladas"""
    operation_name: str
    start_time: float
    end_time: float
    duration_ms: float
    memory_usage_mb: float
    cpu_percent: float
    records_processed: int
    records_per_second: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    optimization_applied: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para logging"""
        return asdict(self)

@dataclass
class SystemResources:
    """Recursos del sistema en un momento dado"""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    disk_usage_percent: float
    
class WerfenPerformanceProfiler:
    """
    Profiler de rendimiento para el pipeline Werfen
    
    Características:
    - Profiling detallado de operaciones
    - Monitoreo de recursos del sistema
    - Benchmarking comparativo
    - Recomendaciones de optimización
    """
    
    def __init__(self):
        self.metrics_history: List[PerformanceMetrics] = []
        self.system_snapshots: List[SystemResources] = []
        
        # Configuración AWS equivalente
        self.aws_config = {
            'cloudwatch_namespace': 'Werfen/DataPipeline',
            'xray_service_name': 'werfen-data-pipeline'
        }
        
        # Logger para métricas
        self.logger = get_pipeline_logger("performance_profiler")
        
        # Baseline del sistema
        self._baseline_resources = self._capture_system_resources()
        
        self.logger.logger.info(
            "Performance profiler initialized",
            component="performance_profiler",
            baseline_memory_mb=self._baseline_resources.memory_available_mb,
            event_type="profiler_init"
        )
    
    def _capture_system_resources(self) -> SystemResources:
        """Capturar estado actual de recursos del sistema"""
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return SystemResources(
            timestamp=time.time(),
            cpu_percent=psutil.cpu_percent(interval=0.1),
            memory_percent=memory.percent,
            memory_available_mb=memory.available / (1024 * 1024),
            disk_usage_percent=disk.percent
        )
    
    @contextmanager
    def profile_operation(self, operation_name: str, records_count: int = 0):
        """Context manager para profiling de operaciones"""
        start_time = time.time()
        start_resources = self._capture_system_resources()
        
        try:
            yield
            
        finally:
            end_time = time.time()
            end_resources = self._capture_system_resources()
            
            # Calcular métricas
            duration_ms = (end_time - start_time) * 1000
            records_per_second = records_count / (duration_ms / 1000) if duration_ms > 0 and records_count > 0 else 0
            
            metrics = PerformanceMetrics(
                operation_name=operation_name,
                start_time=start_time,
                end_time=end_time,
                duration_ms=duration_ms,
                memory_usage_mb=start_resources.memory_available_mb - end_resources.memory_available_mb,
                cpu_percent=end_resources.cpu_percent,
                records_processed=records_count,
                records_per_second=records_per_second,
                disk_io_read_mb=0,  # Simplificado para el POC
                disk_io_write_mb=0
            )
            
            # Almacenar métricas
            self.metrics_history.append(metrics)
            
            # Log estructurado
            self.logger.log_performance_metric(
                metric_name=f"{operation_name}_performance",
                metric_value=duration_ms,
                metric_unit="milliseconds",
                context={
                    "records_processed": records_count,
                    "records_per_second": records_per_second,
                    "memory_usage_mb": metrics.memory_usage_mb,
                    "cpu_percent": metrics.cpu_percent
                }
            )
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Generar resumen de rendimiento"""
        if not self.metrics_history:
            return {"status": "no_metrics"}
        
        durations = [m.duration_ms for m in self.metrics_history]
        records_per_second = [m.records_per_second for m in self.metrics_history if m.records_per_second > 0]
        
        return {
            "total_operations": len(self.metrics_history),
            "total_duration_ms": sum(durations),
            "average_duration_ms": sum(durations) / len(durations),
            "max_records_per_second": max(records_per_second) if records_per_second else 0,
            "operations": [m.operation_name for m in self.metrics_history]
        }

def get_performance_profiler() -> WerfenPerformanceProfiler:
    """Factory function para obtener el profiler de rendimiento"""
    return WerfenPerformanceProfiler() 