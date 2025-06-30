"""
Módulo de Optimización de Rendimiento - Werfen Data Pipeline
===========================================================

Paquete completo de optimización de rendimiento que incluye:
- Profiling y benchmarking
- Optimizaciones DuckDB
- Caching inteligente
- Optimizaciones pandas

Equivalencias AWS preparadas para migración enterprise.

Autor: Lead Software Architect - Werfen Data Team
Fecha: Enero 2025
"""

from .performance_profiler import (
    WerfenPerformanceProfiler,
    PerformanceMetrics,
    get_performance_profiler
)

from .duckdb_optimizer import (
    WerfenDuckDBOptimizer,
    DuckDBOptimizationConfig,
    get_duckdb_optimizer
)

from .intelligent_cache import (
    WerfenIntelligentCache,
    CacheConfig,
    CacheEntry,
    get_intelligent_cache
)

from .pandas_optimizer import (
    WerfenPandasOptimizer,
    PandasOptimizationConfig,
    get_pandas_optimizer
)

# Factory functions principales
def get_performance_suite():
    """Obtener suite completa de optimización de rendimiento"""
    return {
        'profiler': get_performance_profiler(),
        'duckdb_optimizer': get_duckdb_optimizer(),
        'cache': get_intelligent_cache(),
        'pandas_optimizer': get_pandas_optimizer()
    }

__all__ = [
    'WerfenPerformanceProfiler',
    'WerfenDuckDBOptimizer', 
    'WerfenIntelligentCache',
    'WerfenPandasOptimizer',
    'PerformanceMetrics',
    'DuckDBOptimizationConfig',
    'CacheConfig',
    'CacheEntry',
    'PandasOptimizationConfig',
    'get_performance_profiler',
    'get_duckdb_optimizer',
    'get_intelligent_cache',
    'get_pandas_optimizer',
    'get_performance_suite'
] 