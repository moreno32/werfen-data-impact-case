"""
Pipeline ML de Werfen
====================

Módulo completo para análisis de clustering de clientes.

Componentes principales:
- data_loader: Carga de datos desde main_marts.marts_customer_summary
- feature_engineering: Selección y transformación de features
- clustering: Algoritmos K-means con optimización automática
- evaluation: Métricas de evaluación de clusters
- visualization: Gráficos interactivos con branding Werfen
- pipeline: Orquestador principal del proceso completo

Uso básico:
    from ml_pipeline import WerfenMLPipeline
    
    pipeline = WerfenMLPipeline(db_path="path/to/werfen.db")
    results = pipeline.run_interactive_clustering()

Módulos:
- data_loader: Carga de datos desde main_marts.marts_customer_summary
- feature_engineering: Selección y escalado de características
- clustering: Algoritmo K-means y optimización de K
- persona_assignment: Asignación de personas basada en clusters
- visualization: Visualizaciones y reportes del modelo
- pipeline: Orquestador principal del pipeline ML

Autor: Werfen Data Science Team
"""

__version__ = "1.0.0"
__author__ = "Werfen Data Science Team"

from .pipeline import WerfenMLPipeline
from .data_loader import DataLoader
from .feature_engineering import FeatureEngineer
from .clustering import KMeansClusterer
from .persona_assignment import PersonaAssigner
from .visualization import ClusterVisualizer

__all__ = [
    'WerfenMLPipeline',
    'DataLoader', 
    'FeatureEngineer',
    'KMeansClusterer',
    'PersonaAssigner',
    'ClusterVisualizer'
] 