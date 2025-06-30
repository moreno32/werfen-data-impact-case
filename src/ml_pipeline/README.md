# Werfen ML Pipeline 🤖

Pipeline completo de Machine Learning para segmentación de clientes con visualizaciones profesionales y colores corporativos Werfen.

## 🎯 Funcionalidades Principales

### 1. Clustering Interactivo
```python
from ml_pipeline import WerfenMLPipeline

# Inicializar pipeline
ml_pipeline = WerfenMLPipeline(
    db_path="path/to/database.db",
    output_dir="ml_outputs",
    random_state=42
)

# Ejecutar clustering completo
results = ml_pipeline.run_interactive_clustering(
    k_range=range(2, 8),           # Rango para evaluar K
    optimal_k=3,                   # K específico (o None para auto-optimización)
    feature_strategy='business_driven',  # Estrategia de features
    persona_mapping={              # Mapeo cluster -> persona
        0: 'Champions',
        1: 'Potentials', 
        2: 'Loyalists'
    }
)
```

### 2. Visualizaciones con Colores Corporativos

#### Gráfico del Elbow Method
```python
ml_pipeline.visualizer.plot_elbow_method_interactive(
    evaluation_results, 
    optimal_k=3
)
```

#### Perfiles de Clusters/Personas
```python
ml_pipeline.visualizer.plot_branded_cluster_profiles(
    profile_df=centroids_df,
    features_to_plot=['total_sold_quantity', 'weighted_avg_foc_ratio', 'total_sold_transactions'],
    title='Customer Persona Profiles',
    reverse_rank_features=['weighted_avg_foc_ratio']  # Menor valor = mejor ranking
)
```

#### Dashboard Completo
```python
ml_pipeline.visualizer.plot_persona_comparison_dashboard(
    persona_kpis=persona_kpis,
    centroids_df=centroids_df,
    features_to_plot=selected_features
)
```

## 🎨 Colores Corporativos Werfen

El pipeline utiliza la paleta oficial de colores Werfen:

- **🔵 WERFEN_BLUE** (`#06038D`): Color principal para títulos y elementos destacados
- **🟠 ACCENT_ORANGE** (`#E87722`): Color de acento para resaltar K óptimo y elementos importantes
- **⚪ NEUTRAL_GRAY** (`#B0B0B0`): Color neutral para elementos secundarios
- **🟢 SUCCESS_GREEN** (`#7ED321`): Color para indicadores de éxito

## 📊 Componentes del Pipeline

### 1. DataLoader
- Carga datos desde `marts_customer_summary`
- Validaciones básicas de calidad
- Filtrado por actividad mínima

### 2. FeatureEngineer
- **Estrategias de selección**:
  - `business_driven`: Features basadas en conocimiento de negocio
  - `statistical`: Selección basada en varianza estadística
  - `hybrid`: Combinación de ambas estrategias
- Escalado automático con StandardScaler

### 3. KMeansClusterer
- Optimización automática de K usando múltiples métricas
- Evaluación con Elbow Method, Silhouette Score, Calinski-Harabasz
- Entrenamiento con configuración reproducible

### 4. PersonaAssigner
- Mapeo inteligente cluster → persona de negocio
- Análisis de perfiles por características
- Cálculo de KPIs por persona

### 5. ClusterVisualizer
- Gráficos profesionales con colores corporativos
- Múltiples tipos de visualización
- Exportación en alta resolución

## 📋 Resultados del Pipeline

El pipeline retorna un diccionario completo con:

```python
{
    'customer_data_with_clusters': DataFrame,  # Datos con clusters asignados
    'centroids_df': DataFrame,                 # Centroides de clusters
    'persona_centroids_df': DataFrame,         # Centroides por persona
    'persona_kpis': DataFrame,                 # KPIs por persona
    'optimal_k': int,                          # K óptimo utilizado
    'evaluation_results': Dict,                # Resultados de evaluación de K
    'selected_features': List[str]             # Features seleccionadas
}
```

## 🔧 Configuración Flexible

### Selección de K
```python
# Auto-optimización
results = ml_pipeline.run_interactive_clustering(optimal_k=None)

# K específico
results = ml_pipeline.run_interactive_clustering(optimal_k=3)

# Rango personalizado
results = ml_pipeline.run_interactive_clustering(k_range=range(2, 10))
```

### Estrategias de Features
```python
# Basada en negocio (recomendada)
results = ml_pipeline.run_interactive_clustering(feature_strategy='business_driven')

# Basada en estadísticas
results = ml_pipeline.run_interactive_clustering(feature_strategy='statistical')

# Híbrida
results = ml_pipeline.run_interactive_clustering(feature_strategy='hybrid')
```

### Mapeo de Personas
```python
# Mapeo personalizado
custom_mapping = {
    0: 'High Value Customers',
    1: 'Growth Opportunities',
    2: 'Stable Base'
}

results = ml_pipeline.run_interactive_clustering(persona_mapping=custom_mapping)
```

## 🚀 Uso en Notebook

Para usar en Jupyter Notebook:

```python
# Importar y configurar
from ml_pipeline import WerfenMLPipeline
from config import WerfenConfig

config = WerfenConfig()
ml_pipeline = WerfenMLPipeline(db_path=str(config.main_database_path))

# Ejecutar clustering completo con visualizaciones
results = ml_pipeline.run_interactive_clustering(
    optimal_k=3,
    persona_mapping={0: 'Champions', 1: 'Potentials', 2: 'Loyalists'}
)

# Los gráficos se muestran automáticamente en el notebook
```

## 📈 Métricas y KPIs

El pipeline calcula automáticamente:

- **Distribución de clusters**: Conteo y porcentaje por cluster
- **Centroides**: Valores promedio por feature y cluster
- **KPIs por persona**:
  - Customer Count
  - Total Sales Volume
  - Average FOC Ratio
  - % of Total Sales
  - % of Customers

## 🎯 Casos de Uso

1. **Segmentación de Clientes**: Identificar grupos naturales en la base de clientes
2. **Análisis de Personas**: Mapear clusters a arquetipos de negocio
3. **Estrategias Diferenciadas**: Desarrollar tácticas específicas por segmento
4. **Monitoreo de Performance**: Tracking de KPIs por persona
5. **Optimización de Recursos**: Asignación eficiente de esfuerzos comerciales

---

**Desarrollado por el Equipo de Data Science de Werfen** 🧬 