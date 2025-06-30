"""
Visualization Module
====================

Módulo para crear visualizaciones del pipeline ML y análisis de clusters.
Incluye gráficos de optimización de K, perfiles de clusters y KPIs de personas.

Autor: Werfen Data Science Team
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurar estilo visual
plt.style.use('default')
sns.set_palette("husl")


class ClusterVisualizer:
    """
    Visualizador para análisis de clustering y personas Werfen.
    
    Genera gráficos profesionales alineados con la marca.
    """
    
    def __init__(self):
        """Inicializar visualizador con colores de marca Werfen."""
        # Paleta de colores Werfen
        self.WERFEN_BLUE = "#06038D"
        self.ACCENT_ORANGE = "#E87722"
        self.NEUTRAL_GRAY = "#B0B0B0"
        self.LIGHT_BLUE = "#4A90E2"
        self.SUCCESS_GREEN = "#7ED321"
        
        # Configurar estilo
        sns.set_style("whitegrid")
        plt.rcParams.update({
            'font.size': 11,
            'axes.titlesize': 14,
            'axes.labelsize': 12,
            'xtick.labelsize': 10,
            'ytick.labelsize': 10,
            'legend.fontsize': 10,
            'figure.titlesize': 16
        })
    
    def plot_k_optimization(self, 
                           evaluation_results: Dict[str, Any],
                           optimal_k: int,
                           save_path: Optional[str] = None) -> None:
        """
        Crear gráfico de optimización de K con múltiples métricas.
        
        Args:
            evaluation_results: Resultados de evaluación de K.
            optimal_k: Valor K óptimo seleccionado.
            save_path: Ruta para guardar gráfico.
        """
        logger.info("Creando gráfico de optimización de K...")
        
        k_values = evaluation_results['k_values']
        
        # Determinar número de subplots necesarios
        metrics_available = []
        if evaluation_results.get('wcss'):
            metrics_available.append('wcss')
        if evaluation_results.get('silhouette_scores'):
            metrics_available.append('silhouette')
        if evaluation_results.get('calinski_scores'):
            metrics_available.append('calinski')
        
        n_plots = len(metrics_available)
        if n_plots == 0:
            logger.warning("No hay métricas disponibles para graficar")
            return
        
        # Crear subplots
        fig, axes = plt.subplots(1, n_plots, figsize=(6 * n_plots, 6))
        if n_plots == 1:
            axes = [axes]
        
        # Plot WCSS (Elbow Method)
        if 'wcss' in metrics_available:
            ax_idx = metrics_available.index('wcss')
            ax = axes[ax_idx]
            
            ax.plot(k_values, evaluation_results['wcss'], 
                   marker='o', color=self.WERFEN_BLUE, linewidth=2, markersize=8)
            ax.axvline(x=optimal_k, color=self.ACCENT_ORANGE, 
                      linestyle='--', linewidth=2, label=f'Optimal K = {optimal_k}')
            
            ax.set_title('Método del Codo (WCSS)', fontweight='bold', color=self.WERFEN_BLUE)
            ax.set_xlabel('Número de Clusters (K)')
            ax.set_ylabel('WCSS (Inercia)')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        # Plot Silhouette Score
        if 'silhouette' in metrics_available:
            ax_idx = metrics_available.index('silhouette')
            ax = axes[ax_idx]
            
            ax.plot(k_values, evaluation_results['silhouette_scores'], 
                   marker='s', color=self.SUCCESS_GREEN, linewidth=2, markersize=8)
            ax.axvline(x=optimal_k, color=self.ACCENT_ORANGE, 
                      linestyle='--', linewidth=2, label=f'Optimal K = {optimal_k}')
            
            ax.set_title('Silhouette Score', fontweight='bold', color=self.WERFEN_BLUE)
            ax.set_xlabel('Número de Clusters (K)')
            ax.set_ylabel('Silhouette Score')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        # Plot Calinski-Harabasz Score
        if 'calinski' in metrics_available:
            ax_idx = metrics_available.index('calinski')
            ax = axes[ax_idx]
            
            ax.plot(k_values, evaluation_results['calinski_scores'], 
                   marker='^', color=self.LIGHT_BLUE, linewidth=2, markersize=8)
            ax.axvline(x=optimal_k, color=self.ACCENT_ORANGE, 
                      linestyle='--', linewidth=2, label=f'Optimal K = {optimal_k}')
            
            ax.set_title('Calinski-Harabasz Score', fontweight='bold', color=self.WERFEN_BLUE)
            ax.set_xlabel('Número de Clusters (K)')
            ax.set_ylabel('CH Score')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        plt.suptitle('Optimización del Número de Clusters', 
                    fontsize=18, fontweight='bold', color=self.WERFEN_BLUE, y=1.02)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Gráfico guardado en: {save_path}")
        
        plt.show()
    
    def plot_cluster_profiles(self, 
                            cluster_profile: pd.DataFrame,
                            persona_map: Dict[int, str],
                            features_to_plot: Optional[List[str]] = None,
                            save_path: Optional[str] = None) -> None:
        """
        Crear gráficos de barras comparando perfiles de clusters/personas.
        
        Args:
            cluster_profile: DataFrame con perfiles de clusters.
            persona_map: Mapeo cluster -> persona.
            features_to_plot: Features específicas a graficar.
            save_path: Ruta para guardar gráfico.
        """
        logger.info("Creando gráficos de perfiles de clusters...")
        
        # Agregar nombres de personas al profile
        profile_with_personas = cluster_profile.copy()
        profile_with_personas['persona'] = profile_with_personas.index.map(persona_map)
        profile_with_personas.set_index('persona', inplace=True)
        
        # Seleccionar features a graficar
        if features_to_plot is None:
            numeric_cols = profile_with_personas.select_dtypes(include=[np.number]).columns
            exclude_cols = ['customer_count', 'pct_customers', 'within_cluster_distance']
            features_to_plot = [col for col in numeric_cols if col not in exclude_cols][:5]
        
        # Crear subplots
        n_features = len(features_to_plot)
        fig, axes = plt.subplots(1, n_features, figsize=(5 * n_features, 6))
        if n_features == 1:
            axes = [axes]
        
        # Definir colores por persona
        persona_colors = {
            'Champions': self.SUCCESS_GREEN,
            'Loyalists': self.WERFEN_BLUE,
            'Potentials': self.ACCENT_ORANGE
        }
        
        for i, feature in enumerate(features_to_plot):
            ax = axes[i]
            
            # Obtener colores para cada persona
            colors = [persona_colors.get(persona, self.NEUTRAL_GRAY) 
                     for persona in profile_with_personas.index]
            
            # Crear gráfico de barras
            bars = ax.bar(profile_with_personas.index, 
                         profile_with_personas[feature],
                         color=colors, alpha=0.8)
            
            # Agregar valores en las barras
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.1f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom',
                           fontweight='bold')
            
            # Formatear gráfico
            ax.set_title(f'{feature.replace("_", " ").title()}', 
                        fontweight='bold', color=self.WERFEN_BLUE, pad=15)
            ax.set_xlabel('Persona', fontweight='bold')
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
        
        plt.suptitle('Perfiles de Personas - Comparación de Características', 
                    fontsize=18, fontweight='bold', color=self.WERFEN_BLUE, y=1.02)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Gráfico guardado en: {save_path}")
        
        plt.show()
    
    def plot_persona_kpis(self, 
                         persona_kpis: pd.DataFrame,
                         save_path: Optional[str] = None) -> None:
        """
        Crear visualización de KPIs de personas.
        
        Args:
            persona_kpis: DataFrame con KPIs por persona.
            save_path: Ruta para guardar gráfico.
        """
        logger.info("Creando gráficos de KPIs por persona...")
        
        # Seleccionar KPIs principales para visualizar
        kpi_columns = []
        if 'Customer Count' in persona_kpis.columns:
            kpi_columns.append('Customer Count')
        if 'Total Sales Volume' in persona_kpis.columns:
            kpi_columns.append('Total Sales Volume')
        if 'Avg Sales per Customer' in persona_kpis.columns:
            kpi_columns.append('Avg Sales per Customer')
        if 'Avg FOC Ratio' in persona_kpis.columns:
            kpi_columns.append('Avg FOC Ratio')
        
        if not kpi_columns:
            logger.warning("No se encontraron KPIs para visualizar")
            return
        
        # Crear subplots
        n_kpis = len(kpi_columns)
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()
        
        # Colores por persona
        persona_colors = {
            'Champions': self.SUCCESS_GREEN,
            'Loyalists': self.WERFEN_BLUE,
            'Potentials': self.ACCENT_ORANGE
        }
        
        for i, kpi in enumerate(kpi_columns):
            if i >= len(axes):
                break
                
            ax = axes[i]
            
            # Obtener colores
            colors = [persona_colors.get(persona, self.NEUTRAL_GRAY) 
                     for persona in persona_kpis.index]
            
            # Crear gráfico
            bars = ax.bar(persona_kpis.index, persona_kpis[kpi], 
                         color=colors, alpha=0.8)
            
            # Agregar valores
            for bar in bars:
                height = bar.get_height()
                if kpi == 'Avg FOC Ratio':
                    label = f'{height:.3f}'
                elif height > 1000:
                    label = f'{height:,.0f}'
                else:
                    label = f'{height:.1f}'
                
                ax.annotate(label,
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom',
                           fontweight='bold')
            
            # Formatear
            ax.set_title(kpi, fontweight='bold', color=self.WERFEN_BLUE, pad=15)
            ax.set_xlabel('Persona', fontweight='bold')
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
            
            # Formato especial para ratios
            if 'Ratio' in kpi:
                ax.set_ylim(0, max(persona_kpis[kpi]) * 1.2)
        
        # Ocultar subplots vacíos
        for i in range(len(kpi_columns), len(axes)):
            axes[i].set_visible(False)
        
        plt.suptitle('KPIs de Negocio por Persona', 
                    fontsize=18, fontweight='bold', color=self.WERFEN_BLUE, y=0.98)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Gráfico guardado en: {save_path}")
        
        plt.show()
    
    def plot_cluster_distribution(self, 
                                 labels: np.ndarray,
                                 persona_map: Dict[int, str],
                                 save_path: Optional[str] = None) -> None:
        """
        Crear gráfico de distribución de clusters/personas.
        
        Args:
            labels: Array con etiquetas de clusters.
            persona_map: Mapeo cluster -> persona.
            save_path: Ruta para guardar gráfico.
        """
        logger.info("Creando gráfico de distribución de personas...")
        
        # Contar clusters
        cluster_counts = pd.Series(labels).value_counts().sort_index()
        
        # Mapear a personas
        persona_counts = {}
        for cluster_id, count in cluster_counts.items():
            persona = persona_map.get(cluster_id, f'Cluster {cluster_id}')
            persona_counts[persona] = count
        
        # Crear gráfico de pie
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Gráfico de pie
        colors = [self.SUCCESS_GREEN if 'Champions' in persona else 
                 self.WERFEN_BLUE if 'Loyalists' in persona else 
                 self.ACCENT_ORANGE for persona in persona_counts.keys()]
        
        wedges, texts, autotexts = ax1.pie(persona_counts.values(), 
                                          labels=persona_counts.keys(),
                                          colors=colors,
                                          autopct='%1.1f%%',
                                          startangle=90)
        
        ax1.set_title('Distribución de Personas', 
                     fontweight='bold', color=self.WERFEN_BLUE, pad=20)
        
        # Gráfico de barras
        bars = ax2.bar(persona_counts.keys(), persona_counts.values(), 
                      color=colors, alpha=0.8)
        
        # Agregar valores en barras
        for bar in bars:
            height = bar.get_height()
            ax2.annotate(f'{int(height)}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom',
                        fontweight='bold')
        
        ax2.set_title('Número de Clientes por Persona', 
                     fontweight='bold', color=self.WERFEN_BLUE, pad=20)
        ax2.set_xlabel('Persona', fontweight='bold')
        ax2.set_ylabel('Número de Clientes', fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Gráfico guardado en: {save_path}")
        
        plt.show()
    
    def create_executive_summary_plot(self, 
                                    persona_kpis: pd.DataFrame,
                                    cluster_profile: pd.DataFrame,
                                    persona_map: Dict[int, str],
                                    save_path: Optional[str] = None) -> None:
        """
        Crear un dashboard ejecutivo con resumen completo.
        
        Args:
            persona_kpis: DataFrame con KPIs por persona.
            cluster_profile: DataFrame con perfiles de clusters.
            persona_map: Mapeo cluster -> persona.
            save_path: Ruta para guardar gráfico.
        """
        logger.info("Creando dashboard ejecutivo...")
        
        fig = plt.figure(figsize=(16, 12))
        gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # 1. Distribución de personas (top-left)
        ax1 = fig.add_subplot(gs[0, 0])
        if 'Customer Count' in persona_kpis.columns:
            colors = [self.SUCCESS_GREEN if 'Champions' in p else 
                     self.WERFEN_BLUE if 'Loyalists' in p else 
                     self.ACCENT_ORANGE for p in persona_kpis.index]
            
            ax1.pie(persona_kpis['Customer Count'], labels=persona_kpis.index,
                   colors=colors, autopct='%1.1f%%', startangle=90)
            ax1.set_title('Distribución de Clientes', fontweight='bold')
        
        # 2. Volumen de ventas por persona (top-center)
        ax2 = fig.add_subplot(gs[0, 1])
        if 'Total Sales Volume' in persona_kpis.columns:
            bars = ax2.bar(persona_kpis.index, persona_kpis['Total Sales Volume'], 
                          color=colors, alpha=0.8)
            ax2.set_title('Volumen de Ventas', fontweight='bold')
            ax2.tick_params(axis='x', rotation=45)
            
            # Agregar valores
            for bar in bars:
                height = bar.get_height()
                ax2.annotate(f'{height:,.0f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3), textcoords="offset points",
                           ha='center', va='bottom', fontsize=9)
        
        # 3. FOC Ratio por persona (top-right)
        ax3 = fig.add_subplot(gs[0, 2])
        if 'Avg FOC Ratio' in persona_kpis.columns:
            bars = ax3.bar(persona_kpis.index, persona_kpis['Avg FOC Ratio'], 
                          color=colors, alpha=0.8)
            ax3.set_title('FOC Ratio Promedio', fontweight='bold')
            ax3.tick_params(axis='x', rotation=45)
            
            # Agregar valores
            for bar in bars:
                height = bar.get_height()
                ax3.annotate(f'{height:.3f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3), textcoords="offset points",
                           ha='center', va='bottom', fontsize=9)
        
        # 4. Tabla de resumen (bottom)
        ax4 = fig.add_subplot(gs[1:, :])
        ax4.axis('tight')
        ax4.axis('off')
        
        # Crear tabla de resumen
        summary_data = []
        for persona in persona_kpis.index:
            row = [persona]
            if 'Customer Count' in persona_kpis.columns:
                row.append(f"{persona_kpis.loc[persona, 'Customer Count']:,}")
            if '% of Customers' in persona_kpis.columns:
                row.append(f"{persona_kpis.loc[persona, '% of Customers']:.1f}%")
            if 'Total Sales Volume' in persona_kpis.columns:
                row.append(f"{persona_kpis.loc[persona, 'Total Sales Volume']:,.0f}")
            if '% of Total Sales' in persona_kpis.columns:
                row.append(f"{persona_kpis.loc[persona, '% of Total Sales']:.1f}%")
            if 'Avg FOC Ratio' in persona_kpis.columns:
                row.append(f"{persona_kpis.loc[persona, 'Avg FOC Ratio']:.3f}")
            
            summary_data.append(row)
        
        # Columnas de la tabla
        columns = ['Persona', 'Clientes', '% Clientes', 'Volumen Ventas', '% Ventas', 'FOC Ratio']
        
        table = ax4.table(cellText=summary_data,
                         colLabels=columns,
                         cellLoc='center',
                         loc='center')
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 2)
        
        # Colorear filas por persona
        for i, persona in enumerate(persona_kpis.index):
            color = colors[i] if i < len(colors) else self.NEUTRAL_GRAY
            for j in range(len(columns)):
                table[(i+1, j)].set_facecolor(color)
                table[(i+1, j)].set_alpha(0.3)
        
        plt.suptitle('Dashboard Ejecutivo - Segmentación de Clientes Werfen', 
                    fontsize=20, fontweight='bold', color=self.WERFEN_BLUE, y=0.95)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Dashboard guardado en: {save_path}")
        
        plt.show()
    
    def plot_elbow_method_interactive(self, 
                                    evaluation_results: Dict[str, Any],
                                    optimal_k: int,
                                    save_path: Optional[str] = None) -> None:
        """
        Crear gráfico interactivo del Elbow Method con colores corporativos Werfen.
        
        Args:
            evaluation_results: Resultados de evaluación de K.
            optimal_k: Valor K óptimo seleccionado.
            save_path: Ruta para guardar gráfico.
        """
        logger.info("Creando gráfico del Elbow Method...")
        
        k_values = evaluation_results['k_values']
        wcss = evaluation_results.get('wcss', [])
        
        if not wcss:
            logger.warning("No hay datos WCSS disponibles para el gráfico del Elbow Method")
            return
        
        # Crear figura con colores corporativos
        plt.figure(figsize=(10, 6))
        
        # Plot principal con línea azul Werfen
        sns.lineplot(x=k_values, y=wcss, marker='o', color=self.WERFEN_BLUE, 
                    linewidth=2, markersize=8, label='WCSS')
        
        # Resaltar K óptimo con línea naranja
        plt.axvline(x=optimal_k, color=self.ACCENT_ORANGE, linestyle='--', 
                   linewidth=2, label=f'Optimal k = {optimal_k}')
        
        # Formateo profesional
        plt.title('Elbow Method for Optimal K', fontsize=16, fontweight='bold', 
                 color=self.WERFEN_BLUE, pad=20)
        plt.xlabel('Number of Clusters (k)', fontsize=12, fontweight='bold')
        plt.ylabel('WCSS (Inertia)', fontsize=12, fontweight='bold')
        plt.xticks(k_values)
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        
        # Estilo
        sns.despine()
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Gráfico del Elbow Method guardado en: {save_path}")
        
        plt.show()
    
    def plot_branded_cluster_profiles(self, 
                                    profile_df: pd.DataFrame,
                                    features_to_plot: List[str],
                                    title: str = 'Direct Comparison of Cluster Centroids',
                                    reverse_rank_features: List[str] = None,
                                    save_path: Optional[str] = None) -> None:
        """
        Generar gráficos de barras profesionales con colores corporativos Werfen
        para comparar perfiles de clusters.
        
        Args:
            profile_df: DataFrame con perfiles de clusters/personas.
            features_to_plot: Lista de features a graficar.
            title: Título del gráfico.
            reverse_rank_features: Features donde menor valor = mejor rank.
            save_path: Ruta para guardar gráfico.
        """
        logger.info(f"Creando gráficos de perfiles con {len(features_to_plot)} features...")
        
        if reverse_rank_features is None:
            reverse_rank_features = ['foc_ratio', 'weighted_avg_foc_ratio']
        
        # Crear subplots
        num_features = len(features_to_plot)
        fig, axes = plt.subplots(1, num_features, figsize=(6 * num_features, 6))
        if num_features == 1:
            axes = [axes]
        
        # Configurar estilo
        sns.set_style("whitegrid")
        
        for i, feature in enumerate(features_to_plot):
            ax = axes[i]
            
            # Verificar que la feature existe en el DataFrame
            if feature not in profile_df.columns:
                logger.warning(f"Feature '{feature}' no encontrada en profile_df")
                continue
            
            # Ranking estratégico de colores
            ascending_rank = (feature in reverse_rank_features)
            ranks = profile_df[feature].rank(method='first', ascending=ascending_rank)
            
            # Asignar colores basados en ranking
            palette = []
            for rank in ranks:
                if rank == 1.0:
                    palette.append(self.WERFEN_BLUE)  # Mejor performance
                elif rank == 2.0:
                    palette.append(self.ACCENT_ORANGE)  # Segunda mejor
                else:
                    palette.append(self.NEUTRAL_GRAY)  # Resto
            
            # Crear gráfico de barras
            bars = ax.bar(range(len(profile_df)), profile_df[feature], 
                         color=palette, alpha=0.8)
            
            # Etiquetas del eje X
            ax.set_xticks(range(len(profile_df)))
            ax.set_xticklabels(profile_df.index, rotation=45)
            
            # Formateo profesional
            ax.set_title(f'Average {feature.replace("_", " ").title()}', 
                        fontsize=14, color=self.WERFEN_BLUE, fontweight='bold', pad=20)
            ax.set_xlabel(profile_df.index.name.title() if profile_df.index.name else 'Group', 
                         fontsize=12, fontweight='bold')
            ax.set_ylabel('')
            
            # Agregar valores en las barras
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f"{height:,.1f}",
                           xy=(bar.get_x() + bar.get_width() / 2., height),
                           xytext=(0, 10),
                           textcoords='offset points',
                           ha='center', va='center',
                           fontsize=12, fontweight='bold')
            
            # Grid sutil
            ax.grid(True, alpha=0.3, axis='y')
        
        # Título principal
        plt.suptitle(title, fontsize=20, fontweight='bold', 
                    color=self.WERFEN_BLUE, y=1.03)
        
        # Estilo final
        sns.despine()
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Gráfico de perfiles guardado en: {save_path}")
        
        plt.show()
    
    def plot_persona_comparison_dashboard(self,
                                        persona_kpis: pd.DataFrame,
                                        centroids_df: pd.DataFrame,
                                        features_to_plot: List[str],
                                        save_path: Optional[str] = None) -> None:
        """
        Crear dashboard completo de comparación de personas.
        
        Args:
            persona_kpis: DataFrame con KPIs por persona.
            centroids_df: DataFrame con centroides de clusters.
            features_to_plot: Features a incluir en el dashboard.
            save_path: Ruta para guardar gráfico.
        """
        logger.info("Creando dashboard de comparación de personas...")
        
        # Configurar figura
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(2, 3, hspace=0.3, wspace=0.3)
        
        # Colores por persona
        persona_colors = {
            'Champions': self.SUCCESS_GREEN,
            'Loyalists': self.WERFEN_BLUE,
            'Potentials': self.ACCENT_ORANGE
        }
        
        # 1. Distribución de clientes (pie chart)
        ax1 = fig.add_subplot(gs[0, 0])
        if 'Customer Count' in persona_kpis.columns:
            colors = [persona_colors.get(persona, self.NEUTRAL_GRAY) 
                     for persona in persona_kpis.index]
            
            wedges, texts, autotexts = ax1.pie(
                persona_kpis['Customer Count'], 
                labels=persona_kpis.index,
                colors=colors,
                autopct='%1.1f%%',
                startangle=90
            )
            ax1.set_title('Customer Distribution', fontweight='bold', 
                         color=self.WERFEN_BLUE, fontsize=14)
        
        # 2. Volumen de ventas
        ax2 = fig.add_subplot(gs[0, 1])
        if 'Total Sales Volume' in persona_kpis.columns:
            colors = [persona_colors.get(persona, self.NEUTRAL_GRAY) 
                     for persona in persona_kpis.index]
            
            bars = ax2.bar(persona_kpis.index, persona_kpis['Total Sales Volume'], 
                          color=colors, alpha=0.8)
            ax2.set_title('Total Sales Volume', fontweight='bold', 
                         color=self.WERFEN_BLUE, fontsize=14)
            ax2.tick_params(axis='x', rotation=45)
            
            # Agregar valores
            for bar in bars:
                height = bar.get_height()
                ax2.annotate(f'{height:,.0f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3), textcoords="offset points",
                           ha='center', va='bottom', fontsize=10)
        
        # 3. FOC Ratio promedio
        ax3 = fig.add_subplot(gs[0, 2])
        if 'Avg FOC Ratio' in persona_kpis.columns:
            colors = [persona_colors.get(persona, self.NEUTRAL_GRAY) 
                     for persona in persona_kpis.index]
            
            bars = ax3.bar(persona_kpis.index, persona_kpis['Avg FOC Ratio'], 
                          color=colors, alpha=0.8)
            ax3.set_title('Average FOC Ratio', fontweight='bold', 
                         color=self.WERFEN_BLUE, fontsize=14)
            ax3.tick_params(axis='x', rotation=45)
            
            # Agregar valores
            for bar in bars:
                height = bar.get_height()
                ax3.annotate(f'{height:.3f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3), textcoords="offset points",
                           ha='center', va='bottom', fontsize=10)
        
        # 4. Tabla de resumen (parte inferior)
        ax4 = fig.add_subplot(gs[1, :])
        ax4.axis('tight')
        ax4.axis('off')
        
        # Crear tabla de resumen
        table_data = []
        for persona in persona_kpis.index:
            row = [persona]
            for col in persona_kpis.columns:
                value = persona_kpis.loc[persona, col]
                if 'Count' in col:
                    row.append(f"{value:,}")
                elif 'Volume' in col:
                    row.append(f"{value:,.0f}")
                elif '%' in col:
                    row.append(f"{value:.1f}%")
                elif 'Ratio' in col:
                    row.append(f"{value:.3f}")
                else:
                    row.append(f"{value:.2f}")
            table_data.append(row)
        
        # Crear tabla
        columns = ['Persona'] + list(persona_kpis.columns)
        table = ax4.table(cellText=table_data,
                         colLabels=columns,
                         cellLoc='center',
                         loc='center')
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 2)
        
        # Colorear filas por persona
        for i, persona in enumerate(persona_kpis.index):
            color = persona_colors.get(persona, self.NEUTRAL_GRAY)
            for j in range(len(columns)):
                table[(i+1, j)].set_facecolor(color)
                table[(i+1, j)].set_alpha(0.3)
        
        # Título principal
        plt.suptitle('Customer Persona Analysis Dashboard', 
                    fontsize=20, fontweight='bold', color=self.WERFEN_BLUE, y=0.95)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Dashboard guardado en: {save_path}")
        
        plt.show() 