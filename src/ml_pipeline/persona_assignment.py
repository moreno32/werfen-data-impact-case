"""
Persona Assignment Module
=========================

Módulo para asignar personas de negocio a clusters y analizar perfiles.
Incluye lógica de mapeo cluster-persona y análisis de KPIs por segmento.

Autor: Werfen Data Science Team
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PersonaAssigner:
    """
    Asignador de personas para clusters de clientes Werfen.
    
    Convierte clusters técnicos en segmentos de negocio con nombres y estrategias.
    """
    
    def __init__(self):
        """Inicializar el asignador de personas."""
        self.cluster_persona_map = {}
        self.persona_profiles = {}
        self.business_kpis = {}
        
    def analyze_cluster_profiles(self, 
                               cluster_profile: pd.DataFrame,
                               feature_priorities: Dict[str, str] = None) -> Dict[str, Dict]:
        """
        Analizar perfiles de clusters para determinar características de negocio.
        
        Args:
            cluster_profile: DataFrame con perfiles de clusters.
            feature_priorities: Diccionario con prioridades de features ('high', 'low').
            
        Returns:
            Diccionario con análisis de cada cluster.
        """
        logger.info("Analyzing cluster profiles for persona assignment...")
        
        if feature_priorities is None:
            feature_priorities = {
                'total_sold_quantity': 'high',
                'weighted_avg_foc_ratio': 'low',
                'total_sold_transactions': 'high',
                'active_materials_count': 'high',
                'avg_transaction_size': 'high'
            }
        
        cluster_analysis = {}
        
        for cluster_id in cluster_profile.index:
            profile = cluster_profile.loc[cluster_id]
            
            # Analysis of main characteristics
            analysis = {
                'cluster_id': cluster_id,
                'size': profile['customer_count'],
                'percentage': profile['pct_customers'],
                'characteristics': {},
                'business_indicators': {}
            }
            
            # Analyze each feature
            for feature, priority in feature_priorities.items():
                if feature in profile.index:
                    value = profile[feature]
                    rank = self._rank_cluster_feature(cluster_profile, feature, cluster_id)
                    
                    analysis['characteristics'][feature] = {
                        'value': round(value, 2),
                        'rank': rank,
                        'priority': priority,
                        'performance': self._assess_performance(rank, priority)
                    }
            
            # Business indicators
            analysis['business_indicators'] = self._calculate_business_indicators(profile)
            
            cluster_analysis[cluster_id] = analysis
        
        return cluster_analysis
    
    def assign_personas(self, 
                       cluster_analysis: Dict[str, Dict],
                       assignment_strategy: str = 'value_loyalty_frequency') -> Dict[int, str]:
        """
        Asignar personas de negocio a clusters basado en análisis.
        
        Args:
            cluster_analysis: Análisis de perfiles de clusters.
            assignment_strategy: Estrategia de asignación.
            
        Returns:
            Diccionario mapeo cluster_id -> persona_name.
        """
        logger.info(f"Assigning personas using strategy: {assignment_strategy}")
        
        if assignment_strategy == 'value_loyalty_frequency':
            self.cluster_persona_map = self._assign_value_loyalty_frequency(cluster_analysis)
        elif assignment_strategy == 'business_driven':
            self.cluster_persona_map = self._assign_business_driven(cluster_analysis)
        else:
            raise ValueError(f"Strategy not supported: {assignment_strategy}")
        
        # Create persona profiles
        self._create_persona_profiles(cluster_analysis)
        
        logger.info(f"Personas assigned: {self.cluster_persona_map}")
        return self.cluster_persona_map
    
    def _assign_value_loyalty_frequency(self, cluster_analysis: Dict[str, Dict]) -> Dict[int, str]:
        """Assign personas based on value, loyalty and frequency."""
        
        # Extract key metrics for each cluster
        cluster_metrics = {}
        for cluster_id, analysis in cluster_analysis.items():
            chars = analysis['characteristics']
            
            cluster_metrics[cluster_id] = {
                'value_rank': chars.get('total_sold_quantity', {}).get('rank', 3),
                'loyalty_rank': chars.get('weighted_avg_foc_ratio', {}).get('rank', 3),  # Inverted: less FOC = more loyal
                'frequency_rank': chars.get('total_sold_transactions', {}).get('rank', 3),
                'size': analysis['size']
            }
        
        # Assignment logic
        personas = {}
        
        # 1. Champions: High value, good loyalty (low FOC), high frequency
        champions_candidate = None
        best_champion_score = -1
        
        # 2. Loyalists: High volume, high frequency (the largest and most reliable segment)
        loyalists_candidate = None
        best_loyalist_score = -1
        
        # 3. Potentials: High FOC ratio (need development)
        potentials_candidate = None
        worst_loyalty_score = 999
        
        for cluster_id, metrics in cluster_metrics.items():
            # Score for Champions (value + loyalty + frequency)
            champion_score = (4 - metrics['value_rank']) + (metrics['loyalty_rank']) + (4 - metrics['frequency_rank'])
            
            # Score for Loyalists (size + value + frequency)
            loyalist_score = metrics['size'] + (4 - metrics['value_rank']) + (4 - metrics['frequency_rank'])
            
            # Score for Potentials (worst loyalty)
            loyalty_score = metrics['loyalty_rank']
            
            if champion_score > best_champion_score:
                best_champion_score = champion_score
                champions_candidate = cluster_id
            
            if loyalist_score > best_loyalist_score and cluster_id != champions_candidate:
                best_loyalist_score = loyalist_score
                loyalists_candidate = cluster_id
            
            if loyalty_score < worst_loyalty_score:
                worst_loyalty_score = loyalty_score
                potentials_candidate = cluster_id
        
        # Assign personas avoiding duplicates
        assigned_clusters = set()
        
        if champions_candidate is not None:
            personas[champions_candidate] = 'Champions'
            assigned_clusters.add(champions_candidate)
        
        if loyalists_candidate is not None and loyalists_candidate not in assigned_clusters:
            personas[loyalists_candidate] = 'Loyalists'
            assigned_clusters.add(loyalists_candidate)
        
        if potentials_candidate is not None and potentials_candidate not in assigned_clusters:
            personas[potentials_candidate] = 'Potentials'
            assigned_clusters.add(potentials_candidate)
        
        # Assign remaining clusters
        remaining_clusters = set(cluster_metrics.keys()) - assigned_clusters
        for cluster_id in remaining_clusters:
            if len(personas) == 0:
                personas[cluster_id] = 'Champions'
            elif 'Loyalists' not in personas.values():
                personas[cluster_id] = 'Loyalists'
            else:
                personas[cluster_id] = 'Potentials'
        
        return personas
    
    def _assign_business_driven(self, cluster_analysis: Dict[str, Dict]) -> Dict[int, str]:
        """Manual assignment based on business knowledge."""
        # Simplified implementation - can be customized according to specific rules
        return self._assign_value_loyalty_frequency(cluster_analysis)
    
    def _create_persona_profiles(self, cluster_analysis: Dict[str, Dict]) -> None:
        """Create detailed persona profiles."""
        self.persona_profiles = {}
        
        for cluster_id, persona_name in self.cluster_persona_map.items():
            if persona_name not in self.persona_profiles:
                self.persona_profiles[persona_name] = {
                    'cluster_id': cluster_id,
                    'name': persona_name,
                    'description': self._get_persona_description(persona_name),
                    'strategy': self._get_persona_strategy(persona_name),
                    'characteristics': cluster_analysis[cluster_id]['characteristics'],
                    'business_indicators': cluster_analysis[cluster_id]['business_indicators'],
                    'size': cluster_analysis[cluster_id]['size'],
                    'percentage': cluster_analysis[cluster_id]['percentage']
                }
    
    def calculate_persona_kpis(self, 
                              data_with_clusters: pd.DataFrame,
                              persona_map: Dict[int, str]) -> pd.DataFrame:
        """
        Calcular KPIs de negocio por persona.
        
        Args:
            data_with_clusters: DataFrame con datos y clusters asignados.
            persona_map: Mapeo cluster -> persona.
            
        Returns:
            DataFrame con KPIs por persona.
        """
        logger.info("Calculating business KPIs by persona...")
        
        # Assign personas to data
        data_with_personas = data_with_clusters.copy()
        data_with_personas['persona'] = data_with_personas['cluster'].map(persona_map)
        
        # Define desired aggregations with explicit names
        aggregations = {
            'customer_count': pd.NamedAgg(column='customer_id', aggfunc='count'),
            'total_quantity_sum': pd.NamedAgg(column='total_sold_quantity', aggfunc='sum'),
            'total_quantity_mean': pd.NamedAgg(column='total_sold_quantity', aggfunc='mean'),
            'avg_foc_ratio': pd.NamedAgg(column='weighted_avg_foc_ratio', aggfunc='mean'),
            'avg_transactions': pd.NamedAgg(column='total_sold_transactions', aggfunc='mean'),
            'avg_active_materials': pd.NamedAgg(column='active_materials_count', aggfunc='mean'),
        }
        
        # Filter aggregations to only include columns that actually exist in the dataframe
        valid_aggregations = {
            key: agg for key, agg in aggregations.items() 
            if agg.column in data_with_personas.columns
        }

        if not valid_aggregations:
            logger.warning("Could not calculate KPIs, no metrics columns found.")
            return pd.DataFrame()
            
        # Perform aggregation
        persona_kpis = data_with_personas.groupby('persona').agg(**valid_aggregations)
        
        # Calculate customer percentage if customer count is available
        if 'customer_count' in persona_kpis.columns:
            persona_kpis['customer_percentage'] = (persona_kpis['customer_count'] / persona_kpis['customer_count'].sum()) * 100
        
        self.business_kpis = persona_kpis.to_dict('index')
        
        logger.info("Business KPIs by persona calculated successfully.")
        return persona_kpis
    
    def _rank_cluster_feature(self, cluster_profile: pd.DataFrame, feature: str, cluster_id: int) -> int:
        """Rankear cluster en una feature específica (1 = mejor)."""
        if feature not in cluster_profile.columns:
            return 3
        
        feature_values = cluster_profile[feature].sort_values(ascending=False)
        return list(feature_values.index).index(cluster_id) + 1
    
    def _assess_performance(self, rank: int, priority: str) -> str:
        """Evaluar performance basada en rank y prioridad."""
        if priority == 'high':
            if rank == 1:
                return 'Excellent'
            elif rank == 2:
                return 'Good'
            else:
                return 'Needs Improvement'
        else:  # priority == 'low'
            if rank == 1:
                return 'Needs Improvement'
            elif rank == 2:
                return 'Good'
            else:
                return 'Excellent'
    
    def _calculate_business_indicators(self, profile: pd.Series) -> Dict[str, Any]:
        """Calcular indicadores de negocio específicos."""
        indicators = {}
        
        # Eficiencia de compra
        if 'avg_transaction_size' in profile.index:
            size = profile['avg_transaction_size']
            if size > 100:
                indicators['purchase_efficiency'] = 'High'
            elif size > 50:
                indicators['purchase_efficiency'] = 'Medium'
            else:
                indicators['purchase_efficiency'] = 'Low'
        
        # Diversidad de productos
        if 'active_materials_count' in profile.index:
            count = profile['active_materials_count']
            if count > 5:
                indicators['product_diversity'] = 'High'
            elif count > 2:
                indicators['product_diversity'] = 'Medium'
            else:
                indicators['product_diversity'] = 'Low'
        
        # Dependencia FOC
        if 'weighted_avg_foc_ratio' in profile.index:
            foc_ratio = profile['weighted_avg_foc_ratio']
            if foc_ratio < 0.1:
                indicators['foc_dependency'] = 'Low'
            elif foc_ratio < 0.3:
                indicators['foc_dependency'] = 'Medium'
            else:
                indicators['foc_dependency'] = 'High'
        
        return indicators
    
    def _get_persona_description(self, persona_name: str) -> str:
        """Obtener descripción de persona."""
        descriptions = {
            'Champions': 'High value clients with excellent efficiency and low FOC dependency. They represent the benchmark for profitability.',
            'Loyalists': 'Reliable clients that form the core of the business. High volume and frequency of purchases with consistent loyalty.',
            'Potentials': 'Clients with growth potential that require development. High FOC dependency but with opportunities for improvement.'
        }
        return descriptions.get(persona_name, 'Customer segment identified by clustering.')
    
    def _get_persona_strategy(self, persona_name: str) -> str:
        """Obtener estrategia recomendada para persona."""
        strategies = {
            'Champions': 'Retain & Learn - Maintain satisfaction and study best practices to replicate in other segments.',
            'Loyalists': 'Protect & Optimize - Ensure business continuity and seek opportunities for cross-selling.',
            'Potentials': 'Develop & Improve Efficiency - Reduce FOC dependency and increase value per transaction.'
        }
        return strategies.get(persona_name, 'Customized strategy based on segment characteristics.')
    
    def get_persona_summary(self) -> Dict[str, Any]:
        """Obtener resumen completo de personas asignadas."""
        return {
            'cluster_persona_mapping': self.cluster_persona_map,
            'persona_profiles': self.persona_profiles,
            'business_kpis': self.business_kpis
        } 