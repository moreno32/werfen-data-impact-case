"""
ML Pipeline Orchestrator
========================

Main orchestrator for Machine Learning pipeline for Werfen customer segmentation.
Coordinates all pipeline components: data loading, feature engineering, 
clustering and persona assignment.

Author: Werfen Data Science Team
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import logging
from pathlib import Path
import json
import pickle
from datetime import datetime

from .data_loader import DataLoader
from .feature_engineering import FeatureEngineer
from .clustering import KMeansClusterer
from .persona_assignment import PersonaAssigner
from .visualization import ClusterVisualizer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WerfenMLPipeline:
    """
    Complete Machine Learning pipeline for Werfen customer segmentation.
    
    Orchestrates the entire process from data loading to persona assignment
    and generation of executive reports.
    """
    
    def __init__(self, 
                 db_path: Optional[str] = None,
                 output_dir: str = "ml_outputs",
                 random_state: int = 42):
        """
        Initialize ML pipeline.
        
        Args:
            db_path: Path to DuckDB database.
            output_dir: Directory to save outputs.
            random_state: Seed for reproducibility.
        """
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.random_state = random_state
        
        # Initialize components
        self.data_loader = DataLoader(db_path)
        self.feature_engineer = FeatureEngineer()
        self.clusterer = KMeansClusterer(random_state)
        self.persona_assigner = PersonaAssigner()
        self.visualizer = ClusterVisualizer()
        
        # Pipeline state
        self.pipeline_state = {
            'data_loaded': False,
            'features_engineered': False,
            'model_trained': False,
            'personas_assigned': False,
            'visualizations_created': False
        }
        
        # Results
        self.results = {}
        
    def run_complete_pipeline(self, 
                            snapshot_date: Optional[str] = None,
                            min_transactions: int = 1,
                            feature_strategy: str = 'business_driven',
                            k_range: range = range(2, 8),
                            create_visualizations: bool = True) -> Dict[str, Any]:
        """
        Execute complete ML pipeline.
        
        Args:
            snapshot_date: Specific date for analysis.
            min_transactions: Minimum transactions to include customer.
            feature_strategy: Feature selection strategy.
            k_range: Range of K values for optimization.
            create_visualizations: Whether to create visualizations.
            
        Returns:
            Dictionary with all pipeline results.
        """
        logger.info("🚀 Starting complete ML pipeline for Werfen segmentation")
        start_time = datetime.now()
        
        try:
            # 1. Load data
            logger.info("📊 Step 1: Loading data...")
            data = self.load_data(snapshot_date, min_transactions)
            
            # 2. Feature Engineering
            logger.info("🔧 Step 2: Feature engineering...")
            features_df, selected_features, X_scaled, customer_info = self.engineer_features(
                data, feature_strategy
            )
            
            # 3. Optimize K and train model
            logger.info("🎯 Step 3: Optimizing K and training model...")
            optimal_k, evaluation_results, cluster_labels = self.train_clustering_model(
                X_scaled, k_range
            )
            
            # 4. Analyze clusters and assign personas
            logger.info("👥 Step 4: Analyzing clusters and assigning personas...")
            cluster_profile, persona_map, persona_kpis = self.assign_personas(
                X_scaled, selected_features, customer_info, cluster_labels
            )
            
            # 5. Create visualizations
            if create_visualizations:
                logger.info("📈 Step 5: Creating visualizations...")
                self.create_visualizations(
                    evaluation_results, optimal_k, cluster_profile, 
                    persona_map, persona_kpis, cluster_labels
                )
            
            # 6. Generate executive report
            logger.info("📋 Step 6: Generating executive report...")
            executive_summary = self.generate_executive_summary()
            
            # 7. Save results
            self.save_results()
            
            execution_time = datetime.now() - start_time
            logger.info(f"✅ Pipeline completed successfully in {execution_time}")
            
            return {
                'execution_time': str(execution_time),
                'optimal_k': optimal_k,
                'persona_distribution': persona_kpis.to_dict() if hasattr(persona_kpis, 'to_dict') else persona_kpis,
                'executive_summary': executive_summary,
                'model_quality': self.clusterer.get_cluster_quality_report()
            }
            
        except Exception as e:
            logger.error(f"❌ Pipeline error: {str(e)}")
            raise
    
    def load_data(self, 
                  snapshot_date: Optional[str] = None,
                  min_transactions: int = 1) -> pd.DataFrame:
        """Load and validate data."""
        data = self.data_loader.load_customer_summary(snapshot_date, min_transactions)
        
        self.results['data_info'] = {
            'total_records': len(data),
            'total_customers': data['customer_id'].nunique(),
            'available_features': self.data_loader.get_available_features(),
            'summary_stats': self.data_loader.get_summary_stats()
        }
        
        self.pipeline_state['data_loaded'] = True
        logger.info(f"Data loaded: {len(data)} records, {data['customer_id'].nunique()} unique customers")
        
        return data
    
    def engineer_features(self, 
                         data: pd.DataFrame,
                         strategy: str = 'business_driven') -> Tuple[pd.DataFrame, List[str], np.ndarray, pd.DataFrame]:
        """Feature engineering."""
        # Create aggregate features
        features_df = self.feature_engineer.engineer_aggregate_features(data)
        
        # Select features for clustering
        selected_features, features_for_ml = self.feature_engineer.select_features_for_clustering(
            features_df, strategy
        )
        
        # Scale features
        X_scaled, customer_info = self.feature_engineer.scale_features(
            features_for_ml, selected_features
        )
        
        self.results['feature_engineering'] = {
            'total_features_created': len(features_df.columns) - 2,  # Exclude IDs
            'selected_features': selected_features,
            'feature_summary': self.feature_engineer.get_feature_summary()
        }
        
        self.pipeline_state['features_engineered'] = True
        logger.info(f"Selected features: {len(selected_features)}")
        
        return features_df, selected_features, X_scaled, customer_info
    
    def train_clustering_model(self, 
                              X_scaled: np.ndarray,
                              k_range: range = range(2, 8)) -> Tuple[int, Dict, np.ndarray]:
        """Train clustering model."""
        # Optimize K
        evaluation_results = self.clusterer.find_optimal_k(X_scaled, k_range)
        optimal_k = self.clusterer.optimal_k
        
        # Train final model
        cluster_labels = self.clusterer.fit_predict(X_scaled, optimal_k)
        
        self.results['clustering'] = {
            'optimal_k': optimal_k,
            'evaluation_results': evaluation_results,
            'model_quality': self.clusterer.get_cluster_quality_report()
        }
        
        self.pipeline_state['model_trained'] = True
        logger.info(f"Model trained with K={optimal_k}")
        
        return optimal_k, evaluation_results, cluster_labels
    
    def assign_personas(self, 
                       X_scaled: np.ndarray,
                       selected_features: List[str],
                       customer_info: pd.DataFrame,
                       cluster_labels: np.ndarray) -> Tuple[pd.DataFrame, Dict[int, str], pd.DataFrame]:
        """Assign personas to clusters."""
        # Analyze cluster profiles
        cluster_profile = self.clusterer.analyze_clusters(
            X_scaled, selected_features, self.feature_engineer.scaler
        )
        
        # Analyze clusters for persona assignment
        cluster_analysis = self.persona_assigner.analyze_cluster_profiles(cluster_profile)
        
        # Assign personas
        persona_map = self.persona_assigner.assign_personas(cluster_analysis)
        
        # Create data with clusters for KPIs
        data_with_clusters = customer_info.copy()
        data_with_clusters['cluster'] = cluster_labels
        
        # Calculate persona KPIs
        persona_kpis = self.persona_assigner.calculate_persona_kpis(
            data_with_clusters, persona_map
        )
        
        self.results['persona_assignment'] = {
            'cluster_persona_mapping': persona_map,
            'cluster_profiles': cluster_profile.to_dict(),
            'persona_kpis': persona_kpis.to_dict(),
            'persona_summary': self.persona_assigner.get_persona_summary()
        }
        
        self.pipeline_state['personas_assigned'] = True
        logger.info(f"Personas assigned: {list(persona_map.values())}")
        
        return cluster_profile, persona_map, persona_kpis
    
    def create_visualizations(self, 
                            evaluation_results: Dict,
                            optimal_k: int,
                            cluster_profile: pd.DataFrame,
                            persona_map: Dict[int, str],
                            persona_kpis: pd.DataFrame,
                            cluster_labels: np.ndarray) -> None:
        """Create all visualizations."""
        viz_dir = self.output_dir / "visualizations"
        viz_dir.mkdir(exist_ok=True)
        
        # 1. K optimization
        self.visualizer.plot_k_optimization(
            evaluation_results, optimal_k, 
            save_path=viz_dir / "k_optimization.png"
        )
        
        # 2. Cluster profiles
        self.visualizer.plot_cluster_profiles(
            cluster_profile, persona_map,
            save_path=viz_dir / "cluster_profiles.png"
        )
        
        # 3. Persona KPIs
        self.visualizer.plot_persona_kpis(
            persona_kpis,
            save_path=viz_dir / "persona_kpis.png"
        )
        
        # 4. Cluster distribution
        self.visualizer.plot_cluster_distribution(
            cluster_labels, persona_map,
            save_path=viz_dir / "cluster_distribution.png"
        )
        
        # 5. Executive dashboard
        self.visualizer.create_executive_summary_plot(
            persona_kpis, cluster_profile, persona_map,
            save_path=viz_dir / "executive_dashboard.png"
        )
        
        self.pipeline_state['visualizations_created'] = True
        logger.info(f"Visualizations saved to: {viz_dir}")
    
    def generate_executive_summary(self) -> Dict[str, Any]:
        """Generate executive summary."""
        if not all(self.pipeline_state.values()):
            logger.warning("Incomplete pipeline for executive summary generation")
        
        summary = {
            'pipeline_execution': {
                'timestamp': datetime.now().isoformat(),
                'status': 'completed' if all(self.pipeline_state.values()) else 'partial',
                'steps_completed': sum(self.pipeline_state.values()),
                'total_steps': len(self.pipeline_state)
            },
            'data_overview': self.results.get('data_info', {}),
            'model_performance': self.results.get('clustering', {}).get('model_quality', {}),
            'business_insights': self._generate_business_insights()
        }
        
        return summary
    
    def _generate_business_insights(self) -> Dict[str, Any]:
        """Generate business insights."""
        insights = {
            'key_findings': [],
            'recommendations': [],
            'next_steps': []
        }
        
        if 'persona_assignment' in self.results:
            persona_kpis = self.results['persona_assignment']['persona_kpis']
            
            # Insights based on persona distribution
            if 'Customer Count' in persona_kpis:
                total_customers = sum(persona_kpis['Customer Count'].values())
                
                for persona, count in persona_kpis['Customer Count'].items():
                    percentage = (count / total_customers) * 100
                    insights['key_findings'].append(
                        f"{persona}: {count:,} customers ({percentage:.1f}% of total)"
                    )
            
            # Recommendations per persona
            persona_map = self.results['persona_assignment']['cluster_persona_mapping']
            for persona in set(persona_map.values()):
                if persona == 'Champions':
                    insights['recommendations'].append(
                        "Champions: Maintain high satisfaction and study best practices for replication"
                    )
                elif persona == 'Loyalists':
                    insights['recommendations'].append(
                        "Loyalists: Protect customer base and explore cross-selling opportunities"
                    )
                elif persona == 'Potentials':
                    insights['recommendations'].append(
                        "Potentials: Develop programs to reduce FOC dependency"
                    )
        
        # Next steps
        insights['next_steps'] = [
            "Implement differentiated strategies per persona",
            "Monitor monthly evolution of segments", 
            "Develop persona-specific campaigns",
            "Integrate segmentation into CRM systems"
        ]
        
        return insights
    
    def save_results(self) -> None:
        """Save all pipeline results."""
        # Save results to JSON
        results_file = self.output_dir / "pipeline_results.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            # Convert non-serializable objects
            serializable_results = self._make_serializable(self.results)
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        
        # Save trained model
        model_file = self.output_dir / "trained_model.pkl"
        with open(model_file, 'wb') as f:
            pickle.dump({
                'clusterer': self.clusterer,
                'feature_engineer': self.feature_engineer,
                'persona_assigner': self.persona_assigner
            }, f)
        
        logger.info(f"Results saved to: {self.output_dir}")
    
    def _make_serializable(self, obj):
        """Convert non-serializable objects to JSON."""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, (pd.DataFrame, pd.Series)):
            return obj.to_dict()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif hasattr(obj, '__dict__'):
            return str(obj)
        else:
            return obj
    
    def load_saved_model(self, model_path: str) -> None:
        """Load previously trained model."""
        with open(model_path, 'rb') as f:
            saved_components = pickle.load(f)
        
        self.clusterer = saved_components['clusterer']
        self.feature_engineer = saved_components['feature_engineer']
        self.persona_assigner = saved_components['persona_assigner']
        
        logger.info(f"Model loaded from: {model_path}")
    
    def predict_persona(self, new_data: pd.DataFrame) -> pd.DataFrame:
        """Predict persona for new data."""
        if not self.pipeline_state['model_trained']:
            raise ValueError("Must train model first")
        
        # Feature engineering
        features_df = self.feature_engineer.engineer_aggregate_features(new_data)
        selected_features = self.feature_engineer.selected_features
        
        # Scale features
        X_scaled, customer_info = self.feature_engineer.scale_features(
            features_df, selected_features
        )
        
        # Predict clusters
        cluster_labels = self.clusterer.model.predict(X_scaled)
        
        # Map to personas
        persona_map = self.persona_assigner.cluster_persona_map
        personas = [persona_map.get(cluster, f'Cluster {cluster}') for cluster in cluster_labels]
        
        # Create result
        result = customer_info.copy()
        result['cluster'] = cluster_labels
        result['persona'] = personas
        
        return result
    
    def run_interactive_clustering(self, 
                                 k_range: range = range(2, 8),
                                 optimal_k: Optional[int] = None,
                                 feature_strategy: str = 'business_driven',
                                 persona_mapping: Dict[int, str] = None) -> Dict[str, Any]:
        """
        Run interactive clustering with complete visualizations.
        
        Args:
            k_range: Range of K values to evaluate.
            optimal_k: Specific K to use (if None, optimized automatically).
            feature_strategy: Feature selection strategy.
            persona_mapping: Manual cluster -> persona mapping.
            
        Returns:
            Dictionary with all clustering results.
        """
        logger.info("🤖 STARTING INTERACTIVE CLUSTERING ANALYSIS")
        logger.info("=" * 50)
        
        try:
            # 1. Load and prepare data
            logger.info("📊 Loading data from main_marts.marts_customer_summary...")
            data = self.data_loader.load_customer_summary()
            
            # 2. Feature Engineering
            logger.info("🔧 Preparing features for clustering...")
            features_df = self.feature_engineer.engineer_aggregate_features(data)
            selected_features, features_for_ml = self.feature_engineer.select_features_for_clustering(
                features_df, feature_strategy
            )
            X_scaled, customer_info = self.feature_engineer.scale_features(
                features_for_ml, selected_features
            )
            
            # 3. K evaluation if not specified
            if optimal_k is None:
                logger.info("🎯 Evaluating optimal number of clusters...")
                evaluation_results = self.clusterer.find_optimal_k(X_scaled, k_range)
                optimal_k = self.clusterer.optimal_k
            else:
                logger.info(f"🎯 Using specified K: {optimal_k}")
                # Run evaluation to generate metrics
                evaluation_results = self.clusterer.find_optimal_k(X_scaled, k_range)
            
            # 4. Show Elbow Method plot
            self.visualizer.plot_elbow_method_interactive(evaluation_results, optimal_k)
            
            # 5. Train final model
            logger.info(f"🔬 Training final model with K={optimal_k}...")
            cluster_labels = self.clusterer.fit_predict(X_scaled, optimal_k)
            
            # 6. Create DataFrame with clusters
            customer_data_with_clusters = customer_info.copy()
            customer_data_with_clusters['cluster'] = cluster_labels
            
            # Add original features for analysis
            features_original = self.feature_engineer.scaler.inverse_transform(X_scaled)
            for i, feature in enumerate(selected_features):
                customer_data_with_clusters[feature] = features_original[:, i]
            
            # 7. Show cluster distribution
            self._display_cluster_distribution(cluster_labels)
            
            # 8. Calculate and show centroids
            centroids_df = self._calculate_cluster_centroids(
                customer_data_with_clusters, selected_features, cluster_labels
            )
            
            # 9. Visualize cluster profiles
            self.visualizer.plot_branded_cluster_profiles(
                centroids_df, selected_features, 
                title='Direct Comparison of Cluster Centroids'
            )
            
            # 10. Assign personas if mapping provided
            if persona_mapping:
                customer_data_with_clusters['persona'] = customer_data_with_clusters['cluster'].map(persona_mapping)
                
                # Calculate KPIs per persona
                persona_kpis = self._calculate_persona_kpis(customer_data_with_clusters)
                
                # Create centroids DataFrame with personas
                persona_centroids_df = centroids_df.copy()
                persona_centroids_df['persona'] = persona_centroids_df.index.map(persona_mapping)
                persona_centroids_df = persona_centroids_df.set_index('persona')
                
                # Sort by strategic order
                persona_order = ['Champions', 'Loyalists', 'Potentials']
                available_personas = [p for p in persona_order if p in persona_centroids_df.index]
                persona_centroids_df = persona_centroids_df.reindex(available_personas)
                
                # Visualize persona profiles
                self.visualizer.plot_branded_cluster_profiles(
                    persona_centroids_df, selected_features,
                    title='Customer Persona Profiles'
                )
                
                # Show final summary
                logger.info("\n--- Final Segmentation Breakdown ---")
                print(persona_kpis.round(1))
                
                return {
                    'customer_data_with_clusters': customer_data_with_clusters,
                    'centroids_df': centroids_df,
                    'persona_centroids_df': persona_centroids_df,
                    'persona_kpis': persona_kpis,
                    'optimal_k': optimal_k,
                    'evaluation_results': evaluation_results,
                    'selected_features': selected_features
                }
            else:
                return {
                    'customer_data_with_clusters': customer_data_with_clusters,
                    'centroids_df': centroids_df,
                    'optimal_k': optimal_k,
                    'evaluation_results': evaluation_results,
                    'selected_features': selected_features
                }
                
        except Exception as e:
            logger.error(f"❌ Interactive clustering error: {str(e)}")
            raise
    
    def _display_cluster_distribution(self, cluster_labels: np.ndarray) -> None:
        """Display cluster distribution."""
        logger.info(f"✅ Clustering completed with {len(np.unique(cluster_labels))} clusters")
        logger.info(f"\n📊 Cluster distribution:")
        
        cluster_counts = pd.Series(cluster_labels).value_counts().sort_index()
        for cluster_id, count in cluster_counts.items():
            percentage = (count / len(cluster_labels)) * 100
            logger.info(f"  Cluster {cluster_id}: {count:,} customers ({percentage:.1f}%)")
    
    def _calculate_cluster_centroids(self, 
                                   customer_data: pd.DataFrame,
                                   features: List[str],
                                   cluster_labels: np.ndarray) -> pd.DataFrame:
        """Calculate cluster centroids."""
        logger.info("\n--- Cluster Profiles (Averages per Cluster) ---")
        
        # Calculate centroids
        centroids_data = []
        for cluster_id in sorted(np.unique(cluster_labels)):
            cluster_mask = cluster_labels == cluster_id
            cluster_data = customer_data[cluster_mask]
            
            centroid = {}
            for feature in features:
                centroid[feature] = cluster_data[feature].mean()
            
            centroids_data.append(centroid)
        
        centroids_df = pd.DataFrame(centroids_data)
        centroids_df.index.name = 'cluster'
        
        # Add size information
        cluster_sizes = pd.Series(cluster_labels).value_counts().sort_index()
        centroids_df['customer_count'] = cluster_sizes
        centroids_df['pct_customers'] = (cluster_sizes / cluster_sizes.sum() * 100)
        
        print(centroids_df.round(1))
        return centroids_df
    
    def _calculate_persona_kpis(self, customer_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate KPIs per persona."""
        logger.info("\n--- Persona Performance Summary ---")
        
        # Define aggregations
        aggregations = {
            'customer_id': 'count',
            'total_sold_quantity': 'sum',
        }
        
        # Add weighted_avg_foc_ratio if exists
        if 'weighted_avg_foc_ratio' in customer_data.columns:
            aggregations['weighted_avg_foc_ratio'] = 'mean'
        
        # Calculate KPIs
        persona_kpis = customer_data.groupby('persona').agg(aggregations)
        
        # Rename columns
        column_mapping = {
            'customer_id': 'Customer Count',
            'total_sold_quantity': 'Total Sales Volume',
            'weighted_avg_foc_ratio': 'Avg FOC Ratio'
        }
        
        persona_kpis.rename(columns=column_mapping, inplace=True)
        
        # Calculate percentages
        if 'Total Sales Volume' in persona_kpis.columns:
            total_sales = persona_kpis['Total Sales Volume'].sum()
            persona_kpis['% of Total Sales'] = (persona_kpis['Total Sales Volume'] / total_sales * 100)
        
        if 'Customer Count' in persona_kpis.columns:
            total_customers = persona_kpis['Customer Count'].sum()
            persona_kpis['% of Customers'] = (persona_kpis['Customer Count'] / total_customers * 100)
        
        # Sort by strategic order
        persona_order = ['Champions', 'Loyalists', 'Potentials']
        available_personas = [p for p in persona_order if p in persona_kpis.index]
        persona_kpis = persona_kpis.reindex(available_personas)
        
        print(persona_kpis.round(2))
        return persona_kpis 