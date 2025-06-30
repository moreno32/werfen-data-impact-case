"""
Clustering Module
=================

Module for K-means clustering with K optimization and cluster analysis.
Includes evaluation methods and optimal cluster number selection.

Author: Werfen Data Science Team
"""

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score
from typing import Dict, List, Tuple, Optional, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KMeansClusterer:
    """
    K-means clusterer for Werfen customer segmentation.
    
    Includes K optimization and cluster quality analysis.
    """
    
    def __init__(self, random_state: int = 42):
        """
        Initialize clusterer.
        
        Args:
            random_state: Seed for reproducibility.
        """
        self.random_state = random_state
        self.model = None
        self.optimal_k = None
        self.evaluation_metrics = {}
        self.cluster_centers_ = None
        self.labels_ = None
        
    def find_optimal_k(self, 
                      X: np.ndarray, 
                      k_range: range = range(2, 10),
                      methods: List[str] = ['elbow', 'silhouette']) -> Dict[str, Any]:
        """
        Find optimal number of clusters using multiple methods.
        
        Args:
            X: Scaled data for clustering.
            k_range: Range of K values to evaluate.
            methods: Evaluation methods ('elbow', 'silhouette', 'calinski').
            
        Returns:
            Dictionary with evaluation results.
        """
        logger.info(f"Finding optimal K in range {list(k_range)} using methods: {methods}")
        
        results = {
            'k_values': list(k_range),
            'wcss': [],
            'silhouette_scores': [],
            'calinski_scores': [],
            'recommendations': {}
        }
        
        for k in k_range:
            # Train model
            kmeans = KMeans(n_clusters=k, 
                          init='k-means++', 
                          random_state=self.random_state, 
                          n_init=10)
            labels = kmeans.fit_predict(X)
            
            # Calculate metrics
            if 'elbow' in methods:
                results['wcss'].append(kmeans.inertia_)
            
            if 'silhouette' in methods and k > 1:
                sil_score = silhouette_score(X, labels)
                results['silhouette_scores'].append(sil_score)
            
            if 'calinski' in methods and k > 1:
                cal_score = calinski_harabasz_score(X, labels)
                results['calinski_scores'].append(cal_score)
        
        # Generate recommendations
        results['recommendations'] = self._generate_k_recommendations(results, methods)
        
        # Select optimal K (prioritizing silhouette)
        if 'silhouette' in methods and results['silhouette_scores']:
            best_idx = np.argmax(results['silhouette_scores'])
            self.optimal_k = list(k_range)[best_idx]
        elif 'elbow' in methods:
            self.optimal_k = self._find_elbow_point(results['wcss'], list(k_range))
        else:
            self.optimal_k = 3  # Default
        
        logger.info(f"Optimal K selected: {self.optimal_k}")
        self.evaluation_metrics = results
        return results
    
    def fit_predict(self, X: np.ndarray, k: Optional[int] = None) -> np.ndarray:
        """
        Train final model and predict clusters.
        
        Args:
            X: Scaled data for clustering.
            k: Number of clusters. If None, uses optimal_k.
            
        Returns:
            Array with cluster labels.
        """
        if k is None:
            if self.optimal_k is None:
                raise ValueError("Must find optimal K first or specify k")
            k = self.optimal_k
        
        logger.info(f"Training final model with K={k}")
        
        # Train final model
        self.model = KMeans(n_clusters=k, 
                           init='k-means++', 
                           random_state=self.random_state, 
                           n_init=10)
        
        self.labels_ = self.model.fit_predict(X)
        self.cluster_centers_ = self.model.cluster_centers_
        
        # Calculate final metrics
        self._calculate_final_metrics(X)
        
        logger.info(f"Model trained. Cluster distribution: {np.bincount(self.labels_)}")
        return self.labels_
    
    def analyze_clusters(self, 
                        X_scaled: np.ndarray,
                        feature_names: List[str],
                        scaler) -> pd.DataFrame:
        """
        Analyze characteristics of formed clusters.
        
        Args:
            X_scaled: Scaled data used for clustering.
            feature_names: Feature names.
            scaler: Scaler used to transform data.
            
        Returns:
            DataFrame with cluster profiles.
        """
        if self.model is None:
            raise ValueError("Must train model first")
        
        logger.info("Analyzing cluster characteristics...")
        
        # Get centroids in original scale
        centroids_original = scaler.inverse_transform(self.cluster_centers_)
        
        # Create profile DataFrame
        profile_df = pd.DataFrame(centroids_original, 
                                columns=feature_names)
        profile_df.index.name = 'cluster'
        
        # Add cluster size information
        cluster_sizes = pd.Series(self.labels_).value_counts().sort_index()
        profile_df['customer_count'] = cluster_sizes
        profile_df['pct_customers'] = (cluster_sizes / len(self.labels_) * 100).round(1)
        
        # Calculate separation metrics
        profile_df['within_cluster_distance'] = self._calculate_within_cluster_distances(X_scaled)
        
        return profile_df
    
    def _generate_k_recommendations(self, results: Dict, methods: List[str]) -> Dict[str, Any]:
        """Generate K recommendations based on evaluation methods."""
        recommendations = {}
        
        if 'elbow' in methods and results['wcss']:
            elbow_k = self._find_elbow_point(results['wcss'], results['k_values'])
            recommendations['elbow'] = elbow_k
        
        if 'silhouette' in methods and results['silhouette_scores']:
            best_idx = np.argmax(results['silhouette_scores'])
            recommendations['silhouette'] = results['k_values'][best_idx]
        
        if 'calinski' in methods and results['calinski_scores']:
            best_idx = np.argmax(results['calinski_scores'])
            recommendations['calinski'] = results['k_values'][best_idx]
        
        return recommendations
    
    def _find_elbow_point(self, wcss: List[float], k_values: List[int]) -> int:
        """Find elbow point in WCSS curve using second derivative method."""
        if len(wcss) < 3:
            return k_values[0]
        
        # Calculate second derivative
        wcss_array = np.array(wcss)
        second_derivative = np.diff(wcss_array, n=2)
        
        # Find point of maximum change
        elbow_idx = np.argmax(second_derivative) + 2  # +2 for double differences
        
        if elbow_idx >= len(k_values):
            elbow_idx = len(k_values) - 1
        
        return k_values[elbow_idx]
    
    def _calculate_final_metrics(self, X: np.ndarray) -> None:
        """Calculate final model metrics."""
        if self.labels_ is None:
            return
        
        metrics = {
            'inertia': self.model.inertia_,
            'n_clusters': self.model.n_clusters,
            'n_samples': len(X),
            'cluster_distribution': np.bincount(self.labels_).tolist()
        }
        
        # Silhouette score
        if len(np.unique(self.labels_)) > 1:
            metrics['silhouette_score'] = silhouette_score(X, self.labels_)
            metrics['calinski_harabasz_score'] = calinski_harabasz_score(X, self.labels_)
        
        self.evaluation_metrics['final_model'] = metrics
    
    def _calculate_within_cluster_distances(self, X_scaled: np.ndarray) -> List[float]:
        """Calculate average distance within each cluster."""
        distances = []
        
        for cluster_id in range(self.model.n_clusters):
            cluster_mask = self.labels_ == cluster_id
            cluster_points = X_scaled[cluster_mask]
            
            if len(cluster_points) > 1:
                # Average distance to centroid
                centroid = self.cluster_centers_[cluster_id]
                dist = np.mean(np.linalg.norm(cluster_points - centroid, axis=1))
                distances.append(round(dist, 3))
            else:
                distances.append(0.0)
        
        return distances
    
    def get_cluster_quality_report(self) -> Dict[str, Any]:
        """Generate clustering quality report."""
        if 'final_model' not in self.evaluation_metrics:
            raise ValueError("Must train final model first")
        
        final_metrics = self.evaluation_metrics['final_model']
        
        # Evaluate quality
        quality_assessment = "Good"
        if 'silhouette_score' in final_metrics:
            sil_score = final_metrics['silhouette_score']
            if sil_score < 0.3:
                quality_assessment = "Poor"
            elif sil_score < 0.5:
                quality_assessment = "Fair"
            elif sil_score < 0.7:
                quality_assessment = "Good"
            else:
                quality_assessment = "Excellent"
        
        return {
            'optimal_k': self.optimal_k,
            'quality_assessment': quality_assessment,
            'metrics': final_metrics,
            'recommendations': self.evaluation_metrics.get('recommendations', {})
        } 