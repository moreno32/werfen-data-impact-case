"""
Feature Engineering Module
===========================

Module for feature selection and data scaling.
Includes feature selection strategies and transformations for ML.

Author: Werfen Data Science Team
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.feature_selection import VarianceThreshold
from typing import List, Dict, Tuple, Optional, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    Feature engineer for Werfen ML pipeline.
    
    Handles feature selection, scaling, and transformations.
    """
    
    def __init__(self, scaling_method: str = 'standard'):
        """
        Initialize feature engineer.
        
        Args:
            scaling_method: Scaling method ('standard', 'robust').
        """
        self.scaling_method = scaling_method
        self.scaler = None
        self.selected_features = None
        self.feature_importance = {}
        self.metadata = {}
        
        # Initialize scaler
        if scaling_method == 'standard':
            self.scaler = StandardScaler()
        elif scaling_method == 'robust':
            self.scaler = RobustScaler()
        else:
            raise ValueError(f"Scaling method not supported: {scaling_method}")
    
    def engineer_aggregate_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Create aggregate features from pivoted metrics.
        
        Args:
            data: DataFrame with pivoted metrics by material.
            
        Returns:
            DataFrame with aggregate features.
        """
        logger.info("Creating aggregate features...")
        
        # Identify columns by type
        quantity_cols = [col for col in data.columns if 'total_sold_quantity' in col]
        transaction_cols = [col for col in data.columns if 'total_sold_transactions' in col]
        foc_ratio_cols = [col for col in data.columns if 'foc_ratio' in col]
        
        # Create aggregate features DataFrame
        features_df = data[['customer_id', 'transaction_date']].copy()
        
        # 1. Total volume features
        features_df['total_sold_quantity'] = data[quantity_cols].sum(axis=1)
        features_df['total_sold_transactions'] = data[transaction_cols].sum(axis=1)
        
        # 2. Product diversity features
        features_df['active_materials_count'] = (data[quantity_cols] > 0).sum(axis=1)
        features_df['material_concentration'] = self._calculate_concentration_index(data[quantity_cols])
        
        # 3. FOC behavior features
        # Average FOC ratio (weighted by quantity)
        weighted_foc = self._calculate_weighted_foc_ratio(data, quantity_cols, foc_ratio_cols)
        features_df['weighted_avg_foc_ratio'] = weighted_foc
        
        # Main FOC ratio (for clustering) - alias for weighted_avg_foc_ratio
        features_df['foc_ratio'] = weighted_foc
        
        # Maximum FOC ratio (worst case)
        features_df['max_foc_ratio'] = data[foc_ratio_cols].max(axis=1)
        
        # 4. Efficiency features
        features_df['avg_transaction_size'] = np.where(
            features_df['total_sold_transactions'] > 0,
            features_df['total_sold_quantity'] / features_df['total_sold_transactions'],
            0
        )
        
        # 5. Top materials features (top 3 by volume)
        top_materials = self._identify_top_materials(data, quantity_cols, top_n=3)
        for i, material_col in enumerate(top_materials, 1):
            features_df[f'top_{i}_material_quantity'] = data[material_col]
            # Corresponding transactions column
            material_id = material_col.split('_')[1]
            trans_col = f'mat_{material_id}_total_sold_transactions'
            if trans_col in data.columns:
                features_df[f'top_{i}_material_transactions'] = data[trans_col]
        
        logger.info(f"Aggregate features created: {features_df.shape[1] - 2} features")
        return features_df
    
    def select_features_for_clustering(self, 
                                     features_df: pd.DataFrame,
                                     strategy: str = 'business_driven') -> Tuple[List[str], pd.DataFrame]:
        """
        Select features for clustering based on strategy.
        
        Args:
            features_df: DataFrame with engineered features.
            strategy: Selection strategy ('business_driven', 'statistical', 'hybrid').
            
        Returns:
            Tuple with list of selected features and filtered DataFrame.
        """
        logger.info(f"Selecting features with strategy: {strategy}")
        
        if strategy == 'business_driven':
            selected_features = self._business_driven_selection(features_df)
        elif strategy == 'statistical':
            selected_features = self._statistical_selection(features_df)
        elif strategy == 'hybrid':
            selected_features = self._hybrid_selection(features_df)
        else:
            raise ValueError(f"Strategy not supported: {strategy}")
        
        self.selected_features = selected_features
        features_for_ml = features_df[['customer_id', 'transaction_date'] + selected_features]
        
        logger.info(f"Selected features ({len(selected_features)}): {selected_features}")
        return selected_features, features_for_ml
    
    def scale_features(self, features_df: pd.DataFrame, 
                      feature_columns: List[str]) -> Tuple[np.ndarray, pd.DataFrame]:
        """
        Scale features for clustering.
        
        Args:
            features_df: DataFrame with features.
            feature_columns: List of columns to scale.
            
        Returns:
            Tuple with scaled array and DataFrame with customer IDs.
        """
        logger.info(f"Scaling features using {self.scaling_method} scaler...")
        
        # Extract features for scaling
        X = features_df[feature_columns].values
        customer_info = features_df[['customer_id', 'transaction_date']].copy()
        
        # Verify valid data
        if np.isnan(X).any():
            logger.warning("Found NaN values, filling with 0")
            X = np.nan_to_num(X, nan=0.0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Save scaling metadata
        self.metadata['scaling'] = {
            'method': self.scaling_method,
            'feature_names': feature_columns,
            'original_shape': X.shape,
            'scaled_shape': X_scaled.shape
        }
        
        if hasattr(self.scaler, 'mean_'):
            self.metadata['scaling']['means'] = self.scaler.mean_.tolist()
            self.metadata['scaling']['stds'] = self.scaler.scale_.tolist()
        
        logger.info(f"Scaling completed: {X_scaled.shape}")
        return X_scaled, customer_info
    
    def _calculate_concentration_index(self, quantity_data: pd.DataFrame) -> pd.Series:
        """Calculate material concentration index (Herfindahl-Hirschman)."""
        # Normalize by row
        row_totals = quantity_data.sum(axis=1)
        normalized = quantity_data.div(row_totals, axis=0).fillna(0)
        
        # Calculate HHI
        hhi = (normalized ** 2).sum(axis=1)
        return hhi
    
    def _calculate_weighted_foc_ratio(self, data: pd.DataFrame, 
                                    quantity_cols: List[str], 
                                    foc_ratio_cols: List[str]) -> pd.Series:
        """Calculate quantity-weighted average FOC ratio."""
        total_quantity = data[quantity_cols].sum(axis=1)
        
        weighted_sum = 0
        for qty_col, foc_col in zip(quantity_cols, foc_ratio_cols):
            if foc_col in data.columns:
                weight = data[qty_col] / total_quantity.replace(0, np.nan)
                weighted_sum += (data[foc_col] * weight).fillna(0)
        
        return weighted_sum
    
    def _identify_top_materials(self, data: pd.DataFrame, 
                              quantity_cols: List[str], 
                              top_n: int = 3) -> List[str]:
        """Identify top N materials by total volume."""
        material_totals = data[quantity_cols].sum().sort_values(ascending=False)
        return material_totals.head(top_n).index.tolist()
    
    def _business_driven_selection(self, features_df: pd.DataFrame) -> List[str]:
        """Feature selection based on business knowledge."""
        # Specific features defined for Werfen clustering
        # These are the ONLY 3 features that will be used
        features_for_clustering = [
            'total_sold_quantity',      # Represents customer VALUE
            'foc_ratio',                # Represents LOYALTY TYPE (organic vs. promotion-driven)
            'total_sold_transactions'   # Represents customer FREQUENCY
        ]
        
        # Verify all features exist
        missing_features = [f for f in features_for_clustering if f not in features_df.columns]
        if missing_features:
            raise ValueError(f"Required features not found: {missing_features}")
        
        logger.info(f"Using the 3 specific features for clustering: {features_for_clustering}")
        return features_for_clustering
    
    def _statistical_selection(self, features_df: pd.DataFrame) -> List[str]:
        """Feature selection based on statistical criteria."""
        # Exclude ID columns
        numeric_cols = features_df.select_dtypes(include=[np.number]).columns
        feature_cols = [col for col in numeric_cols 
                       if col not in ['customer_id', 'transaction_date']]
        
        # Filter by variance
        selector = VarianceThreshold(threshold=0.01)  # Remove features with very low variance
        X = features_df[feature_cols].fillna(0)
        selector.fit(X)
        
        selected_features = [feature_cols[i] for i, selected 
                           in enumerate(selector.get_support()) if selected]
        
        return selected_features
    
    def _hybrid_selection(self, features_df: pd.DataFrame) -> List[str]:
        """Hybrid selection combining business and statistical criteria."""
        business_features = self._business_driven_selection(features_df)
        statistical_features = self._statistical_selection(features_df)
        
        # Combine prioritizing business features
        hybrid_features = business_features.copy()
        
        # Add statistical features not already included
        for feature in statistical_features:
            if feature not in hybrid_features:
                hybrid_features.append(feature)
        
        # Limit to maximum 10 features to avoid curse of dimensionality
        return hybrid_features[:10]
    
    def get_feature_summary(self) -> Dict[str, Any]:
        """Get summary of selected features."""
        if not self.selected_features:
            raise ValueError("Must select features first")
        
        return {
            'selected_features': self.selected_features,
            'feature_count': len(self.selected_features),
            'scaling_method': self.scaling_method,
            'metadata': self.metadata
        } 