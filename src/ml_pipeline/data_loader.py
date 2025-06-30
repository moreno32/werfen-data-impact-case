"""
Data Loader Module
==================

Module to load data from DuckDB

Author: Werfen Data Science Team
"""

import pandas as pd
import duckdb
from typing import Optional, Dict, Any
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Data loader for Werfen ML pipeline.
    
    Loads data from main_marts.marts_customer_summary and performs basic validations.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize data loader.
        
        Args:
            db_path: Path to DuckDB database. If None, uses default configuration.
        """
        if db_path:
            self.db_path = db_path
        else:
            # Build path to project root directory and then to DB
            project_root = Path(__file__).resolve().parent.parent.parent
            self.db_path = project_root / "artifacts" / "werfen.db"
            
        self.data = None
        self.metadata = {}
        
    def load_customer_summary(self, 
                            snapshot_date: Optional[str] = None,
                            min_transactions: int = 1) -> pd.DataFrame:
        """
        Load data from main_marts.marts_customer_summary.
        
        Args:
            snapshot_date: Specific date for analysis. If None, uses most recent.
            min_transactions: Minimum transactions to include customer.
            
        Returns:
            DataFrame with customer summary data.
        """
        logger.info("Loading data from main_marts.marts_customer_summary...")
        
        try:
            # Connect to DuckDB
            conn = duckdb.connect(self.db_path)
            
            # Base query
            base_query = """
            SELECT *
            FROM main_marts.marts_customer_summary
            WHERE 1=1
            """
            
            # Optional filters
            if snapshot_date:
                base_query += f" AND transaction_date = '{snapshot_date}'"
            
            # Execute query
            self.data = conn.execute(base_query).df()
            conn.close()
            
            # Basic validations
            self._validate_data()
            
            # Filter by minimum transactions
            self._filter_by_activity(min_transactions)
            
            # Save metadata
            self._extract_metadata()
            
            logger.info(f"Data loaded successfully: {self.data.shape[0]} records, {self.data.shape[1]} columns")
            return self.data
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _validate_data(self) -> None:
        """Basic data quality validations."""
        if self.data is None or self.data.empty:
            raise ValueError("No data found in main_marts.marts_customer_summary")
        
        # Check required columns
        required_cols = ['customer_id', 'transaction_date', 'surrogate_id']
        missing_cols = [col for col in required_cols if col not in self.data.columns]
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")
        
        # Check null values in critical columns
        null_counts = self.data[required_cols].isnull().sum()
        if null_counts.any():
            logger.warning(f"Null values found: {null_counts[null_counts > 0].to_dict()}")
        
        logger.info("Data validations completed")
    
    def _filter_by_activity(self, min_transactions: int) -> None:
        """Filter customers by minimum activity level."""
        if min_transactions <= 1:
            return
        
        # Identify transaction columns
        transaction_cols = [col for col in self.data.columns if 'total_sold_transactions' in col]
        
        if transaction_cols:
            # Calculate total transactions per customer
            self.data['total_transactions'] = self.data[transaction_cols].sum(axis=1)
            
            # Filter
            initial_count = len(self.data)
            self.data = self.data[self.data['total_transactions'] >= min_transactions]
            final_count = len(self.data)
            
            logger.info(f"Activity filtering: {initial_count} -> {final_count} records")
    
    def _extract_metadata(self) -> None:
        """Extract metadata from loaded data."""
        if self.data is None:
            return
        
        # Identify columns by metric type
        quantity_cols = [col for col in self.data.columns if 'total_sold_quantity' in col]
        transaction_cols = [col for col in self.data.columns if 'total_sold_transactions' in col]
        foc_ratio_cols = [col for col in self.data.columns if 'foc_ratio' in col]
        
        # Extract available material_ids
        material_ids = []
        for col in quantity_cols:
            try:
                mat_id = int(col.split('_')[1])
                material_ids.append(mat_id)
            except (IndexError, ValueError):
                continue
        
        self.metadata = {
            'total_records': len(self.data),
            'total_customers': self.data['customer_id'].nunique(),
            'date_range': {
                'min': self.data['transaction_date'].min(),
                'max': self.data['transaction_date'].max()
            },
            'available_materials': sorted(material_ids),
            'feature_columns': {
                'quantity': quantity_cols,
                'transactions': transaction_cols,
                'foc_ratio': foc_ratio_cols
            }
        }
        
        logger.info(f"Metadata extracted: {len(material_ids)} materials available")
    
    def get_available_features(self) -> Dict[str, Any]:
        """
        Get information about available features.
        
        Returns:
            Dictionary with available features information.
        """
        if not self.metadata:
            raise ValueError("Must load data first using load_customer_summary()")
        
        return {
            'materials': self.metadata['available_materials'],
            'feature_types': list(self.metadata['feature_columns'].keys()),
            'total_features': sum(len(cols) for cols in self.metadata['feature_columns'].values()),
            'base_columns': ['customer_id', 'transaction_date', 'surrogate_id']
        }
    
    def get_summary_stats(self) -> pd.DataFrame:
        """
        Get descriptive statistics of the data.
        
        Returns:
            DataFrame with descriptive statistics.
        """
        if self.data is None:
            raise ValueError("Must load data first using load_customer_summary()")
        
        # Select only numeric columns (excluding IDs)
        numeric_cols = self.data.select_dtypes(include=['number']).columns
        exclude_cols = ['customer_id', 'surrogate_id']
        feature_cols = [col for col in numeric_cols if col not in exclude_cols]
        
        return self.data[feature_cols].describe() 