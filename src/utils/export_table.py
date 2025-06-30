#!/usr/bin/env python3
"""
Export fct_transactions table from DuckDB to processed data folder
"""

import duckdb
import pandas as pd
from pathlib import Path
import sys
import os

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config import get_config

def export_fct_transactions():
    """Export fct_transactions table to processed data folder"""
    
    print("🔄 EXPORTING FCT_TRANSACTIONS TABLE")
    print("=" * 50)
    
    # Get configuration
    config = get_config()
    
    # Paths
    db_path = config.main_database_path
    processed_folder = config.processed_data_folder
    
    # Ensure processed folder exists
    processed_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"📍 Database: {db_path}")
    print(f"📁 Output folder: {processed_folder}")
    
    try:
        # Connect to DuckDB
        conn = duckdb.connect(str(db_path))
        
        # Check if table exists
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_name = 'fct_transactions'"
        tables_result = conn.execute(tables_query).fetchall()
        
        if not tables_result:
            print("❌ Table fct_transactions not found")
            print("💡 Run dbt transformations first: cd dbt_project && dbt run")
            return False
        
        # Export to DataFrame
        print("\n📊 Extracting data from fct_transactions...")
        df = conn.execute("SELECT * FROM main_marts.fct_transactions").df()
        
        print(f"✅ Extracted {len(df):,} records")
        print(f"📋 Columns: {len(df.columns)} ({', '.join(df.columns[:5])}...)")
        
        # Export to CSV
        csv_path = processed_folder / "fct_transactions.csv"
        df.to_csv(csv_path, index=False)
        print(f"💾 CSV exported: {csv_path}")
        
        # Export to Parquet (more efficient for large datasets)
        parquet_path = processed_folder / "fct_transactions.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"💾 Parquet exported: {parquet_path}")
        
        # Export sample for quick analysis
        sample_path = processed_folder / "fct_transactions_sample_1000.csv"
        df.sample(min(1000, len(df))).to_csv(sample_path, index=False)
        print(f"💾 Sample exported: {sample_path}")
        
        # Basic statistics
        print(f"\n📈 BASIC STATISTICS:")
        print(f"   • Total records: {len(df):,}")
        print(f"   • Date range: {df['transaction_date'].min()} to {df['transaction_date'].max()}")
        print(f"   • Unique customers: {df['customer_id'].nunique():,}")
        print(f"   • Unique materials: {df['material_id'].nunique():,}")
        print(f"   • File sizes:")
        print(f"     - CSV: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")
        print(f"     - Parquet: {parquet_path.stat().st_size / 1024 / 1024:.1f} MB")
        
        conn.close()
        
        print("\n✅ EXPORT COMPLETED SUCCESSFULLY")
        return True
        
    except Exception as e:
        print(f"❌ Error during export: {e}")
        return False

if __name__ == "__main__":
    export_fct_transactions() 