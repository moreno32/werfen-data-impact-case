#!/usr/bin/env python3
"""
Script to clean all tables created by dbt.
Allows starting the process from scratch each time the notebook is executed.

Author: Daniel - Senior Data Analyst (Tech Lead) Candidate
"""

import sys
import duckdb
from pathlib import Path
from typing import List, Dict, Any
import subprocess
import os

# Add root directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config import WerfenConfig


class DbtTableCleaner:
    """
    dbt table cleaner to restart the process from scratch.
    """
    
    def __init__(self):
        """Initialize cleaner with unified configuration."""
        self.config = WerfenConfig()
        
    def clean_all_dbt_tables(self, verbose: bool = True) -> Dict[str, Any]:
        """
        Clean all tables created by dbt in all schemas.
        
        Args:
            verbose: Whether to show detailed output
            
        Returns:
            Dict with cleanup results
        """
        if verbose:
            print("🧹 COMPLETE DBT TABLE CLEANUP")
            print("=" * 60)
            print("🎯 Removing all tables to start from scratch")
            print()
        
        results = {
            'schemas_cleaned': [],
            'tables_dropped': [],
            'errors': [],
            'success': True
        }
        
        try:
            # Connect to database
            db_path = self.config.main_database_path
            conn = duckdb.connect(str(db_path))
            
            if verbose:
                print("🔌 Connected to DuckDB database")
                print(f"📍 Path: {db_path}")
                print()
            
            # Schemas to clean (all dbt schemas)
            schemas_to_clean = [
                'main_staging',
                'main_intermediate', 
                'main_marts'
            ]
            
            # Clean each schema
            for schema in schemas_to_clean:
                if verbose:
                    print(f"🗂️  Cleaning schema: {schema}")
                    print("-" * 40)
                
                # Get all tables in schema
                try:
                    tables_query = f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema}'
                    """
                    
                    tables_result = conn.execute(tables_query).fetchall()
                    
                    if not tables_result:
                        if verbose:
                            print(f"   ℹ️  No tables in schema {schema}")
                        continue
                    
                    # Drop each table/view
                    for (table_name,) in tables_result:
                        success = self._drop_object(conn, schema, table_name, verbose)
                        if success:
                            results['tables_dropped'].append(f"{schema}.{table_name}")
                        else:
                            error_msg = f"Could not drop {schema}.{table_name}"
                            results['errors'].append(error_msg)
                    
                    results['schemas_cleaned'].append(schema)
                    
                except Exception as e:
                    error_msg = f"Error accessing schema {schema}: {str(e)}"
                    results['errors'].append(error_msg)
                    if verbose:
                        print(f"   ❌ {error_msg}")
            
            # Also clean main schema (intermediate tables)
            if verbose:
                print(f"\n🗂️  Cleaning dbt tables in schema: main")
                print("-" * 40)
            
            # Specific dbt tables in main that we want to clean
            main_dbt_tables = [
                'stg_customers',
                'stg_sales_transactions', 
                'stg_foc_transactions',
                'int_customer_metrics',
                'int_sales_aggregated',
                'fct_transactions',
                'marts_customer_summary',
                'marts_persona_status_change'
            ]
            
            for table_name in main_dbt_tables:
                success = self._drop_object(conn, 'main', table_name, verbose)
                if success:
                    results['tables_dropped'].append(f"main.{table_name}")
                else:
                    error_msg = f"Could not drop main.{table_name}"
                    results['errors'].append(error_msg)
            
            conn.close()
            
            if verbose:
                print(f"\n📊 CLEANUP SUMMARY:")
                print(f"   • Schemas processed: {len(results['schemas_cleaned'])}")
                print(f"   • Tables dropped: {len(results['tables_dropped'])}")
                print(f"   • Errors: {len(results['errors'])}")
                
                if results['errors']:
                    results['success'] = False
                    print(f"\n⚠️  ERRORS FOUND:")
                    for error in results['errors']:
                        print(f"   • {error}")
                else:
                    print(f"\n✅ CLEANUP COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            results['success'] = False
            results['errors'].append(f"General error: {str(e)}")
            if verbose:
                print(f"❌ General cleanup error: {str(e)}")
        
        return results
    
    def _drop_object(self, conn, schema: str, object_name: str, verbose: bool = True) -> bool:
        """
        Try to drop an object (table or view) from database.
        
        Args:
            conn: DuckDB connection
            schema: Object schema
            object_name: Object name
            verbose: Whether to show detailed output
            
        Returns:
            True if dropped successfully, False otherwise
        """
        # First try as table
        try:
            drop_query = f"DROP TABLE IF EXISTS {schema}.{object_name}"
            conn.execute(drop_query)
            
            if verbose:
                print(f"   ✅ Dropped table: {schema}.{object_name}")
            return True
            
        except Exception:
            # If fails as table, try as view
            try:
                drop_query = f"DROP VIEW IF EXISTS {schema}.{object_name}"
                conn.execute(drop_query)
                
                if verbose:
                    print(f"   ✅ Dropped view: {schema}.{object_name}")
                return True
                
            except Exception as e:
                if verbose:
                    print(f"   ❌ Error dropping {schema}.{object_name}: {str(e)}")
                return False
    
    def clean_dbt_artifacts(self, verbose: bool = True) -> bool:
        """
        Clean dbt artifacts (target/, logs/, etc.).
        
        Args:
            verbose: Whether to show detailed output
            
        Returns:
            True if successful, False if errors
        """
        if verbose:
            print("\n🧹 DBT ARTIFACTS CLEANUP")
            print("=" * 40)
        
        try:
            # Change to dbt directory
            original_dir = os.getcwd()
            dbt_dir = self.config.dbt_project_folder
            os.chdir(str(dbt_dir))
            
            # Execute dbt clean
            if verbose:
                print("🔄 Running dbt clean...")
            
            clean_result = subprocess.run(
                ["dbt", "clean"], 
                capture_output=True, 
                text=True
            )
            
            # Also clean dependencies if they exist
            dbt_packages_dir = Path("dbt_packages")
            if dbt_packages_dir.exists():
                if verbose:
                    print("🔄 Cleaning dbt_packages dependencies...")
                try:
                    import shutil
                    shutil.rmtree(dbt_packages_dir)
                    if verbose:
                        print("✅ dbt_packages directory removed")
                except Exception as e:
                    if verbose:
                        print(f"⚠️  Error removing dbt_packages: {str(e)}")
            else:
                if verbose:
                    print("ℹ️  No dbt_packages directory to clean")
            
            if clean_result.returncode == 0:
                if verbose:
                    print("✅ dbt artifacts cleaned successfully")
                return True
            else:
                if verbose:
                    print(f"❌ Error in dbt clean: {clean_result.stderr}")
                return False
                
        except Exception as e:
            if verbose:
                print(f"❌ Error cleaning artifacts: {str(e)}")
            return False
        finally:
            os.chdir(original_dir)
    
    def full_clean_reset(self, verbose: bool = True) -> Dict[str, Any]:
        """
        Complete cleanup: tables + artifacts + preparation to start from scratch.
        
        Args:
            verbose: Whether to show detailed output
            
        Returns:
            Dict with complete results
        """
        if verbose:
            print("🔄 COMPLETE DBT ENVIRONMENT RESET")
            print("=" * 60)
            print("🎯 Preparing environment for execution from scratch")
            print()
        
        # 1. Clean tables
        table_results = self.clean_all_dbt_tables(verbose=verbose)
        
        # 2. Clean artifacts
        artifacts_success = self.clean_dbt_artifacts(verbose=verbose)
        
        # 3. Prepare summary
        full_results = {
            'table_cleanup': table_results,
            'artifacts_cleanup': artifacts_success,
            'overall_success': table_results['success'] and artifacts_success,
            'ready_for_fresh_start': True
        }
        
        if verbose:
            print(f"\n🎯 COMPLETE RESET:")
            if full_results['overall_success']:
                print("✅ ENVIRONMENT COMPLETELY CLEAN")
                print("🚀 Ready to execute notebook from scratch")
            else:
                print("⚠️  PARTIAL RESET - Review errors")
        
        return full_results


def main():
    """Main function for direct script usage."""
    print("🧹 DBT TABLE CLEANER")
    print("=" * 50)
    
    cleaner = DbtTableCleaner()
    results = cleaner.full_clean_reset(verbose=True)
    
    if results['overall_success']:
        print("\n🎉 Environment clean and ready!")
        return 0
    else:
        print("\n⚠️  Cleanup with errors - review output")
        return 1


if __name__ == "__main__":
    exit(main()) 