#!/usr/bin/env python3
"""
Werfen Data Pipeline - Unified and Simplified Configuration
==========================================================

Consolidated configuration without dependencies on .env files or environment variables.
All values are hardcoded with sensible defaults.

Author: Lead Software Architect
Purpose: Simple and unified configuration for a single environment
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
import platform


class WerfenConfig:
    """Unified and simplified configuration for the Werfen pipeline"""
    
    def __init__(self):
        # Automatically detect the project root directory
        self._project_root = self._detect_project_root()
        self._system_info = self._get_system_info()
        
        # Consolidated configuration (without environment variables)
        self._setup_unified_config()
    
    def _detect_project_root(self) -> Path:
        """Automatically detects the project root."""
        # Search from current directory upward until finding requirements.txt
        current_path = Path(__file__).resolve().parent
        
        for parent in [current_path] + list(current_path.parents):
            if (parent / "requirements.txt").exists() and (parent / "dbt_project").exists():
                return parent
        
        # Fallback: use current directory
        return current_path
    
    def _get_system_info(self) -> Dict[str, str]:
        """Gets operating system information."""
        return {
            'os': platform.system().lower(),
            'architecture': platform.architecture()[0],
            'python_version': platform.python_version(),
            'is_windows': platform.system().lower() == 'windows'
        }
    
    def _setup_unified_config(self):
        """Unified configuration - all values in one place"""
        
        # ===========================================
        # MAIN CONFIGURATION
        # ===========================================
        
        # Credentials and basic configuration
        self.admin_email = 'admin@werfen-data-pipeline.local'
        self.admin_password = 'werfen2025'
        self.db_name = 'werfen.db'
        
        # Environment configuration
        self.environment = 'development'
        self.log_level = 'INFO'
        self.debug_mode = False
        
        # Logging configuration
        self.log_format = 'json'  # json | console
        
        # Security configuration
        self.master_password = 'werfen_default_key_2025'
        
        # ===========================================
        # DATA CONFIGURATION
        # ===========================================
        
        # Data sources and table mapping
        self.data_sources_config = {
            "chinook.db": {
                "customers": "raw_customer",
            },
            "example.db": {
                "sales_qty": "raw_sales_quantity",
                "foc_qty": "raw_free_of_charge_quantity",
            }
        }
        
        # Expected row counts for validation
        self.expected_row_counts = {
            "raw_customer": 59,
            "raw_sales_quantity": 500000,
            "raw_free_of_charge_quantity": 500000
        }
        
        # Data quality configuration
        self.data_quality_threshold = 0.95
        self.min_transaction_threshold = 1
        
        # ===========================================
        # ML CONFIGURATION
        # ===========================================
        
        self.ml_config = {
            'random_state': 42,
            'k_range': (2, 8),
            'feature_strategy': 'business_driven',
            'clustering_method': 'kmeans'
        }
        
        # ===========================================
        # AIRFLOW CONFIGURATION
        # ===========================================
        
        self.airflow_config = {
            'load_examples': False,
            'load_default_connections': False,
            'expose_config': True,
            'executor': 'LocalExecutor',
            'schedule_interval': '@daily'
        }
    
    # ===========================================
    # PROJECT PATHS (All relative)
    # ===========================================
    
    @property
    def project_root(self) -> Path:
        """Project root directory"""
        return self._project_root
    
    @property
    def src_folder(self) -> Path:
        """Source code directory"""
        return self.project_root / "src"
    
    @property
    def dbt_project_folder(self) -> Path:
        """dbt project directory"""
        return self.project_root / "dbt_project"
    
    @property
    def dags_folder(self) -> Path:
        """Airflow DAGs directory"""
        return self.project_root / "dags"
    
    @property
    def data_folder(self) -> Path:
        """Base data directory"""
        return self.project_root / "data"
    
    @property
    def raw_data_folder(self) -> Path:
        """Raw data directory"""
        return self.data_folder / "raw"
    
    @property
    def processed_data_folder(self) -> Path:
        """Processed data directory"""
        return self.data_folder / "processed"
    
    @property
    def artifacts_folder(self) -> Path:
        """Artifacts directory"""
        return self.project_root / "artifacts"
    
    @property
    def logs_folder(self) -> Path:
        """Logs directory"""
        return self.project_root / "logs"
    
    @property
    def docs_folder(self) -> Path:
        """Documentation directory"""
        return self.project_root / "docs"
    
    @property
    def notebooks_folder(self) -> Path:
        """Notebooks directory"""
        return self.project_root / "notebooks"
    
    @property
    def ml_outputs_folder(self) -> Path:
        """ML outputs directory"""
        return self.project_root / "ml_outputs"
    
    # ===========================================
    # DATABASE PATHS
    # ===========================================
    
    @property
    def main_database_path(self) -> Path:
        """Main DuckDB database path"""
        return self.artifacts_folder / self.db_name
    
    @property
    def chinook_db_path(self) -> Path:
        """Chinook database path (source)"""
        return self.raw_data_folder / "chinook.db"
    
    @property
    def example_db_path(self) -> Path:
        """Example database path (source)"""
        return self.raw_data_folder / "example.db"
    
    # ===========================================
    # CONFIGURACIÓN DE AIRFLOW
    # ===========================================
    
    @property
    def airflow_home(self) -> Path:
        """Airflow home directory"""
        return self.project_root / "airflow"
    
    @property
    def airflow_environment_vars(self) -> Dict[str, str]:
        """Airflow environment variables"""
        return {
            'AIRFLOW_HOME': str(self.airflow_home),
            'AIRFLOW__CORE__DAGS_FOLDER': str(self.dags_folder),
            'AIRFLOW__CORE__LOAD_EXAMPLES': str(self.airflow_config['load_examples']),
            'AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS': str(self.airflow_config['load_default_connections']),
            'AIRFLOW__WEBSERVER__EXPOSE_CONFIG': str(self.airflow_config['expose_config']),
            'AIRFLOW__CORE__EXECUTOR': self.airflow_config['executor']
        }
    
    # ===========================================
    # CONFIGURACIÓN DE DBT  
    # ===========================================
    
    @property
    def dbt_profiles_dir(self) -> Path:
        """dbt profiles directory"""
        return self.dbt_project_folder
    
    @property
    def dbt_target_dir(self) -> Path:
        """dbt target directory"""
        return self.dbt_project_folder / "target"
    
    # ===========================================
    # MÉTODOS DE UTILIDAD
    # ===========================================
    
    def ensure_directories(self):
        """Creates all necessary directories if they don't exist"""
        directories = [
            self.src_folder,
            self.data_folder,
            self.raw_data_folder,
            self.processed_data_folder,
            self.artifacts_folder,
            self.logs_folder,
            self.ml_outputs_folder,
            self.airflow_home,
            self.airflow_home / "logs",
            self.airflow_home / "plugins",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        print(f"✅ Directories created/verified: {len(directories)} directories")
    
    def apply_airflow_environment(self):
        """Applies Airflow environment variables to current process"""
        for key, value in self.airflow_environment_vars.items():
            os.environ[key] = value
        print("✅ Airflow environment variables configured")
    
    def get_database_connection_string(self, database_name: str = None) -> str:
        """Generates database connection string"""
        db_path = self.main_database_path if not database_name else self.artifacts_folder / database_name
        return f"duckdb:///{db_path}"
    
    def get_credential(self, key: str, default: str = None) -> str:
        """Gets credential from internal configuration (without environment variables)"""
        credentials_map = {
            'WERFEN_ADMIN_EMAIL': self.admin_email,
            'WERFEN_ADMIN_PASSWORD': self.admin_password,
            'WERFEN_DB_NAME': self.db_name,
            'WERFEN_MASTER_PASSWORD': self.master_password,
            'LOG_LEVEL': self.log_level,
            'LOG_FORMAT': self.log_format,
            'ENVIRONMENT': self.environment
        }
        
        return credentials_map.get(key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """Converts configuration to dictionary for debugging"""
        return {
            'project_root': str(self.project_root),
            'environment': self.environment,
            'system_info': self._system_info,
            'admin_email': self.admin_email,
            'database_path': str(self.main_database_path),
            'airflow_home': str(self.airflow_home),
            'dbt_project': str(self.dbt_project_folder),
            'log_level': self.log_level,
            'debug_mode': self.debug_mode,
            'ml_config': self.ml_config,
            'data_quality_threshold': self.data_quality_threshold
        }
    
    def print_config_summary(self):
        """Prints a summary of the current configuration"""
        print("🔧 WERFEN DATA PIPELINE - UNIFIED CONFIGURATION")
        print("=" * 60)
        print(f"📁 Project Root: {self.project_root}")
        print(f"🖥️  System: {self._system_info['os']} ({self._system_info['architecture']})")
        print(f"🐍 Python: {self._system_info['python_version']}")
        print(f"🌍 Environment: {self.environment}")
        print(f"📊 Database: {self.main_database_path}")
        print(f"🚁 Airflow Home: {self.airflow_home}")
        print(f"🔨 dbt Project: {self.dbt_project_folder}")
        print(f"👤 Admin Email: {self.admin_email}")
        print(f"🔍 Debug Mode: {self.debug_mode}")
        print(f"📈 Log Level: {self.log_level}")
        print(f"🤖 ML Random State: {self.ml_config['random_state']}")
        print(f"📊 Data Quality Threshold: {self.data_quality_threshold}")
        print("=" * 60)
        print("✅ Configuration loaded from internal values (without .env files)")


# Global configuration instance
_config_instance = None

def get_config() -> WerfenConfig:
    """Returns the global configuration instance (Singleton)"""
    global _config_instance
    if _config_instance is None:
        _config_instance = WerfenConfig()
    return _config_instance


if __name__ == "__main__":
    # Configuration test
    cfg = get_config()
    cfg.print_config_summary()
    
    print("\n🧪 VALIDATING PATHS:")
    print(f"✅ Project exists: {cfg.project_root.exists()}")
    print(f"✅ requirements.txt: {(cfg.project_root / 'requirements.txt').exists()}")
    print(f"✅ dbt_project: {cfg.dbt_project_folder.exists()}")
    print(f"✅ src folder: {cfg.src_folder.exists()}")
    
    print("\n🔐 CREDENTIALS CONFIGURATION:")
    print(f"✅ Admin Email: {cfg.get_credential('WERFEN_ADMIN_EMAIL')}")
    print(f"✅ Environment: {cfg.get_credential('ENVIRONMENT')}")
    print(f"✅ Log Level: {cfg.get_credential('LOG_LEVEL')}")
    
    print("\n📂 CREATING NECESSARY DIRECTORIES:")
    cfg.ensure_directories() 