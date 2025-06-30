"""
Werfen Config Manager - Configuration Management
===============================================

Centralized configuration management for portability.
AWS Equivalent: AWS Systems Manager Parameter Store + Secrets Manager

Author: Werfen Data Team
Date: 2024
"""

import os
import json
import yaml
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from dataclasses import dataclass, asdict
import configparser
from enum import Enum

# Import logging from our centralized system
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.logging.structured_logger import setup_structured_logging

class ConfigFormat(Enum):
    """Supported configuration formats"""
    JSON = "json"
    YAML = "yaml"
    INI = "ini"
    ENV = "env"

@dataclass
class PortabilityConfig:
    """
    Unified portability configuration.
    AWS Equivalent: Systems Manager Parameter hierarchy
    """
    # Environment configuration
    environment: str = "local"
    debug_mode: bool = False
    
    # Data configuration
    data_sources: Optional[Dict[str, Any]] = None
    storage_configs: Optional[Dict[str, Any]] = None
    format_configs: Optional[Dict[str, Any]] = None
    
    # Connection configuration
    database_connections: Optional[Dict[str, Any]] = None
    cloud_connections: Optional[Dict[str, Any]] = None
    
    # Security configuration
    encryption_enabled: bool = True
    secrets_backend: str = "local"  # local, aws_secrets_manager, vault
    
    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "structured"
    
    # Performance configuration
    cache_enabled: bool = True
    parallel_processing: bool = True
    max_workers: int = 4
    
    # AWS configuration
    aws_region: str = "us-east-1"
    aws_profile: Optional[str] = None
    s3_bucket: Optional[str] = None
    
    # Custom configuration
    custom_config: Optional[Dict[str, Any]] = None

@dataclass
class EnvironmentConfig:
    """
    Environment-specific configuration.
    AWS Equivalent: Environment-specific parameter groups
    """
    name: str
    config_path: str
    secrets_path: Optional[str] = None
    overrides: Optional[Dict[str, Any]] = None
    aws_equivalent: str = "Systems Manager Parameter Store hierarchy"

class WerfenConfigManager:
    """
    Centralized configuration manager.
    AWS Equivalent: Systems Manager Parameter Store + Secrets Manager client
    """
    
    def __init__(self, config_path: Optional[str] = None, environment: str = "local"):
        self.logger = setup_structured_logging(service_name="werfen.config.WerfenConfigManager")
        self.environment = environment
        self.config_path = Path(config_path) if config_path else Path("./config")
        self.config_path.mkdir(parents=True, exist_ok=True)
        
        # Configuration cache
        self._config_cache: Dict[str, Any] = {}
        self._environment_configs: Dict[str, EnvironmentConfig] = {}
        
        # Default configuration
        self._default_config = PortabilityConfig()
        
        # Initialize environment configurations
        self._initialize_environment_configs()
        
    def _initialize_environment_configs(self):
        """
        Initialize predefined environment configurations.
        AWS Equivalent: Create default parameter hierarchies
        """
        environments = {
            "local": EnvironmentConfig(
                name="local",
                config_path=str(self.config_path / "local.yaml"),
                secrets_path=str(self.config_path / ".secrets.local"),
                overrides={
                    "debug_mode": True,
                    "log_level": "DEBUG",
                    "cache_enabled": True,
                    "encryption_enabled": False
                }
            ),
            "development": EnvironmentConfig(
                name="development",
                config_path=str(self.config_path / "development.yaml"),
                secrets_path=str(self.config_path / ".secrets.dev"),
                overrides={
                    "debug_mode": True,
                    "log_level": "DEBUG",
                    "aws_region": "us-east-1",
                    "s3_bucket": "werfen-dev-data-lake"
                }
            ),
            "staging": EnvironmentConfig(
                name="staging",
                config_path=str(self.config_path / "staging.yaml"),
                secrets_path=str(self.config_path / ".secrets.staging"),
                overrides={
                    "debug_mode": False,
                    "log_level": "INFO",
                    "aws_region": "us-east-1",
                    "s3_bucket": "werfen-staging-data-lake"
                }
            ),
            "production": EnvironmentConfig(
                name="production",
                config_path=str(self.config_path / "production.yaml"),
                secrets_path=None,  # Use AWS Secrets Manager in prod
                overrides={
                    "debug_mode": False,
                    "log_level": "WARNING",
                    "secrets_backend": "aws_secrets_manager",
                    "aws_region": "us-east-1",
                    "s3_bucket": "werfen-prod-data-lake",
                    "encryption_enabled": True
                }
            )
        }
        
        self._environment_configs.update(environments)
        
    def load_config(self, environment: Optional[str] = None) -> PortabilityConfig:
        """
        Load configuration for specific environment.
        AWS Equivalent: Get parameters by path from Parameter Store
        
        Args:
            environment: Environment name
            
        Returns:
            PortabilityConfig: Loaded configuration
        """
        env = environment or self.environment
        
        try:
            # Check cache
            cache_key = f"config_{env}"
            if cache_key in self._config_cache:
                self.logger.debug(f"Loading config from cache for environment: {env}")
                return PortabilityConfig(**self._config_cache[cache_key])
                
            # Load base configuration
            config_data = asdict(self._default_config)
            
            # Apply environment configuration
            if env in self._environment_configs:
                env_config = self._environment_configs[env]
                
                # Try to load configuration file
                config_file = Path(env_config.config_path)
                if config_file.exists():
                    file_config = self._load_config_file(config_file)
                    config_data.update(file_config)
                    
                # Apply environment overrides
                if env_config.overrides:
                    config_data.update(env_config.overrides)
                    
                # Load secrets if file exists
                if env_config.secrets_path:
                    secrets_file = Path(env_config.secrets_path)
                    if secrets_file.exists():
                        secrets = self._load_secrets_file(secrets_file)
                        config_data.update(secrets)
                        
            # Aplicar variables de entorno
            env_overrides = self._load_environment_variables()
            config_data.update(env_overrides)
            
            # Cachear configuración
            self._config_cache[cache_key] = config_data
            
            self.logger.info(f"Loaded configuration for environment: {env}")
            return PortabilityConfig(**config_data)
            
        except Exception as e:
            self.logger.error(f"Failed to load config for environment {env}: {str(e)}")
            return self._default_config
            
    def save_config(self, config: PortabilityConfig, environment: Optional[str] = None, 
                   format_type: ConfigFormat = ConfigFormat.YAML) -> bool:
        """
        Guardar configuración a archivo.
        AWS Equivalent: Put parameters to Parameter Store
        
        Args:
            config: Configuración a guardar
            environment: Nombre del entorno
            format_type: Formato de archivo
            
        Returns:
            bool: True si se guardó exitosamente
        """
        env = environment or self.environment
        
        try:
            if env not in self._environment_configs:
                self.logger.error(f"Unknown environment: {env}")
                return False
                
            env_config = self._environment_configs[env]
            config_file = Path(env_config.config_path)
            config_file.parent.mkdir(parents=True, exist_ok=True)
            
            config_data = asdict(config)
            
            # Remover campos sensibles para archivo de configuración
            sensitive_fields = ['database_connections', 'cloud_connections']
            secrets_data = {}
            
            for field in sensitive_fields:
                if field in config_data and config_data[field]:
                    secrets_data[field] = config_data[field]
                    config_data[field] = None
                    
            # Guardar configuración principal
            if format_type == ConfigFormat.YAML:
                with open(config_file, 'w') as f:
                    yaml.dump(config_data, f, default_flow_style=False, indent=2)
            elif format_type == ConfigFormat.JSON:
                with open(config_file, 'w') as f:
                    json.dump(config_data, f, indent=2)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
            # Guardar secrets por separado
            if secrets_data and env_config.secrets_path:
                secrets_file = Path(env_config.secrets_path)
                with open(secrets_file, 'w') as f:
                    json.dump(secrets_data, f, indent=2)
                    
                # Cambiar permisos de archivo de secrets
                os.chmod(secrets_file, 0o600)
                
            # Invalidar cache
            cache_key = f"config_{env}"
            if cache_key in self._config_cache:
                del self._config_cache[cache_key]
                
            self.logger.info(f"Saved configuration for environment: {env}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save config for environment {env}: {str(e)}")
            return False
            
    def get_data_source_config(self, source_name: str, environment: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Obtener configuración de fuente de datos específica.
        AWS Equivalent: Get specific parameter from Parameter Store
        
        Args:
            source_name: Nombre de la fuente de datos
            environment: Entorno
            
        Returns:
            Dict con configuración de la fuente o None
        """
        config = self.load_config(environment)
        
        if config.data_sources and source_name in config.data_sources:
            return config.data_sources[source_name]
            
        return None
        
    def get_connection_string(self, connection_name: str, environment: Optional[str] = None) -> Optional[str]:
        """
        Obtener string de conexión.
        AWS Equivalent: Get secret from Secrets Manager
        
        Args:
            connection_name: Nombre de la conexión
            environment: Entorno
            
        Returns:
            String de conexión o None
        """
        config = self.load_config(environment)
        
        # Buscar en conexiones de base de datos
        if config.database_connections and connection_name in config.database_connections:
            conn_config = config.database_connections[connection_name]
            return self._build_connection_string(conn_config)
            
        # Buscar en conexiones cloud
        if config.cloud_connections and connection_name in config.cloud_connections:
            return config.cloud_connections[connection_name].get('connection_string')
            
        return None
        
    def list_environments(self) -> List[str]:
        """
        Listar entornos disponibles.
        AWS Equivalent: List parameter hierarchies
        
        Returns:
            Lista de nombres de entornos
        """
        return list(self._environment_configs.keys())
        
    def validate_config(self, config: PortabilityConfig) -> Dict[str, Any]:
        """
        Validar configuración.
        AWS Equivalent: Parameter validation rules
        
        Args:
            config: Configuración a validar
            
        Returns:
            Diccionario con resultados de validación
        """
        validation_results = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Validar configuración básica
            if not config.environment:
                validation_results['errors'].append("Environment is required")
                validation_results['is_valid'] = False
                
            if config.max_workers < 1:
                validation_results['errors'].append("max_workers must be greater than 0")
                validation_results['is_valid'] = False
                
            # Validar configuración AWS
            if config.environment in ['staging', 'production']:
                if not config.s3_bucket:
                    validation_results['warnings'].append("S3 bucket not configured for cloud environment")
                    
                if not config.aws_region:
                    validation_results['warnings'].append("AWS region not specified")
                    
            # Validar configuración de seguridad
            if config.environment == 'production' and not config.encryption_enabled:
                validation_results['warnings'].append("Encryption should be enabled in production")
                
            return validation_results
            
        except Exception as e:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")
            return validation_results
            
    def create_sample_configs(self) -> bool:
        """
        Crear archivos de configuración de ejemplo.
        AWS Equivalent: Create sample parameter templates
        
        Returns:
            bool: True si se crearon exitosamente
        """
        try:
            for env_name, env_config in self._environment_configs.items():
                config_file = Path(env_config.config_path)
                
                if not config_file.exists():
                    # Crear configuración base
                    base_config = asdict(self._default_config)
                    
                    # Aplicar overrides de entorno
                    if env_config.overrides:
                        base_config.update(env_config.overrides)
                        
                    # Agregar configuración de ejemplo específica del entorno
                    if env_name == "local":
                        base_config.update({
                            'data_sources': {
                                'customers': {
                                    'type': 'local_file',
                                    'path': './data/customers.csv',
                                    'format': 'csv'
                                },
                                'sales': {
                                    'type': 'local_file', 
                                    'path': './data/sales.parquet',
                                    'format': 'parquet'
                                }
                            },
                            'storage_configs': {
                                'default': {
                                    'type': 'local',
                                    'base_path': './data'
                                }
                            }
                        })
                    elif env_name in ["development", "staging", "production"]:
                        base_config.update({
                            'data_sources': {
                                'customers': {
                                    'type': 'cloud_storage',
                                    'bucket': f'werfen-{env_name}-data-lake',
                                    'path': 'raw/customers/',
                                    'format': 'parquet'
                                },
                                'sales': {
                                    'type': 'cloud_storage',
                                    'bucket': f'werfen-{env_name}-data-lake', 
                                    'path': 'raw/sales/',
                                    'format': 'parquet'
                                }
                            },
                            'storage_configs': {
                                'default': {
                                    'type': 'cloud',
                                    'bucket_name': f'werfen-{env_name}-data-lake',
                                    'region': 'us-east-1'
                                }
                            }
                        })
                        
                    # Guardar archivo de configuración
                    config_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(config_file, 'w') as f:
                        yaml.dump(base_config, f, default_flow_style=False, indent=2)
                        
                    self.logger.info(f"Created sample config: {config_file}")
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create sample configs: {str(e)}")
            return False
            
    def _load_config_file(self, config_file: Path) -> Dict[str, Any]:
        """Cargar archivo de configuración"""
        try:
            with open(config_file, 'r') as f:
                if config_file.suffix.lower() in ['.yaml', '.yml']:
                    return yaml.safe_load(f) or {}
                elif config_file.suffix.lower() == '.json':
                    return json.load(f)
                else:
                    raise ValueError(f"Unsupported config format: {config_file.suffix}")
        except Exception as e:
            self.logger.error(f"Failed to load config file {config_file}: {str(e)}")
            return {}
            
    def _load_secrets_file(self, secrets_file: Path) -> Dict[str, Any]:
        """Cargar archivo de secrets"""
        try:
            with open(secrets_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"Failed to load secrets file {secrets_file}: {str(e)}")
            return {}
            
    def _load_environment_variables(self) -> Dict[str, Any]:
        """Cargar variables de entorno con prefijo WERFEN_"""
        env_vars = {}
        prefix = "WERFEN_"
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower()
                
                # Intentar convertir a tipo apropiado
                if value.lower() in ['true', 'false']:
                    env_vars[config_key] = value.lower() == 'true'
                elif value.isdigit():
                    env_vars[config_key] = int(value)
                else:
                    env_vars[config_key] = value
                    
        return env_vars
        
    def _build_connection_string(self, conn_config: Dict[str, Any]) -> str:
        """Construir string de conexión desde configuración"""
        conn_type = conn_config.get('type', 'postgresql')
        host = conn_config.get('host', 'localhost')
        port = conn_config.get('port', 5432)
        database = conn_config.get('database', '')
        username = conn_config.get('username', '')
        password = conn_config.get('password', '')
        
        if conn_type == 'postgresql':
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
        elif conn_type == 'mysql':
            return f"mysql://{username}:{password}@{host}:{port}/{database}"
        elif conn_type == 'sqlite':
            return f"sqlite:///{database}"
        else:
            return conn_config.get('connection_string', '') 