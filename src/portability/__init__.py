"""
Werfen Data Impact Case - Portability Module
==================================================

Sistema de portabilidad para abstracción de fuentes de datos, formatos y entornos.
Diseñado para POC con capacidades enterprise y roadmap AWS.

AWS Equivalents:
- Data Connectors → AWS Glue Connectors + Data Catalog
- Storage Adapters → S3 + EFS + FSx
- Format Handlers → AWS Glue Data Format Library
- Config Manager → AWS Systems Manager Parameter Store
- Environment Manager → AWS CloudFormation + CDK

Author: Werfen Data Team
Date: 2024
"""

from .data_connectors import (
    WerfenDataConnector,
    LocalFileConnector,
    DatabaseConnector,
    CloudStorageConnector,
    DataConnectorFactory,
    ConnectionConfig
)

from .storage_adapters import (
    WerfenStorageAdapter,
    LocalStorageAdapter,
    CloudStorageAdapter,
    StorageAdapterFactory,
    StorageConfig
)

from .format_handlers import (
    WerfenFormatHandler,
    CSVFormatHandler,
    ParquetFormatHandler,
    JSONFormatHandler,
    FormatHandlerFactory,
    FormatConfig
)

from .config_manager import (
    WerfenConfigManager,
    PortabilityConfig,
    EnvironmentConfig
)

from .environment_manager import (
    WerfenEnvironmentManager,
    EnvironmentType,
    DeploymentConfig
)

__all__ = [
    # Data Connectors
    'WerfenDataConnector',
    'LocalFileConnector', 
    'DatabaseConnector',
    'CloudStorageConnector',
    'DataConnectorFactory',
    'ConnectionConfig',
    
    # Storage Adapters
    'WerfenStorageAdapter',
    'LocalStorageAdapter',
    'CloudStorageAdapter', 
    'StorageAdapterFactory',
    'StorageConfig',
    
    # Format Handlers
    'WerfenFormatHandler',
    'CSVFormatHandler',
    'ParquetFormatHandler',
    'JSONFormatHandler',
    'FormatHandlerFactory',
    'FormatConfig',
    
    # Config & Environment
    'WerfenConfigManager',
    'PortabilityConfig',
    'EnvironmentConfig',
    'WerfenEnvironmentManager', 
    'EnvironmentType',
    'DeploymentConfig'
]

def get_portability_suite(environment: str = "local") -> dict:
    """
    Factory function para obtener suite completa de portabilidad.
    
    AWS Equivalent: AWS CDK construct for complete data platform setup
    
    Args:
        environment: Tipo de entorno (local, dev, staging, prod)
        
    Returns:
        dict: Suite completa de componentes de portabilidad
    """
    from .environment_manager import WerfenEnvironmentManager
    
    env_manager = WerfenEnvironmentManager()
    
    return {
        'config_manager': WerfenConfigManager(environment=environment),
        'environment_manager': env_manager,
        'data_connector_factory': DataConnectorFactory(),
        'storage_adapter_factory': StorageAdapterFactory(),
        'format_handler_factory': FormatHandlerFactory(),
        'aws_equivalent': 'AWS CDK + CloudFormation + Systems Manager'
    } 