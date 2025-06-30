"""
Werfen Environment Manager - Gestión de Entornos
================================================

Gestión de entornos de desarrollo y deployment.
AWS Equivalent: AWS CloudFormation + CDK + Parameter Store

Author: Werfen Data Team
Date: 2024
"""

import os
import json
from enum import Enum
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from dataclasses import dataclass, asdict
import platform
import subprocess

# Import logging from our centralized system
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.logging.structured_logger import setup_structured_logging

class EnvironmentType(Enum):
    """Tipos de entorno soportados"""
    LOCAL = "local"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

@dataclass
class DeploymentConfig:
    """
    Configuración de deployment.
    AWS Equivalent: CloudFormation stack parameters
    """
    environment: EnvironmentType
    region: str = "us-east-1"
    
    # Configuración de recursos
    compute_type: str = "small"  # small, medium, large
    storage_tier: str = "standard"  # standard, ia, glacier
    backup_enabled: bool = True
    monitoring_enabled: bool = True
    
    # Configuración de red
    vpc_id: Optional[str] = None
    subnet_ids: Optional[List[str]] = None
    security_group_ids: Optional[List[str]] = None
    
    # Configuración de datos
    data_encryption: bool = True
    data_retention_days: int = 90
    
    # Configuración AWS específica
    aws_account_id: Optional[str] = None
    iam_role_arn: Optional[str] = None
    kms_key_id: Optional[str] = None
    
    # Tags de recursos
    tags: Optional[Dict[str, str]] = None
    
    # Configuración personalizada
    custom_config: Optional[Dict[str, Any]] = None

@dataclass
class EnvironmentStatus:
    """
    Estado del entorno.
    AWS Equivalent: CloudFormation stack status
    """
    environment: str
    status: str  # active, inactive, deploying, error
    version: Optional[str] = None
    last_updated: Optional[str] = None
    health_check: Optional[Dict[str, Any]] = None
    resource_usage: Optional[Dict[str, Any]] = None
    errors: Optional[List[str]] = None

class WerfenEnvironmentManager:
    """
    Gestor de entornos de desarrollo y deployment.
    AWS Equivalent: CloudFormation + CDK + Systems Manager integration
    """
    
    def __init__(self, base_path: Optional[str] = None):
        self.logger = setup_structured_logging(service_name="werfen.environment.WerfenEnvironmentManager")
        self.base_path = Path(base_path) if base_path else Path("./environments")
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Configuraciones de entorno
        self._deployment_configs: Dict[str, DeploymentConfig] = {}
        self._environment_status: Dict[str, EnvironmentStatus] = {}
        
        # Inicializar configuraciones por defecto
        self._initialize_default_configs()
        
    def _initialize_default_configs(self):
        """
        Inicializar configuraciones de entorno por defecto.
        AWS Equivalent: Default CloudFormation templates
        """
        # Configuración local
        local_config = DeploymentConfig(
            environment=EnvironmentType.LOCAL,
            region="local",
            compute_type="small",
            storage_tier="standard",
            backup_enabled=False,
            monitoring_enabled=False,
            data_encryption=False,
            data_retention_days=30,
            tags={
                "Environment": "local",
                "Project": "werfen-data-impact",
                "Purpose": "development"
            }
        )
        
        # Configuración de desarrollo
        dev_config = DeploymentConfig(
            environment=EnvironmentType.DEVELOPMENT,
            region="us-east-1",
            compute_type="small",
            storage_tier="standard",
            backup_enabled=True,
            monitoring_enabled=True,
            data_encryption=True,
            data_retention_days=30,
            tags={
                "Environment": "development",
                "Project": "werfen-data-impact",
                "Purpose": "development-testing"
            }
        )
        
        # Configuración de staging
        staging_config = DeploymentConfig(
            environment=EnvironmentType.STAGING,
            region="us-east-1",
            compute_type="medium",
            storage_tier="standard",
            backup_enabled=True,
            monitoring_enabled=True,
            data_encryption=True,
            data_retention_days=60,
            tags={
                "Environment": "staging",
                "Project": "werfen-data-impact",
                "Purpose": "pre-production-testing"
            }
        )
        
        # Configuración de producción
        prod_config = DeploymentConfig(
            environment=EnvironmentType.PRODUCTION,
            region="us-east-1",
            compute_type="large",
            storage_tier="standard",
            backup_enabled=True,
            monitoring_enabled=True,
            data_encryption=True,
            data_retention_days=2555,  # 7 años
            tags={
                "Environment": "production",
                "Project": "werfen-data-impact",
                "Purpose": "production-workload"
            }
        )
        
        self._deployment_configs = {
            "local": local_config,
            "development": dev_config,
            "staging": staging_config,
            "production": prod_config
        }
        
    def get_environment_config(self, environment: str) -> Optional[DeploymentConfig]:
        """
        Obtener configuración de entorno.
        AWS Equivalent: Get CloudFormation template parameters
        
        Args:
            environment: Nombre del entorno
            
        Returns:
            DeploymentConfig o None si no existe
        """
        return self._deployment_configs.get(environment)
        
    def set_environment_config(self, environment: str, config: DeploymentConfig) -> bool:
        """
        Establecer configuración de entorno.
        AWS Equivalent: Update CloudFormation stack parameters
        
        Args:
            environment: Nombre del entorno
            config: Configuración de deployment
            
        Returns:
            bool: True si se estableció exitosamente
        """
        try:
            self._deployment_configs[environment] = config
            
            # Guardar configuración a archivo
            config_file = self.base_path / f"{environment}.json"
            with open(config_file, 'w') as f:
                json.dump(asdict(config), f, indent=2, default=str)
                
            self.logger.info(f"Set configuration for environment: {environment}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set environment config: {str(e)}")
            return False
            
    def list_environments(self) -> List[str]:
        """
        Listar entornos disponibles.
        AWS Equivalent: List CloudFormation stacks
        
        Returns:
            Lista de nombres de entornos
        """
        return list(self._deployment_configs.keys())
        
    def get_current_environment(self) -> str:
        """
        Detectar entorno actual basado en variables de entorno y contexto.
        AWS Equivalent: Detect current AWS environment context
        
        Returns:
            Nombre del entorno actual
        """
        # Verificar variable de entorno
        env_var = os.getenv('WERFEN_ENVIRONMENT')
        if env_var and env_var in self._deployment_configs:
            return env_var
            
        # Verificar contexto AWS
        aws_profile = os.getenv('AWS_PROFILE')
        if aws_profile:
            if 'prod' in aws_profile.lower():
                return 'production'
            elif 'staging' in aws_profile.lower():
                return 'staging'
            elif 'dev' in aws_profile.lower():
                return 'development'
                
        # Default a local
        return 'local'
        
    def validate_environment(self, environment: str) -> Dict[str, Any]:
        """
        Validar entorno y sus recursos.
        AWS Equivalent: CloudFormation stack validation + resource health checks
        
        Args:
            environment: Nombre del entorno
            
        Returns:
            Diccionario con resultados de validación
        """
        validation_results = {
            'environment': environment,
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'checks': {}
        }
        
        try:
            # Verificar que el entorno existe
            if environment not in self._deployment_configs:
                validation_results['errors'].append(f"Environment '{environment}' not found")
                validation_results['is_valid'] = False
                return validation_results
                
            config = self._deployment_configs[environment]
            
            # Validaciones básicas
            validation_results['checks']['config_exists'] = True
            
            # Validar configuración local
            if environment == 'local':
                validation_results['checks'].update(self._validate_local_environment())
            else:
                validation_results['checks'].update(self._validate_cloud_environment(config))
                
            # Verificar dependencias
            validation_results['checks'].update(self._validate_dependencies())
            
            # Calcular estado final
            if any(not check for check in validation_results['checks'].values()):
                validation_results['warnings'].append("Some environment checks failed")
                
            return validation_results
            
        except Exception as e:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")
            return validation_results
            
    def get_environment_status(self, environment: str) -> Optional[EnvironmentStatus]:
        """
        Obtener estado actual del entorno.
        AWS Equivalent: CloudFormation stack status + CloudWatch metrics
        
        Args:
            environment: Nombre del entorno
            
        Returns:
            EnvironmentStatus o None si no está disponible
        """
        try:
            if environment not in self._deployment_configs:
                return None
                
            # Para el POC, simularemos el estado
            if environment == 'local':
                status = self._get_local_environment_status()
            else:
                status = self._get_cloud_environment_status(environment)
                
            self._environment_status[environment] = status
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to get environment status: {str(e)}")
            return None
            
    def deploy_environment(self, environment: str, dry_run: bool = True) -> Dict[str, Any]:
        """
        Deployar entorno.
        AWS Equivalent: CloudFormation stack deployment
        
        Args:
            environment: Nombre del entorno
            dry_run: Si es True, solo valida sin deployar
            
        Returns:
            Diccionario con resultados del deployment
        """
        deployment_results = {
            'environment': environment,
            'success': False,
            'dry_run': dry_run,
            'actions': [],
            'errors': []
        }
        
        try:
            if environment not in self._deployment_configs:
                deployment_results['errors'].append(f"Environment '{environment}' not found")
                return deployment_results
                
            config = self._deployment_configs[environment]
            
            # Validar antes de deployar
            validation = self.validate_environment(environment)
            if not validation['is_valid']:
                deployment_results['errors'].extend(validation['errors'])
                return deployment_results
                
            # Preparar acciones de deployment
            if environment == 'local':
                deployment_results['actions'] = self._prepare_local_deployment(config)
            else:
                deployment_results['actions'] = self._prepare_cloud_deployment(config)
                
            # Ejecutar deployment si no es dry_run
            if not dry_run:
                deployment_results['success'] = self._execute_deployment(environment, deployment_results['actions'])
            else:
                deployment_results['success'] = True
                deployment_results['actions'].append("DRY RUN: No changes were made")
                
            return deployment_results
            
        except Exception as e:
            deployment_results['errors'].append(f"Deployment error: {str(e)}")
            return deployment_results
            
    def create_environment_template(self, environment: str, output_path: Optional[str] = None) -> bool:
        """
        Crear template de infraestructura para el entorno.
        AWS Equivalent: Generate CloudFormation template
        
        Args:
            environment: Nombre del entorno
            output_path: Ruta de salida para el template
            
        Returns:
            bool: True si se creó exitosamente
        """
        try:
            if environment not in self._deployment_configs:
                self.logger.error(f"Environment '{environment}' not found")
                return False
                
            config = self._deployment_configs[environment]
            
            # Generar template según el entorno
            if environment == 'local':
                template = self._generate_local_template(config)
            else:
                template = self._generate_aws_cloudformation_template(config)
                
            # Determinar ruta de salida
            if not output_path:
                output_path = self.base_path / f"{environment}_template.json"
            else:
                output_path = Path(output_path)
                
            # Guardar template
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(template, f, indent=2)
                
            self.logger.info(f"Created environment template: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create environment template: {str(e)}")
            return False
            
    def _validate_local_environment(self) -> Dict[str, bool]:
        """Validar entorno local"""
        checks = {}
        
        # Verificar Python
        try:
            import sys
            checks['python_version'] = sys.version_info >= (3, 8)
        except:
            checks['python_version'] = False
            
        # Verificar dependencias críticas
        critical_packages = ['pandas', 'duckdb', 'yaml']
        for package in critical_packages:
            try:
                __import__(package)
                checks[f'{package}_available'] = True
            except ImportError:
                checks[f'{package}_available'] = False
                
        # Verificar espacio en disco
        try:
            import shutil
            free_space_gb = shutil.disk_usage('.').free / (1024**3)
            checks['disk_space_sufficient'] = free_space_gb > 5  # At least 5GB
        except:
            checks['disk_space_sufficient'] = False
            
        return checks
        
    def _validate_cloud_environment(self, config: DeploymentConfig) -> Dict[str, bool]:
        """Validar entorno cloud"""
        checks = {}
        
        # Para el POC, simulamos validaciones AWS
        checks['aws_credentials'] = os.getenv('AWS_ACCESS_KEY_ID') is not None
        checks['aws_region_set'] = config.region is not None
        checks['vpc_configuration'] = config.vpc_id is not None
        checks['iam_roles_configured'] = config.iam_role_arn is not None
        
        return checks
        
    def _validate_dependencies(self) -> Dict[str, bool]:
        """Validar dependencias del sistema"""
        checks = {}
        
        # Verificar herramientas de línea de comandos
        tools = ['git', 'python']
        for tool in tools:
            try:
                result = subprocess.run([tool, '--version'], 
                                      capture_output=True, text=True, timeout=5)
                checks[f'{tool}_available'] = result.returncode == 0
            except:
                checks[f'{tool}_available'] = False
                
        return checks
        
    def _get_local_environment_status(self) -> EnvironmentStatus:
        """Obtener estado del entorno local"""
        return EnvironmentStatus(
            environment='local',
            status='active',
            version='1.0.0',
            last_updated=None,
            health_check={
                'overall': 'healthy',
                'components': {
                    'data_pipeline': 'running',
                    'logging': 'active',
                    'security': 'enabled'
                }
            },
            resource_usage={
                'cpu_percent': 15.5,
                'memory_mb': 256,
                'disk_gb': 2.1
            }
        )
        
    def _get_cloud_environment_status(self, environment: str) -> EnvironmentStatus:
        """Obtener estado del entorno cloud (simulado)"""
        return EnvironmentStatus(
            environment=environment,
            status='active',
            version='1.0.0',
            last_updated='2024-01-01T00:00:00Z',
            health_check={
                'overall': 'healthy',
                'components': {
                    's3_bucket': 'active',
                    'glue_jobs': 'running',
                    'redshift_cluster': 'available',
                    'lambda_functions': 'active'
                }
            },
            resource_usage={
                's3_storage_gb': 150.5,
                'redshift_cpu_percent': 25.0,
                'lambda_invocations': 1250
            }
        )
        
    def _prepare_local_deployment(self, config: DeploymentConfig) -> List[str]:
        """Preparar acciones de deployment local"""
        actions = [
            "Create local data directories",
            "Initialize DuckDB database",
            "Setup local logging configuration",
            "Configure development environment variables"
        ]
        
        if config.backup_enabled:
            actions.append("Setup local backup strategy")
            
        return actions
        
    def _prepare_cloud_deployment(self, config: DeploymentConfig) -> List[str]:
        """Preparar acciones de deployment cloud"""
        actions = [
            f"Create S3 bucket for data lake in {config.region}",
            "Setup AWS Glue catalog and crawlers",
            "Deploy Redshift cluster for analytics",
            "Configure Lambda functions for data processing",
            "Setup CloudWatch logging and monitoring"
        ]
        
        if config.data_encryption:
            actions.append("Configure KMS encryption for data at rest")
            
        if config.backup_enabled:
            actions.append("Setup automated backup policies")
            
        return actions
        
    def _execute_deployment(self, environment: str, actions: List[str]) -> bool:
        """Ejecutar deployment (simulado para POC)"""
        try:
            self.logger.info(f"Executing deployment for environment: {environment}")
            
            for action in actions:
                self.logger.info(f"Executing: {action}")
                # En una implementación real, aquí ejecutaríamos los comandos correspondientes
                
            return True
            
        except Exception as e:
            self.logger.error(f"Deployment execution failed: {str(e)}")
            return False
            
    def _generate_local_template(self, config: DeploymentConfig) -> Dict[str, Any]:
        """Generar template para entorno local"""
        return {
            "type": "local_environment",
            "configuration": asdict(config),
            "setup_commands": [
                "mkdir -p ./data/{raw,processed,archive}",
                "mkdir -p ./logs",
                "mkdir -p ./config",
                "pip install -r requirements.txt"
            ],
            "environment_variables": {
                "WERFEN_ENVIRONMENT": "local",
                "WERFEN_DATA_PATH": "./data",
                "WERFEN_LOG_LEVEL": "DEBUG"
            }
        }
        
    def _generate_aws_cloudformation_template(self, config: DeploymentConfig) -> Dict[str, Any]:
        """Generar template CloudFormation"""
        template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": f"Werfen Data Impact Case - {config.environment.value} Environment",
            "Parameters": {
                "Environment": {
                    "Type": "String",
                    "Default": config.environment.value,
                    "Description": "Environment name"
                },
                "ProjectName": {
                    "Type": "String", 
                    "Default": "werfen-data-impact",
                    "Description": "Project name for resource naming"
                }
            },
            "Resources": {
                "DataLakeBucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {
                        "BucketName": f"werfen-{config.environment.value}-data-lake",
                        "BucketEncryption": {
                            "ServerSideEncryptionConfiguration": [
                                {
                                    "ServerSideEncryptionByDefault": {
                                        "SSEAlgorithm": "AES256"
                                    }
                                }
                            ]
                        },
                        "Tags": [
                            {"Key": k, "Value": v} for k, v in (config.tags or {}).items()
                        ]
                    }
                },
                "GlueDatabase": {
                    "Type": "AWS::Glue::Database",
                    "Properties": {
                        "CatalogId": {"Ref": "AWS::AccountId"},
                        "DatabaseInput": {
                            "Name": f"werfen_{config.environment.value}_catalog",
                            "Description": f"Data catalog for {config.environment.value} environment"
                        }
                    }
                }
            },
            "Outputs": {
                "DataLakeBucketName": {
                    "Description": "Name of the data lake S3 bucket",
                    "Value": {"Ref": "DataLakeBucket"},
                    "Export": {
                        "Name": f"werfen-{config.environment.value}-data-lake-bucket"
                    }
                },
                "GlueDatabaseName": {
                    "Description": "Name of the Glue database",
                    "Value": {"Ref": "GlueDatabase"},
                    "Export": {
                        "Name": f"werfen-{config.environment.value}-glue-database"
                    }
                }
            }
        }
        
        # Agregar Redshift si es staging/production
        if config.environment in [EnvironmentType.STAGING, EnvironmentType.PRODUCTION]:
            template["Resources"]["RedshiftCluster"] = {
                "Type": "AWS::Redshift::Cluster",
                "Properties": {
                    "ClusterIdentifier": f"werfen-{config.environment.value}-cluster",
                    "NodeType": "dc2.large" if config.compute_type == "large" else "dc2.small",
                    "NumberOfNodes": 2 if config.compute_type == "large" else 1,
                    "MasterUsername": "werfenuser",
                    "MasterUserPassword": {"Ref": "RedshiftPassword"},
                    "DBName": "werfen_analytics",
                    "Encrypted": config.data_encryption
                }
            }
            
            template["Parameters"]["RedshiftPassword"] = {
                "Type": "String",
                "NoEcho": True,
                "Description": "Password for Redshift master user"
            }
            
        return template 