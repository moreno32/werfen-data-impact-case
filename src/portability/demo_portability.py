"""
Werfen Portability Demo - Demostración del Sistema de Portabilidad
==================================================================

Script de demostración para mostrar las capacidades del sistema de portabilidad.
Incluye equivalencias AWS documentadas.

AWS Equivalent: Comprehensive AWS data platform demonstration

Author: Werfen Data Team
Date: 2024
"""

import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
import json
import tempfile

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Import portability components
from src.portability import (
    get_portability_suite,
    DataConnectorFactory, ConnectionConfig,
    StorageAdapterFactory, StorageConfig,
    FormatHandlerFactory, FormatConfig,
    WerfenConfigManager, PortabilityConfig,
    WerfenEnvironmentManager, EnvironmentType
)

# Import logging
from src.logging.structured_logger import setup_structured_logging

def main():
    """
    Función principal de demostración.
    AWS Equivalent: Complete AWS data platform workflow demo
    """
    logger = setup_structured_logging("demo.portability")
    
    print("=" * 70)
    print("🔄 WERFEN PORTABILITY SYSTEM DEMONSTRATION")
    print("=" * 70)
    print("Sistema de portabilidad enterprise con equivalencias AWS")
    print("POC → AWS Migration Ready")
    print()
    
    try:
        # 1. Demostrar gestión de entornos
        print("📋 1. ENVIRONMENT MANAGEMENT")
        print("-" * 40)
        demo_environment_management()
        print()
        
        # 2. Demostrar gestión de configuración
        print("⚙️  2. CONFIGURATION MANAGEMENT")
        print("-" * 40)
        demo_configuration_management()
        print()
        
        # 3. Demostrar conectores de datos
        print("🔌 3. DATA CONNECTORS")
        print("-" * 40)
        demo_data_connectors()
        print()
        
        # 4. Demostrar adaptadores de almacenamiento
        print("💾 4. STORAGE ADAPTERS")
        print("-" * 40)
        demo_storage_adapters()
        print()
        
        # 5. Demostrar manejadores de formato
        print("📄 5. FORMAT HANDLERS")
        print("-" * 40)
        demo_format_handlers()
        print()
        
        # 6. Demostrar integración completa
        print("🎯 6. COMPLETE INTEGRATION")
        print("-" * 40)
        demo_complete_integration()
        print()
        
        # 7. Equivalencias AWS
        print("☁️  7. AWS EQUIVALENTS")
        print("-" * 40)
        show_aws_equivalents()
        print()
        
        print("✅ DEMONSTRATION COMPLETED SUCCESSFULLY!")
        print("🚀 Ready for AWS migration with documented equivalences")
        
    except Exception as e:
        logger.error(f"Demo failed: {str(e)}")
        print(f"❌ Demo failed: {str(e)}")
        return False
        
    return True

def demo_environment_management():
    """
    Demostrar gestión de entornos.
    AWS Equivalent: CloudFormation + CDK environment management
    """
    logger = setup_structured_logging("demo.environment")
    
    try:
        # Crear gestor de entornos
        env_manager = WerfenEnvironmentManager()
        
        print("🏗️  Environment Manager initialized")
        print(f"   AWS Equivalent: CloudFormation + CDK")
        
        # Listar entornos disponibles
        environments = env_manager.list_environments()
        print(f"📋 Available environments: {environments}")
        
        # Detectar entorno actual
        current_env = env_manager.get_current_environment()
        print(f"🎯 Current environment: {current_env}")
        
        # Validar entorno local
        validation = env_manager.validate_environment('local')
        print(f"✅ Local validation: {'PASSED' if validation['is_valid'] else 'FAILED'}")
        
        if validation['errors']:
            print(f"   Errors: {validation['errors']}")
        if validation['warnings']:
            print(f"   Warnings: {validation['warnings']}")
            
        # Obtener estado del entorno
        status = env_manager.get_environment_status('local')
        if status:
            print(f"📊 Environment status: {status.status}")
            print(f"   Health: {status.health_check.get('overall', 'unknown')}")
            
        # Simular deployment
        deployment = env_manager.deploy_environment('local', dry_run=True)
        print(f"🚀 Deployment dry-run: {'SUCCESS' if deployment['success'] else 'FAILED'}")
        print(f"   Actions planned: {len(deployment['actions'])}")
        
        logger.info("Environment management demo completed")
        
    except Exception as e:
        logger.error(f"Environment demo failed: {str(e)}")
        raise

def demo_configuration_management():
    """
    Demostrar gestión de configuración.
    AWS Equivalent: Systems Manager Parameter Store + Secrets Manager
    """
    logger = setup_structured_logging("demo.config")
    
    try:
        # Crear gestor de configuración
        config_manager = WerfenConfigManager()
        
        print("⚙️  Configuration Manager initialized")
        print(f"   AWS Equivalent: Systems Manager Parameter Store")
        
        # Crear configuraciones de ejemplo
        success = config_manager.create_sample_configs()
        print(f"📝 Sample configs created: {'SUCCESS' if success else 'FAILED'}")
        
        # Cargar configuración local
        local_config = config_manager.load_config('local')
        print(f"🔧 Local config loaded: {local_config.environment}")
        print(f"   Debug mode: {local_config.debug_mode}")
        print(f"   Cache enabled: {local_config.cache_enabled}")
        
        # Validar configuración
        validation = config_manager.validate_config(local_config)
        print(f"✅ Config validation: {'PASSED' if validation['is_valid'] else 'FAILED'}")
        
        # Obtener configuración de fuente de datos
        data_source = config_manager.get_data_source_config('customers', 'local')
        if data_source:
            print(f"📊 Data source config: {data_source.get('type', 'unknown')}")
            
        logger.info("Configuration management demo completed")
        
    except Exception as e:
        logger.error(f"Configuration demo failed: {str(e)}")
        raise

def demo_data_connectors():
    """
    Demostrar conectores de datos.
    AWS Equivalent: AWS Glue Connectors + Data Catalog
    """
    logger = setup_structured_logging("demo.connectors")
    
    try:
        print("🔌 Testing Data Connectors")
        print(f"   AWS Equivalent: AWS Glue Connectors + Data Catalog")
        
        # Crear datos de prueba
        test_data = create_test_data()
        
        # 1. Conector de archivos locales
        local_config = ConnectionConfig(
            connection_type='local_file',
            path='./temp_data'
        )
        
        local_connector = DataConnectorFactory.create_connector(local_config)
        
        # Probar conexión
        test_result = local_connector.test_connection()
        print(f"📁 Local File Connector: {test_result['status'].upper()}")
        
        if test_result['status'] == 'success':
            # Escribir datos de prueba
            local_connector.connect()
            success = local_connector.write_data(test_data, 'customers_test.csv')
            print(f"   Write test: {'SUCCESS' if success else 'FAILED'}")
            
            # Leer datos de vuelta
            if success:
                read_data = local_connector.read_data('customers_test.csv')
                print(f"   Read test: {len(read_data)} rows retrieved")
                
            # Listar recursos
            resources = local_connector.list_resources()
            print(f"   Resources found: {len(resources)}")
            
            local_connector.disconnect()
            
        # 2. Conector de base de datos (simulado)
        db_config = ConnectionConfig(
            connection_type='database',
            host='localhost',
            database='werfen_test',
            username='test_user'
        )
        
        db_connector = DataConnectorFactory.create_connector(db_config)
        db_test = db_connector.test_connection()
        print(f"🗄️  Database Connector: {db_test['status'].upper()}")
        
        # 3. Conector cloud (simulado)
        cloud_config = ConnectionConfig(
            connection_type='cloud_storage',
            bucket='werfen-test-bucket',
            region='us-east-1'
        )
        
        cloud_connector = DataConnectorFactory.create_connector(cloud_config)
        cloud_test = cloud_connector.test_connection()
        print(f"☁️  Cloud Storage Connector: {cloud_test['status'].upper()}")
        
        # 4. Factory desde URL
        file_connector = DataConnectorFactory.create_from_url('file://./temp_data')
        s3_connector = DataConnectorFactory.create_from_url('s3://werfen-data-lake/raw/')
        
        print(f"🏭 Factory from URL: Local={type(file_connector).__name__}")
        print(f"   Factory from URL: S3={type(s3_connector).__name__}")
        
        logger.info("Data connectors demo completed")
        
    except Exception as e:
        logger.error(f"Data connectors demo failed: {str(e)}")
        raise

def demo_storage_adapters():
    """
    Demostrar adaptadores de almacenamiento.
    AWS Equivalent: S3 + EFS + FSx abstraction
    """
    logger = setup_structured_logging("demo.storage")
    
    try:
        print("💾 Testing Storage Adapters")
        print(f"   AWS Equivalent: S3 + EFS + FSx")
        
        # 1. Adaptador local
        local_config = StorageConfig(
            storage_type='local',
            base_path='./temp_storage',
            compression_enabled=True,
            backup_enabled=True
        )
        
        local_adapter = StorageAdapterFactory.create_adapter(local_config)
        
        # Probar conexión
        connected = local_adapter.connect()
        print(f"📁 Local Storage: {'CONNECTED' if connected else 'FAILED'}")
        
        if connected:
            # Crear archivo de prueba
            test_content = "Test data for storage adapter\nLine 2\nLine 3"
            
            # Subir datos
            upload_success = local_adapter.upload_data(test_content, 'test_file.txt')
            print(f"   Upload test: {'SUCCESS' if upload_success else 'FAILED'}")
            
            # Verificar existencia
            exists = local_adapter.file_exists('test_file.txt')
            print(f"   File exists: {exists}")
            
            # Obtener metadatos
            metadata = local_adapter.get_file_metadata('test_file.txt')
            if metadata:
                print(f"   File size: {metadata.size_bytes} bytes")
                
            # Listar archivos
            files = local_adapter.list_files()
            print(f"   Files listed: {len(files)}")
            
            # Descargar datos
            downloaded_data = local_adapter.download_data('test_file.txt')
            if downloaded_data:
                print(f"   Download test: {len(downloaded_data)} bytes retrieved")
                
            local_adapter.disconnect()
            
        # 2. Adaptador cloud (simulado)
        cloud_config = StorageConfig(
            storage_type='cloud',
            bucket_name='werfen-test-bucket',
            region='us-east-1',
            encryption_enabled=True
        )
        
        cloud_adapter = StorageAdapterFactory.create_adapter(cloud_config)
        cloud_connected = cloud_adapter.connect()
        print(f"☁️  Cloud Storage: {'CONNECTED' if cloud_connected else 'FAILED'}")
        
        if cloud_connected:
            # Probar operaciones básicas (simuladas)
            cloud_upload = cloud_adapter.upload_data("Cloud test data", 'cloud_test.txt')
            print(f"   Cloud upload: {'SUCCESS' if cloud_upload else 'FAILED'}")
            
            cloud_files = cloud_adapter.list_files()
            print(f"   Cloud files: {len(cloud_files)}")
            
            cloud_adapter.disconnect()
            
        # 3. Factory helpers
        simple_local = StorageAdapterFactory.create_local_adapter('./simple_test')
        simple_cloud = StorageAdapterFactory.create_cloud_adapter('werfen-simple-bucket')
        
        print(f"🏭 Simple factory: Local={type(simple_local).__name__}")
        print(f"   Simple factory: Cloud={type(simple_cloud).__name__}")
        
        logger.info("Storage adapters demo completed")
        
    except Exception as e:
        logger.error(f"Storage adapters demo failed: {str(e)}")
        raise

def demo_format_handlers():
    """
    Demostrar manejadores de formato.
    AWS Equivalent: AWS Glue Data Format Library + Schema Registry
    """
    logger = setup_structured_logging("demo.formats")
    
    try:
        print("📄 Testing Format Handlers")
        print(f"   AWS Equivalent: AWS Glue Data Format Library")
        
        # Crear datos de prueba
        test_data = create_test_data()
        
        # Crear directorio temporal
        temp_dir = Path('./temp_formats')
        temp_dir.mkdir(exist_ok=True)
        
        # 1. Manejador CSV
        csv_config = FormatConfig(
            format_type='csv',
            has_header=True,
            delimiter=',',
            encoding='utf-8'
        )
        
        csv_handler = FormatHandlerFactory.create_handler(csv_config)
        
        # Escribir CSV
        csv_file = temp_dir / 'test_data.csv'
        csv_success = csv_handler.write_data(test_data, str(csv_file))
        print(f"📊 CSV Handler: Write {'SUCCESS' if csv_success else 'FAILED'}")
        
        if csv_success:
            # Leer CSV
            read_csv = csv_handler.read_data(str(csv_file))
            print(f"   CSV Read: {len(read_csv)} rows, {len(read_csv.columns)} columns")
            
            # Inferir schema
            schema = csv_handler.infer_schema(str(csv_file))
            print(f"   Schema inferred: {len(schema.columns)} columns")
            
            # Validar datos
            validation = csv_handler.validate_data(read_csv, schema)
            print(f"   Validation: {'PASSED' if validation['is_valid'] else 'FAILED'}")
            
        # 2. Manejador JSON
        json_config = FormatConfig(
            format_type='json',
            encoding='utf-8'
        )
        
        json_handler = FormatHandlerFactory.create_handler(json_config)
        
        # Escribir JSON
        json_file = temp_dir / 'test_data.json'
        json_success = json_handler.write_data(test_data, str(json_file))
        print(f"🗂️  JSON Handler: Write {'SUCCESS' if json_success else 'FAILED'}")
        
        if json_success:
            # Leer JSON
            read_json = json_handler.read_data(str(json_file))
            print(f"   JSON Read: {len(read_json)} rows, {len(read_json.columns)} columns")
            
        # 3. Manejador Parquet
        parquet_config = FormatConfig(
            format_type='parquet',
            compression='snappy'
        )
        
        parquet_handler = FormatHandlerFactory.create_handler(parquet_config)
        
        # Escribir Parquet
        parquet_file = temp_dir / 'test_data.parquet'
        parquet_success = parquet_handler.write_data(test_data, str(parquet_file))
        print(f"🗜️  Parquet Handler: Write {'SUCCESS' if parquet_success else 'FAILED'}")
        
        if parquet_success:
            # Leer Parquet
            read_parquet = parquet_handler.read_data(str(parquet_file))
            print(f"   Parquet Read: {len(read_parquet)} rows, {len(read_parquet.columns)} columns")
            
        # 4. Auto-detección de formato
        detected_csv = FormatHandlerFactory.detect_format('data.csv')
        detected_parquet = FormatHandlerFactory.detect_format('data.parquet')
        detected_json = FormatHandlerFactory.detect_format('data.json')
        
        print(f"🔍 Format detection: CSV={detected_csv}, Parquet={detected_parquet}, JSON={detected_json}")
        
        # 5. Factory desde archivo
        auto_handler = FormatHandlerFactory.create_from_file(str(csv_file))
        print(f"🏭 Auto handler: {type(auto_handler).__name__}")
        
        logger.info("Format handlers demo completed")
        
    except Exception as e:
        logger.error(f"Format handlers demo failed: {str(e)}")
        raise

def demo_complete_integration():
    """
    Demostrar integración completa del sistema.
    AWS Equivalent: Complete AWS data platform workflow
    """
    logger = setup_structured_logging("demo.integration")
    
    try:
        print("🎯 Complete Integration Demo")
        print(f"   AWS Equivalent: End-to-end data platform")
        
        # Obtener suite completa de portabilidad
        portability_suite = get_portability_suite('local')
        
        print(f"📦 Portability suite loaded:")
        print(f"   Components: {len(portability_suite)}")
        print(f"   AWS Equivalent: {portability_suite['aws_equivalent']}")
        
        # Configurar entorno
        config_manager = portability_suite['config_manager']
        env_manager = portability_suite['environment_manager']
        
        # Cargar configuración actual
        current_config = config_manager.load_config()
        print(f"⚙️  Current environment: {current_config.environment}")
        
        # Validar entorno
        env_validation = env_manager.validate_environment(current_config.environment)
        print(f"✅ Environment validation: {'PASSED' if env_validation['is_valid'] else 'FAILED'}")
        
        # Crear pipeline de datos portable
        print(f"🔄 Creating portable data pipeline...")
        
        # 1. Configurar conector de datos
        data_connector_factory = portability_suite['data_connector_factory']
        
        # Crear conector basado en configuración
        local_config = ConnectionConfig(
            connection_type='local_file',
            path='./temp_integration'
        )
        
        connector = data_connector_factory.create_connector(local_config)
        
        # 2. Configurar adaptador de almacenamiento
        storage_factory = portability_suite['storage_adapter_factory']
        
        storage_config = StorageConfig(
            storage_type='local',
            base_path='./temp_integration'
        )
        
        storage = storage_factory.create_adapter(storage_config)
        
        # 3. Configurar manejador de formato
        format_factory = portability_suite['format_handler_factory']
        
        format_config = FormatConfig(
            format_type='csv',
            has_header=True
        )
        
        format_handler = format_factory.create_handler(format_config)
        
        # 4. Ejecutar pipeline portable
        print(f"🚀 Executing portable pipeline...")
        
        # Crear datos de prueba
        test_data = create_test_data()
        
        # Conectar y procesar datos
        if connector.connect() and storage.connect():
            
            # Escribir datos usando conector
            write_success = connector.write_data(test_data, 'pipeline_input.csv')
            print(f"   Data ingestion: {'SUCCESS' if write_success else 'FAILED'}")
            
            if write_success:
                # Leer y procesar datos
                processed_data = connector.read_data('pipeline_input.csv')
                
                # Aplicar transformación
                processed_data['processed_at'] = pd.Timestamp.now()
                processed_data['environment'] = current_config.environment
                
                # Guardar resultado usando storage adapter
                temp_file = './temp_integration/processed_output.csv'
                format_success = format_handler.write_data(processed_data, temp_file)
                
                if format_success:
                    # Subir a storage final
                    storage_success = storage.upload_file(temp_file, 'final_output.csv')
                    print(f"   Data storage: {'SUCCESS' if storage_success else 'FAILED'}")
                    
                    # Verificar resultado final
                    final_files = storage.list_files()
                    print(f"   Final files: {len(final_files)}")
                    
            connector.disconnect()
            storage.disconnect()
            
        print(f"✅ Portable pipeline completed successfully!")
        print(f"🎯 Ready for AWS migration:")
        print(f"   - Local file connector → S3 connector")
        print(f"   - Local storage → S3 bucket")
        print(f"   - CSV format → Parquet + compression")
        
        logger.info("Complete integration demo completed")
        
    except Exception as e:
        logger.error(f"Integration demo failed: {str(e)}")
        raise

def show_aws_equivalents():
    """
    Mostrar equivalencias AWS completas.
    """
    print("☁️  AWS MIGRATION EQUIVALENTS")
    print("=" * 50)
    
    equivalents = {
        "Data Connectors": {
            "POC": "LocalFileConnector, DatabaseConnector, CloudStorageConnector",
            "AWS": "AWS Glue Connectors + Data Catalog + Athena",
            "Migration": "Direct API replacement with boto3 + glue clients"
        },
        "Storage Adapters": {
            "POC": "LocalStorageAdapter, CloudStorageAdapter (simulated)",
            "AWS": "Amazon S3 + EFS + FSx for Windows",
            "Migration": "Replace local paths with S3 URIs"
        },
        "Format Handlers": {
            "POC": "CSV, JSON, Parquet handlers with pandas",
            "AWS": "AWS Glue Data Format Library + Schema Registry",
            "Migration": "Native AWS Glue job transformations"
        },
        "Config Manager": {
            "POC": "YAML/JSON configuration files",
            "AWS": "Systems Manager Parameter Store + Secrets Manager",
            "Migration": "Migrate configs to Parameter Store hierarchies"
        },
        "Environment Manager": {
            "POC": "Local environment validation and deployment",
            "AWS": "CloudFormation + CDK + CodePipeline",
            "Migration": "Infrastructure as Code templates"
        }
    }
    
    for component, details in equivalents.items():
        print(f"🔧 {component}")
        print(f"   POC: {details['POC']}")
        print(f"   AWS: {details['AWS']}")
        print(f"   Migration: {details['Migration']}")
        print()
    
    print("💰 ESTIMATED AWS COSTS (Monthly)")
    print("-" * 30)
    print("• S3 Standard (100GB): ~$23")
    print("• AWS Glue (10 DPU-hours): ~$44")
    print("• Redshift dc2.large (1 node): ~$180")
    print("• Systems Manager: ~$3")
    print("• CloudWatch: ~$15")
    print("• Lambda (1M requests): ~$2")
    print("TOTAL ESTIMATED: ~$267/month")
    print()
    
    print("⏰ MIGRATION TIMELINE")
    print("-" * 20)
    print("Week 1-2: S3 + Glue setup")
    print("Week 3-4: Redshift + analytics")
    print("Week 5-6: CI/CD + monitoring")
    print("Week 7-8: Go-live + optimization")

def create_test_data() -> pd.DataFrame:
    """
    Crear datos de prueba para las demostraciones.
    
    Returns:
        DataFrame con datos de prueba
    """
    np.random.seed(42)
    
    n_rows = 100
    
    data = {
        'customer_id': range(1, n_rows + 1),
        'customer_name': [f'Customer_{i:03d}' for i in range(1, n_rows + 1)],
        'industry': np.random.choice(['Healthcare', 'Pharma', 'Diagnostics', 'Research'], n_rows),
        'country': np.random.choice(['USA', 'Germany', 'Spain', 'France', 'UK'], n_rows),
        'annual_revenue': np.random.uniform(100000, 10000000, n_rows).round(2),
        'contract_start': pd.date_range('2020-01-01', periods=n_rows, freq='3D'),
        'is_active': np.random.choice([True, False], n_rows, p=[0.8, 0.2]),
        'risk_score': np.random.uniform(0.1, 1.0, n_rows).round(3)
    }
    
    return pd.DataFrame(data)

def cleanup_temp_directories():
    """Limpiar directorios temporales creados durante la demo"""
    import shutil
    
    temp_dirs = [
        './temp_data', './temp_storage', './temp_formats', 
        './temp_integration', './config', './environments'
    ]
    
    for temp_dir in temp_dirs:
        if Path(temp_dir).exists():
            try:
                shutil.rmtree(temp_dir)
                print(f"🧹 Cleaned up: {temp_dir}")
            except Exception as e:
                print(f"⚠️  Could not clean {temp_dir}: {e}")

if __name__ == "__main__":
    print("🚀 Starting Werfen Portability System Demo...")
    print()
    
    try:
        success = main()
        
        if success:
            print()
            print("🎉 DEMO COMPLETED SUCCESSFULLY!")
            print("The portability system is ready for production use")
            print("and AWS migration following documented equivalences.")
        else:
            print("❌ Demo completed with errors")
            
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
        
    except Exception as e:
        print(f"\n💥 Demo failed with error: {str(e)}")
        
    finally:
        print("\n🧹 Cleaning up temporary files...")
        cleanup_temp_directories()
        print("✅ Cleanup completed")
        
    print("\nThank you for exploring the Werfen Portability System! 🔄") 