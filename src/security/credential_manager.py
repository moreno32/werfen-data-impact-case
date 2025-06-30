"""
Secure Credential Management - Werfen Data Pipeline
===================================================

Enterprise credential management system implementing security best practices,
prepared for AWS Secrets Manager integration and enterprise security services.

Author: Lead Software Architect - Werfen Data Team
Date: January 2025
"""

import os
import json
import base64
from pathlib import Path
from typing import Dict, Any, Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import keyring
from dotenv import load_dotenv, set_key
import logging
from config import get_config
import hashlib

# Configure basic logging
logger = logging.getLogger(__name__)

class WerfenCredentialManager:
    """
    Secure credential manager for Werfen pipeline.
    
    Provides encryption, secure storage and credential management
    for various services (DB, APIs, AWS, etc.).
    
    Now uses unified configuration instead of environment variables.
    """
    
    def __init__(self, config_path: str = ".env"):
        """
        Initialize credential manager.
        
        Args:
            config_path: Path to configuration file (DEPRECATED - not used)
        """
        # Import unified configuration
        self.config = get_config()
        
        # Configure paths and files
        self.credentials_dir = self.config.project_root / ".credentials"
        self.credentials_dir.mkdir(exist_ok=True)
        
        self.credentials_file = self.credentials_dir / "encrypted_credentials.json"
        self.key_file = self.credentials_dir / "master.key"
        
        # Initialize encryption
        self._initialize_encryption()
        
        # Configure demo credentials if they don't exist
        if not self.credentials_file.exists():
            self.setup_demo_credentials()
    
    def _initialize_encryption(self):
        """Initialize encryption system."""
        try:
            # Try to use cryptography for robust encryption
            from cryptography.fernet import Fernet
            self.encryption_available = True
            self._use_fernet_encryption()
        except ImportError:
            # Fallback to simple password encryption
            self.encryption_available = False
            self._use_master_password_encryption()
    
    def _use_fernet_encryption(self):
        """Use Fernet encryption with a locally stored key."""
        from cryptography.fernet import Fernet
        
        # Generate or load encryption key
        if not self.key_file.exists():
            key = Fernet.generate_key()
            with open(self.key_file, "wb") as f:
                f.write(key)
        else:
            with open(self.key_file, "rb") as f:
                key = f.read()
        
        self.fernet = Fernet(key)
        print("Using robust encryption (cryptography)")
    
    def _use_master_password_encryption(self):
        """Use master password-based encryption (fallback)."""
        # Use master password from unified configuration
        master_password = self.config.master_password
        self.encryption_key = hashlib.sha256(master_password.encode()).digest()
        
        print("⚠️ Using basic encryption (install cryptography for better security)")
    
    def _encrypt_value(self, value: str) -> str:
        """Encrypt a value"""
        if not self.encryption_key:
            raise ValueError("Encryption system not initialized")
        
        if self.encryption_available:
            fernet = Fernet(self.encryption_key)
            encrypted_bytes = fernet.encrypt(value.encode())
            return base64.urlsafe_b64encode(encrypted_bytes).decode()
        else:
            # Simple encryption if Fernet is not available
            # (Not secure for production)
            encrypted_bytes = bytearray()
            for i, byte in enumerate(value.encode()):
                encrypted_bytes.append(byte ^ self.encryption_key[i % len(self.encryption_key)])
            return base64.b64encode(encrypted_bytes).decode()
    
    def _decrypt_value(self, encrypted_value: str) -> str:
        """Decrypt a value"""
        if not self.encryption_key:
            raise ValueError("Encryption system not initialized")
        
        try:
            if self.encryption_available:
                fernet = Fernet(self.encryption_key)
                encrypted_bytes = base64.urlsafe_b64decode(encrypted_value.encode())
                return fernet.decrypt(encrypted_bytes).decode()
            else:
                # Decrypt value using simple encryption
                encrypted_bytes = base64.b64decode(encrypted_value)
                decrypted_bytes = bytearray()
                for i, byte in enumerate(encrypted_bytes):
                    decrypted_bytes.append(byte ^ self.encryption_key[i % len(self.encryption_key)])
                return decrypted_bytes.decode()
        except Exception as e:
            raise ValueError(f"Error decrypting credential: {e}")
    
    def set_credential(self, key: str, value: str, encrypt: bool = True) -> None:
        """
        Save a credential to the secure store (JSON file).
        
        Args:
            key: Credential key
            value: Credential value
            encrypt: Whether to encrypt the value (True by default for sensitive credentials)
        """
        try:
            # Load existing credentials or create new dictionary
            if self.credentials_file.exists():
                with open(self.credentials_file, 'r') as f:
                    data = json.load(f)
            else:
                data = {}
            
            if encrypt:
                # Encrypt value and save
                if self.encryption_available:
                    encrypted_value = self.fernet.encrypt(value.encode()).decode()
                else:
                    encrypted_value = self._encrypt_value(value)
                
                data[f"{key}_encrypted"] = encrypted_value
                if f"{key}_plain" in data:
                    del data[f"{key}_plain"]
            else:
                # Save plain value
                data[f"{key}_plain"] = value
                if f"{key}_encrypted" in data:
                    del data[f"{key}_encrypted"]
            
            # Write all credentials back to file
            with open(self.credentials_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            print(f"❌ Error saving credential {key}: {e}")
    
    def get_credential(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a credential from the secure store.
        
        Args:
            key: Credential key
            default: Default value if not found
            
        Returns:
            Credential value or None if it doesn't exist
        """
        try:
            # First search in unified configuration
            unified_value = self.config.get_credential(key, None)
            if unified_value is not None:
                return unified_value
            
            # If not in unified configuration, search in encrypted store
            if not self.credentials_file.exists():
                return default
        
            with open(self.credentials_file, 'r') as f:
                data = json.load(f)
            
            # Search for encrypted key
            encrypted_key = f"{key}_encrypted"
            if encrypted_key in data:
                encrypted_value = data[encrypted_key]
                if self.encryption_available:
                    return self.fernet.decrypt(encrypted_value.encode()).decode()
                else:
                    return self._decrypt_value(encrypted_value)
            
            # Search for plain key (legacy)
            plain_key = f"{key}_plain"
            if plain_key in data:
                return data[plain_key]
            
            # Search for direct key
            if key in data:
                return data[key]
        
            return default
            
        except Exception as e:
            print(f"⚠️ Error getting credential {key}: {e}")
            return default
    
    def get_database_credentials(self) -> Dict[str, str]:
        """Get database credentials"""
        return {
            'host': self.get_credential('db_host', 'localhost'),
            'port': self.get_credential('db_port', '5432'),
            'database': self.get_credential('db_name', 'werfen'),
            'username': self.get_credential('db_user', 'werfen_user'),
            'password': self.get_credential('db_password', ''),
            'ssl_mode': self.get_credential('db_ssl_mode', 'prefer')
        }
    
    def get_aws_credentials(self) -> Dict[str, str]:
        """Get AWS credentials"""
        return {
            'aws_access_key_id': self.get_credential('aws_access_key_id', ''),
            'aws_secret_access_key': self.get_credential('aws_secret_access_key', ''),
            'aws_session_token': self.get_credential('aws_session_token', ''),
            'aws_region': self.get_credential('aws_region', 'us-east-1'),
            's3_bucket': self.get_credential('s3_bucket', 'werfen-data-lake'),
            's3_prefix': self.get_credential('s3_prefix', 'data/')
        }
    
    def get_api_credentials(self) -> Dict[str, str]:
        """Get external API credentials"""
        return {
            'salesforce_client_id': self.get_credential('salesforce_client_id', ''),
            'salesforce_client_secret': self.get_credential('salesforce_client_secret', ''),
            'salesforce_username': self.get_credential('salesforce_username', ''),
            'salesforce_password': self.get_credential('salesforce_password', ''),
            'sap_api_key': self.get_credential('sap_api_key', ''),
            'sap_endpoint': self.get_credential('sap_endpoint', ''),
            'werfen_api_token': self.get_credential('werfen_api_token', '')
        }
    
    def validate_credentials(self) -> Dict[str, bool]:
        """Validate that critical credentials are available"""
        validations = {}
        
        # Validate database credentials
        db_creds = self.get_database_credentials()
        validations['database'] = bool(db_creds['username'] and db_creds['password'])
        
        # Validate AWS credentials
        aws_creds = self.get_aws_credentials()
        validations['aws'] = bool(
            aws_creds['aws_access_key_id'] and 
            aws_creds['aws_secret_access_key']
        )
        
        # Validate API credentials
        api_creds = self.get_api_credentials()
        validations['salesforce'] = bool(
            api_creds['salesforce_client_id'] and 
            api_creds['salesforce_client_secret']
        )
        validations['sap'] = bool(api_creds['sap_api_key'])
        
        return validations
    
    def setup_demo_credentials(self) -> None:
        """Set up demo credentials for POC"""
        logger.info("Setting up demo credentials...")
        
        # Database credentials (demo)
        self.set_credential('db_host', 'localhost', encrypt=False)
        self.set_credential('db_port', '5432', encrypt=False)
        self.set_credential('db_name', 'werfen_demo', encrypt=False)
        self.set_credential('db_user', 'werfen_user', encrypt=False)
        self.set_credential('db_password', 'demo_password_2025', encrypt=True)
        
        # AWS credentials (demo)
        self.set_credential('aws_access_key_id', 'AKIA_DEMO_ACCESS_KEY', encrypt=True)
        self.set_credential('aws_secret_access_key', 'demo_secret_access_key_12345', encrypt=True)
        self.set_credential('aws_region', 'us-east-1', encrypt=False)
        self.set_credential('s3_bucket', 'werfen-data-lake-demo', encrypt=False)
        
        # API credentials (demo)
        self.set_credential('salesforce_client_id', 'sf_demo_client_id_12345', encrypt=True)
        self.set_credential('salesforce_client_secret', 'sf_demo_client_secret_67890', encrypt=True)
        self.set_credential('salesforce_username', 'werfen@salesforce.demo', encrypt=False)
        self.set_credential('salesforce_password', 'sf_demo_password_2025', encrypt=True)
        
        self.set_credential('sap_api_key', 'sap_demo_api_key_abcdef', encrypt=True)
        self.set_credential('sap_endpoint', 'https://sap-demo.werfen.com/api', encrypt=False)
        
        self.set_credential('werfen_api_token', 'werfen_demo_token_xyz123', encrypt=True)
        
        logger.info("Demo credentials configured successfully")
    
    def export_aws_format(self) -> Dict[str, Any]:
        """
        Export configuration in AWS Secrets Manager compatible format
        """
        aws_secrets = {
            "database": {
                "engine": "postgresql",
                "host": self.get_credential('db_host'),
                "port": int(self.get_credential('db_port', '5432')),
                "dbname": self.get_credential('db_name'),
                "username": self.get_credential('db_user'),
                "password": self.get_credential('db_password')
            },
            "aws": {
                "region": self.get_credential('aws_region'),
                "s3_bucket": self.get_credential('s3_bucket'),
                "s3_prefix": self.get_credential('s3_prefix')
            },
            "salesforce": {
                "client_id": self.get_credential('salesforce_client_id'),
                "client_secret": self.get_credential('salesforce_client_secret'),
                "username": self.get_credential('salesforce_username'),
                "password": self.get_credential('salesforce_password'),
                "endpoint": "https://login.salesforce.com"
            },
            "sap": {
                "api_key": self.get_credential('sap_api_key'),
                "endpoint": self.get_credential('sap_endpoint'),
                "version": "v1"
            }
        }
        
        return aws_secrets
    
    def get_connection_string(self, service: str) -> str:
        """Generate secure connection string for different services"""
        if service.lower() == 'database':
            creds = self.get_database_credentials()
            if creds['password']:
                return f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
            else:
                return f"postgresql://{creds['host']}:{creds['port']}/{creds['database']}"
        
        elif service.lower() == 's3':
            creds = self.get_aws_credentials()
            return f"s3://{creds['s3_bucket']}/{creds['s3_prefix']}"
        
        else:
            raise ValueError(f"Unsupported service: {service}")

def get_credential_manager() -> WerfenCredentialManager:
    """Factory function to get credential manager"""
    return WerfenCredentialManager()

# AWS Secrets Manager integration example
AWS_INTEGRATION_EXAMPLE = """
import boto3

# AWS Secrets Manager client
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')

# Get credentials from AWS Secrets Manager
def get_secret_from_aws(secret_name: str) -> dict:
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Error getting secret {secret_name}: {e}")
        return {}

# Configure credentials using AWS Secrets Manager
def setup_aws_credentials():
    db_secret = get_secret_from_aws('werfen/database/credentials')
    api_secret = get_secret_from_aws('werfen/api/credentials')
    
    # Use credentials obtained from AWS
    os.environ['WERFEN_DB_PASSWORD'] = db_secret.get('password', '')
    os.environ['WERFEN_SALESFORCE_CLIENT_SECRET'] = api_secret.get('salesforce_secret', '')

# Use IAM Roles instead of access keys
def get_aws_session_with_role():
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn='arn:aws:iam::123456789012:role/WerfenDataPipelineRole',
        RoleSessionName='werfen-pipeline-session'
    )
    
    credentials = assumed_role['Credentials']
    return boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
""" 