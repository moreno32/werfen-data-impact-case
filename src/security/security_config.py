"""
Configuración de Seguridad - Werfen Data Pipeline
================================================

Configuración centralizada de políticas y configuraciones de seguridad.

Autor: Lead Software Architect - Werfen Data Team
Fecha: Enero 2025
"""

import os
from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum

class SecurityLevel(Enum):
    """Niveles de seguridad para diferentes entornos"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

@dataclass
class SecurityConfig:
    """Configuración de seguridad para el pipeline"""
    
    # Configuración general
    security_level: SecurityLevel = SecurityLevel.DEVELOPMENT
    enable_encryption: bool = True
    enable_audit_logging: bool = True
    
    # Configuración de contraseñas
    min_password_length: int = 12
    require_special_chars: bool = True
    password_expiry_days: int = 90
    
    # Configuración de acceso
    max_failed_login_attempts: int = 3
    session_timeout_minutes: int = 30
    enable_mfa: bool = False
    
    # Configuración de red
    allowed_ip_ranges: List[str] = None
    enable_ssl: bool = True
    ssl_verify: bool = True
    
    # Configuración de AWS
    aws_region: str = "us-east-1"
    enable_cloudtrail: bool = True
    enable_guardduty: bool = True
    kms_encryption: bool = True
    
    def __post_init__(self):
        if self.allowed_ip_ranges is None:
            self.allowed_ip_ranges = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]

def get_security_config() -> SecurityConfig:
    """Obtener configuración de seguridad basada en el entorno"""
    
    env = os.getenv('WERFEN_ENVIRONMENT', 'development').lower()
    
    if env == 'production':
        return SecurityConfig(
            security_level=SecurityLevel.PRODUCTION,
            enable_encryption=True,
            enable_audit_logging=True,
            min_password_length=16,
            require_special_chars=True,
            password_expiry_days=60,
            max_failed_login_attempts=3,
            session_timeout_minutes=15,
            enable_mfa=True,
            enable_ssl=True,
            ssl_verify=True,
            enable_cloudtrail=True,
            enable_guardduty=True,
            kms_encryption=True
        )
    
    elif env == 'staging':
        return SecurityConfig(
            security_level=SecurityLevel.STAGING,
            enable_encryption=True,
            enable_audit_logging=True,
            min_password_length=14,
            password_expiry_days=90,
            max_failed_login_attempts=5,
            session_timeout_minutes=30,
            enable_mfa=False,
            enable_ssl=True,
            ssl_verify=True
        )
    
    else:  # development
        return SecurityConfig(
            security_level=SecurityLevel.DEVELOPMENT,
            enable_encryption=True,
            enable_audit_logging=False,
            min_password_length=8,
            require_special_chars=False,
            password_expiry_days=365,
            max_failed_login_attempts=10,
            session_timeout_minutes=60,
            enable_mfa=False,
            enable_ssl=False,
            ssl_verify=False
        )

# Configuración de políticas de seguridad
SECURITY_POLICIES = {
    "data_classification": {
        "public": {
            "encryption_required": False,
            "access_logging": False,
            "retention_days": 30
        },
        "internal": {
            "encryption_required": True,
            "access_logging": True,
            "retention_days": 90
        },
        "confidential": {
            "encryption_required": True,
            "access_logging": True,
            "retention_days": 2555,  # 7 años
            "approval_required": True
        },
        "restricted": {
            "encryption_required": True,
            "access_logging": True,
            "retention_days": 2555,
            "approval_required": True,
            "mfa_required": True
        }
    },
    
    "network_security": {
        "allowed_protocols": ["https", "ssh", "postgres-ssl"],
        "blocked_ports": [23, 135, 139, 445],
        "firewall_rules": {
            "inbound": ["10.0.0.0/8", "172.16.0.0/12"],
            "outbound": ["0.0.0.0/0"]
        }
    },
    
    "aws_security": {
        "required_tags": ["Environment", "Owner", "Project", "DataClassification"],
        "s3_policies": {
            "block_public_access": True,
            "enable_versioning": True,
            "enable_logging": True,
            "encryption_at_rest": True
        },
        "iam_policies": {
            "enforce_mfa": True,
            "max_session_duration": 3600,
            "require_external_id": True
        }
    }
}

# Configuración de compliance
COMPLIANCE_REQUIREMENTS = {
    "gdpr": {
        "enabled": True,
        "data_retention_days": 2555,
        "right_to_erasure": True,
        "consent_required": True,
        "data_portability": True
    },
    
    "sox": {
        "enabled": True,
        "audit_trail_required": True,
        "segregation_of_duties": True,
        "change_management": True
    },
    
    "iso27001": {
        "enabled": True,
        "risk_assessment": True,
        "incident_management": True,
        "access_control": True,
        "business_continuity": True
    }
} 