"""
Módulo de Seguridad - Werfen Data Pipeline
==========================================

Sistema de gestión segura de credenciales y configuración de seguridad enterprise.

Funcionalidades:
- Gestión encriptada de credenciales
- Configuración de políticas de seguridad
- Integración con AWS Secrets Manager
- Compliance y auditoría

Autor: Lead Software Architect - Werfen Data Team
Fecha: Enero 2025
"""

from .credential_manager import WerfenCredentialManager, get_credential_manager
from .security_config import SecurityConfig, SecurityLevel, get_security_config

__all__ = [
    'WerfenCredentialManager',
    'get_credential_manager',
    'SecurityConfig',
    'SecurityLevel',
    'get_security_config'
]

# Versión del módulo de seguridad
__version__ = "1.0.0"

# Metadatos del módulo
__author__ = "Werfen Data Team"
__email__ = "data-team@werfen.com"
__description__ = "Sistema de gestión segura de credenciales enterprise" 