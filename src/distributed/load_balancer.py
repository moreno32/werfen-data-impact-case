"""
Werfen Load Balancer - Distributed Load Balancing
===============================================

Este módulo implementa balanceado de carga distribuido,
simulando funcionalidades de AWS ALB y NLB para el POC.

AWS Equivalencias:
- Application Load Balancer (ALB): Para HTTP/HTTPS
- Network Load Balancer (NLB): Para TCP/UDP
- Target Groups: Para agrupación de targets
- Health Checks: Para verificación de estado
"""

import time
import threading
import random
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Tuple
from enum import Enum
from datetime import datetime, timedelta
import logging

# Configurar logging estructurado
try:
    from src.logging.structured_logger import setup_structured_logging
    logger = setup_structured_logging(__name__)
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

class LoadBalancingAlgorithm(Enum):
    """Algoritmos de balanceado de carga."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    RANDOM = "random"
    IP_HASH = "ip_hash"
    HEALTH_BASED = "health_based"

class TargetStatus(Enum):
    """Estados de los targets."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"
    UNAVAILABLE = "unavailable"
    MAINTENANCE = "maintenance"

class RequestType(Enum):
    """Tipos de requests."""
    HTTP = "http"
    HTTPS = "https"
    TCP = "tcp"
    UDP = "udp"
    CUSTOM = "custom"

@dataclass
class LoadBalancerConfig:
    """Configuración del balanceador de carga."""
    algorithm: LoadBalancingAlgorithm = LoadBalancingAlgorithm.ROUND_ROBIN
    health_check_interval: int = 30
    health_check_timeout: int = 5
    health_check_retries: int = 3
    max_retries: int = 3
    retry_delay: float = 0.5
    enable_sticky_sessions: bool = False
    session_timeout: int = 3600
    connection_timeout: int = 30
    
    def __post_init__(self):
        """Validar configuración."""
        if self.health_check_interval < 1:
            raise ValueError("health_check_interval debe ser mayor a 0")
        if self.max_retries < 0:
            raise ValueError("max_retries debe ser mayor o igual a 0")

@dataclass
class Target:
    """Representa un target/backend."""
    target_id: str
    address: str
    port: int
    weight: int = 1
    status: TargetStatus = TargetStatus.HEALTHY
    created_at: datetime = field(default_factory=datetime.now)
    last_health_check: datetime = field(default_factory=datetime.now)
    health_check_failures: int = 0
    current_connections: int = 0
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    average_response_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir target a diccionario."""
        return {
            "target_id": self.target_id,
            "address": self.address,
            "port": self.port,
            "weight": self.weight,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "last_health_check": self.last_health_check.isoformat(),
            "health_check_failures": self.health_check_failures,
            "current_connections": self.current_connections,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "average_response_time": self.average_response_time,
            "metadata": self.metadata
        }

@dataclass
class Request:
    """Representa una request."""
    request_id: str
    client_id: str
    request_type: RequestType = RequestType.HTTP
    path: str = "/"
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    created_at: datetime = field(default_factory=datetime.now)
    processed_by: Optional[str] = None
    response_time: Optional[float] = None
    status_code: Optional[int] = None
    
@dataclass
class TargetGroup:
    """Grupo de targets."""
    group_name: str
    targets: Dict[str, Target] = field(default_factory=dict)
    algorithm: LoadBalancingAlgorithm = LoadBalancingAlgorithm.ROUND_ROBIN
    health_check_path: str = "/health"
    health_check_interval: int = 30
    created_at: datetime = field(default_factory=datetime.now)
    
    # Para round robin
    _current_target_index: int = 0
    
    def add_target(self, target: Target):
        """Agregar target al grupo."""
        self.targets[target.target_id] = target
        logger.info(f"Target added to group {self.group_name}: {target.target_id}")
    
    def remove_target(self, target_id: str) -> bool:
        """Remover target del grupo."""
        if target_id in self.targets:
            del self.targets[target_id]
            logger.info(f"Target removed from group {self.group_name}: {target_id}")
            return True
        return False
    
    def get_healthy_targets(self) -> List[Target]:
        """Obtener targets saludables."""
        return [target for target in self.targets.values() 
                if target.status == TargetStatus.HEALTHY]
    
    def select_target(self, request: Request = None) -> Optional[Target]:
        """Seleccionar target según algoritmo."""
        healthy_targets = self.get_healthy_targets()
        if not healthy_targets:
            return None
        
        if self.algorithm == LoadBalancingAlgorithm.ROUND_ROBIN:
            return self._round_robin_selection(healthy_targets)
        elif self.algorithm == LoadBalancingAlgorithm.LEAST_CONNECTIONS:
            return self._least_connections_selection(healthy_targets)
        elif self.algorithm == LoadBalancingAlgorithm.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin_selection(healthy_targets)
        elif self.algorithm == LoadBalancingAlgorithm.RANDOM:
            return random.choice(healthy_targets)
        elif self.algorithm == LoadBalancingAlgorithm.IP_HASH:
            return self._ip_hash_selection(healthy_targets, request)
        elif self.algorithm == LoadBalancingAlgorithm.HEALTH_BASED:
            return self._health_based_selection(healthy_targets)
        else:
            return healthy_targets[0]  # Fallback
    
    def _round_robin_selection(self, targets: List[Target]) -> Target:
        """Selección round robin."""
        if not targets:
            return None
        
        target = targets[self._current_target_index % len(targets)]
        self._current_target_index += 1
        return target
    
    def _least_connections_selection(self, targets: List[Target]) -> Target:
        """Selección por menor número de conexiones."""
        return min(targets, key=lambda t: t.current_connections)
    
    def _weighted_round_robin_selection(self, targets: List[Target]) -> Target:
        """Selección round robin ponderada."""
        # Crear lista expandida por peso
        weighted_targets = []
        for target in targets:
            weighted_targets.extend([target] * target.weight)
        
        if not weighted_targets:
            return None
        
        target = weighted_targets[self._current_target_index % len(weighted_targets)]
        self._current_target_index += 1
        return target
    
    def _ip_hash_selection(self, targets: List[Target], request: Request) -> Target:
        """Selección basada en hash de IP cliente."""
        if not request or not request.client_id:
            return random.choice(targets)
        
        # Usar hash de client_id para consistencia
        hash_value = hash(request.client_id)
        index = hash_value % len(targets)
        return targets[index]
    
    def _health_based_selection(self, targets: List[Target]) -> Target:
        """Selección basada en salud y performance."""
        # Sort by number of failures and response time
        sorted_targets = sorted(targets, 
                              key=lambda t: (t.health_check_failures, t.average_response_time))
        return sorted_targets[0]

class WerfenLoadBalancer:
    """
    Balanceador de carga distribuido.
    
    Simula funcionalidades de AWS ALB y NLB para distribución
    eficiente de tráfico entre múltiples targets.
    
    AWS Equivalencias:
    - ALB/NLB: Load balancing functionality
    - Target Groups: target group management
    - Health Checks: health monitoring
    - CloudWatch: metrics collection
    """
    
    def __init__(self, config: LoadBalancerConfig):
        self.config = config
        self.target_groups: Dict[str, TargetGroup] = {}
        self.is_running = False
        self.lock = threading.Lock()
        
        # Management threads
        self.health_check_thread = None
        self.metrics_thread = None
        
        # Sticky sessions if enabled
        self.session_store: Dict[str, Tuple[str, datetime]] = {}
        
        # Global metrics
        self.global_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "average_response_time": 0.0,
            "requests_per_minute": 0.0,
            "active_connections": 0,
            "healthy_targets": 0,
            "unhealthy_targets": 0
        }
        
        # Historial de requests para throughput
        self.request_history: deque = deque(maxlen=1000)
        
        logger.info(f"LoadBalancer initialized with algorithm: {config.algorithm.value}")
    
    def start(self):
        """Iniciar balanceador de carga."""
        if self.is_running:
            logger.warning("Load balancer already running")
            return
        
        self.is_running = True
        
        # Iniciar threads de monitoreo
        self.health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.metrics_thread = threading.Thread(target=self._metrics_loop, daemon=True)
        
        self.health_check_thread.start()
        self.metrics_thread.start()
        
        logger.info("Load balancer started")
    
    def stop(self):
        """Detener balanceador de carga."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # Esperar threads
        if self.health_check_thread:
            self.health_check_thread.join(timeout=5)
        if self.metrics_thread:
            self.metrics_thread.join(timeout=5)
        
        logger.info("Load balancer stopped")
    
    def create_target_group(self, 
                           group_name: str, 
                           algorithm: LoadBalancingAlgorithm = None,
                           health_check_path: str = "/health") -> bool:
        """
        Crear grupo de targets.
        
        AWS Equivalente: Create Target Group
        """
        algorithm = algorithm or self.config.algorithm
        
        with self.lock:
            if group_name in self.target_groups:
                logger.warning(f"Target group already exists: {group_name}")
                return False
            
            self.target_groups[group_name] = TargetGroup(
                group_name=group_name,
                algorithm=algorithm,
                health_check_path=health_check_path,
                health_check_interval=self.config.health_check_interval
            )
        
        logger.info(f"Target group created: {group_name} [{algorithm.value}]")
        return True
    
    def delete_target_group(self, group_name: str) -> bool:
        """Eliminar grupo de targets."""
        with self.lock:
            if group_name not in self.target_groups:
                return False
            
            del self.target_groups[group_name]
        
        logger.info(f"Target group deleted: {group_name}")
        return True
    
    def register_target(self, 
                       group_name: str, 
                       target_id: str, 
                       address: str, 
                       port: int,
                       weight: int = 1) -> bool:
        """
        Registrar target en grupo.
        
        AWS Equivalente: Register Targets
        """
        target_group = self.target_groups.get(group_name)
        if not target_group:
            logger.error(f"Target group not found: {group_name}")
            return False
        
        target = Target(
            target_id=target_id,
            address=address,
            port=port,
            weight=weight
        )
        
        target_group.add_target(target)
        logger.info(f"Target registered: {target_id} -> {group_name}")
        return True
    
    def deregister_target(self, group_name: str, target_id: str) -> bool:
        """
        Desregistrar target de grupo.
        
        AWS Equivalente: Deregister Targets
        """
        target_group = self.target_groups.get(group_name)
        if not target_group:
            return False
        
        return target_group.remove_target(target_id)
    
    def route_request(self, 
                     group_name: str, 
                     request: Request) -> Optional[Tuple[Target, Any]]:
        """
        Rutear request a target apropiado.
        
        AWS Equivalente: Request routing
        """
        target_group = self.target_groups.get(group_name)
        if not target_group:
            logger.error(f"Target group not found: {group_name}")
            return None
        
        # Verificar sticky sessions
        if self.config.enable_sticky_sessions:
            sticky_target = self._get_sticky_session_target(request.client_id, target_group)
            if sticky_target:
                return self._process_request(sticky_target, request)
        
        # Seleccionar target
        target = target_group.select_target(request)
        if not target:
            logger.warning(f"No healthy targets available in group: {group_name}")
            return None
        
        # Procesar request con retries
        for attempt in range(self.config.max_retries + 1):
            try:
                result = self._process_request(target, request)
                
                # Establecer sticky session si está habilitado
                if self.config.enable_sticky_sessions:
                    self._set_sticky_session(request.client_id, target.target_id)
                
                return result
                
            except Exception as e:
                logger.warning(f"Request failed on target {target.target_id}, attempt {attempt + 1}: {e}")
                
                # Marcar target como fallido temporalmente
                target.failed_requests += 1
                
                if attempt < self.config.max_retries:
                    # Intentar con otro target
                    target = target_group.select_target(request)
                    if not target:
                        break
                    time.sleep(self.config.retry_delay)
                else:
                    # Máximo de retries alcanzado
                    self.global_metrics["failed_requests"] += 1
                    return None
        
        return None
    
    def _process_request(self, target: Target, request: Request) -> Tuple[Target, Any]:
        """Procesar request en target específico."""
        start_time = time.time()
        
        # Incrementar conexiones activas
        target.current_connections += 1
        target.total_requests += 1
        self.global_metrics["total_requests"] += 1
        self.global_metrics["active_connections"] += 1
        
        try:
            # Simular procesamiento de request
            # En un caso real, aquí se haría la llamada HTTP/TCP real
            processing_time = random.uniform(0.01, 0.5)  # Simular tiempo de procesamiento
            time.sleep(processing_time)
            
            # Simular respuesta exitosa (90% de éxito)
            if random.random() < 0.9:
                response_time = time.time() - start_time
                
                # Actualizar métricas del target
                target.successful_requests += 1
                target.current_connections -= 1
                self._update_target_response_time(target, response_time)
                
                # Actualizar métricas globales
                self.global_metrics["successful_requests"] += 1
                self.global_metrics["active_connections"] -= 1
                self._update_global_response_time(response_time)
                
                # Guardar en historial
                self.request_history.append({
                    "timestamp": datetime.now(),
                    "target_id": target.target_id,
                    "response_time": response_time,
                    "success": True
                })
                
                request.processed_by = target.target_id
                request.response_time = response_time
                request.status_code = 200
                
                logger.debug(f"Request processed successfully: {request.request_id} -> {target.target_id}")
                return target, {"status": "success", "response_time": response_time}
            
            else:
                # Simular error
                raise Exception("Simulated processing error")
                
        except Exception as e:
            # Request falló
            target.failed_requests += 1
            target.current_connections -= 1
            self.global_metrics["failed_requests"] += 1
            self.global_metrics["active_connections"] -= 1
            
            # Guardar en historial
            self.request_history.append({
                "timestamp": datetime.now(),
                "target_id": target.target_id,
                "response_time": time.time() - start_time,
                "success": False,
                "error": str(e)
            })
            
            raise e
    
    def _get_sticky_session_target(self, client_id: str, target_group: TargetGroup) -> Optional[Target]:
        """Obtener target de sticky session."""
        if client_id in self.session_store:
            target_id, created_at = self.session_store[client_id]
            
            # Verificar si la sesión ha expirado
            if (datetime.now() - created_at).total_seconds() > self.config.session_timeout:
                del self.session_store[client_id]
                return None
            
            # Verificar si el target sigue saludable
            target = target_group.targets.get(target_id)
            if target and target.status == TargetStatus.HEALTHY:
                return target
            else:
                # Target no disponible, remover sesión
                del self.session_store[client_id]
        
        return None
    
    def _set_sticky_session(self, client_id: str, target_id: str):
        """Establecer sticky session."""
        self.session_store[client_id] = (target_id, datetime.now())
    
    def _update_target_response_time(self, target: Target, response_time: float):
        """Actualizar tiempo promedio de respuesta del target."""
        if target.successful_requests == 1:
            target.average_response_time = response_time
        else:
            current_avg = target.average_response_time
            total_requests = target.successful_requests
            new_avg = (current_avg * (total_requests - 1) + response_time) / total_requests
            target.average_response_time = new_avg
    
    def _update_global_response_time(self, response_time: float):
        """Actualizar tiempo promedio de respuesta global."""
        current_avg = self.global_metrics["average_response_time"]
        total_successful = self.global_metrics["successful_requests"]
        
        if total_successful == 1:
            self.global_metrics["average_response_time"] = response_time
        else:
            new_avg = (current_avg * (total_successful - 1) + response_time) / total_successful
            self.global_metrics["average_response_time"] = new_avg
    
    def _health_check_loop(self):
        """Loop de health checks."""
        logger.info("Health check loop started")
        
        while self.is_running:
            try:
                self._perform_health_checks()
                time.sleep(self.config.health_check_interval)
                
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
        
        logger.info("Health check loop stopped")
    
    def _perform_health_checks(self):
        """Realizar health checks en todos los targets."""
        current_time = datetime.now()
        
        with self.lock:
            for target_group in self.target_groups.values():
                for target in target_group.targets.values():
                    # Simular health check
                    try:
                        # En un caso real, aquí se haría HTTP GET al health check path
                        # o verificación TCP/UDP según el tipo
                        health_check_success = random.random() > 0.05  # 95% éxito
                        
                        if health_check_success:
                            if target.status == TargetStatus.UNHEALTHY:
                                # Target se recuperó
                                target.status = TargetStatus.HEALTHY
                                target.health_check_failures = 0
                                logger.info(f"Target recovered: {target.target_id}")
                        else:
                            target.health_check_failures += 1
                            if target.health_check_failures >= self.config.health_check_retries:
                                if target.status == TargetStatus.HEALTHY:
                                    target.status = TargetStatus.UNHEALTHY
                                    logger.warning(f"Target marked unhealthy: {target.target_id}")
                        
                        target.last_health_check = current_time
                        
                    except Exception as e:
                        target.health_check_failures += 1
                        logger.error(f"Health check error for target {target.target_id}: {e}")
    
    def _metrics_loop(self):
        """Loop de recolección de métricas."""
        logger.info("Metrics collection loop started")
        
        while self.is_running:
            try:
                self._update_global_metrics()
                time.sleep(60)  # Actualizar cada minuto
                
            except Exception as e:
                logger.error(f"Metrics loop error: {e}")
        
        logger.info("Metrics collection loop stopped")
    
    def _update_global_metrics(self):
        """Actualizar métricas globales."""
        with self.lock:
            healthy_count = 0
            unhealthy_count = 0
            
            for target_group in self.target_groups.values():
                for target in target_group.targets.values():
                    if target.status == TargetStatus.HEALTHY:
                        healthy_count += 1
                    else:
                        unhealthy_count += 1
            
            self.global_metrics["healthy_targets"] = healthy_count
            self.global_metrics["unhealthy_targets"] = unhealthy_count
            
            # Calcular requests por minuto basado en historial reciente
            current_time = datetime.now()
            recent_requests = [
                req for req in self.request_history
                if (current_time - req["timestamp"]).total_seconds() <= 60
            ]
            self.global_metrics["requests_per_minute"] = len(recent_requests)
    
    def get_target_group_status(self, group_name: str) -> Optional[Dict[str, Any]]:
        """Obtener estado de un grupo de targets."""
        target_group = self.target_groups.get(group_name)
        if not target_group:
            return None
        
        targets_info = []
        for target in target_group.targets.values():
            targets_info.append(target.to_dict())
        
        return {
            "group_name": group_name,
            "algorithm": target_group.algorithm.value,
            "health_check_path": target_group.health_check_path,
            "created_at": target_group.created_at.isoformat(),
            "targets": targets_info,
            "healthy_targets": len(target_group.get_healthy_targets()),
            "total_targets": len(target_group.targets)
        }
    
    def get_global_metrics(self) -> Dict[str, Any]:
        """Obtener métricas globales."""
        with self.lock:
            metrics = self.global_metrics.copy()
            metrics.update({
                "target_groups": len(self.target_groups),
                "sticky_sessions": len(self.session_store) if self.config.enable_sticky_sessions else 0,
                "recent_requests": len(self.request_history)
            })
        
        return metrics
    
    def get_load_balancer_status(self) -> Dict[str, Any]:
        """Obtener estado completo del balanceador."""
        target_groups_info = {}
        for group_name in self.target_groups.keys():
            target_groups_info[group_name] = self.get_target_group_status(group_name)
        
        return {
            "is_running": self.is_running,
            "algorithm": self.config.algorithm.value,
            "sticky_sessions_enabled": self.config.enable_sticky_sessions,
            "target_groups": target_groups_info,
            "global_metrics": self.get_global_metrics(),
            "configuration": {
                "health_check_interval": self.config.health_check_interval,
                "health_check_timeout": self.config.health_check_timeout,
                "max_retries": self.config.max_retries,
                "connection_timeout": self.config.connection_timeout
            }
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()

# Funciones de utilidad

def create_web_load_balancer_config() -> LoadBalancerConfig:
    """
    Crear configuración para web load balancer.
    
    AWS Equivalente: ALB configuration
    """
    return LoadBalancerConfig(
        algorithm=LoadBalancingAlgorithm.LEAST_CONNECTIONS,
        health_check_interval=30,
        max_retries=2,
        enable_sticky_sessions=True,
        session_timeout=1800
    )

def create_api_load_balancer_config() -> LoadBalancerConfig:
    """
    Crear configuración para API load balancer.
    
    AWS Equivalente: ALB for API Gateway
    """
    return LoadBalancerConfig(
        algorithm=LoadBalancingAlgorithm.ROUND_ROBIN,
        health_check_interval=15,
        max_retries=3,
        enable_sticky_sessions=False,
        connection_timeout=10
    ) 