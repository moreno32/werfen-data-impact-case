"""
Werfen Worker Manager - Distributed Worker Management
===================================================

Este módulo implementa la gestión de workers distribuidos,
simulando funcionalidades de AWS ECS y EKS para el POC.

AWS Equivalencias:
- Amazon ECS: Para gestión de contenedores
- Amazon EKS: Para clusters Kubernetes
- AWS Auto Scaling Groups: Para scaling automático
- AWS Systems Manager: Para health checks
"""

import time
import uuid
import threading
import psutil
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
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

class WorkerStatus(Enum):
    """Estados de los workers."""
    INITIALIZING = "initializing"
    HEALTHY = "healthy"
    BUSY = "busy"
    UNHEALTHY = "unhealthy"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"

class WorkerType(Enum):
    """Tipos de workers."""
    CPU_OPTIMIZED = "cpu_optimized"
    MEMORY_OPTIMIZED = "memory_optimized"
    GPU_ENABLED = "gpu_enabled"
    GENERAL_PURPOSE = "general_purpose"

@dataclass
class WorkerConfig:
    """Configuración para el gestor de workers."""
    max_workers: int = 4
    min_workers: int = 1
    heartbeat_interval: int = 10
    health_check_timeout: int = 5
    auto_scale: bool = True
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.2
    worker_type: WorkerType = WorkerType.GENERAL_PURPOSE
    resource_limits: Dict[str, int] = field(default_factory=lambda: {
        "max_cpu_percent": 80,
        "max_memory_mb": 1024,
        "max_tasks": 10
    })
    
    def __post_init__(self):
        """Validar configuración."""
        if self.max_workers < self.min_workers:
            raise ValueError("max_workers debe ser mayor o igual a min_workers")
        if self.heartbeat_interval < 1:
            raise ValueError("heartbeat_interval debe ser mayor a 0")

@dataclass
class WorkerNode:
    """Representa un nodo worker."""
    worker_id: str = field(default_factory=lambda: f"worker-{uuid.uuid4().hex[:8]}")
    status: WorkerStatus = WorkerStatus.INITIALIZING
    worker_type: WorkerType = WorkerType.GENERAL_PURPOSE
    created_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    last_health_check: datetime = field(default_factory=datetime.now)
    current_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    uptime: float = 0.0
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, any]:
        """Convertir worker a diccionario."""
        return {
            "worker_id": self.worker_id,
            "status": self.status.value,
            "worker_type": self.worker_type.value,
            "created_at": self.created_at.isoformat(),
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "last_health_check": self.last_health_check.isoformat(),
            "current_tasks": self.current_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "cpu_usage": self.cpu_usage,
            "memory_usage": self.memory_usage,
            "uptime": self.uptime,
            "errors": self.errors[-10:],  # Solo últimos 10 errores
            "metadata": self.metadata
        }

@dataclass
class ClusterMetrics:
    """Métricas del cluster de workers."""
    total_workers: int = 0
    healthy_workers: int = 0
    busy_workers: int = 0
    unhealthy_workers: int = 0
    total_tasks_running: int = 0
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    cluster_cpu_usage: float = 0.0
    cluster_memory_usage: float = 0.0
    cluster_uptime: float = 0.0
    last_scale_event: Optional[datetime] = None
    scale_events: int = 0

class WerfenWorkerManager:
    """
    Gestor de workers para arquitectura distribuida.
    
    Simula funcionalidades de AWS ECS y EKS para gestión
    eficiente de workers distribuidos.
    
    AWS Equivalencias:
    - ECS Cluster: self.cluster_name
    - ECS Service: worker management logic
    - Auto Scaling Group: auto scaling functionality
    - CloudWatch: metrics and monitoring
    """
    
    def __init__(self, config: WorkerConfig, cluster_name: str = "werfen-cluster"):
        self.config = config
        self.cluster_name = cluster_name
        self.workers: Dict[str, WorkerNode] = {}
        self.is_running = False
        self.lock = threading.Lock()
        
        # Threads para gestión
        self.management_thread = None
        self.health_check_thread = None
        self.metrics_thread = None
        
        # Métricas del cluster
        self.cluster_metrics = ClusterMetrics()
        
        # Callbacks para eventos
        self.event_callbacks: Dict[str, List[Callable]] = {
            "worker_added": [],
            "worker_removed": [],
            "worker_unhealthy": [],
            "scale_event": []
        }
        
        logger.info(f"WorkerManager initialized for cluster: {cluster_name}")
        logger.info(f"Worker limits: {config.min_workers}-{config.max_workers} workers")
    
    def start_cluster(self):
        """
        Iniciar el cluster de workers.
        
        AWS Equivalente: ECS Service start
        """
        if self.is_running:
            logger.warning("Cluster already running")
            return
        
        self.is_running = True
        
        # Inicializar workers mínimos
        for _ in range(self.config.min_workers):
            self._add_worker()
        
        # Iniciar threads de gestión
        self.management_thread = threading.Thread(target=self._management_loop, daemon=True)
        self.health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.metrics_thread = threading.Thread(target=self._metrics_loop, daemon=True)
        
        self.management_thread.start()
        self.health_check_thread.start()
        self.metrics_thread.start()
        
        logger.info(f"Cluster {self.cluster_name} started with {len(self.workers)} workers")
    
    def stop_cluster(self):
        """
        Detener el cluster de workers.
        
        AWS Equivalente: ECS Service stop
        """
        if not self.is_running:
            return
        
        self.is_running = False
        
        # Detener todos los workers
        with self.lock:
            for worker in self.workers.values():
                worker.status = WorkerStatus.STOPPING
        
        # Esperar a que terminen los threads
        if self.management_thread:
            self.management_thread.join(timeout=5)
        if self.health_check_thread:
            self.health_check_thread.join(timeout=5)
        if self.metrics_thread:
            self.metrics_thread.join(timeout=5)
        
        # Marcar workers como detenidos
        with self.lock:
            for worker in self.workers.values():
                worker.status = WorkerStatus.STOPPED
        
        logger.info(f"Cluster {self.cluster_name} stopped")
    
    def _add_worker(self, worker_type: WorkerType = None) -> str:
        """
        Agregar nuevo worker al cluster.
        
        AWS Equivalente: ECS Task launch
        """
        worker_type = worker_type or self.config.worker_type
        
        worker = WorkerNode(
            worker_type=worker_type,
            status=WorkerStatus.INITIALIZING
        )
        
        with self.lock:
            self.workers[worker.worker_id] = worker
        
        # Simular inicialización
        time.sleep(0.5)
        worker.status = WorkerStatus.HEALTHY
        worker.last_heartbeat = datetime.now()
        
        # Ejecutar callbacks
        self._trigger_event("worker_added", worker)
        
        logger.info(f"Worker added: {worker.worker_id} [{worker_type.value}]")
        return worker.worker_id
    
    def _remove_worker(self, worker_id: str):
        """
        Remover worker del cluster.
        
        AWS Equivalente: ECS Task stop
        """
        with self.lock:
            worker = self.workers.get(worker_id)
            if worker:
                worker.status = WorkerStatus.STOPPING
                # Simular tiempo de parada
                time.sleep(0.2)
                worker.status = WorkerStatus.STOPPED
                self.workers.pop(worker_id)
                
                # Ejecutar callbacks
                self._trigger_event("worker_removed", worker)
                
                logger.info(f"Worker removed: {worker_id}")
    
    def _management_loop(self):
        """Loop principal de gestión del cluster."""
        logger.info("Worker management loop started")
        
        while self.is_running:
            try:
                if self.config.auto_scale:
                    self._check_auto_scaling()
                
                time.sleep(self.config.heartbeat_interval)
                
            except Exception as e:
                logger.error(f"Management loop error: {e}")
        
        logger.info("Worker management loop stopped")
    
    def _health_check_loop(self):
        """Loop de health checks de workers."""
        logger.info("Health check loop started")
        
        while self.is_running:
            try:
                self._perform_health_checks()
                time.sleep(self.config.health_check_timeout)
                
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
        
        logger.info("Health check loop stopped")
    
    def _metrics_loop(self):
        """Loop de recolección de métricas."""
        logger.info("Metrics collection loop started")
        
        while self.is_running:
            try:
                self._update_cluster_metrics()
                time.sleep(30)  # Actualizar métricas cada 30 segundos
                
            except Exception as e:
                logger.error(f"Metrics loop error: {e}")
        
        logger.info("Metrics collection loop stopped")
    
    def _check_auto_scaling(self):
        """
        Verificar si se necesita auto scaling.
        
        AWS Equivalente: Auto Scaling Group scaling policies
        """
        with self.lock:
            healthy_workers = [w for w in self.workers.values() 
                             if w.status == WorkerStatus.HEALTHY]
            busy_workers = [w for w in self.workers.values() 
                          if w.status == WorkerStatus.BUSY]
            
            total_workers = len(self.workers)
            utilization = len(busy_workers) / max(len(healthy_workers), 1)
        
        # Scale up si utilización alta
        if (utilization > self.config.scale_up_threshold and 
            total_workers < self.config.max_workers):
            
            self._add_worker()
            self.cluster_metrics.scale_events += 1
            self.cluster_metrics.last_scale_event = datetime.now()
            self._trigger_event("scale_event", {"type": "scale_up", "utilization": utilization})
            
            logger.info(f"Auto-scaled up: {total_workers} -> {len(self.workers)} workers")
        
        # Scale down si utilización baja
        elif (utilization < self.config.scale_down_threshold and 
              total_workers > self.config.min_workers):
            
            # Encontrar worker menos utilizado
            least_busy_worker = min(healthy_workers, 
                                  key=lambda w: w.current_tasks, 
                                  default=None)
            
            if least_busy_worker and least_busy_worker.current_tasks == 0:
                self._remove_worker(least_busy_worker.worker_id)
                self.cluster_metrics.scale_events += 1
                self.cluster_metrics.last_scale_event = datetime.now()
                self._trigger_event("scale_event", {"type": "scale_down", "utilization": utilization})
                
                logger.info(f"Auto-scaled down: {total_workers} -> {len(self.workers)} workers")
    
    def _perform_health_checks(self):
        """Realizar health checks en todos los workers."""
        current_time = datetime.now()
        timeout_threshold = timedelta(seconds=self.config.health_check_timeout * 3)
        
        with self.lock:
            for worker in self.workers.values():
                if worker.status in [WorkerStatus.STOPPING, WorkerStatus.STOPPED]:
                    continue
                
                # Simular health check
                worker.last_health_check = current_time
                
                # Verificar timeout de heartbeat
                if current_time - worker.last_heartbeat > timeout_threshold:
                    if worker.status != WorkerStatus.UNHEALTHY:
                        worker.status = WorkerStatus.UNHEALTHY
                        worker.errors.append(f"Heartbeat timeout at {current_time}")
                        self._trigger_event("worker_unhealthy", worker)
                        logger.warning(f"Worker unhealthy: {worker.worker_id}")
                else:
                    # Simular métricas de recursos
                    try:
                        worker.cpu_usage = psutil.cpu_percent(interval=0.1)
                        worker.memory_usage = psutil.virtual_memory().percent
                        worker.uptime = (current_time - worker.created_at).total_seconds()
                        
                        # Verificar límites de recursos
                        if (worker.cpu_usage > self.config.resource_limits["max_cpu_percent"] or
                            worker.memory_usage > self.config.resource_limits.get("max_memory_percent", 90)):
                            
                            if worker.status == WorkerStatus.HEALTHY:
                                worker.status = WorkerStatus.BUSY
                        else:
                            if worker.status == WorkerStatus.BUSY and worker.current_tasks == 0:
                                worker.status = WorkerStatus.HEALTHY
                        
                        # Actualizar heartbeat (simular que el worker responde)
                        worker.last_heartbeat = current_time
                        
                    except Exception as e:
                        worker.errors.append(f"Health check error: {str(e)}")
    
    def _update_cluster_metrics(self):
        """Actualizar métricas del cluster."""
        with self.lock:
            workers_list = list(self.workers.values())
        
        # Contadores por estado
        status_counts = {}
        for status in WorkerStatus:
            status_counts[status] = len([w for w in workers_list if w.status == status])
        
        # Métricas agregadas
        total_cpu = sum(w.cpu_usage for w in workers_list) / max(len(workers_list), 1)
        total_memory = sum(w.memory_usage for w in workers_list) / max(len(workers_list), 1)
        total_uptime = sum(w.uptime for w in workers_list) / max(len(workers_list), 1)
        
        # Actualizar métricas del cluster
        self.cluster_metrics.total_workers = len(workers_list)
        self.cluster_metrics.healthy_workers = status_counts[WorkerStatus.HEALTHY]
        self.cluster_metrics.busy_workers = status_counts[WorkerStatus.BUSY]
        self.cluster_metrics.unhealthy_workers = status_counts[WorkerStatus.UNHEALTHY]
        self.cluster_metrics.total_tasks_running = sum(w.current_tasks for w in workers_list)
        self.cluster_metrics.total_tasks_completed = sum(w.completed_tasks for w in workers_list)
        self.cluster_metrics.total_tasks_failed = sum(w.failed_tasks for w in workers_list)
        self.cluster_metrics.cluster_cpu_usage = total_cpu
        self.cluster_metrics.cluster_memory_usage = total_memory
        self.cluster_metrics.cluster_uptime = total_uptime
    
    def _trigger_event(self, event_type: str, data: any):
        """Ejecutar callbacks para eventos."""
        callbacks = self.event_callbacks.get(event_type, [])
        for callback in callbacks:
            try:
                callback(data)
            except Exception as e:
                logger.error(f"Event callback error [{event_type}]: {e}")
    
    def register_event_callback(self, event_type: str, callback: Callable):
        """Registrar callback para eventos del cluster."""
        if event_type not in self.event_callbacks:
            self.event_callbacks[event_type] = []
        self.event_callbacks[event_type].append(callback)
        logger.info(f"Event callback registered for: {event_type}")
    
    def get_worker_info(self, worker_id: str) -> Optional[Dict[str, any]]:
        """Obtener información de un worker específico."""
        with self.lock:
            worker = self.workers.get(worker_id)
            return worker.to_dict() if worker else None
    
    def get_cluster_status(self) -> Dict[str, any]:
        """
        Obtener estado completo del cluster.
        
        AWS Equivalente: ECS Cluster describe
        """
        with self.lock:
            workers_info = [worker.to_dict() for worker in self.workers.values()]
        
        return {
            "cluster_name": self.cluster_name,
            "is_running": self.is_running,
            "workers": workers_info,
            "metrics": {
                "total_workers": self.cluster_metrics.total_workers,
                "healthy_workers": self.cluster_metrics.healthy_workers,
                "busy_workers": self.cluster_metrics.busy_workers,
                "unhealthy_workers": self.cluster_metrics.unhealthy_workers,
                "total_tasks_running": self.cluster_metrics.total_tasks_running,
                "total_tasks_completed": self.cluster_metrics.total_tasks_completed,
                "total_tasks_failed": self.cluster_metrics.total_tasks_failed,
                "cluster_cpu_usage": self.cluster_metrics.cluster_cpu_usage,
                "cluster_memory_usage": self.cluster_metrics.cluster_memory_usage,
                "cluster_uptime": self.cluster_metrics.cluster_uptime,
                "last_scale_event": self.cluster_metrics.last_scale_event.isoformat() if self.cluster_metrics.last_scale_event else None,
                "scale_events": self.cluster_metrics.scale_events
            },
            "configuration": {
                "min_workers": self.config.min_workers,
                "max_workers": self.config.max_workers,
                "auto_scale": self.config.auto_scale,
                "worker_type": self.config.worker_type.value,
                "resource_limits": self.config.resource_limits
            }
        }
    
    def get_healthy_workers(self) -> List[str]:
        """Obtener lista de workers saludables."""
        with self.lock:
            return [worker_id for worker_id, worker in self.workers.items()
                   if worker.status == WorkerStatus.HEALTHY]
    
    def assign_task_to_worker(self, worker_id: str) -> bool:
        """
        Asignar una tarea a un worker específico.
        
        AWS Equivalente: ECS Task placement
        """
        with self.lock:
            worker = self.workers.get(worker_id)
            if worker and worker.status == WorkerStatus.HEALTHY:
                worker.current_tasks += 1
                if worker.current_tasks > 0:
                    worker.status = WorkerStatus.BUSY
                return True
        return False
    
    def complete_task_on_worker(self, worker_id: str, success: bool = True):
        """Marcar tarea como completada en un worker."""
        with self.lock:
            worker = self.workers.get(worker_id)
            if worker:
                if worker.current_tasks > 0:
                    worker.current_tasks -= 1
                
                if success:
                    worker.completed_tasks += 1
                else:
                    worker.failed_tasks += 1
                
                # Cambiar estado si no hay más tareas
                if worker.current_tasks == 0 and worker.status == WorkerStatus.BUSY:
                    worker.status = WorkerStatus.HEALTHY
    
    def scale_cluster(self, target_workers: int):
        """
        Escalar cluster manualmente a un número específico de workers.
        
        AWS Equivalente: ECS Service update desired count
        """
        if not self.is_running:
            logger.error("Cannot scale cluster: not running")
            return
        
        if target_workers < self.config.min_workers:
            target_workers = self.config.min_workers
        elif target_workers > self.config.max_workers:
            target_workers = self.config.max_workers
        
        current_workers = len(self.workers)
        
        if target_workers > current_workers:
            # Scale up
            for _ in range(target_workers - current_workers):
                self._add_worker()
            logger.info(f"Manually scaled up to {target_workers} workers")
        
        elif target_workers < current_workers:
            # Scale down
            healthy_workers = [w for w in self.workers.values() 
                             if w.status == WorkerStatus.HEALTHY and w.current_tasks == 0]
            
            workers_to_remove = min(current_workers - target_workers, len(healthy_workers))
            
            for i in range(workers_to_remove):
                self._remove_worker(healthy_workers[i].worker_id)
            
            logger.info(f"Manually scaled down to {len(self.workers)} workers")
    
    def __enter__(self):
        """Context manager entry."""
        self.start_cluster()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_cluster()

# Funciones de utilidad para configuraciones específicas

def create_cpu_optimized_config(max_workers: int = 8) -> WorkerConfig:
    """
    Crear configuración optimizada para CPU.
    
    AWS Equivalente: ECS Task Definition con CPU optimized instances
    """
    return WorkerConfig(
        max_workers=max_workers,
        worker_type=WorkerType.CPU_OPTIMIZED,
        resource_limits={
            "max_cpu_percent": 90,
            "max_memory_mb": 512,
            "max_tasks": 5
        },
        auto_scale=True,
        scale_up_threshold=0.7,
        scale_down_threshold=0.3
    )

def create_memory_optimized_config(max_workers: int = 4) -> WorkerConfig:
    """
    Crear configuración optimizada para memoria.
    
    AWS Equivalente: ECS Task Definition con memory optimized instances
    """
    return WorkerConfig(
        max_workers=max_workers,
        worker_type=WorkerType.MEMORY_OPTIMIZED,
        resource_limits={
            "max_cpu_percent": 70,
            "max_memory_mb": 2048,
            "max_tasks": 3
        },
        auto_scale=True,
        scale_up_threshold=0.8,
        scale_down_threshold=0.2
    ) 