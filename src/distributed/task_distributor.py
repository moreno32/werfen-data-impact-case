"""
Werfen Task Distributor - Distributed Task Management
===================================================

Este módulo implementa la distribución de tareas de manera distribuida,
simulando funcionalidades de AWS Batch y ECS para el POC.

AWS Equivalencias:
- AWS Batch: Para jobs de procesamiento batch
- Amazon ECS: Para contenedores distribuidos
- AWS Lambda: Para funciones serverless
- AWS Step Functions: Para workflows complejos
"""

import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Callable, Union
from enum import Enum
import logging
from datetime import datetime, timedelta

# Configurar logging estructurado
try:
    from src.logging.structured_logger import setup_structured_logging
    logger = setup_structured_logging(__name__)
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    """Estados de las tareas distribuidas."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRY = "retry"

class TaskPriority(Enum):
    """Prioridades de las tareas."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

@dataclass
class TaskConfig:
    """Configuración para el distribuidor de tareas."""
    max_workers: int = 4
    batch_size: int = 10
    timeout_seconds: int = 300
    retry_attempts: int = 3
    retry_delay: int = 5
    enable_batching: bool = True
    enable_priority_queue: bool = True
    
    def __post_init__(self):
        """Validar configuración."""
        if self.max_workers < 1:
            raise ValueError("max_workers debe ser mayor a 0")
        if self.batch_size < 1:
            raise ValueError("batch_size debe ser mayor a 0")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds debe ser mayor a 0")

@dataclass
class Task:
    """Representa una tarea distribuida."""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "unnamed_task"
    function: Optional[Callable] = None
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0
    worker_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir task a diccionario para serialización."""
        return {
            "task_id": self.task_id,
            "name": self.name,
            "priority": self.priority.value,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "retry_count": self.retry_count,
            "worker_id": self.worker_id,
            "metadata": self.metadata,
            "error": self.error
        }

class WerfenTaskDistributor:
    """
    Distribuidor de tareas para arquitectura distribuida.
    
    Simula funcionalidades de AWS Batch y ECS para distribución
    eficiente de tareas entre múltiples workers.
    
    AWS Equivalencias:
    - AWS Batch Job Queue: self.task_queue
    - ECS Task Definition: Task class
    - ECS Service: worker management
    - CloudWatch Logs: logging integration
    """
    
    def __init__(self, config: TaskConfig):
        self.config = config
        self.task_queue: List[Task] = []
        self.running_tasks: Dict[str, Task] = {}
        self.completed_tasks: Dict[str, Task] = {}
        self.failed_tasks: Dict[str, Task] = {}
        
        # Thread pool para workers
        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self.is_running = False
        self.lock = threading.Lock()
        
        # Métricas
        self.metrics = {
            "tasks_submitted": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_retried": 0,
            "average_execution_time": 0.0,
            "queue_size": 0,
            "active_workers": 0
        }
        
        logger.info(f"TaskDistributor initialized with {config.max_workers} workers")
    
    def submit_task(self, 
                   name: str,
                   function: Callable,
                   args: tuple = (),
                   kwargs: dict = None,
                   priority: TaskPriority = TaskPriority.NORMAL,
                   metadata: dict = None) -> str:
        """
        Enviar tarea para ejecución distribuida.
        
        Args:
            name: Nombre de la tarea
            function: Función a ejecutar
            args: Argumentos posicionales
            kwargs: Argumentos con nombre
            priority: Prioridad de la tarea
            metadata: Metadatos adicionales
            
        Returns:
            ID de la tarea
        """
        kwargs = kwargs or {}
        metadata = metadata or {}
        
        task = Task(
            name=name,
            function=function,
            args=args,
            kwargs=kwargs,
            priority=priority,
            metadata=metadata
        )
        
        with self.lock:
            self.task_queue.append(task)
            if self.config.enable_priority_queue:
                # Ordenar por prioridad (más alta primero)
                self.task_queue.sort(key=lambda t: t.priority.value, reverse=True)
            
            self.metrics["tasks_submitted"] += 1
            self.metrics["queue_size"] = len(self.task_queue)
        
        logger.info(f"Task submitted: {task.name} [{task.task_id}] - Priority: {priority.name}")
        return task.task_id
    
    def submit_batch_tasks(self, 
                          tasks: List[Dict[str, Any]]) -> List[str]:
        """
        Enviar múltiples tareas en batch.
        
        AWS Equivalente: AWS Batch Array Jobs
        
        Args:
            tasks: Lista de diccionarios con configuración de tareas
            
        Returns:
            Lista de IDs de tareas
        """
        task_ids = []
        
        for task_def in tasks:
            task_id = self.submit_task(
                name=task_def.get("name", "batch_task"),
                function=task_def["function"],
                args=task_def.get("args", ()),
                kwargs=task_def.get("kwargs", {}),
                priority=TaskPriority(task_def.get("priority", 2)),
                metadata=task_def.get("metadata", {})
            )
            task_ids.append(task_id)
        
        logger.info(f"Batch submitted: {len(tasks)} tasks")
        return task_ids
    
    def start_processing(self):
        """Iniciar procesamiento de tareas."""
        self.is_running = True
        logger.info("Task processing started")
        
        # Iniciar workers
        for i in range(self.config.max_workers):
            self.executor.submit(self._worker_loop, f"worker-{i}")
    
    def stop_processing(self):
        """Detener procesamiento de tareas."""
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info("Task processing stopped")
    
    def _worker_loop(self, worker_id: str):
        """Loop principal del worker."""
        logger.info(f"Worker {worker_id} started")
        
        while self.is_running:
            try:
                task = self._get_next_task()
                if task:
                    self._execute_task(task, worker_id)
                else:
                    # No hay tareas, esperar un poco
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
        
        logger.info(f"Worker {worker_id} stopped")
    
    def _get_next_task(self) -> Optional[Task]:
        """Obtener siguiente tarea de la cola."""
        with self.lock:
            if self.task_queue:
                task = self.task_queue.pop(0)
                self.running_tasks[task.task_id] = task
                self.metrics["queue_size"] = len(self.task_queue)
                return task
            return None
    
    def _execute_task(self, task: Task, worker_id: str):
        """
        Ejecutar una tarea específica.
        
        AWS Equivalente: ECS Task execution
        """
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        task.worker_id = worker_id
        
        try:
            logger.info(f"Executing task {task.name} [{task.task_id}] on {worker_id}")
            
            # Ejecutar la función con timeout
            result = task.function(*task.args, **task.kwargs)
            
            # Tarea completada exitosamente
            task.result = result
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            
            # Mover a completadas
            with self.lock:
                self.running_tasks.pop(task.task_id, None)
                self.completed_tasks[task.task_id] = task
                self.metrics["tasks_completed"] += 1
                self._update_execution_time_metric(task)
            
            logger.info(f"Task completed: {task.name} [{task.task_id}]")
            
        except Exception as e:
            # Tarea falló
            task.error = str(e)
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now()
            
            logger.error(f"Task failed: {task.name} [{task.task_id}] - Error: {e}")
            
            # Intentar retry si es posible
            if task.retry_count < self.config.retry_attempts:
                self._retry_task(task)
            else:
                # Mover a fallidas
                with self.lock:
                    self.running_tasks.pop(task.task_id, None)
                    self.failed_tasks[task.task_id] = task
                    self.metrics["tasks_failed"] += 1
    
    def _retry_task(self, task: Task):
        """Reintentar tarea fallida."""
        task.retry_count += 1
        task.status = TaskStatus.RETRY
        task.error = None
        task.started_at = None
        task.completed_at = None
        
        # Esperar antes del retry
        time.sleep(self.config.retry_delay)
        
        with self.lock:
            self.running_tasks.pop(task.task_id, None)
            self.task_queue.append(task)
            self.metrics["tasks_retried"] += 1
        
        logger.info(f"Task retry {task.retry_count}/{self.config.retry_attempts}: {task.name} [{task.task_id}]")
    
    def _update_execution_time_metric(self, task: Task):
        """Actualizar métrica de tiempo de ejecución."""
        if task.started_at and task.completed_at:
            execution_time = (task.completed_at - task.started_at).total_seconds()
            current_avg = self.metrics["average_execution_time"]
            completed_count = self.metrics["tasks_completed"]
            
            # Calcular nuevo promedio
            new_avg = (current_avg * (completed_count - 1) + execution_time) / completed_count
            self.metrics["average_execution_time"] = new_avg
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Obtener estado de una tarea."""
        # Buscar en todas las colas
        for task_dict in [self.running_tasks, self.completed_tasks, self.failed_tasks]:
            if task_id in task_dict:
                return task_dict[task_id].status
        
        # Buscar en cola pendiente
        with self.lock:
            for task in self.task_queue:
                if task.task_id == task_id:
                    return task.status
        
        return None
    
    def get_task_result(self, task_id: str) -> Any:
        """Obtener resultado de una tarea completada."""
        if task_id in self.completed_tasks:
            return self.completed_tasks[task_id].result
        return None
    
    def get_task_error(self, task_id: str) -> Optional[str]:
        """Obtener error de una tarea fallida."""
        if task_id in self.failed_tasks:
            return self.failed_tasks[task_id].error
        return None
    
    def wait_for_completion(self, task_ids: List[str], timeout: int = None) -> Dict[str, TaskStatus]:
        """
        Esperar a que se completen las tareas especificadas.
        
        Args:
            task_ids: IDs de tareas a esperar
            timeout: Timeout en segundos
            
        Returns:
            Dict con el estado final de cada tarea
        """
        start_time = time.time()
        timeout = timeout or self.config.timeout_seconds
        
        while True:
            statuses = {}
            all_done = True
            
            for task_id in task_ids:
                status = self.get_task_status(task_id)
                statuses[task_id] = status
                
                if status in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.RETRY]:
                    all_done = False
            
            if all_done:
                return statuses
            
            if time.time() - start_time > timeout:
                logger.warning(f"Timeout waiting for tasks completion")
                return statuses
            
            time.sleep(0.5)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Obtener métricas del distribuidor."""
        with self.lock:
            current_metrics = self.metrics.copy()
            current_metrics.update({
                "queue_size": len(self.task_queue),
                "running_tasks": len(self.running_tasks),
                "completed_tasks": len(self.completed_tasks),
                "failed_tasks": len(self.failed_tasks),
                "active_workers": self.config.max_workers if self.is_running else 0,
                "uptime": time.time()
            })
        
        return current_metrics
    
    def get_task_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Obtener historial de tareas."""
        history = []
        
        # Agregar tareas completadas
        for task in list(self.completed_tasks.values())[-limit:]:
            history.append(task.to_dict())
        
        # Agregar tareas fallidas
        for task in list(self.failed_tasks.values())[-limit:]:
            history.append(task.to_dict())
        
        # Ordenar por fecha de creación
        history.sort(key=lambda x: x["created_at"], reverse=True)
        
        return history[:limit]
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancelar una tarea pendiente."""
        with self.lock:
            # Buscar en cola pendiente
            for i, task in enumerate(self.task_queue):
                if task.task_id == task_id:
                    task.status = TaskStatus.CANCELLED
                    self.task_queue.pop(i)
                    logger.info(f"Task cancelled: {task.name} [{task_id}]")
                    return True
        
        return False
    
    def __enter__(self):
        """Context manager entry."""
        self.start_processing()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_processing()

# Funciones de utilidad para tareas comunes

def create_data_processing_task(data_path: str, 
                              processing_function: Callable,
                              output_path: str = None) -> Dict[str, Any]:
    """
    Crear tarea de procesamiento de datos.
    
    AWS Equivalente: AWS Batch Job Definition para data processing
    """
    return {
        "name": f"process_data_{data_path.split('/')[-1]}",
        "function": processing_function,
        "args": (data_path,),
        "kwargs": {"output_path": output_path} if output_path else {},
        "priority": TaskPriority.NORMAL.value,
        "metadata": {
            "type": "data_processing",
            "input_path": data_path,
            "output_path": output_path
        }
    }

def create_model_training_task(model_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crear tarea de entrenamiento de modelo.
    
    AWS Equivalente: SageMaker Training Job
    """
    return {
        "name": f"train_model_{model_config.get('name', 'unnamed')}",
        "function": lambda config: f"Training model {config['name']}",
        "args": (model_config,),
        "priority": TaskPriority.HIGH.value,
        "metadata": {
            "type": "model_training",
            "model_name": model_config.get("name"),
            "algorithm": model_config.get("algorithm")
        }
    } 