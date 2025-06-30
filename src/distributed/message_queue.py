"""
Werfen Message Queue - Distributed Messaging System
=================================================

Este módulo implementa un sistema de mensajería distribuida,
simulando funcionalidades de AWS SQS y SNS para el POC.

AWS Equivalencias:
- Amazon SQS: Para colas de mensajes confiables
- Amazon SNS: Para pub/sub messaging
- Amazon EventBridge: Para event routing
- AWS Lambda: Para event processing
"""

import time
import uuid
import json
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Union
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

class MessageStatus(Enum):
    """Estados de los mensajes."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    DEAD_LETTER = "dead_letter"

class QueueType(Enum):
    """Tipos de colas."""
    STANDARD = "standard"      # SQS Standard
    FIFO = "fifo"             # SQS FIFO
    PRIORITY = "priority"      # Custom priority queue
    TOPIC = "topic"           # SNS Topic
    EVENT_BUS = "event_bus"   # EventBridge

@dataclass
class MessageConfig:
    """Configuración para el sistema de mensajería."""
    max_queue_size: int = 1000
    message_ttl: int = 3600  # TTL en segundos
    visibility_timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 5
    enable_dead_letter: bool = True
    dead_letter_max_size: int = 100
    batch_size: int = 10
    
    def __post_init__(self):
        """Validar configuración."""
        if self.max_queue_size < 1:
            raise ValueError("max_queue_size debe ser mayor a 0")
        if self.message_ttl < 1:
            raise ValueError("message_ttl debe ser mayor a 0")

@dataclass
class QueueMessage:
    """Representa un mensaje en la cola."""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    body: Union[str, Dict[str, Any]] = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    status: MessageStatus = MessageStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    processing_started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    priority: int = 0  # Para priority queues (0 = baja, 9 = alta)
    visibility_timeout_until: Optional[datetime] = None
    processor_id: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir mensaje a diccionario."""
        return {
            "message_id": self.message_id,
            "body": self.body,
            "attributes": self.attributes,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "processing_started_at": self.processing_started_at.isoformat() if self.processing_started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "retry_count": self.retry_count,
            "priority": self.priority,
            "processor_id": self.processor_id,
            "error_message": self.error_message
        }
    
    def is_expired(self, ttl_seconds: int) -> bool:
        """Verificar si el mensaje ha expirado."""
        return (datetime.now() - self.created_at).total_seconds() > ttl_seconds
    
    def is_visible(self) -> bool:
        """Verificar si el mensaje es visible (no en timeout)."""
        if self.visibility_timeout_until is None:
            return True
        return datetime.now() > self.visibility_timeout_until

@dataclass
class QueueStats:
    """Estadísticas de una cola."""
    total_messages: int = 0
    pending_messages: int = 0
    processing_messages: int = 0
    completed_messages: int = 0
    failed_messages: int = 0
    dead_letter_messages: int = 0
    average_processing_time: float = 0.0
    throughput_per_minute: float = 0.0
    last_processed: Optional[datetime] = None

class MessageQueue:
    """
    Cola de mensajes individual.
    
    AWS Equivalente: Una cola SQS individual
    """
    
    def __init__(self, 
                 queue_name: str, 
                 queue_type: QueueType = QueueType.STANDARD,
                 config: MessageConfig = None):
        self.queue_name = queue_name
        self.queue_type = queue_type
        self.config = config or MessageConfig()
        
        # Colas de mensajes
        self.messages: deque = deque()
        self.processing_messages: Dict[str, QueueMessage] = {}
        self.dead_letter_queue: deque = deque()
        
        # Lock para thread safety
        self.lock = threading.Lock()
        
        # Estadísticas
        self.stats = QueueStats()
        
        # Subscribers para topic queues
        self.subscribers: List[Callable] = []
        
        logger.info(f"Queue created: {queue_name} [{queue_type.value}]")
    
    def send_message(self, 
                    body: Union[str, Dict[str, Any]], 
                    attributes: Dict[str, Any] = None,
                    priority: int = 0) -> str:
        """
        Enviar mensaje a la cola.
        
        AWS Equivalente: SQS SendMessage API
        """
        attributes = attributes or {}
        
        message = QueueMessage(
            body=body,
            attributes=attributes,
            priority=priority
        )
        
        with self.lock:
            # Verificar límite de cola
            if len(self.messages) >= self.config.max_queue_size:
                logger.warning(f"Queue {self.queue_name} full, dropping message")
                return None
            
            # Insertar según tipo de cola
            if self.queue_type == QueueType.PRIORITY:
                # Insertar por prioridad
                inserted = False
                for i, existing_message in enumerate(self.messages):
                    if message.priority > existing_message.priority:
                        self.messages.insert(i, message)
                        inserted = True
                        break
                if not inserted:
                    self.messages.append(message)
            else:
                # FIFO o Standard
                self.messages.append(message)
            
            self.stats.total_messages += 1
            self.stats.pending_messages += 1
        
        # Si es un topic, notificar subscribers
        if self.queue_type == QueueType.TOPIC:
            self._notify_subscribers(message)
        
        logger.debug(f"Message sent to {self.queue_name}: {message.message_id}")
        return message.message_id
    
    def receive_messages(self, max_messages: int = 1) -> List[QueueMessage]:
        """
        Recibir mensajes de la cola.
        
        AWS Equivalente: SQS ReceiveMessage API
        """
        received_messages = []
        current_time = datetime.now()
        
        with self.lock:
            messages_to_process = []
            
            # Buscar mensajes disponibles
            for _ in range(min(max_messages, len(self.messages))):
                if not self.messages:
                    break
                
                message = self.messages.popleft()
                
                # Verificar si ha expirado
                if message.is_expired(self.config.message_ttl):
                    message.status = MessageStatus.EXPIRED
                    logger.debug(f"Message expired: {message.message_id}")
                    continue
                
                # Verificar visibilidad
                if not message.is_visible():
                    self.messages.appendleft(message)  # Devolver a la cola
                    continue
                
                messages_to_process.append(message)
            
            # Procesar mensajes seleccionados
            for message in messages_to_process:
                message.status = MessageStatus.PROCESSING
                message.processing_started_at = current_time
                message.visibility_timeout_until = current_time + timedelta(
                    seconds=self.config.visibility_timeout
                )
                
                # Mover a processing
                self.processing_messages[message.message_id] = message
                received_messages.append(message)
                
                self.stats.pending_messages -= 1
                self.stats.processing_messages += 1
        
        if received_messages:
            logger.debug(f"Received {len(received_messages)} messages from {self.queue_name}")
        
        return received_messages
    
    def delete_message(self, message_id: str) -> bool:
        """
        Eliminar mensaje procesado exitosamente.
        
        AWS Equivalente: SQS DeleteMessage API
        """
        with self.lock:
            message = self.processing_messages.pop(message_id, None)
            if message:
                message.status = MessageStatus.COMPLETED
                message.completed_at = datetime.now()
                
                self.stats.processing_messages -= 1
                self.stats.completed_messages += 1
                self.stats.last_processed = datetime.now()
                
                # Actualizar tiempo promedio de procesamiento
                if message.processing_started_at:
                    processing_time = (message.completed_at - message.processing_started_at).total_seconds()
                    self._update_average_processing_time(processing_time)
                
                logger.debug(f"Message deleted: {message_id}")
                return True
        
        return False
    
    def fail_message(self, message_id: str, error_message: str = None):
        """Marcar mensaje como fallido y manejar retry."""
        with self.lock:
            message = self.processing_messages.get(message_id)
            if not message:
                return
            
            message.retry_count += 1
            message.error_message = error_message
            message.visibility_timeout_until = None
            
            if message.retry_count <= self.config.retry_attempts:
                # Retry: devolver a la cola con delay
                message.status = MessageStatus.PENDING
                message.processing_started_at = None
                message.processor_id = None
                
                # Simular delay de retry
                time.sleep(self.config.retry_delay)
                self.messages.append(message)
                
                self.processing_messages.pop(message_id)
                self.stats.processing_messages -= 1
                self.stats.pending_messages += 1
                
                logger.info(f"Message retry {message.retry_count}/{self.config.retry_attempts}: {message_id}")
            else:
                # Max retries alcanzado, mover a dead letter
                message.status = MessageStatus.DEAD_LETTER
                
                if self.config.enable_dead_letter:
                    self.dead_letter_queue.append(message)
                    
                    # Limitar tamaño de dead letter queue
                    while len(self.dead_letter_queue) > self.config.dead_letter_max_size:
                        self.dead_letter_queue.popleft()
                    
                    self.stats.dead_letter_messages += 1
                
                self.processing_messages.pop(message_id)
                self.stats.processing_messages -= 1
                self.stats.failed_messages += 1
                
                logger.warning(f"Message moved to dead letter: {message_id}")
    
    def _notify_subscribers(self, message: QueueMessage):
        """Notificar subscribers para topic queues."""
        for subscriber in self.subscribers:
            try:
                subscriber(message)
            except Exception as e:
                logger.error(f"Subscriber notification error: {e}")
    
    def subscribe(self, callback: Callable):
        """Suscribirse a un topic queue."""
        if self.queue_type != QueueType.TOPIC:
            raise ValueError("Subscribe only available for TOPIC queues")
        
        self.subscribers.append(callback)
        logger.info(f"New subscriber added to topic {self.queue_name}")
    
    def _update_average_processing_time(self, processing_time: float):
        """Actualizar tiempo promedio de procesamiento."""
        current_avg = self.stats.average_processing_time
        completed_count = self.stats.completed_messages
        
        if completed_count == 1:
            self.stats.average_processing_time = processing_time
        else:
            new_avg = (current_avg * (completed_count - 1) + processing_time) / completed_count
            self.stats.average_processing_time = new_avg
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de la cola."""
        with self.lock:
            return {
                "queue_name": self.queue_name,
                "queue_type": self.queue_type.value,
                "total_messages": self.stats.total_messages,
                "pending_messages": self.stats.pending_messages,
                "processing_messages": self.stats.processing_messages,
                "completed_messages": self.stats.completed_messages,
                "failed_messages": self.stats.failed_messages,
                "dead_letter_messages": self.stats.dead_letter_messages,
                "average_processing_time": self.stats.average_processing_time,
                "last_processed": self.stats.last_processed.isoformat() if self.stats.last_processed else None,
                "queue_size": len(self.messages),
                "dead_letter_size": len(self.dead_letter_queue)
            }

class WerfenMessageQueue:
    """
    Sistema de mensajería distribuida principal.
    
    Simula funcionalidades de AWS SQS, SNS y EventBridge
    para comunicación eficiente entre componentes distribuidos.
    
    AWS Equivalencias:
    - SQS Service: queue management
    - SNS Service: topic/subscription management
    - EventBridge: event routing
    - CloudWatch: metrics and monitoring
    """
    
    def __init__(self, config: MessageConfig = None):
        self.config = config or MessageConfig()
        self.queues: Dict[str, MessageQueue] = {}
        self.lock = threading.Lock()
        
        # Event routing patterns
        self.event_patterns: Dict[str, List[str]] = {}
        
        # Cleanup thread
        self.cleanup_thread = None
        self.is_running = False
        
        logger.info("MessageQueue system initialized")
    
    def start_service(self):
        """Iniciar servicio de mensajería."""
        self.is_running = True
        
        # Iniciar thread de limpieza
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        logger.info("MessageQueue service started")
    
    def stop_service(self):
        """Detener servicio de mensajería."""
        self.is_running = False
        
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        
        logger.info("MessageQueue service stopped")
    
    def create_queue(self, 
                    queue_name: str, 
                    queue_type: QueueType = QueueType.STANDARD) -> bool:
        """
        Crear nueva cola.
        
        AWS Equivalente: SQS CreateQueue API
        """
        with self.lock:
            if queue_name in self.queues:
                logger.warning(f"Queue already exists: {queue_name}")
                return False
            
            self.queues[queue_name] = MessageQueue(queue_name, queue_type, self.config)
            
        logger.info(f"Queue created: {queue_name} [{queue_type.value}]")
        return True
    
    def delete_queue(self, queue_name: str) -> bool:
        """
        Eliminar cola.
        
        AWS Equivalente: SQS DeleteQueue API
        """
        with self.lock:
            if queue_name not in self.queues:
                return False
            
            del self.queues[queue_name]
        
        logger.info(f"Queue deleted: {queue_name}")
        return True
    
    def send_message(self, 
                    queue_name: str, 
                    body: Union[str, Dict[str, Any]], 
                    attributes: Dict[str, Any] = None,
                    priority: int = 0) -> Optional[str]:
        """Enviar mensaje a cola específica."""
        queue = self.queues.get(queue_name)
        if not queue:
            logger.error(f"Queue not found: {queue_name}")
            return None
        
        return queue.send_message(body, attributes, priority)
    
    def receive_messages(self, 
                        queue_name: str, 
                        max_messages: int = 1) -> List[QueueMessage]:
        """Recibir mensajes de cola específica."""
        queue = self.queues.get(queue_name)
        if not queue:
            logger.error(f"Queue not found: {queue_name}")
            return []
        
        return queue.receive_messages(max_messages)
    
    def delete_message(self, queue_name: str, message_id: str) -> bool:
        """Eliminar mensaje de cola específica."""
        queue = self.queues.get(queue_name)
        if not queue:
            return False
        
        return queue.delete_message(message_id)
    
    def fail_message(self, queue_name: str, message_id: str, error_message: str = None):
        """Marcar mensaje como fallido en cola específica."""
        queue = self.queues.get(queue_name)
        if queue:
            queue.fail_message(message_id, error_message)
    
    def publish_to_topic(self, 
                        topic_name: str, 
                        message: Union[str, Dict[str, Any]], 
                        attributes: Dict[str, Any] = None) -> str:
        """
        Publicar mensaje a topic (SNS-like).
        
        AWS Equivalente: SNS Publish API
        """
        # Crear topic si no existe
        if topic_name not in self.queues:
            self.create_queue(topic_name, QueueType.TOPIC)
        
        return self.send_message(topic_name, message, attributes)
    
    def subscribe_to_topic(self, topic_name: str, callback: Callable):
        """
        Suscribirse a un topic.
        
        AWS Equivalente: SNS Subscribe API
        """
        queue = self.queues.get(topic_name)
        if not queue:
            logger.error(f"Topic not found: {topic_name}")
            return
        
        queue.subscribe(callback)
    
    def create_event_pattern(self, pattern_name: str, source_queues: List[str]):
        """
        Crear patrón de eventos para routing.
        
        AWS Equivalente: EventBridge Rules
        """
        self.event_patterns[pattern_name] = source_queues
        logger.info(f"Event pattern created: {pattern_name} -> {source_queues}")
    
    def send_batch_messages(self, 
                           queue_name: str, 
                           messages: List[Dict[str, Any]]) -> List[str]:
        """
        Enviar múltiples mensajes en batch.
        
        AWS Equivalente: SQS SendMessageBatch API
        """
        message_ids = []
        
        for msg_data in messages:
            message_id = self.send_message(
                queue_name,
                msg_data.get("body"),
                msg_data.get("attributes"),
                msg_data.get("priority", 0)
            )
            if message_id:
                message_ids.append(message_id)
        
        logger.info(f"Batch sent to {queue_name}: {len(message_ids)} messages")
        return message_ids
    
    def get_queue_stats(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """Obtener estadísticas de cola específica."""
        queue = self.queues.get(queue_name)
        return queue.get_stats() if queue else None
    
    def get_all_queue_stats(self) -> Dict[str, Dict[str, Any]]:
        """Obtener estadísticas de todas las colas."""
        stats = {}
        with self.lock:
            for queue_name, queue in self.queues.items():
                stats[queue_name] = queue.get_stats()
        return stats
    
    def _cleanup_loop(self):
        """Loop de limpieza para mensajes expirados."""
        logger.info("Cleanup loop started")
        
        while self.is_running:
            try:
                self._cleanup_expired_messages()
                time.sleep(60)  # Limpiar cada minuto
                
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
        
        logger.info("Cleanup loop stopped")
    
    def _cleanup_expired_messages(self):
        """Limpiar mensajes expirados de todas las colas."""
        current_time = datetime.now()
        
        with self.lock:
            for queue in self.queues.values():
                with queue.lock:
                    # Limpiar mensajes expirados en cola principal
                    expired_messages = []
                    remaining_messages = deque()
                    
                    while queue.messages:
                        message = queue.messages.popleft()
                        if message.is_expired(self.config.message_ttl):
                            expired_messages.append(message)
                        else:
                            remaining_messages.append(message)
                    
                    queue.messages = remaining_messages
                    
                    if expired_messages:
                        logger.info(f"Cleaned {len(expired_messages)} expired messages from {queue.queue_name}")
    
    def __enter__(self):
        """Context manager entry."""
        self.start_service()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_service()

# Funciones de utilidad para casos comunes

def create_data_pipeline_queue(queue_name: str = "data-pipeline") -> MessageQueue:
    """
    Crear cola optimizada para pipeline de datos.
    
    AWS Equivalente: SQS Queue for data processing
    """
    config = MessageConfig(
        max_queue_size=5000,
        message_ttl=7200,  # 2 horas
        retry_attempts=3,
        batch_size=25
    )
    
    return MessageQueue(queue_name, QueueType.STANDARD, config)

def create_high_priority_queue(queue_name: str = "priority-tasks") -> MessageQueue:
    """
    Crear cola de alta prioridad.
    
    AWS Equivalente: SQS FIFO Queue with priority
    """
    config = MessageConfig(
        max_queue_size=1000,
        message_ttl=3600,  # 1 hora
        retry_attempts=5,
        visibility_timeout=60
    )
    
    return MessageQueue(queue_name, QueueType.PRIORITY, config) 