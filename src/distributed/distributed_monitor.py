"""
Werfen Distributed Monitor - Distributed System Monitoring
========================================================

Este módulo implementa monitoreo distribuido completo,
simulando funcionalidades de AWS CloudWatch para el POC.

AWS Equivalencias:
- Amazon CloudWatch: Para métricas y logs
- CloudWatch Alarms: Para alertas automáticas
- CloudWatch Dashboards: Para visualización
- AWS X-Ray: Para tracing distribuido
"""

import time
import threading
import json
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Callable, Any, Tuple
from enum import Enum
from datetime import datetime, timedelta
import logging
import statistics

# Configurar logging estructurado
try:
    from src.logging.structured_logger import setup_structured_logging
    logger = setup_structured_logging(__name__)
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Tipos de métricas."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    RATE = "rate"

class AlertSeverity(Enum):
    """Severidad de alertas."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertStatus(Enum):
    """Estados de alertas."""
    ACTIVE = "active"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"
    ACKNOWLEDGED = "acknowledged"

@dataclass
class MonitoringConfig:
    """Configuración del sistema de monitoreo."""
    metrics_interval: int = 60  # Intervalo de recolección en segundos
    retention_days: int = 7     # Días de retención de métricas
    log_level: str = "INFO"
    enable_alerts: bool = True
    alert_check_interval: int = 30
    max_metrics_per_series: int = 1000
    enable_tracing: bool = True
    dashboard_refresh_interval: int = 30
    
    def __post_init__(self):
        """Validar configuración."""
        if self.metrics_interval < 1:
            raise ValueError("metrics_interval debe ser mayor a 0")
        if self.retention_days < 1:
            raise ValueError("retention_days debe ser mayor a 0")

@dataclass
class MetricPoint:
    """Punto de métrica individual."""
    timestamp: datetime
    value: float
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "tags": self.tags
        }

@dataclass
class MetricSeries:
    """Serie de métricas."""
    name: str
    metric_type: MetricType
    unit: str = "count"
    description: str = ""
    points: deque = field(default_factory=lambda: deque(maxlen=1000))
    created_at: datetime = field(default_factory=datetime.now)
    
    def add_point(self, value: float, tags: Dict[str, str] = None):
        """Agregar punto a la serie."""
        point = MetricPoint(
            timestamp=datetime.now(),
            value=value,
            tags=tags or {}
        )
        self.points.append(point)
    
    def get_latest_value(self) -> Optional[float]:
        """Obtener último valor."""
        return self.points[-1].value if self.points else None
    
    def get_values_in_range(self, start: datetime, end: datetime) -> List[MetricPoint]:
        """Obtener valores en rango de tiempo."""
        return [point for point in self.points 
                if start <= point.timestamp <= end]
    
    def calculate_statistics(self, minutes: int = 5) -> Dict[str, float]:
        """Calcular estadísticas de los últimos N minutos."""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        recent_values = [point.value for point in self.points 
                        if point.timestamp >= cutoff_time]
        
        if not recent_values:
            return {}
        
        return {
            "count": len(recent_values),
            "min": min(recent_values),
            "max": max(recent_values),
            "avg": statistics.mean(recent_values),
            "median": statistics.median(recent_values),
            "sum": sum(recent_values),
            "std_dev": statistics.stdev(recent_values) if len(recent_values) > 1 else 0
        }

@dataclass
class AlertRule:
    """Regla de alerta."""
    rule_id: str
    name: str
    metric_name: str
    condition: str  # >, <, >=, <=, ==, !=
    threshold: float
    severity: AlertSeverity
    evaluation_period: int = 300  # segundos
    datapoints_to_alarm: int = 2
    description: str = ""
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    
    def evaluate(self, metric_series: MetricSeries) -> bool:
        """Evaluar si se debe disparar la alerta."""
        if not self.enabled:
            return False
        
        # Obtener puntos en el período de evaluación
        cutoff_time = datetime.now() - timedelta(seconds=self.evaluation_period)
        recent_points = [p for p in metric_series.points if p.timestamp >= cutoff_time]
        
        if len(recent_points) < self.datapoints_to_alarm:
            return False
        
        # Verificar condición en los últimos datapoints_to_alarm puntos
        trigger_count = 0
        for point in recent_points[-self.datapoints_to_alarm:]:
            if self._check_condition(point.value):
                trigger_count += 1
        
        return trigger_count >= self.datapoints_to_alarm
    
    def _check_condition(self, value: float) -> bool:
        """Verificar condición específica."""
        if self.condition == ">":
            return value > self.threshold
        elif self.condition == "<":
            return value < self.threshold
        elif self.condition == ">=":
            return value >= self.threshold
        elif self.condition == "<=":
            return value <= self.threshold
        elif self.condition == "==":
            return value == self.threshold
        elif self.condition == "!=":
            return value != self.threshold
        return False

@dataclass
class Alert:
    """Alerta activa."""
    alert_id: str
    rule_id: str
    rule_name: str
    metric_name: str
    severity: AlertSeverity
    status: AlertStatus = AlertStatus.ACTIVE
    message: str = ""
    triggered_at: datetime = field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    trigger_value: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario."""
        return {
            "alert_id": self.alert_id,
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "metric_name": self.metric_name,
            "severity": self.severity.value,
            "status": self.status.value,
            "message": self.message,
            "triggered_at": self.triggered_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "acknowledged_by": self.acknowledged_by,
            "trigger_value": self.trigger_value,
            "metadata": self.metadata
        }

@dataclass
class TraceSpan:
    """Span de tracing distribuido."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    service_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    
    def finish(self):
        """Finalizar span."""
        self.end_time = datetime.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
    
    def add_log(self, level: str, message: str, **kwargs):
        """Agregar log al span."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message,
            **kwargs
        }
        self.logs.append(log_entry)

class WerfenDistributedMonitor:
    """
    Sistema de monitoreo distribuido.
    
    Simula funcionalidades de AWS CloudWatch, X-Ray y otros
    servicios de observabilidad para el POC.
    
    AWS Equivalencias:
    - CloudWatch Metrics: metric collection and storage
    - CloudWatch Alarms: alerting system
    - CloudWatch Dashboards: visualization
    - AWS X-Ray: distributed tracing
    - CloudWatch Logs: log aggregation
    """
    
    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.metrics: Dict[str, MetricSeries] = {}
        self.alert_rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        
        # Tracing
        self.traces: Dict[str, List[TraceSpan]] = defaultdict(list)
        self.active_spans: Dict[str, TraceSpan] = {}
        
        # Threads de monitoreo
        self.is_running = False
        self.metrics_thread = None
        self.alerts_thread = None
        self.cleanup_thread = None
        
        # Locks para thread safety
        self.metrics_lock = threading.Lock()
        self.alerts_lock = threading.Lock()
        self.traces_lock = threading.Lock()
        
        # Callbacks para notificaciones
        self.alert_callbacks: List[Callable] = []
        
        logger.info(f"DistributedMonitor initialized with {config.metrics_interval}s metrics interval")
    
    def start_monitoring(self):
        """Iniciar sistema de monitoreo."""
        if self.is_running:
            logger.warning("Monitoring already running")
            return
        
        self.is_running = True
        
        # Iniciar threads
        self.metrics_thread = threading.Thread(target=self._metrics_collection_loop, daemon=True)
        self.alerts_thread = threading.Thread(target=self._alerts_evaluation_loop, daemon=True)
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        
        self.metrics_thread.start()
        if self.config.enable_alerts:
            self.alerts_thread.start()
        self.cleanup_thread.start()
        
        logger.info("Distributed monitoring started")
    
    def stop_monitoring(self):
        """Detener sistema de monitoreo."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # Esperar threads
        if self.metrics_thread:
            self.metrics_thread.join(timeout=5)
        if self.alerts_thread:
            self.alerts_thread.join(timeout=5)
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        
        logger.info("Distributed monitoring stopped")
    
    def record_metric(self, 
                     name: str, 
                     value: float, 
                     metric_type: MetricType = MetricType.GAUGE,
                     unit: str = "count",
                     tags: Dict[str, str] = None,
                     description: str = ""):
        """
        Registrar métrica.
        
        AWS Equivalente: CloudWatch PutMetricData API
        """
        with self.metrics_lock:
            if name not in self.metrics:
                self.metrics[name] = MetricSeries(
                    name=name,
                    metric_type=metric_type,
                    unit=unit,
                    description=description
                )
            
            self.metrics[name].add_point(value, tags)
        
        logger.debug(f"Metric recorded: {name} = {value} {unit}")
    
    def increment_counter(self, name: str, value: float = 1.0, tags: Dict[str, str] = None):
        """Incrementar contador."""
        current_value = self.get_latest_metric_value(name) or 0
        self.record_metric(name, current_value + value, MetricType.COUNTER, tags=tags)
    
    def set_gauge(self, name: str, value: float, tags: Dict[str, str] = None):
        """Establecer valor de gauge."""
        self.record_metric(name, value, MetricType.GAUGE, tags=tags)
    
    def record_timer(self, name: str, duration: float, tags: Dict[str, str] = None):
        """Registrar duración de timer."""
        self.record_metric(name, duration, MetricType.TIMER, unit="seconds", tags=tags)
    
    def get_metric_series(self, name: str) -> Optional[MetricSeries]:
        """Obtener serie de métricas."""
        with self.metrics_lock:
            return self.metrics.get(name)
    
    def get_latest_metric_value(self, name: str) -> Optional[float]:
        """Obtener último valor de métrica."""
        series = self.get_metric_series(name)
        return series.get_latest_value() if series else None
    
    def get_metric_statistics(self, name: str, minutes: int = 5) -> Dict[str, float]:
        """Obtener estadísticas de métrica."""
        series = self.get_metric_series(name)
        return series.calculate_statistics(minutes) if series else {}
    
    def create_alert_rule(self,
                         rule_id: str,
                         name: str,
                         metric_name: str,
                         condition: str,
                         threshold: float,
                         severity: AlertSeverity = AlertSeverity.MEDIUM,
                         description: str = ""):
        """
        Crear regla de alerta.
        
        AWS Equivalente: CloudWatch CreateAlarm API
        """
        rule = AlertRule(
            rule_id=rule_id,
            name=name,
            metric_name=metric_name,
            condition=condition,
            threshold=threshold,
            severity=severity,
            description=description
        )
        
        with self.alerts_lock:
            self.alert_rules[rule_id] = rule
        
        logger.info(f"Alert rule created: {name} [{rule_id}]")
    
    def delete_alert_rule(self, rule_id: str) -> bool:
        """Eliminar regla de alerta."""
        with self.alerts_lock:
            if rule_id in self.alert_rules:
                del self.alert_rules[rule_id]
                logger.info(f"Alert rule deleted: {rule_id}")
                return True
        return False
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str = "system"):
        """Reconocer alerta."""
        with self.alerts_lock:
            alert = self.active_alerts.get(alert_id)
            if alert:
                alert.status = AlertStatus.ACKNOWLEDGED
                alert.acknowledged_at = datetime.now()
                alert.acknowledged_by = acknowledged_by
                logger.info(f"Alert acknowledged: {alert_id} by {acknowledged_by}")
    
    def resolve_alert(self, alert_id: str):
        """Resolver alerta."""
        with self.alerts_lock:
            alert = self.active_alerts.pop(alert_id, None)
            if alert:
                alert.status = AlertStatus.RESOLVED
                alert.resolved_at = datetime.now()
                self.alert_history.append(alert)
                logger.info(f"Alert resolved: {alert_id}")
    
    def start_trace(self, operation_name: str, service_name: str) -> str:
        """
        Iniciar trace distribuido.
        
        AWS Equivalente: X-Ray begin_segment
        """
        trace_id = f"trace-{int(time.time() * 1000000)}"
        span_id = f"span-{int(time.time() * 1000000)}"
        
        span = TraceSpan(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=None,
            operation_name=operation_name,
            service_name=service_name,
            start_time=datetime.now()
        )
        
        with self.traces_lock:
            self.active_spans[span_id] = span
            self.traces[trace_id].append(span)
        
        logger.debug(f"Trace started: {operation_name} [{trace_id}]")
        return span_id
    
    def start_span(self, parent_span_id: str, operation_name: str, service_name: str) -> str:
        """Iniciar span hijo."""
        parent_span = self.active_spans.get(parent_span_id)
        if not parent_span:
            logger.warning(f"Parent span not found: {parent_span_id}")
            return self.start_trace(operation_name, service_name)
        
        span_id = f"span-{int(time.time() * 1000000)}"
        
        span = TraceSpan(
            trace_id=parent_span.trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            operation_name=operation_name,
            service_name=service_name,
            start_time=datetime.now()
        )
        
        with self.traces_lock:
            self.active_spans[span_id] = span
            self.traces[parent_span.trace_id].append(span)
        
        return span_id
    
    def finish_span(self, span_id: str, **tags):
        """Finalizar span."""
        with self.traces_lock:
            span = self.active_spans.pop(span_id, None)
            if span:
                span.finish()
                span.tags.update(tags)
                logger.debug(f"Span finished: {span.operation_name} [{span_id}] - {span.duration:.3f}s")
    
    def add_span_log(self, span_id: str, level: str, message: str, **kwargs):
        """Agregar log a span."""
        span = self.active_spans.get(span_id)
        if span:
            span.add_log(level, message, **kwargs)
    
    def register_alert_callback(self, callback: Callable):
        """Registrar callback para notificaciones de alertas."""
        self.alert_callbacks.append(callback)
        logger.info("Alert callback registered")
    
    def _metrics_collection_loop(self):
        """Loop de recolección de métricas del sistema."""
        logger.info("Metrics collection loop started")
        
        while self.is_running:
            try:
                self._collect_system_metrics()
                time.sleep(self.config.metrics_interval)
                
            except Exception as e:
                logger.error(f"Metrics collection error: {e}")
        
        logger.info("Metrics collection loop stopped")
    
    def _collect_system_metrics(self):
        """Recolectar métricas del sistema."""
        try:
            import psutil
            
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            self.set_gauge("system.cpu.percent", cpu_percent)
            
            # Memoria
            memory = psutil.virtual_memory()
            self.set_gauge("system.memory.percent", memory.percent)
            self.set_gauge("system.memory.available_mb", memory.available / 1024 / 1024)
            
            # Disco
            disk = psutil.disk_usage('/')
            self.set_gauge("system.disk.percent", disk.percent)
            self.set_gauge("system.disk.free_gb", disk.free / 1024 / 1024 / 1024)
            
            # Red (si está disponible)
            try:
                net_io = psutil.net_io_counters()
                self.increment_counter("system.network.bytes_sent", net_io.bytes_sent)
                self.increment_counter("system.network.bytes_recv", net_io.bytes_recv)
            except:
                pass
            
        except ImportError:
            # Si psutil no está disponible, usar métricas simuladas
            import random
            self.set_gauge("system.cpu.percent", random.uniform(10, 80))
            self.set_gauge("system.memory.percent", random.uniform(30, 90))
            self.set_gauge("system.disk.percent", random.uniform(20, 70))
        
        except Exception as e:
            logger.error(f"System metrics collection error: {e}")
    
    def _alerts_evaluation_loop(self):
        """Loop de evaluación de alertas."""
        logger.info("Alerts evaluation loop started")
        
        while self.is_running:
            try:
                self._evaluate_alert_rules()
                time.sleep(self.config.alert_check_interval)
                
            except Exception as e:
                logger.error(f"Alerts evaluation error: {e}")
        
        logger.info("Alerts evaluation loop stopped")
    
    def _evaluate_alert_rules(self):
        """Evaluar todas las reglas de alerta."""
        with self.alerts_lock:
            rules_to_evaluate = list(self.alert_rules.values())
        
        with self.metrics_lock:
            available_metrics = dict(self.metrics)
        
        for rule in rules_to_evaluate:
            if not rule.enabled:
                continue
            
            metric_series = available_metrics.get(rule.metric_name)
            if not metric_series:
                continue
            
            should_trigger = rule.evaluate(metric_series)
            alert_id = f"alert-{rule.rule_id}-{int(time.time())}"
            
            # Verificar si ya hay una alerta activa para esta regla
            existing_alert = None
            with self.alerts_lock:
                for alert in self.active_alerts.values():
                    if alert.rule_id == rule.rule_id and alert.status == AlertStatus.ACTIVE:
                        existing_alert = alert
                        break
            
            if should_trigger and not existing_alert:
                # Crear nueva alerta
                current_value = metric_series.get_latest_value()
                alert = Alert(
                    alert_id=alert_id,
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    metric_name=rule.metric_name,
                    severity=rule.severity,
                    message=f"{rule.name}: {rule.metric_name} {rule.condition} {rule.threshold} (current: {current_value})",
                    trigger_value=current_value
                )
                
                with self.alerts_lock:
                    self.active_alerts[alert_id] = alert
                
                # Notificar callbacks
                self._notify_alert_callbacks(alert)
                
                logger.warning(f"Alert triggered: {rule.name} [{alert_id}]")
            
            elif not should_trigger and existing_alert:
                # Resolver alerta existente
                self.resolve_alert(existing_alert.alert_id)
    
    def _notify_alert_callbacks(self, alert: Alert):
        """Notificar callbacks de alertas."""
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")
    
    def _cleanup_loop(self):
        """Loop de limpieza de datos antiguos."""
        logger.info("Cleanup loop started")
        
        while self.is_running:
            try:
                self._cleanup_old_data()
                time.sleep(3600)  # Limpiar cada hora
                
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
        
        logger.info("Cleanup loop stopped")
    
    def _cleanup_old_data(self):
        """Limpiar datos antiguos según política de retención."""
        cutoff_time = datetime.now() - timedelta(days=self.config.retention_days)
        
        # Limpiar métricas antigas
        with self.metrics_lock:
            for series in self.metrics.values():
                old_points = deque()
                for point in series.points:
                    if point.timestamp >= cutoff_time:
                        old_points.append(point)
                series.points = old_points
        
        # Limpiar traces antiguos
        with self.traces_lock:
            traces_to_remove = []
            for trace_id, spans in self.traces.items():
                if spans and spans[0].start_time < cutoff_time:
                    traces_to_remove.append(trace_id)
            
            for trace_id in traces_to_remove:
                del self.traces[trace_id]
        
        if traces_to_remove:
            logger.info(f"Cleaned up {len(traces_to_remove)} old traces")
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """
        Obtener datos para dashboard.
        
        AWS Equivalente: CloudWatch Dashboard data
        """
        with self.metrics_lock:
            metrics_summary = {}
            for name, series in self.metrics.items():
                latest_value = series.get_latest_value()
                stats = series.calculate_statistics(5)  # Últimos 5 minutos
                
                metrics_summary[name] = {
                    "latest_value": latest_value,
                    "unit": series.unit,
                    "type": series.metric_type.value,
                    "statistics": stats,
                    "points_count": len(series.points)
                }
        
        with self.alerts_lock:
            alerts_summary = {
                "active_alerts": len(self.active_alerts),
                "alerts_by_severity": {},
                "recent_alerts": []
            }
            
            # Contar por severidad
            for alert in self.active_alerts.values():
                severity = alert.severity.value
                alerts_summary["alerts_by_severity"][severity] = alerts_summary["alerts_by_severity"].get(severity, 0) + 1
            
            # Alertas recientes
            recent_alerts = list(self.alert_history)[-10:]  # Últimas 10
            alerts_summary["recent_alerts"] = [alert.to_dict() for alert in recent_alerts]
        
        with self.traces_lock:
            traces_summary = {
                "total_traces": len(self.traces),
                "active_spans": len(self.active_spans),
                "traces_in_last_hour": 0
            }
            
            # Contar traces de la última hora
            cutoff_time = datetime.now() - timedelta(hours=1)
            for spans in self.traces.values():
                if spans and spans[0].start_time >= cutoff_time:
                    traces_summary["traces_in_last_hour"] += 1
        
        return {
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics_summary,
            "alerts": alerts_summary,
            "traces": traces_summary,
            "system_status": {
                "monitoring_uptime": time.time(),
                "is_running": self.is_running,
                "config": asdict(self.config)
            }
        }
    
    def get_metric_history(self, metric_name: str, hours: int = 1) -> List[Dict[str, Any]]:
        """Obtener historial de métrica."""
        series = self.get_metric_series(metric_name)
        if not series:
            return []
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_points = [point for point in series.points 
                        if point.timestamp >= cutoff_time]
        
        return [point.to_dict() for point in recent_points]
    
    def get_trace_details(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Obtener detalles de trace específico."""
        with self.traces_lock:
            spans = self.traces.get(trace_id, [])
            if not spans:
                return None
            
            return {
                "trace_id": trace_id,
                "total_duration": max(span.duration or 0 for span in spans),
                "spans_count": len(spans),
                "services": list(set(span.service_name for span in spans)),
                "spans": [asdict(span) for span in spans]
            }
    
    def __enter__(self):
        """Context manager entry."""
        self.start_monitoring()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_monitoring()

# Funciones de utilidad

def create_basic_alert_rules(monitor: WerfenDistributedMonitor):
    """Crear reglas de alerta básicas."""
    # CPU alto
    monitor.create_alert_rule(
        "cpu_high",
        "High CPU Usage",
        "system.cpu.percent",
        ">",
        80.0,
        AlertSeverity.HIGH,
        "CPU usage is above 80%"
    )
    
    # Memoria alta
    monitor.create_alert_rule(
        "memory_high",
        "High Memory Usage",
        "system.memory.percent",
        ">",
        90.0,
        AlertSeverity.CRITICAL,
        "Memory usage is above 90%"
    )
    
    # Disco lleno
    monitor.create_alert_rule(
        "disk_full",
        "Disk Space Low",
        "system.disk.percent",
        ">",
        85.0,
        AlertSeverity.MEDIUM,
        "Disk usage is above 85%"
    )

def setup_distributed_monitoring(config: MonitoringConfig = None) -> WerfenDistributedMonitor:
    """Setup completo del sistema de monitoreo."""
    config = config or MonitoringConfig()
    monitor = WerfenDistributedMonitor(config)
    
    # Crear reglas básicas
    create_basic_alert_rules(monitor)
    
    # Callback de ejemplo para alertas
    def alert_handler(alert: Alert):
        logger.warning(f"ALERT [{alert.severity.value.upper()}]: {alert.message}")
    
    monitor.register_alert_callback(alert_handler)
    
    return monitor 