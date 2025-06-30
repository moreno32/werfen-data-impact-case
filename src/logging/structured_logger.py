"""
Structured Logging - Werfen Data Pipeline
==========================================

Centralized logging system implementing structured logging with JSON format,
prepared for AWS CloudWatch integration and enterprise observability services.

Author: Lead Software Architect - Werfen Data Team
Date: January 2025
"""

import structlog
import logging
import logging.handlers
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from contextvars import ContextVar

# Context variables for correlation tracking
correlation_id: ContextVar[str] = ContextVar('correlation_id', default='')
pipeline_run_id: ContextVar[str] = ContextVar('pipeline_run_id', default='')
user_context: ContextVar[str] = ContextVar('user_context', default='system')

class WerfenLoggerConfig:
    """Structured logging system configuration"""
    
    def __init__(self):
        # Import unified configuration
        from config import get_config
        config = get_config()
        
        # Use unified configuration instead of environment variables
        self.log_level = config.log_level
        self.log_format = config.log_format  # json | console
        self.environment = config.environment
        self.project_root = str(config.project_root)
        self.logs_dir = str(config.logs_folder)
        
        # Create logs directory if it doesn't exist
        config.logs_folder.mkdir(exist_ok=True)
        
        # Log files
        self.pipeline_log_file = Path(self.logs_dir) / 'werfen_pipeline.log'
        self.error_log_file = Path(self.logs_dir) / 'werfen_errors.log'
        self.audit_log_file = Path(self.logs_dir) / 'werfen_audit.log'

def generate_correlation_id() -> str:
    """Generate unique correlation ID for tracking"""
    return str(uuid.uuid4())[:8]

def generate_pipeline_run_id() -> str:
    """Generate unique pipeline run ID"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"run_{timestamp}_{generate_correlation_id()}"

def add_app_context(logger, method_name, event_dict):
    """
    Add application context to each log entry.
    
    Includes information about environment, application and current user.
    """
    # Import unified configuration
    from config import get_config
    config = get_config()
    
    event_dict['environment'] = config.environment
    event_dict['application'] = 'werfen-data-pipeline'
    event_dict['version'] = '1.0.0'
    
    # Add correlation context if exists
    if correlation_id.get():
        event_dict['correlation_id'] = correlation_id.get()
    
    if pipeline_run_id.get():
        event_dict['pipeline_run_id'] = pipeline_run_id.get()
        
    event_dict['user'] = user_context.get()
    
    return event_dict

def add_performance_context(logger, method_name, event_dict):
    """Add performance metrics"""
    if 'duration_ms' in event_dict:
        # Classify performance
        duration = event_dict['duration_ms']
        if duration < 1000:
            event_dict['performance_tier'] = 'fast'
        elif duration < 5000:
            event_dict['performance_tier'] = 'normal'
        else:
            event_dict['performance_tier'] = 'slow'
    
    return event_dict

def setup_structured_logging(
    service_name: str = "werfen-pipeline",
    log_level: str = "INFO",
    enable_console: bool = True,
    enable_file: bool = True
) -> structlog.stdlib.BoundLogger:
    """
    Configure structured logging for Werfen pipeline
    """
    
    config = WerfenLoggerConfig()
    
    # Configure processors
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        add_app_context,
        add_performance_context,
        structlog.processors.UnicodeDecoder(),
    ]
    
    # Configure renderer based on format
    if config.log_format == 'json':
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure Python standard logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s" if config.log_format == 'json' else None,
        handlers=_create_handlers(config, enable_console, enable_file)
    )
    
    # Create logger
    logger = structlog.get_logger(service_name)
    
    return logger

def _create_handlers(config: WerfenLoggerConfig, enable_console: bool, enable_file: bool):
    """Create handlers for logging"""
    handlers = []
    
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        handlers.append(console_handler)
    
    if enable_file:
        # Main handler for pipeline
        file_handler = logging.handlers.RotatingFileHandler(
            config.pipeline_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        handlers.append(file_handler)
        
        # Specific handler for errors
        error_handler = logging.handlers.RotatingFileHandler(
            config.error_log_file,
            maxBytes=5*1024*1024,   # 5MB
            backupCount=3
        )
        error_handler.setLevel(logging.ERROR)
        handlers.append(error_handler)
    
    return handlers

class PipelineLogger:
    """Specialized logger for Werfen data pipeline"""
    
    def __init__(self, component_name: str):
        self.logger = setup_structured_logging(service_name=f"werfen.{component_name}")
        self.component = component_name
        
    def start_pipeline_run(self, run_type: str = "manual") -> str:
        """Start a pipeline run with tracking"""
        run_id = generate_pipeline_run_id()
        corr_id = generate_correlation_id()
        
        # Set context
        pipeline_run_id.set(run_id)
        correlation_id.set(corr_id)
        
        self.logger.info(
            "Pipeline run started",
            component=self.component,
            run_type=run_type,
            pipeline_run_id=run_id,
            correlation_id=corr_id,
            event_type="pipeline_start"
        )
        
        return run_id
    
    def log_data_operation(
        self, 
        operation: str, 
        table_name: str, 
        record_count: int,
        duration_ms: Optional[int] = None,
        success: bool = True,
        **kwargs
    ):
        """Log data operations"""
        log_data = {
            "component": self.component,
            "operation": operation,
            "table_name": table_name,
            "record_count": record_count,
            "success": success,
            "event_type": "data_operation"
        }
        
        if duration_ms:
            log_data["duration_ms"] = duration_ms
            log_data["records_per_second"] = int(record_count / (duration_ms / 1000)) if duration_ms > 0 else 0
        
        # Add additional fields
        log_data.update(kwargs)
        
        if success:
            self.logger.info("Data operation completed", **log_data)
        else:
            self.logger.error("Data operation failed", **log_data)
    
    def log_quality_check(
        self, 
        check_name: str, 
        status: str, 
        details: Dict[str, Any] = None
    ):
        """Log data quality validations"""
        self.logger.info(
            "Quality check executed",
            component=self.component,
            check_name=check_name,
            status=status,
            details=details or {},
            event_type="quality_check"
        )
    
    def log_error(
        self, 
        error_message: str, 
        error_type: str = "unknown",
        stack_trace: str = None,
        **context
    ):
        """Log errors with enriched context"""
        error_data = {
            "component": self.component,
            "error_message": error_message,
            "error_type": error_type,
            "event_type": "error"
        }
        
        if stack_trace:
            error_data["stack_trace"] = stack_trace
        
        error_data.update(context)
        
        self.logger.error("Error occurred", **error_data)
    
    def log_performance_metric(
        self, 
        metric_name: str, 
        value: float, 
        unit: str = "ms",
        **tags
    ):
        """Log performance metrics"""
        self.logger.info(
            "Performance metric",
            component=self.component,
            metric_name=metric_name,
            metric_value=value,
            metric_unit=unit,
            event_type="performance_metric",
            **tags
        )
    
    def end_pipeline_run(self, success: bool = True, summary: Dict[str, Any] = None):
        """End pipeline run"""
        self.logger.info(
            "Pipeline run completed",
            component=self.component,
            success=success,
            summary=summary or {},
            event_type="pipeline_end"
        )

def get_pipeline_logger(component_name: str) -> PipelineLogger:
    """Factory function to create pipeline loggers"""
    return PipelineLogger(component_name)

# Decorator for automatic function execution logging
def log_execution(operation_name: str = None):
    """Decorator for automatic function execution logging"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_pipeline_logger(func.__module__)
            op_name = operation_name or func.__name__
            
            start_time = datetime.now()
            
            try:
                logger.logger.info(
                    f"Starting {op_name}",
                    operation=op_name,
                    function=func.__name__,
                    event_type="function_start"
                )
                
                result = func(*args, **kwargs)
                
                duration = (datetime.now() - start_time).total_seconds() * 1000
                
                logger.logger.info(
                    f"Completed {op_name}",
                    operation=op_name,
                    function=func.__name__,
                    duration_ms=int(duration),
                    success=True,
                    event_type="function_end"
                )
                
                return result
                
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds() * 1000
                
                logger.log_error(
                    error_message=str(e),
                    error_type=type(e).__name__,
                    operation=op_name,
                    function=func.__name__,
                    duration_ms=int(duration)
                )
                raise
        
        return wrapper
    return decorator 