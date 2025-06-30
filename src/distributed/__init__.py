"""
Distributed System Module for Werfen Data Pipeline
=================================================

This module implements a simple but scalable distributed architecture
for the POC. It includes AWS service equivalents:

- Task Distribution: Task distribution among workers
- Worker Management: Worker and node management
- Message Queue: Messaging system (simulated)
- Load Balancing: Basic load balancer
- Monitoring: Performance monitoring

AWS Equivalencies:
- Task Distribution → AWS Batch / ECS
- Message Queue → AWS SQS / SNS
- Load Balancer → AWS ALB / NLB
- Worker Management → AWS EC2 Auto Scaling
- Monitoring → AWS CloudWatch

Author: Lead Software Architect - Werfen Data Team
Date: January 2025
"""

import logging
from typing import Dict, Any, Optional, List
import json
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Public exports
from .task_distributor import TaskDistributor, TaskDistributorConfig
from .worker_manager import WorkerManager, WorkerConfig
from .message_queue import MessageQueue, MessageQueueConfig
from .load_balancer import LoadBalancer, LoadBalancerConfig
from .distributed_monitor import DistributedMonitor

__all__ = [
    'TaskDistributor',
    'TaskDistributorConfig', 
    'WorkerManager',
    'WorkerConfig',
    'MessageQueue',
    'MessageQueueConfig',
    'LoadBalancer',
    'LoadBalancerConfig',
    'DistributedMonitor',
    'create_distributed_environment',
    'get_distributed_config'
]

def create_distributed_environment(
    environment: str = 'local',
    config_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Creates a complete distributed environment for the POC.
    
    Args:
        environment: Execution environment (local, dev, staging, prod)
        config_path: Optional configuration file path
        
    Returns:
        Dict with all distributed components
        
    Note:
        This function would be replaced by CloudFormation/CDK stack
        in a real AWS environment.
    """
    
    logger.info(f"Creating distributed environment: {environment}")
    
    # Load configuration
    config = get_distributed_config(environment, config_path)
    
    # Validate environment
    if environment not in ['local', 'dev', 'staging', 'prod']:
        raise ValueError(f"Invalid environment: {environment}")
    
    # Adjust configuration based on environment
    if environment == 'local':
        config['task_distributor']['max_workers'] = 2
        config['message_queue']['max_queue_size'] = 100
        config['load_balancer']['max_targets'] = 3
    elif environment == 'prod':
        config['task_distributor']['max_workers'] = 20
        config['message_queue']['max_queue_size'] = 10000
        config['load_balancer']['max_targets'] = 50
    
    # Initialize components
    components = {}
    
    try:
        # Task distributor
        task_config = TaskDistributorConfig(**config['task_distributor'])
        components['task_distributor'] = TaskDistributor(task_config)
        
        # Worker manager
        worker_config = WorkerConfig(**config['worker_manager'])
        components['worker_manager'] = WorkerManager(worker_config)
        
        # Message queue
        queue_config = MessageQueueConfig(**config['message_queue'])
        components['message_queue'] = MessageQueue(queue_config)
        
        # Load balancer
        lb_config = LoadBalancerConfig(**config['load_balancer'])
        components['load_balancer'] = LoadBalancer(lb_config)
        
        # Monitor
        components['monitor'] = DistributedMonitor(components)
        
        logger.info("Distributed environment created successfully")
        
        # Return complete environment
        return {
            'environment': environment,
            'config': config,
            'components': components,
            'status': 'ready'
        }
        
    except Exception as e:
        logger.error(f"Error creating distributed environment: {e}")
        raise

def get_distributed_config(
    environment: str = 'local',
    config_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Gets distributed configuration for the specified environment.
    
    Args:
        environment: Target environment
        config_path: Optional custom configuration path
        
    Returns:
        Complete configuration dictionary
    """
    
    # Default configuration
    default_config = {
        'task_distributor': {
            'max_workers': 4,
            'max_queue_size': 1000,
            'task_timeout': 300,
            'retry_attempts': 3,
            'batch_size': 10
        },
        'worker_manager': {
            'min_workers': 1,
            'max_workers': 10,
            'scale_up_threshold': 0.8,
            'scale_down_threshold': 0.3,
            'health_check_interval': 30
        },
        'message_queue': {
            'max_queue_size': 5000,
            'message_retention': 3600,
            'dead_letter_threshold': 3,
            'visibility_timeout': 30
        },
        'load_balancer': {
            'algorithm': 'round_robin',
            'health_check_interval': 10,
            'max_targets': 20,
            'sticky_sessions': False,
            'connection_timeout': 30
        }
    }
    
    # Load custom configuration if provided
    if config_path:
        try:
            with open(config_path) as f:
                custom_config = json.load(f)
                # Merge configurations
                for key, value in custom_config.items():
                    if key in default_config:
                        default_config[key].update(value)
        except Exception as e:
            logger.warning(f"Could not load custom config: {e}")
    
    return default_config

# Module metadata
__version__ = "1.0.0"
__author__ = "Werfen Data Team"
__description__ = "Distributed system components for Werfen Data Pipeline POC" 