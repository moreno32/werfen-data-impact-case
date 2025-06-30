"""
Intelligent Caching System - Werfen Data Pipeline
================================================

Caching system to optimize pipeline performance with intelligent
strategies, prepared for migration to Amazon ElastiCache and S3.

Author: Lead Software Architect - Werfen Data Team
Date: January 2025
"""

import pickle
import hashlib
import json
import time
import pandas as pd
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Union, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

# Import existing components
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.logging.structured_logger import get_pipeline_logger

@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    data: Any
    timestamp: float
    ttl_seconds: int
    size_bytes: int
    access_count: int = 0
    last_access: float = 0
    
    def is_expired(self) -> bool:
        """Check if entry has expired"""
        return time.time() - self.timestamp > self.ttl_seconds
    
    def is_hot(self, threshold_accesses: int = 3) -> bool:
        """Check if it's a 'hot' entry (heavily accessed)"""
        return self.access_count >= threshold_accesses

@dataclass
class CacheConfig:
    """Cache system configuration"""
    max_memory_mb: int = 500
    default_ttl_seconds: int = 3600  # 1 hour
    cache_directory: str = "cache"
    enable_disk_cache: bool = True
    enable_compression: bool = True
    max_entries: int = 1000
    
class WerfenIntelligentCache:
    """
    Intelligent caching system for Werfen pipeline
    
    AWS Equivalences:
    - Memory cache → Amazon ElastiCache (Redis/Memcached)
    - Disk cache → Amazon S3 with intelligent tiering
    - TTL management → ElastiCache TTL policies
    - Compression → S3 compression algorithms
    """
    
    def __init__(self, config: Optional[CacheConfig] = None):
        self.config = config or CacheConfig()
        self.memory_cache: Dict[str, CacheEntry] = {}
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "memory_usage_mb": 0
        }
        
        # Create cache directory
        self.cache_dir = Path(self.config.cache_directory)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Logger
        self.logger = get_pipeline_logger("intelligent_cache")
        
        # AWS equivalent configuration
        self.aws_config = {
            'elasticache_cluster': 'werfen-pipeline-cache',
            's3_bucket': 'werfen-cache-bucket', 
            'intelligent_tiering': True,
            'compression_enabled': True
        }
        
        self.logger.logger.info(
            "Intelligent cache initialized",
            component="intelligent_cache",
            max_memory_mb=self.config.max_memory_mb,
            cache_directory=str(self.cache_dir),
            event_type="cache_init"
        )
    
    def _generate_cache_key(self, key_data: Union[str, Dict, list]) -> str:
        """Generate unique cache key"""
        if isinstance(key_data, str):
            content = key_data
        else:
            content = json.dumps(key_data, sort_keys=True, default=str)
        
        return hashlib.md5(content.encode()).hexdigest()
    
    def _estimate_size(self, data: Any) -> int:
        """Estimate object size in bytes"""
        try:
            if isinstance(data, pd.DataFrame):
                return data.memory_usage(deep=True).sum()
            else:
                return len(pickle.dumps(data))
        except:
            return sys.getsizeof(data)
    
    def _cleanup_expired_entries(self):
        """Clean up expired entries"""
        current_time = time.time()
        expired_keys = [
            key for key, entry in self.memory_cache.items()
            if entry.is_expired()
        ]
        
        for key in expired_keys:
            self._evict_entry(key, reason="expired")
    
    def _evict_entry(self, key: str, reason: str = "manual"):
        """Remove entry from cache"""
        if key in self.memory_cache:
            entry = self.memory_cache[key]
            
            # Save to disk if enabled and is hot entry
            if (self.config.enable_disk_cache and 
                entry.is_hot() and 
                reason != "expired"):
                self._save_to_disk(key, entry)
            
            del self.memory_cache[key]
            self.cache_stats["evictions"] += 1
            self._update_memory_usage()
            
            self.logger.logger.debug(
                "Cache entry evicted",
                component="intelligent_cache",
                cache_key=key,
                reason=reason,
                event_type="cache_eviction"
            )
    
    def _evict_lru_entries(self, target_memory_mb: int):
        """Evict least recently used entries"""
        # Sort by last access
        sorted_entries = sorted(
            self.memory_cache.items(),
            key=lambda x: x[1].last_access
        )
        
        current_memory_mb = self._calculate_memory_usage()
        
        for key, entry in sorted_entries:
            if current_memory_mb <= target_memory_mb:
                break
            
            self._evict_entry(key, reason="lru")
            current_memory_mb -= entry.size_bytes / (1024 * 1024)
    
    def _calculate_memory_usage(self) -> float:
        """Calculate current memory usage in MB"""
        total_bytes = sum(entry.size_bytes for entry in self.memory_cache.values())
        return total_bytes / (1024 * 1024)
    
    def _update_memory_usage(self):
        """Update memory usage statistics"""
        self.cache_stats["memory_usage_mb"] = self._calculate_memory_usage()
    
    def _save_to_disk(self, key: str, entry: CacheEntry):
        """Save entry to disk cache"""
        try:
            disk_path = self.cache_dir / f"{key}.cache"
            
            # Prepare data for disk
            disk_data = {
                "data": entry.data,
                "timestamp": entry.timestamp,
                "ttl_seconds": entry.ttl_seconds,
                "access_count": entry.access_count
            }
            
            # Save with compression if enabled
            if self.config.enable_compression:
                import gzip
                with gzip.open(f"{disk_path}.gz", 'wb') as f:
                    pickle.dump(disk_data, f)
            else:
                with open(disk_path, 'wb') as f:
                    pickle.dump(disk_data, f)
                    
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="save_to_disk",
                cache_key=key
            )
    
    def _load_from_disk(self, key: str) -> Optional[CacheEntry]:
        """Load entry from disk cache"""
        try:
            # Try loading compressed first
            compressed_path = self.cache_dir / f"{key}.cache.gz"
            regular_path = self.cache_dir / f"{key}.cache"
            
            disk_data = None
            
            if compressed_path.exists():
                import gzip
                with gzip.open(compressed_path, 'rb') as f:
                    disk_data = pickle.load(f)
            elif regular_path.exists():
                with open(regular_path, 'rb') as f:
                    disk_data = pickle.load(f)
            
            if disk_data:
                entry = CacheEntry(
                    key=key,
                    data=disk_data["data"],
                    timestamp=disk_data["timestamp"],
                    ttl_seconds=disk_data["ttl_seconds"],
                    size_bytes=self._estimate_size(disk_data["data"]),
                    access_count=disk_data["access_count"],
                    last_access=time.time()
                )
                
                # Check if not expired
                if not entry.is_expired():
                    return entry
                    
        except Exception as e:
            self.logger.log_error(
                error_message=str(e),
                error_type=type(e).__name__,
                operation="load_from_disk",
                cache_key=key
            )
        
        return None
    
    def get(self, key: Union[str, Dict, list]) -> Optional[Any]:
        """Get value from cache"""
        cache_key = self._generate_cache_key(key)
        
        # Clean expired entries first
        self._cleanup_expired_entries()
        
        # Search in memory
        if cache_key in self.memory_cache:
            entry = self.memory_cache[cache_key]
            if not entry.is_expired():
                entry.access_count += 1
                entry.last_access = time.time()
                self.cache_stats["hits"] += 1
                
                self.logger.logger.debug(
                    "Cache hit (memory)",
                    component="intelligent_cache",
                    cache_key=cache_key,
                    access_count=entry.access_count,
                    event_type="cache_hit"
                )
                
                return entry.data
            else:
                self._evict_entry(cache_key, reason="expired")
        
        # Search on disk if enabled
        if self.config.enable_disk_cache:
            disk_entry = self._load_from_disk(cache_key)
            if disk_entry:
                # Move back to memory
                self.set(key, disk_entry.data, disk_entry.ttl_seconds)
                self.cache_stats["hits"] += 1
                
                self.logger.logger.debug(
                    "Cache hit (disk)",
                    component="intelligent_cache",
                    cache_key=cache_key,
                    event_type="cache_hit_disk"
                )
                
                return disk_entry.data
        
        # Cache miss
        self.cache_stats["misses"] += 1
        
        self.logger.logger.debug(
            "Cache miss",
            component="intelligent_cache",
            cache_key=cache_key,
            event_type="cache_miss"
        )
        
        return None
    
    def set(self, key: Union[str, Dict, list], value: Any, 
            ttl_seconds: Optional[int] = None) -> bool:
        """Store value in cache"""
        cache_key = self._generate_cache_key(key)
        ttl = ttl_seconds or self.config.default_ttl_seconds
        
        # Create entry
        entry = CacheEntry(
            key=cache_key,
            data=value,
            timestamp=time.time(),
            ttl_seconds=ttl,
            size_bytes=self._estimate_size(value),
            last_access=time.time()
        )
        
        # Check memory limits
        new_memory_usage = (self._calculate_memory_usage() + 
                           entry.size_bytes / (1024 * 1024))
        
        if new_memory_usage > self.config.max_memory_mb:
            # Evict entries to make space
            target_memory = self.config.max_memory_mb * 0.8  # 80% of limit
            self._evict_lru_entries(target_memory)
        
        # Check entry limit
        if len(self.memory_cache) >= self.config.max_entries:
            # Evict least accessed
            lru_key = min(
                self.memory_cache.keys(),
                key=lambda k: self.memory_cache[k].last_access
            )
            self._evict_entry(lru_key, reason="max_entries")
        
        # Store in memory
        self.memory_cache[cache_key] = entry
        self._update_memory_usage()
        
        self.logger.logger.debug(
            "Cache entry stored",
            component="intelligent_cache",
            cache_key=cache_key,
            size_bytes=entry.size_bytes,
            ttl_seconds=ttl,
            event_type="cache_set"
        )
        
        return True
    
    def cached_function(self, ttl_seconds: Optional[int] = None):
        """Decorator to cache function results"""
        def decorator(func: Callable):
            def wrapper(*args, **kwargs):
                # Generate key based on function and arguments
                cache_key = {
                    "function": f"{func.__module__}.{func.__name__}",
                    "args": args,
                    "kwargs": kwargs
                }
                
                # Try to get from cache
                cached_result = self.get(cache_key)
                if cached_result is not None:
                    return cached_result
                
                # Execute function and cache result
                result = func(*args, **kwargs)
                self.set(cache_key, result, ttl_seconds)
                
                return result
            
            return wrapper
        return decorator
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_requests = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_ratio = (self.cache_stats["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        stats = {
            "memory_cache_entries": len(self.memory_cache),
            "memory_usage_mb": self.cache_stats["memory_usage_mb"],
            "max_memory_mb": self.config.max_memory_mb,
            "memory_utilization_percent": (self.cache_stats["memory_usage_mb"] / self.config.max_memory_mb * 100),
            "cache_hits": self.cache_stats["hits"],
            "cache_misses": self.cache_stats["misses"],
            "hit_ratio_percent": hit_ratio,
            "evictions": self.cache_stats["evictions"],
            "disk_cache_enabled": self.config.enable_disk_cache,
            "compression_enabled": self.config.enable_compression
        }
        
        return stats
    
    def clear_cache(self, reason: str = "manual"):
        """Clear entire cache"""
        entries_cleared = len(self.memory_cache)
        
        for key in list(self.memory_cache.keys()):
            self._evict_entry(key, reason)
        
        self.logger.logger.info(
            "Cache cleared",
            component="intelligent_cache",
            entries_cleared=entries_cleared,
            reason=reason,
            event_type="cache_cleared"
        )

def get_intelligent_cache(config: Optional[CacheConfig] = None) -> WerfenIntelligentCache:
    """Factory function to get the cache system"""
    return WerfenIntelligentCache(config) 