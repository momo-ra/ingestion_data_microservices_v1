"""
Metrics Collection Utilities

This module provides a metrics collection system for monitoring application performance.
Metrics are collected in-memory and can be exposed via an HTTP endpoint.
The implementation is inspired by Prometheus but simplified for this application.
"""

import time
from typing import Dict, List, Any, Optional, Callable
import threading
from datetime import datetime, timedelta
import json
from functools import wraps
import asyncio

class MetricType:
    """Metric types enumeration"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"

class Metric:
    """Base class for metrics"""
    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.labels = labels or []
        self.created_at = datetime.now()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary"""
        return {
            "name": self.name,
            "description": self.description,
            "type": self.get_type(),
            "created_at": self.created_at.isoformat(),
        }
        
    def get_type(self) -> str:
        """Get metric type"""
        raise NotImplementedError("Subclasses must implement get_type")

class Counter(Metric):
    """Counter metric that only increases"""
    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        super().__init__(name, description, labels)
        self._values: Dict[str, float] = {}
        self._lock = threading.RLock()
        
    def inc(self, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment counter by value"""
        with self._lock:
            key = self._labels_to_key(labels)
            if key not in self._values:
                self._values[key] = 0.0
            self._values[key] += value
            
    def get(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current counter value"""
        with self._lock:
            key = self._labels_to_key(labels)
            return self._values.get(key, 0.0)
            
    def _labels_to_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Convert labels to string key"""
        if not labels or not self.labels:
            return ""
        
        parts = []
        for label in self.labels:
            if label in labels:
                parts.append(f"{label}={labels[label]}")
        
        return ",".join(sorted(parts))
        
    def get_type(self) -> str:
        return MetricType.COUNTER
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary"""
        base_dict = super().to_dict()
        
        # Add values
        values = []
        for key, value in self._values.items():
            if key:
                label_pairs = {}
                for label_pair in key.split(","):
                    if "=" in label_pair:
                        label, label_value = label_pair.split("=", 1)
                        label_pairs[label] = label_value
                values.append({"labels": label_pairs, "value": value})
            else:
                values.append({"value": value})
                
        base_dict["values"] = values
        return base_dict

class Gauge(Metric):
    """Gauge metric that can go up and down"""
    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        super().__init__(name, description, labels)
        self._values: Dict[str, float] = {}
        self._lock = threading.RLock()
        
    def set(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set gauge to value"""
        with self._lock:
            key = self._labels_to_key(labels)
            self._values[key] = value
            
    def inc(self, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment gauge by value"""
        with self._lock:
            key = self._labels_to_key(labels)
            if key not in self._values:
                self._values[key] = 0.0
            self._values[key] += value
            
    def dec(self, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """Decrement gauge by value"""
        with self._lock:
            key = self._labels_to_key(labels)
            if key not in self._values:
                self._values[key] = 0.0
            self._values[key] -= value
            
    def get(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value"""
        with self._lock:
            key = self._labels_to_key(labels)
            return self._values.get(key, 0.0)
            
    def _labels_to_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Convert labels to string key"""
        if not labels or not self.labels:
            return ""
        
        parts = []
        for label in self.labels:
            if label in labels:
                parts.append(f"{label}={labels[label]}")
        
        return ",".join(sorted(parts))
        
    def get_type(self) -> str:
        return MetricType.GAUGE
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary"""
        base_dict = super().to_dict()
        
        # Add values
        values = []
        for key, value in self._values.items():
            if key:
                label_pairs = {}
                for label_pair in key.split(","):
                    if "=" in label_pair:
                        label, label_value = label_pair.split("=", 1)
                        label_pairs[label] = label_value
                values.append({"labels": label_pairs, "value": value})
            else:
                values.append({"value": value})
                
        base_dict["values"] = values
        return base_dict

class Histogram(Metric):
    """Histogram metric for measuring distributions"""
    def __init__(self, name: str, description: str, buckets: List[float], labels: Optional[List[str]] = None):
        super().__init__(name, description, labels)
        self.buckets = sorted(buckets) + [float('inf')]
        self._values: Dict[str, List[int]] = {}  # Key -> [bucket1_count, bucket2_count, ...]
        self._sums: Dict[str, float] = {}  # Key -> sum of all observations
        self._counts: Dict[str, int] = {}  # Key -> count of all observations
        self._lock = threading.RLock()
        
    def observe(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Observe a value"""
        with self._lock:
            key = self._labels_to_key(labels)
            
            # Initialize if not exists
            if key not in self._values:
                self._values[key] = [0] * len(self.buckets)
                self._sums[key] = 0.0
                self._counts[key] = 0
                
            # Increment buckets
            for i, bucket in enumerate(self.buckets):
                if value <= bucket:
                    self._values[key][i] += 1
                    
            # Update sum and count
            self._sums[key] += value
            self._counts[key] += 1
            
    def get_buckets(self, labels: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """Get histogram buckets"""
        with self._lock:
            key = self._labels_to_key(labels)
            if key not in self._values:
                return []
                
            result = []
            for i, bucket in enumerate(self.buckets):
                result.append({
                    "le": bucket if bucket != float('inf') else "+Inf",
                    "count": self._values[key][i]
                })
            return result
            
    def get_sum(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get sum of all observations"""
        with self._lock:
            key = self._labels_to_key(labels)
            return self._sums.get(key, 0.0)
            
    def get_count(self, labels: Optional[Dict[str, str]] = None) -> int:
        """Get count of all observations"""
        with self._lock:
            key = self._labels_to_key(labels)
            return self._counts.get(key, 0)
            
    def _labels_to_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Convert labels to string key"""
        if not labels or not self.labels:
            return ""
        
        parts = []
        for label in self.labels:
            if label in labels:
                parts.append(f"{label}={labels[label]}")
        
        return ",".join(sorted(parts))
        
    def get_type(self) -> str:
        return MetricType.HISTOGRAM
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary"""
        base_dict = super().to_dict()
        base_dict["buckets"] = [b if b != float('inf') else "+Inf" for b in self.buckets]
        
        # Add values
        values = []
        for key in self._values:
            entry = {
                "sum": self._sums[key],
                "count": self._counts[key],
                "buckets": [{"le": b if b != float('inf') else "+Inf", "count": c} 
                            for b, c in zip(self.buckets, self._values[key])]
            }
            
            if key:
                label_pairs = {}
                for label_pair in key.split(","):
                    if "=" in label_pair:
                        label, label_value = label_pair.split("=", 1)
                        label_pairs[label] = label_value
                entry["labels"] = label_pairs
                
            values.append(entry)
                
        base_dict["values"] = values
        return base_dict
    
class MetricsRegistry:
    """Registry for all metrics"""
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        self._metrics: Dict[str, Metric] = {}
        self._lock = threading.RLock()
        
    def register(self, metric: Metric) -> Metric:
        """Register a metric"""
        with self._lock:
            if metric.name in self._metrics:
                raise ValueError(f"Metric with name {metric.name} already exists")
            self._metrics[metric.name] = metric
            return metric
            
    def get_metric(self, name: str) -> Optional[Metric]:
        """Get a metric by name"""
        with self._lock:
            return self._metrics.get(name)
            
    def get_all_metrics(self) -> List[Metric]:
        """Get all metrics"""
        with self._lock:
            return list(self._metrics.values())
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert all metrics to dictionary"""
        with self._lock:
            return {
                "metrics": [metric.to_dict() for metric in self._metrics.values()],
                "timestamp": datetime.now().isoformat()
            }
            
    def counter(self, name: str, description: str, labels: Optional[List[str]] = None) -> Counter:
        """Create and register a counter"""
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Counter):
                    raise ValueError(f"Metric {name} exists but is not a Counter")
                return metric
            
            counter = Counter(name, description, labels)
            self._metrics[name] = counter
            return counter
            
    def gauge(self, name: str, description: str, labels: Optional[List[str]] = None) -> Gauge:
        """Create and register a gauge"""
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Gauge):
                    raise ValueError(f"Metric {name} exists but is not a Gauge")
                return metric
            
            gauge = Gauge(name, description, labels)
            self._metrics[name] = gauge
            return gauge
            
    def histogram(self, name: str, description: str, buckets: List[float], 
                 labels: Optional[List[str]] = None) -> Histogram:
        """Create and register a histogram"""
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Histogram):
                    raise ValueError(f"Metric {name} exists but is not a Histogram")
                return metric
            
            histogram = Histogram(name, description, buckets, labels)
            self._metrics[name] = histogram
            return histogram
    
# Default metrics registry
metrics_registry = MetricsRegistry.get_instance()

def time_metric(metric_name: str, labels: Optional[Dict[str, str]] = None) -> Callable:
    """Decorator to measure execution time of a function"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get or create histogram metric
            metric = metrics_registry.get_metric(metric_name)
            if metric is None:
                metric = metrics_registry.histogram(
                    name=metric_name,
                    description=f"Execution time of {func.__name__}",
                    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
                    labels=["function"] + (list(labels.keys()) if labels else [])
                )
            
            # Combine labels
            combined_labels = {"function": func.__name__}
            if labels:
                combined_labels.update(labels)
            
            # Measure execution time
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                metric.observe(duration, combined_labels)
        
        return wrapper
    
    return decorator

def async_time_metric(metric_name: str, labels: Optional[Dict[str, str]] = None) -> Callable:
    """Decorator to measure execution time of an async function"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get or create histogram metric
            metric = metrics_registry.get_metric(metric_name)
            if metric is None:
                metric = metrics_registry.histogram(
                    name=metric_name,
                    description=f"Execution time of {func.__name__}",
                    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
                    labels=["function"] + (list(labels.keys()) if labels else [])
                )
            
            # Combine labels
            combined_labels = {"function": func.__name__}
            if labels:
                combined_labels.update(labels)
            
            # Measure execution time
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                metric.observe(duration, combined_labels)
        
        return wrapper
    
    return decorator

# Initialize some default metrics
opcua_connection_attempts = metrics_registry.counter(
    name="opcua_connection_attempts_total",
    description="Total number of connection attempts to OPC-UA server",
    labels=["success", "url"]
)

opcua_requests_total = metrics_registry.counter(
    name="opcua_requests_total",
    description="Total number of requests to OPC-UA server",
    labels=["operation", "success"]
)

opcua_request_duration = metrics_registry.histogram(
    name="opcua_request_duration_seconds",
    description="Duration of OPC-UA requests",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
    labels=["operation"]
)

opcua_connection_status = metrics_registry.gauge(
    name="opcua_connection_status",
    description="Status of OPC-UA connection (1=connected, 0=disconnected)",
    labels=["url"]
)

polling_task_count = metrics_registry.gauge(
    name="polling_task_count",
    description="Number of active polling tasks",
)

subscription_count = metrics_registry.gauge(
    name="subscription_count",
    description="Number of active subscriptions",
)

def get_metrics() -> Dict[str, Any]:
    """Get all metrics as dictionary"""
    return metrics_registry.to_dict() 