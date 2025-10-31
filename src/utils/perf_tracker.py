"""
Performance Tracking Module
Tracks and documents performance metrics for all components
"""
import time
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from pathlib import Path

from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class ComponentMetrics:
    """Metrics for a single component operation."""
    component: str
    operation: str
    start_time: float
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    success: bool = True
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def finish(self, success: bool = True, error: Optional[str] = None, **metadata):
        """Mark operation as complete and calculate duration."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        self.success = success
        self.error = error
        self.metadata.update(metadata)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "component": self.component,
            "operation": self.operation,
            "duration_ms": round(self.duration_ms, 2) if self.duration_ms else None,
            "success": self.success,
            "error": self.error,
            "metadata": self.metadata
        }


@dataclass
class IncidentPerformanceReport:
    """Complete performance report for an incident."""
    incident_id: str
    start_time: float
    end_time: Optional[float] = None
    total_duration_ms: Optional[float] = None
    components: List[ComponentMetrics] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def finish(self):
        """Mark incident processing as complete."""
        self.end_time = time.time()
        self.total_duration_ms = (self.end_time - self.start_time) * 1000
        self._generate_summary()
        
    def _generate_summary(self):
        """Generate performance summary."""
        successful_ops = sum(1 for c in self.components if c.success)
        failed_ops = sum(1 for c in self.components if not c.success)
        
        # Component breakdown
        component_times = {}
        for comp in self.components:
            if comp.component not in component_times:
                component_times[comp.component] = []
            if comp.duration_ms:
                component_times[comp.component].append(comp.duration_ms)
        
        component_summary = {}
        for comp_name, times in component_times.items():
            component_summary[comp_name] = {
                "total_ms": round(sum(times), 2),
                "avg_ms": round(sum(times) / len(times), 2),
                "count": len(times),
                "percentage": round((sum(times) / self.total_duration_ms) * 100, 2) if self.total_duration_ms else 0
            }
        
        self.summary = {
            "total_duration_ms": round(self.total_duration_ms, 2) if self.total_duration_ms else None,
            "total_duration_seconds": round(self.total_duration_ms / 1000, 2) if self.total_duration_ms else None,
            "total_operations": len(self.components),
            "successful_operations": successful_ops,
            "failed_operations": failed_ops,
            "success_rate": round((successful_ops / len(self.components)) * 100, 2) if self.components else 0,
            "component_breakdown": component_summary
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "incident_id": self.incident_id,
            "timestamp": datetime.fromtimestamp(self.start_time, tz=timezone.utc).isoformat(),
            "summary": self.summary,
            "components": [c.to_dict() for c in self.components]
        }


class PerformanceTracker:
    """
    Centralized performance tracking for all components.
    
    Usage:
        tracker = PerformanceTracker(incident_id="INC_001")
        
        # Track an operation
        with tracker.track("llm", "query_generation") as metrics:
            # Do work
            metrics.metadata["tokens"] = 1500
        
        # Or manually
        metrics = tracker.start("signoz", "fetch_logs")
        # Do work
        tracker.finish(metrics, rows_fetched=1000)
        
        # Save report
        tracker.save_report()
    """
    
    def __init__(self, incident_id: str, output_dir: str = "performance_reports"):
        """Initialize performance tracker.
        
        Args:
            incident_id: Unique incident identifier
            output_dir: Directory to save performance reports
        """
        self.incident_id = incident_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.report = IncidentPerformanceReport(
            incident_id=incident_id,
            start_time=time.time()
        )
        
        logger.info(
            "performance_tracker_initialized",
            incident_id=incident_id,
            output_dir=str(self.output_dir)
        )
    
    def start(self, component: str, operation: str, **metadata) -> ComponentMetrics:
        """Start tracking a component operation.
        
        Args:
            component: Component name (e.g., "llm", "signoz", "s3")
            operation: Operation name (e.g., "query_generation", "fetch_logs")
            **metadata: Additional metadata to track
            
        Returns:
            ComponentMetrics instance
        """
        metrics = ComponentMetrics(
            component=component,
            operation=operation,
            start_time=time.time(),
            metadata=metadata
        )
        
        logger.debug(
            "perf_operation_started",
            incident_id=self.incident_id,
            component=component,
            operation=operation
        )
        
        return metrics
    
    def finish(self, metrics: ComponentMetrics, success: bool = True, 
              error: Optional[str] = None, **metadata):
        """Finish tracking a component operation.
        
        Args:
            metrics: ComponentMetrics instance from start()
            success: Whether operation succeeded
            error: Error message if failed
            **metadata: Additional metadata to add
        """
        metrics.finish(success=success, error=error, **metadata)
        self.report.components.append(metrics)
        
        logger.info(
            "perf_operation_completed",
            incident_id=self.incident_id,
            component=metrics.component,
            operation=metrics.operation,
            duration_ms=metrics.duration_ms,
            success=metrics.success
        )
    
    def track(self, component: str, operation: str, **metadata):
        """Context manager for tracking operations.
        
        Args:
            component: Component name
            operation: Operation name
            **metadata: Initial metadata
            
        Usage:
            with tracker.track("llm", "query_generation") as metrics:
                # Do work
                metrics.metadata["tokens"] = 1500
        """
        return _PerformanceContext(self, component, operation, metadata)
    
    def save_report(self, filename: Optional[str] = None) -> str:
        """Save performance report to file.
        
        Args:
            filename: Optional custom filename
            
        Returns:
            Path to saved report file
        """
        self.report.finish()
        
        if not filename:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"perf_{self.incident_id}_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(self.report.to_dict(), f, indent=2)
        
        logger.info(
            "performance_report_saved",
            incident_id=self.incident_id,
            filepath=str(filepath),
            total_duration_ms=self.report.total_duration_ms,
            total_operations=len(self.report.components)
        )
        
        return str(filepath)
    
    def print_summary(self):
        """Print performance summary to console."""
        self.report.finish()
        
        print("\n" + "="*80)
        print("📊 PERFORMANCE REPORT")
        print("="*80)
        print(f"Incident ID: {self.incident_id}")
        print(f"Total Duration: {self.report.summary['total_duration_seconds']:.2f}s")
        print(f"Total Operations: {self.report.summary['total_operations']}")
        print(f"Success Rate: {self.report.summary['success_rate']:.1f}%")
        print()
        
        print("Component Breakdown:")
        for comp_name, stats in self.report.summary['component_breakdown'].items():
            print(f"  {comp_name}:")
            print(f"    Total Time: {stats['total_ms']:.2f}ms ({stats['percentage']:.1f}%)")
            print(f"    Avg Time: {stats['avg_ms']:.2f}ms")
            print(f"    Operations: {stats['count']}")
        
        print("="*80 + "\n")


class _PerformanceContext:
    """Context manager for performance tracking."""
    
    def __init__(self, tracker: PerformanceTracker, component: str, 
                 operation: str, metadata: Dict[str, Any]):
        self.tracker = tracker
        self.component = component
        self.operation = operation
        self.metadata = metadata
        self.metrics = None
    
    def __enter__(self) -> ComponentMetrics:
        self.metrics = self.tracker.start(self.component, self.operation, **self.metadata)
        return self.metrics
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        error = str(exc_val) if exc_val else None
        self.tracker.finish(self.metrics, success=success, error=error)
        return False  # Don't suppress exceptions


