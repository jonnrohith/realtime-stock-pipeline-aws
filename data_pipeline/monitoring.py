"""
Data Pipeline Monitoring and Alerting
"""
import psutil
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import structlog
import json
from pathlib import Path

from .models import (
    Alert, PipelineJob, PipelineStatus, DataQualityCheck, 
    DataQualityStatus, PipelineMetrics
)

logger = structlog.get_logger()


class PipelineMonitor:
    """Pipeline monitoring and alerting system."""
    
    def __init__(self, config):
        self.config = config
        self.alerts: Dict[str, Alert] = {}
        self.metrics: List[PipelineMetrics] = []
        self.alert_thresholds = {
            "job_failure_rate": 0.1,  # 10%
            "data_quality_score": 0.8,  # 80%
            "memory_usage": 0.8,  # 80%
            "cpu_usage": 0.8,  # 80%
            "api_error_rate": 0.05,  # 5%
            "processing_time": 300  # 5 minutes
        }
    
    def monitor_job(self, job: PipelineJob) -> List[Alert]:
        """Monitor a pipeline job and generate alerts if needed."""
        alerts = []
        
        # Check job status
        if job.status == PipelineStatus.FAILED:
            alert = self._create_alert(
                alert_type="error",
                severity="high",
                title=f"Job Failed: {job.job_id}",
                message=f"Job {job.job_id} failed: {job.error_message}",
                data_source=job.data_source,
                job_id=job.job_id
            )
            alerts.append(alert)
        
        # Check processing time
        if job.duration_seconds and job.duration_seconds > self.alert_thresholds["processing_time"]:
            alert = self._create_alert(
                alert_type="warning",
                severity="medium",
                title=f"Slow Job: {job.job_id}",
                message=f"Job {job.job_id} took {job.duration_seconds:.2f} seconds",
                data_source=job.data_source,
                job_id=job.job_id
            )
            alerts.append(alert)
        
        # Check failure rate
        failure_rate = self._calculate_failure_rate(job.data_source)
        if failure_rate > self.alert_thresholds["job_failure_rate"]:
            alert = self._create_alert(
                alert_type="warning",
                severity="medium",
                title=f"High Failure Rate: {job.data_source}",
                message=f"Failure rate for {job.data_source} is {failure_rate:.1%}",
                data_source=job.data_source
            )
            alerts.append(alert)
        
        # Store alerts
        for alert in alerts:
            self.alerts[alert.alert_id] = alert
        
        return alerts
    
    def monitor_data_quality(self, quality_checks: List[DataQualityCheck]) -> List[Alert]:
        """Monitor data quality and generate alerts."""
        alerts = []
        
        for check in quality_checks:
            if check.status == DataQualityStatus.ERROR:
                alert = self._create_alert(
                    alert_type="error",
                    severity="high",
                    title=f"Data Quality Error: {check.data_source}",
                    message=f"{check.check_type}: {check.message}",
                    data_source=check.data_source
                )
                alerts.append(alert)
            elif check.status == DataQualityStatus.WARNING:
                alert = self._create_alert(
                    alert_type="warning",
                    severity="medium",
                    title=f"Data Quality Warning: {check.data_source}",
                    message=f"{check.check_type}: {check.message}",
                    data_source=check.data_source
                )
                alerts.append(alert)
        
        # Check overall data quality score
        avg_quality_score = self._calculate_average_quality_score(quality_checks)
        if avg_quality_score < self.alert_thresholds["data_quality_score"]:
            alert = self._create_alert(
                alert_type="warning",
                severity="medium",
                title="Low Data Quality Score",
                message=f"Average data quality score is {avg_quality_score:.1%}",
            )
            alerts.append(alert)
        
        # Store alerts
        for alert in alerts:
            self.alerts[alert.alert_id] = alert
        
        return alerts
    
    def monitor_system_resources(self) -> List[Alert]:
        """Monitor system resources and generate alerts."""
        alerts = []
        
        # Memory usage
        memory_percent = psutil.virtual_memory().percent / 100
        if memory_percent > self.alert_thresholds["memory_usage"]:
            alert = self._create_alert(
                alert_type="warning",
                severity="medium",
                title="High Memory Usage",
                message=f"Memory usage is {memory_percent:.1%}",
            )
            alerts.append(alert)
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1) / 100
        if cpu_percent > self.alert_thresholds["cpu_usage"]:
            alert = self._create_alert(
                alert_type="warning",
                severity="medium",
                title="High CPU Usage",
                message=f"CPU usage is {cpu_percent:.1%}",
            )
            alerts.append(alert)
        
        # Disk usage
        disk_percent = psutil.disk_usage('/').percent / 100
        if disk_percent > 0.9:  # 90%
            alert = self._create_alert(
                alert_type="error",
                severity="high",
                title="High Disk Usage",
                message=f"Disk usage is {disk_percent:.1%}",
            )
            alerts.append(alert)
        
        # Store alerts
        for alert in alerts:
            self.alerts[alert.alert_id] = alert
        
        return alerts
    
    def collect_metrics(self, job: PipelineJob) -> PipelineMetrics:
        """Collect metrics for a pipeline job."""
        # Get system metrics
        memory_info = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent()
        
        metrics = PipelineMetrics(
            job_id=job.job_id,
            data_source=job.data_source,
            records_processed=job.processed_items,
            records_failed=job.failed_items,
            processing_time_seconds=job.duration_seconds or 0,
            memory_usage_mb=memory_info.used / (1024 * 1024),
            cpu_usage_percent=cpu_percent
        )
        
        # Calculate data quality score
        if job.processed_items > 0:
            metrics.data_quality_score = job.processed_items / (job.processed_items + job.failed_items)
        
        self.metrics.append(metrics)
        
        # Keep only last 1000 metrics
        if len(self.metrics) > 1000:
            self.metrics = self.metrics[-1000:]
        
        return metrics
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary."""
        total_alerts = len(self.alerts)
        unresolved_alerts = len([a for a in self.alerts.values() if not a.resolved])
        
        alerts_by_type = {}
        alerts_by_severity = {}
        
        for alert in self.alerts.values():
            alerts_by_type[alert.alert_type] = alerts_by_type.get(alert.alert_type, 0) + 1
            alerts_by_severity[alert.severity] = alerts_by_severity.get(alert.severity, 0) + 1
        
        return {
            "total_alerts": total_alerts,
            "unresolved_alerts": unresolved_alerts,
            "alerts_by_type": alerts_by_type,
            "alerts_by_severity": alerts_by_severity,
            "recent_alerts": self._get_recent_alerts(5)
        }
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        if not self.metrics:
            return {"message": "No metrics available"}
        
        recent_metrics = self.metrics[-100:]  # Last 100 metrics
        
        return {
            "total_jobs": len(self.metrics),
            "avg_processing_time": sum(m.processing_time_seconds for m in recent_metrics) / len(recent_metrics),
            "avg_records_processed": sum(m.records_processed for m in recent_metrics) / len(recent_metrics),
            "avg_data_quality_score": sum(m.data_quality_score or 0 for m in recent_metrics) / len(recent_metrics),
            "avg_memory_usage": sum(m.memory_usage_mb or 0 for m in recent_metrics) / len(recent_metrics),
            "avg_cpu_usage": sum(m.cpu_usage_percent or 0 for m in recent_metrics) / len(recent_metrics)
        }
    
    def resolve_alert(self, alert_id: str):
        """Resolve an alert."""
        if alert_id in self.alerts:
            self.alerts[alert_id].resolved = True
            self.alerts[alert_id].resolved_at = datetime.utcnow()
            logger.info("Alert resolved", alert_id=alert_id)
    
    def cleanup_old_alerts(self, days: int = 30):
        """Clean up old alerts."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        old_alerts = [
            alert_id for alert_id, alert in self.alerts.items()
            if alert.created_at < cutoff_date
        ]
        
        for alert_id in old_alerts:
            del self.alerts[alert_id]
        
        logger.info("Cleaned up old alerts", count=len(old_alerts))
    
    def export_metrics(self, file_path: str):
        """Export metrics to file."""
        metrics_data = [metric.dict() for metric in self.metrics]
        
        with open(file_path, 'w') as f:
            json.dump(metrics_data, f, indent=2, default=str)
        
        logger.info("Metrics exported", file_path=file_path)
    
    def _create_alert(self, alert_type: str, severity: str, title: str, 
                     message: str, data_source: Optional[str] = None, 
                     job_id: Optional[str] = None) -> Alert:
        """Create a new alert."""
        alert_id = f"{alert_type}_{int(datetime.utcnow().timestamp())}"
        
        return Alert(
            alert_id=alert_id,
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=message,
            data_source=data_source,
            job_id=job_id
        )
    
    def _calculate_failure_rate(self, data_source: str) -> float:
        """Calculate failure rate for a data source."""
        # This would need access to job history
        # For now, return 0
        return 0.0
    
    def _calculate_average_quality_score(self, quality_checks: List[DataQualityCheck]) -> float:
        """Calculate average data quality score."""
        if not quality_checks:
            return 1.0
        
        # Simple scoring based on status
        scores = []
        for check in quality_checks:
            if check.status == DataQualityStatus.VALID:
                scores.append(1.0)
            elif check.status == DataQualityStatus.WARNING:
                scores.append(0.7)
            elif check.status == DataQualityStatus.ERROR:
                scores.append(0.3)
            else:
                scores.append(0.5)
        
        return sum(scores) / len(scores)
    
    def _get_recent_alerts(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        recent_alerts = sorted(
            self.alerts.values(),
            key=lambda x: x.created_at,
            reverse=True
        )[:limit]
        
        return [alert.dict() for alert in recent_alerts]
