"""
Main Data Pipeline Orchestrator
"""
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import structlog

from .config import pipeline_config, data_source_config
from .scheduler import PipelineScheduler
from .monitoring import PipelineMonitor
from .transformers import DataTransformer
from .models import DataSourceType, PipelineStatus

logger = structlog.get_logger()


class DataPipeline:
    """Main data pipeline orchestrator."""
    
    def __init__(self, rapidapi_key: str = None):
        self.rapidapi_key = rapidapi_key or os.getenv("RAPIDAPI_KEY")
        if not self.rapidapi_key:
            raise ValueError("RAPIDAPI_KEY is required")
        
        self.config = pipeline_config
        self.data_source_config = data_source_config
        
        # Initialize components
        self.scheduler = PipelineScheduler(self.rapidapi_key)
        self.monitor = PipelineMonitor(self.config)
        self.transformer = DataTransformer()
        
        logger.info("Data pipeline initialized")
    
    def start(self):
        """Start the data pipeline."""
        logger.info("Starting data pipeline")
        
        # Start scheduler
        self.scheduler.start()
        
        # Start monitoring
        self._start_monitoring()
        
        logger.info("Data pipeline started")
    
    def stop(self):
        """Stop the data pipeline."""
        logger.info("Stopping data pipeline")
        
        # Stop scheduler
        self.scheduler.stop()
        
        logger.info("Data pipeline stopped")
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete data pipeline."""
        logger.info("Running full data pipeline")
        
        results = {}
        start_time = time.time()
        
        try:
            # Run all enabled data sources
            for data_source in self.config.enabled_sources:
                if data_source == "market_tickers":
                    job = self.scheduler.run_immediate(DataSourceType.MARKET_TICKERS)
                    results["market_tickers"] = self._process_job(job)
                
                elif data_source == "stock_quotes":
                    job = self.scheduler.run_immediate(DataSourceType.STOCK_QUOTES)
                    results["stock_quotes"] = self._process_job(job)
                
                elif data_source == "stock_history":
                    job = self.scheduler.run_immediate(DataSourceType.STOCK_HISTORY)
                    results["stock_history"] = self._process_job(job)
                
                elif data_source == "market_screeners":
                    job = self.scheduler.run_immediate(DataSourceType.MARKET_SCREENERS)
                    results["market_screeners"] = self._process_job(job)
                
                elif data_source == "stock_news":
                    job = self.scheduler.run_immediate(DataSourceType.STOCK_NEWS)
                    results["stock_news"] = self._process_job(job)
            
            # Calculate total time
            total_time = time.time() - start_time
            
            # Generate summary
            summary = {
                "total_time_seconds": total_time,
                "data_sources_processed": len(results),
                "successful_sources": len([r for r in results.values() if r["success"]]),
                "failed_sources": len([r for r in results.values() if not r["success"]]),
                "results": results
            }
            
            logger.info("Full pipeline completed", summary=summary)
            return summary
            
        except Exception as e:
            logger.error("Full pipeline failed", error=str(e))
            return {"error": str(e), "success": False}
    
    def run_data_source(self, data_source: str, **kwargs) -> Dict[str, Any]:
        """Run a specific data source."""
        logger.info("Running data source", data_source=data_source, kwargs=kwargs)
        
        try:
            if data_source == "market_tickers":
                job = self.scheduler.run_immediate(DataSourceType.MARKET_TICKERS, **kwargs)
            elif data_source == "stock_quotes":
                job = self.scheduler.run_immediate(DataSourceType.STOCK_QUOTES, **kwargs)
            elif data_source == "stock_history":
                job = self.scheduler.run_immediate(DataSourceType.STOCK_HISTORY, **kwargs)
            elif data_source == "market_screeners":
                job = self.scheduler.run_immediate(DataSourceType.MARKET_SCREENERS, **kwargs)
            elif data_source == "stock_news":
                job = self.scheduler.run_immediate(DataSourceType.STOCK_NEWS, **kwargs)
            else:
                raise ValueError(f"Unknown data source: {data_source}")
            
            return self._process_job(job)
            
        except Exception as e:
            logger.error("Data source failed", data_source=data_source, error=str(e))
            return {"error": str(e), "success": False}
    
    def _process_job(self, job) -> Dict[str, Any]:
        """Process a pipeline job."""
        # Monitor job
        alerts = self.monitor.monitor_job(job)
        
        # Collect metrics
        metrics = self.monitor.collect_metrics(job)
        
        # Transform data if job was successful
        if job.status == PipelineStatus.COMPLETED:
            # Load and transform data
            transformed_data = self._transform_data(job)
            
            return {
                "success": True,
                "job_id": job.job_id,
                "data_source": job.data_source,
                "total_items": job.total_items,
                "processed_items": job.processed_items,
                "failed_items": job.failed_items,
                "duration_seconds": job.duration_seconds,
                "alerts": [alert.dict() for alert in alerts],
                "metrics": metrics.dict(),
                "transformed_data": transformed_data
            }
        else:
            return {
                "success": False,
                "job_id": job.job_id,
                "data_source": job.data_source,
                "error": job.error_message,
                "alerts": [alert.dict() for alert in alerts],
                "metrics": metrics.dict()
            }
    
    def _transform_data(self, job) -> Optional[Dict[str, Any]]:
        """Transform data for a job."""
        try:
            # Load raw data
            raw_data = self._load_raw_data(job)
            if not raw_data:
                return None
            
            # Transform data
            transformed = self.transformer.transform_data(raw_data, job.data_source.value)
            
            # Monitor data quality
            quality_checks = [DataQualityCheck(**check) for check in transformed["quality_checks"]]
            self.monitor.monitor_data_quality(quality_checks)
            
            return transformed
            
        except Exception as e:
            logger.error("Data transformation failed", job_id=job.job_id, error=str(e))
            return None
    
    def _load_raw_data(self, job) -> List[Dict[str, Any]]:
        """Load raw data for a job."""
        # This would load the raw data from files
        # For now, return empty list
        return []
    
    def _start_monitoring(self):
        """Start background monitoring."""
        import threading
        
        def monitor_loop():
            while True:
                try:
                    # Monitor system resources
                    self.monitor.monitor_system_resources()
                    
                    # Sleep for 5 minutes
                    time.sleep(300)
                    
                except Exception as e:
                    logger.error("Monitoring error", error=str(e))
                    time.sleep(300)
        
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
    
    def get_status(self) -> Dict[str, Any]:
        """Get pipeline status."""
        return {
            "scheduler_status": self.scheduler.get_schedule_status(),
            "alert_summary": self.monitor.get_alert_summary(),
            "metrics_summary": self.monitor.get_metrics_summary(),
            "recent_jobs": [job.dict() for job in self.scheduler.list_jobs()[:10]]
        }
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        job = self.scheduler.get_job_status(job_id)
        if job:
            return job.dict()
        return None
    
    def list_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs."""
        if status:
            status_enum = PipelineStatus(status)
            jobs = self.scheduler.list_jobs(status_enum)
        else:
            jobs = self.scheduler.list_jobs()
        
        return [job.dict() for job in jobs]
    
    def cleanup(self, days: int = 7):
        """Cleanup old data and jobs."""
        logger.info("Starting cleanup", days=days)
        
        # Cleanup old jobs
        self.scheduler.cleanup_old_jobs(days)
        
        # Cleanup old alerts
        self.monitor.cleanup_old_alerts(days)
        
        logger.info("Cleanup completed")
