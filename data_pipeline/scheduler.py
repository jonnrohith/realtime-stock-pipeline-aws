"""
Data Pipeline Scheduler
"""
import schedule
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
import structlog

from .models import PipelineJob, DataSourceType, PipelineStatus, PipelineSchedule
from .extractors import (
    MarketTickersExtractor, StockQuotesExtractor, StockHistoryExtractor,
    MarketScreenerExtractor, StockNewsExtractor
)
from .config import pipeline_config, data_source_config

logger = structlog.get_logger()


class PipelineScheduler:
    """Main pipeline scheduler."""
    
    def __init__(self, rapidapi_key: str):
        self.rapidapi_key = rapidapi_key
        self.config = pipeline_config
        self.data_source_config = data_source_config
        self.running = False
        self.scheduler_thread = None
        self.jobs: Dict[str, PipelineJob] = {}
        self.schedules: Dict[str, PipelineSchedule] = {}
        
        # Initialize extractors
        self.extractors = {
            DataSourceType.MARKET_TICKERS: MarketTickersExtractor(rapidapi_key),
            DataSourceType.STOCK_QUOTES: StockQuotesExtractor(rapidapi_key),
            DataSourceType.STOCK_HISTORY: StockHistoryExtractor(rapidapi_key),
            DataSourceType.MARKET_SCREENERS: MarketScreenerExtractor(rapidapi_key),
            DataSourceType.STOCK_NEWS: StockNewsExtractor(rapidapi_key)
        }
        
        # Setup default schedules
        self._setup_default_schedules()
    
    def _setup_default_schedules(self):
        """Setup default pipeline schedules."""
        default_schedules = [
            # Market tickers - daily at 6 AM
            PipelineSchedule(
                schedule_id="market_tickers_daily",
                data_source=DataSourceType.MARKET_TICKERS,
                cron_expression="0 6 * * *",
                enabled=True
            ),
            
            # Stock quotes - every 5 minutes during market hours
            PipelineSchedule(
                schedule_id="stock_quotes_frequent",
                data_source=DataSourceType.STOCK_QUOTES,
                cron_expression="*/5 9-16 * * 1-5",  # Every 5 min, 9 AM - 4 PM, Mon-Fri
                enabled=True
            ),
            
            # Stock history - daily at 7 PM
            PipelineSchedule(
                schedule_id="stock_history_daily",
                data_source=DataSourceType.STOCK_HISTORY,
                cron_expression="0 19 * * *",
                enabled=True
            ),
            
            # Market screeners - every 15 minutes during market hours
            PipelineSchedule(
                schedule_id="market_screeners_frequent",
                data_source=DataSourceType.MARKET_SCREENERS,
                cron_expression="*/15 9-16 * * 1-5",  # Every 15 min, 9 AM - 4 PM, Mon-Fri
                enabled=True
            ),
            
            # Stock news - every 30 minutes
            PipelineSchedule(
                schedule_id="stock_news_frequent",
                data_source=DataSourceType.STOCK_NEWS,
                cron_expression="*/30 * * * *",
                enabled=True
            )
        ]
        
        for schedule in default_schedules:
            self.schedules[schedule.schedule_id] = schedule
    
    def add_schedule(self, schedule: PipelineSchedule):
        """Add a new schedule."""
        self.schedules[schedule.schedule_id] = schedule
        logger.info("Schedule added", schedule_id=schedule.schedule_id)
    
    def remove_schedule(self, schedule_id: str):
        """Remove a schedule."""
        if schedule_id in self.schedules:
            del self.schedules[schedule_id]
            logger.info("Schedule removed", schedule_id=schedule_id)
    
    def enable_schedule(self, schedule_id: str):
        """Enable a schedule."""
        if schedule_id in self.schedules:
            self.schedules[schedule_id].enabled = True
            logger.info("Schedule enabled", schedule_id=schedule_id)
    
    def disable_schedule(self, schedule_id: str):
        """Disable a schedule."""
        if schedule_id in self.schedules:
            self.schedules[schedule_id].enabled = False
            logger.info("Schedule disabled", schedule_id=schedule_id)
    
    def start(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        logger.info("Pipeline scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        logger.info("Pipeline scheduler stopped")
    
    def _run_scheduler(self):
        """Main scheduler loop."""
        while self.running:
            try:
                # Check for scheduled jobs
                self._check_scheduled_jobs()
                
                # Sleep for 1 minute
                time.sleep(60)
                
            except Exception as e:
                logger.error("Error in scheduler loop", error=str(e))
                time.sleep(60)
    
    def _check_scheduled_jobs(self):
        """Check for jobs that should be executed."""
        current_time = datetime.now()
        
        for schedule_id, schedule in self.schedules.items():
            if not schedule.enabled:
                continue
            
            # Check if it's time to run this schedule
            if self._should_run_schedule(schedule, current_time):
                self._execute_schedule(schedule)
    
    def _should_run_schedule(self, schedule: PipelineSchedule, current_time: datetime) -> bool:
        """Check if a schedule should run now."""
        # Simple implementation - in production, use a proper cron parser
        if schedule.cron_expression == "0 6 * * *":  # Daily at 6 AM
            return current_time.hour == 6 and current_time.minute == 0
        elif schedule.cron_expression == "*/5 9-16 * * 1-5":  # Every 5 min, 9 AM - 4 PM, Mon-Fri
            return (9 <= current_time.hour < 16 and 
                   current_time.weekday() < 5 and 
                   current_time.minute % 5 == 0)
        elif schedule.cron_expression == "0 19 * * *":  # Daily at 7 PM
            return current_time.hour == 19 and current_time.minute == 0
        elif schedule.cron_expression == "*/15 9-16 * * 1-5":  # Every 15 min, 9 AM - 4 PM, Mon-Fri
            return (9 <= current_time.hour < 16 and 
                   current_time.weekday() < 5 and 
                   current_time.minute % 15 == 0)
        elif schedule.cron_expression == "*/30 * * * *":  # Every 30 minutes
            return current_time.minute % 30 == 0
        
        return False
    
    def _execute_schedule(self, schedule: PipelineSchedule):
        """Execute a scheduled job."""
        logger.info("Executing scheduled job", 
                   schedule_id=schedule.schedule_id, 
                   data_source=schedule.data_source)
        
        try:
            # Update schedule last run time
            schedule.last_run = datetime.utcnow()
            
            # Execute the appropriate extractor
            if schedule.data_source in self.extractors:
                extractor = self.extractors[schedule.data_source]
                
                # Run extraction based on data source
                if schedule.data_source == DataSourceType.MARKET_TICKERS:
                    job = extractor.extract()
                elif schedule.data_source == DataSourceType.STOCK_QUOTES:
                    job = extractor.extract()
                elif schedule.data_source == DataSourceType.STOCK_HISTORY:
                    job = extractor.extract()
                elif schedule.data_source == DataSourceType.MARKET_SCREENERS:
                    job = extractor.extract()
                elif schedule.data_source == DataSourceType.STOCK_NEWS:
                    job = extractor.extract()
                else:
                    logger.error("Unknown data source", data_source=schedule.data_source)
                    return
                
                # Store job result
                self.jobs[job.job_id] = job
                
                logger.info("Scheduled job completed", 
                           schedule_id=schedule.schedule_id,
                           job_id=job.job_id,
                           status=job.status)
                
            else:
                logger.error("No extractor found for data source", 
                           data_source=schedule.data_source)
                
        except Exception as e:
            logger.error("Error executing scheduled job", 
                        schedule_id=schedule.schedule_id, 
                        error=str(e))
    
    def run_immediate(self, data_source: DataSourceType, **kwargs) -> PipelineJob:
        """Run a job immediately."""
        logger.info("Running immediate job", data_source=data_source, kwargs=kwargs)
        
        if data_source not in self.extractors:
            raise ValueError(f"No extractor found for data source: {data_source}")
        
        extractor = self.extractors[data_source]
        
        # Run extraction
        if data_source == DataSourceType.MARKET_TICKERS:
            job = extractor.extract(**kwargs)
        elif data_source == DataSourceType.STOCK_QUOTES:
            job = extractor.extract(**kwargs)
        elif data_source == DataSourceType.STOCK_HISTORY:
            job = extractor.extract(**kwargs)
        elif data_source == DataSourceType.MARKET_SCREENERS:
            job = extractor.extract(**kwargs)
        elif data_source == DataSourceType.STOCK_NEWS:
            job = extractor.extract(**kwargs)
        else:
            raise ValueError(f"Unknown data source: {data_source}")
        
        # Store job result
        self.jobs[job.job_id] = job
        
        logger.info("Immediate job completed", 
                   job_id=job.job_id, 
                   status=job.status)
        
        return job
    
    def get_job_status(self, job_id: str) -> Optional[PipelineJob]:
        """Get job status."""
        return self.jobs.get(job_id)
    
    def list_jobs(self, status: Optional[PipelineStatus] = None) -> List[PipelineJob]:
        """List jobs, optionally filtered by status."""
        jobs = list(self.jobs.values())
        
        if status:
            jobs = [job for job in jobs if job.status == status]
        
        return sorted(jobs, key=lambda x: x.created_at, reverse=True)
    
    def get_schedule_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all schedules."""
        status = {}
        
        for schedule_id, schedule in self.schedules.items():
            status[schedule_id] = {
                "enabled": schedule.enabled,
                "data_source": schedule.data_source,
                "cron_expression": schedule.cron_expression,
                "last_run": schedule.last_run.isoformat() if schedule.last_run else None,
                "next_run": schedule.next_run.isoformat() if schedule.next_run else None
            }
        
        return status
    
    def cleanup_old_jobs(self, days: int = 7):
        """Clean up old job records."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        old_jobs = [
            job_id for job_id, job in self.jobs.items()
            if job.created_at < cutoff_date
        ]
        
        for job_id in old_jobs:
            del self.jobs[job_id]
        
        logger.info("Cleaned up old jobs", count=len(old_jobs))
