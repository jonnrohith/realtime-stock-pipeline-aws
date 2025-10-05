"""
Data Pipeline Models
"""
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from enum import Enum


class DataSourceType(str, Enum):
    """Types of data sources."""
    MARKET_TICKERS = "market_tickers"
    STOCK_QUOTES = "stock_quotes"
    STOCK_HISTORY = "stock_history"
    MARKET_SCREENERS = "market_screeners"
    STOCK_NEWS = "stock_news"
    STOCK_MODULES = "stock_modules"


class PipelineStatus(str, Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DataQualityStatus(str, Enum):
    """Data quality status."""
    VALID = "valid"
    WARNING = "warning"
    ERROR = "error"
    UNKNOWN = "unknown"


# Base Models
class BaseDataModel(BaseModel):
    """Base model for all data entities."""
    id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "yahoo_finance_api"
    
    class Config:
        use_enum_values = True


class Stock(BaseDataModel):
    """Stock information model."""
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    currency: str = "USD"
    country: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None


class StockQuote(BaseDataModel):
    """Real-time stock quote model."""
    symbol: str
    name: Optional[str] = None
    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    currency: str = "USD"
    exchange: Optional[str] = None
    quote_type: Optional[str] = None
    timestamp: Optional[Union[int, str, datetime]] = None


class StockHistory(BaseDataModel):
    """Historical stock data model."""
    symbol: str
    date: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    adjusted_close: Optional[float] = None
    interval: str = "1d"  # 1d, 1h, 1m, etc.


class MarketScreener(BaseDataModel):
    """Market screener data model."""
    symbol: str
    name: Optional[str] = None
    price: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    screener_type: str  # day_gainers, day_losers, most_actives
    rank: Optional[int] = None


class StockNews(BaseDataModel):
    """Stock news model."""
    symbol: str
    title: str
    url: str
    text: Optional[str] = None
    source: Optional[str] = None
    news_type: Optional[str] = None
    published_time: Optional[datetime] = None
    image_url: Optional[str] = None


class StockModule(BaseDataModel):
    """Stock module data model."""
    symbol: str
    module_type: str  # asset-profile, financial-data, etc.
    data: Dict[str, Any] = Field(default_factory=dict)


# Pipeline Models
class PipelineJob(BaseDataModel):
    """Pipeline job model."""
    job_id: str
    job_type: str
    status: PipelineStatus = PipelineStatus.PENDING
    data_source: DataSourceType
    parameters: Dict[str, Any] = Field(default_factory=dict)
    
    # Execution details
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Results
    total_items: Optional[int] = None
    processed_items: int = 0
    failed_items: int = 0
    
    # Error handling
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3


class DataQualityCheck(BaseDataModel):
    """Data quality check model."""
    check_id: str
    data_source: DataSourceType
    check_type: str  # completeness, validity, consistency, etc.
    status: DataQualityStatus
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    checked_at: datetime = Field(default_factory=datetime.utcnow)


class PipelineMetrics(BaseDataModel):
    """Pipeline metrics model."""
    job_id: str
    data_source: DataSourceType
    records_processed: int
    records_failed: int
    processing_time_seconds: float
    data_quality_score: Optional[float] = None
    api_calls_made: int = 0
    api_errors: int = 0
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None


class Alert(BaseDataModel):
    """Alert model for monitoring."""
    alert_id: str
    alert_type: str  # error, warning, info
    severity: str  # critical, high, medium, low
    title: str
    message: str
    data_source: Optional[DataSourceType] = None
    job_id: Optional[str] = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None


# Data Processing Models
class DataBatch(BaseDataModel):
    """Data batch processing model."""
    batch_id: str
    data_source: DataSourceType
    batch_size: int
    records: List[Dict[str, Any]] = Field(default_factory=list)
    processing_status: PipelineStatus = PipelineStatus.PENDING
    quality_checks: List[DataQualityCheck] = Field(default_factory=list)


class DataTransformation(BaseDataModel):
    """Data transformation model."""
    transformation_id: str
    data_source: DataSourceType
    transformation_type: str  # clean, enrich, aggregate, etc.
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    transformation_rules: Dict[str, Any] = Field(default_factory=dict)
    applied_at: datetime = Field(default_factory=datetime.utcnow)


# Configuration Models
class PipelineSchedule(BaseDataModel):
    """Pipeline schedule model."""
    schedule_id: str
    data_source: DataSourceType
    cron_expression: str
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    timezone: str = "UTC"


class DataRetentionPolicy(BaseDataModel):
    """Data retention policy model."""
    data_source: DataSourceType
    retention_days: int
    archive_before_delete: bool = True
    archive_location: Optional[str] = None
    enabled: bool = True
