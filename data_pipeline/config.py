"""
Data Pipeline Configuration
"""
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    
    # API Configuration
    rapidapi_key: str = os.getenv("RAPIDAPI_KEY", "")
    yahoo_api_base_url: str = os.getenv("YAHOO_API_BASE_URL", "https://yahoo-finance15.p.rapidapi.com")
    
    # Data Storage
    data_dir: str = "data"
    raw_data_dir: str = "data/raw"
    processed_data_dir: str = "data/processed"
    output_dir: str = "output"
    
    # Pipeline Settings
    batch_size: int = 100
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Rate Limiting
    requests_per_minute: int = 60
    delay_between_requests: float = 1.0
    
    # Data Retention
    raw_data_retention_days: int = 30
    processed_data_retention_days: int = 90
    
    # Monitoring
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    enable_monitoring: bool = True
    
    # Data Sources
    enabled_sources: List[str] = None
    
    def __post_init__(self):
        if self.enabled_sources is None:
            self.enabled_sources = [
                "market_tickers",
                "stock_quotes", 
                "stock_history",
                "market_screeners",
                "stock_news"
            ]
        
        # Create directories
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)


@dataclass
class DataSourceConfig:
    """Configuration for specific data sources."""
    
    # Market Tickers
    tickers_pages: int = 5  # Number of pages to fetch
    tickers_types: List[str] = None  # STOCKS, ETFs, CRYPTO
    
    # Stock Quotes
    quote_symbols: List[str] = None  # Symbols to track
    quote_update_frequency: int = 300  # seconds
    
    # Stock History
    history_symbols: List[str] = None  # Symbols for historical data
    history_intervals: List[str] = None  # 1d, 1h, 1m
    history_limits: Dict[str, int] = None  # Limit per interval
    
    # Market Screeners
    screener_lists: List[str] = None  # day_gainers, day_losers, most_actives
    screener_update_frequency: int = 600  # seconds
    
    # News
    news_symbols: List[str] = None  # Symbols for news
    news_update_frequency: int = 1800  # seconds
    
    def __post_init__(self):
        if self.tickers_types is None:
            self.tickers_types = ["STOCKS"]
        
        if self.quote_symbols is None:
            self.quote_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        
        if self.history_symbols is None:
            self.history_symbols = ["AAPL", "MSFT", "GOOGL"]
        
        if self.history_intervals is None:
            self.history_intervals = ["1d", "1h"]
        
        if self.history_limits is None:
            self.history_limits = {"1d": 30, "1h": 168, "1m": 1440}
        
        if self.screener_lists is None:
            self.screener_lists = ["day_gainers", "day_losers", "most_actives"]
        
        if self.news_symbols is None:
            self.news_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


@dataclass
class DatabaseConfig:
    """Database configuration."""
    
    # Database type: sqlite, postgresql, mysql
    db_type: str = os.getenv("DB_TYPE", "sqlite")
    db_url: str = os.getenv("DATABASE_URL", "sqlite:///data/finance_data.db")
    
    # Connection settings
    pool_size: int = 5
    max_overflow: int = 10
    echo: bool = False
    
    # Table settings
    create_tables: bool = True
    drop_tables: bool = False


# Global configuration instances
pipeline_config = PipelineConfig()
data_source_config = DataSourceConfig()
database_config = DatabaseConfig()
