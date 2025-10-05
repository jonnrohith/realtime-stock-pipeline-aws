"""
Data models for Yahoo Finance stocks data extraction.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, validator


class Stock(BaseModel):
    """Model for basic stock information."""
    
    # Basic stock information
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    
    # Market information
    market_cap: Optional[float] = None
    currency: str = "USD"
    country: Optional[str] = None
    
    # Company information
    website: Optional[str] = None
    description: Optional[str] = None
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "yahoo_finance_rapidapi"
    
    class Config:
        validate_by_name = True


class StockQuote(BaseModel):
    """Model for real-time stock quote data."""
    
    # Basic information
    symbol: str
    name: Optional[str] = None
    
    # Price data
    price: Optional[float] = None
    previous_close: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    
    # Volume and market data
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    
    # Financial ratios
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    
    # Price changes
    change: Optional[float] = None
    change_percent: Optional[float] = None
    
    # Exchange information
    currency: str = "USD"
    exchange: Optional[str] = None
    quote_type: Optional[str] = None
    
    # Timestamp
    timestamp: Optional[Union[int, str, datetime]] = None
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "yahoo_finance_rapidapi"
    
    class Config:
        validate_by_name = True


class StockHistory(BaseModel):
    """Model for historical stock data."""
    
    symbol: str
    date: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    adjusted_close: Optional[float] = None
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "yahoo_finance_rapidapi"


class MarketTickers(BaseModel):
    """Model for market tickers response."""
    
    total_count: Optional[int] = None
    page: Optional[int] = None
    page_size: Optional[int] = None
    stocks: List[Stock] = Field(default_factory=list)
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "yahoo_finance_rapidapi"


class StockSearchResult(BaseModel):
    """Model for stock search results."""
    
    query: str
    total_results: Optional[int] = None
    stocks: List[Stock] = Field(default_factory=list)
    
    # Search metadata
    search_time: Optional[float] = None
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "yahoo_finance_rapidapi"


class APIResponse(BaseModel):
    """Generic API response wrapper."""
    
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    response_time: Optional[float] = None
    
    # Request metadata
    endpoint: Optional[str] = None
    method: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DataExtractionJob(BaseModel):
    """Model for tracking data extraction jobs."""
    
    job_id: str
    job_type: str  # 'tickers', 'quotes', 'history', 'search'
    status: str  # 'pending', 'running', 'completed', 'failed'
    
    # Job parameters
    parameters: Dict[str, Any] = Field(default_factory=dict)
    
    # Results
    total_items: Optional[int] = None
    processed_items: int = 0
    failed_items: int = 0
    
    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Error handling
    error_message: Optional[str] = None
    retry_count: int = 0
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
