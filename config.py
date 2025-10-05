"""
Configuration management for API data sourcing.
"""
import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class APIConfig(BaseSettings):
    """API configuration settings."""
    
    # Yahoo Finance RapidAPI Settings
    yahoo_api_base_url: str = Field(default="https://yahoo-finance15.p.rapidapi.com", env="YAHOO_API_BASE_URL")
    rapidapi_key: str = Field(default="", env="RAPIDAPI_KEY")
    rapidapi_host: str = Field(default="yahoo-finance15.p.rapidapi.com", env="RAPIDAPI_HOST")
    
    # Legacy Walmart settings (for backward compatibility)
    walmart_api_base_url: str = Field(default="https://walmart-data.p.rapidapi.com", env="WALMART_API_BASE_URL")
    
    # Rate Limiting
    rate_limit_requests_per_minute: int = Field(60, env="RATE_LIMIT_REQUESTS_PER_MINUTE")
    rate_limit_burst: int = Field(10, env="RATE_LIMIT_BURST")
    
    # Retry Configuration
    max_retries: int = Field(3, env="MAX_RETRIES")
    retry_delay: float = Field(1.0, env="RETRY_DELAY")
    
    # Database
    database_url: Optional[str] = Field(None, env="DATABASE_URL")
    
    # Logging
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_format: str = Field("json", env="LOG_FORMAT")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


class DatabaseConfig(BaseSettings):
    """Database configuration settings."""
    
    database_url: str = Field(default="", env="DATABASE_URL")
    pool_size: int = Field(5, env="DB_POOL_SIZE")
    max_overflow: int = Field(10, env="DB_MAX_OVERFLOW")
    echo: bool = Field(False, env="DB_ECHO")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"
        extra = "ignore"


# Global configuration instances
api_config = APIConfig()
db_config = DatabaseConfig()
