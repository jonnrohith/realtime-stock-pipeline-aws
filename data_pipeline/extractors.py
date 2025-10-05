"""
Data Extractors for Yahoo Finance API
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import json
import structlog

from updated_yahoo_client import UpdatedYahooFinanceAPIClient, UpdatedYahooFinanceDataParser
from .models import (
    PipelineJob, DataSourceType, PipelineStatus, 
    Stock, StockQuote, StockHistory, MarketScreener, StockNews, StockModule
)
from .config import pipeline_config, data_source_config

logger = structlog.get_logger()


class BaseExtractor:
    """Base class for all data extractors."""
    
    def __init__(self, rapidapi_key: str):
        self.client = UpdatedYahooFinanceAPIClient(rapidapi_key)
        self.parser = UpdatedYahooFinanceDataParser()
        self.config = pipeline_config
        self.data_source_config = data_source_config
    
    def create_job(self, job_type: str, data_source: DataSourceType, parameters: Dict[str, Any]) -> PipelineJob:
        """Create a new pipeline job."""
        job_id = f"{data_source.value}_{job_type}_{int(time.time())}"
        return PipelineJob(
            job_id=job_id,
            job_type=job_type,
            data_source=data_source,
            parameters=parameters,
            status=PipelineStatus.PENDING
        )
    
    def save_raw_data(self, data: Any, job: PipelineJob, filename: str) -> str:
        """Save raw data to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = Path(self.config.raw_data_dir) / f"{filename}_{timestamp}.json"
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info("Raw data saved", file_path=str(file_path), job_id=job.job_id)
        return str(file_path)


class MarketTickersExtractor(BaseExtractor):
    """Extractor for market tickers data."""
    
    def extract(self, pages: int = None, types: List[str] = None) -> PipelineJob:
        """Extract market tickers data."""
        if pages is None:
            pages = self.data_source_config.tickers_pages
        if types is None:
            types = self.data_source_config.tickers_types
        
        job = self.create_job("extract", DataSourceType.MARKET_TICKERS, {
            "pages": pages,
            "types": types
        })
        
        try:
            job.status = PipelineStatus.RUNNING
            job.started_at = datetime.utcnow()
            
            all_stocks = []
            raw_data = []
            
            for page in range(1, pages + 1):
                for ticker_type in types:
                    logger.info("Extracting market tickers", 
                               page=page, type=ticker_type, job_id=job.job_id)
                    
                    response = self.client.get_market_tickers(page=page, type_filter=ticker_type)
                    
                    if response.success:
                        stocks = self.parser.parse_tickers_response(response.data)
                        all_stocks.extend(stocks)
                        raw_data.append(response.data)
                        job.processed_items += len(stocks)
                        
                        # Rate limiting
                        time.sleep(self.config.delay_between_requests)
                    else:
                        job.failed_items += 1
                        logger.error("Failed to extract tickers", 
                                   page=page, type=ticker_type, error=response.error)
            
            job.total_items = len(all_stocks)
            
            # Save raw data
            self.save_raw_data(raw_data, job, "market_tickers")
            
            # Save processed data
            self.save_processed_data(all_stocks, job, "market_tickers")
            
            job.status = PipelineStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Market tickers extraction completed", 
                       job_id=job.job_id, total_items=job.total_items)
            
            return job
            
        except Exception as e:
            job.status = PipelineStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Market tickers extraction failed", 
                        job_id=job.job_id, error=str(e))
            return job
    
    def save_processed_data(self, stocks: List[Stock], job: PipelineJob, data_type: str):
        """Save processed stock data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = Path(self.config.processed_data_dir) / f"{data_type}_{timestamp}.json"
        
        data = [stock.dict() for stock in stocks]
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info("Processed data saved", file_path=str(file_path), job_id=job.job_id)


class StockQuotesExtractor(BaseExtractor):
    """Extractor for stock quotes data."""
    
    def extract(self, symbols: List[str] = None) -> PipelineJob:
        """Extract stock quotes data."""
        if symbols is None:
            symbols = self.data_source_config.quote_symbols
        
        job = self.create_job("extract", DataSourceType.STOCK_QUOTES, {
            "symbols": symbols
        })
        
        try:
            job.status = PipelineStatus.RUNNING
            job.started_at = datetime.utcnow()
            
            # Process in batches
            batch_size = self.config.batch_size
            all_quotes = []
            raw_data = []
            
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                
                logger.info("Extracting stock quotes", 
                           symbols=batch_symbols, job_id=job.job_id)
                
                response = self.client.get_multiple_quotes(batch_symbols)
                
                if response.success:
                    quotes = self.parser.parse_quotes_response(response.data)
                    all_quotes.extend(quotes)
                    raw_data.append(response.data)
                    job.processed_items += len(quotes)
                else:
                    job.failed_items += len(batch_symbols)
                    logger.error("Failed to extract quotes", 
                               symbols=batch_symbols, error=response.error)
                
                # Rate limiting
                time.sleep(self.config.delay_between_requests)
            
            job.total_items = len(all_quotes)
            
            # Save data
            self.save_raw_data(raw_data, job, "stock_quotes")
            self.save_processed_data(all_quotes, job, "stock_quotes")
            
            job.status = PipelineStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Stock quotes extraction completed", 
                       job_id=job.job_id, total_items=job.total_items)
            
            return job
            
        except Exception as e:
            job.status = PipelineStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Stock quotes extraction failed", 
                        job_id=job.job_id, error=str(e))
            return job
    
    def save_processed_data(self, quotes: List[StockQuote], job: PipelineJob, data_type: str):
        """Save processed quote data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = Path(self.config.processed_data_dir) / f"{data_type}_{timestamp}.json"
        
        data = [quote.dict() for quote in quotes]
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info("Processed data saved", file_path=str(file_path), job_id=job.job_id)


class StockHistoryExtractor(BaseExtractor):
    """Extractor for stock historical data."""
    
    def extract(self, symbols: List[str] = None, intervals: List[str] = None) -> PipelineJob:
        """Extract stock historical data."""
        if symbols is None:
            symbols = self.data_source_config.history_symbols
        if intervals is None:
            intervals = self.data_source_config.history_intervals
        
        job = self.create_job("extract", DataSourceType.STOCK_HISTORY, {
            "symbols": symbols,
            "intervals": intervals
        })
        
        try:
            job.status = PipelineStatus.RUNNING
            job.started_at = datetime.utcnow()
            
            all_history = []
            raw_data = []
            
            for symbol in symbols:
                for interval in intervals:
                    limit = self.data_source_config.history_limits.get(interval, 30)
                    
                    logger.info("Extracting stock history", 
                               symbol=symbol, interval=interval, limit=limit, job_id=job.job_id)
                    
                    response = self.client.get_stock_history(symbol, interval, limit)
                    
                    if response.success:
                        # Parse historical data
                        history_data = response.data.get("body", [])
                        for record in history_data:
                            history = StockHistory(
                                symbol=symbol,
                                date=datetime.fromtimestamp(record["timestamp_unix"]),
                                open=record.get("open"),
                                high=record.get("high"),
                                low=record.get("low"),
                                close=record.get("close"),
                                volume=record.get("volume"),
                                interval=interval
                            )
                            all_history.append(history)
                        
                        raw_data.append(response.data)
                        job.processed_items += len(history_data)
                    else:
                        job.failed_items += 1
                        logger.error("Failed to extract history", 
                                   symbol=symbol, interval=interval, error=response.error)
                    
                    # Rate limiting
                    time.sleep(self.config.delay_between_requests)
            
            job.total_items = len(all_history)
            
            # Save data
            self.save_raw_data(raw_data, job, "stock_history")
            self.save_processed_data(all_history, job, "stock_history")
            
            job.status = PipelineStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Stock history extraction completed", 
                       job_id=job.job_id, total_items=job.total_items)
            
            return job
            
        except Exception as e:
            job.status = PipelineStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Stock history extraction failed", 
                        job_id=job.job_id, error=str(e))
            return job
    
    def save_processed_data(self, history: List[StockHistory], job: PipelineJob, data_type: str):
        """Save processed history data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = Path(self.config.processed_data_dir) / f"{data_type}_{timestamp}.json"
        
        data = [h.dict() for h in history]
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info("Processed data saved", file_path=str(file_path), job_id=job.job_id)


class MarketScreenerExtractor(BaseExtractor):
    """Extractor for market screener data."""
    
    def extract(self, screener_lists: List[str] = None) -> PipelineJob:
        """Extract market screener data."""
        if screener_lists is None:
            screener_lists = self.data_source_config.screener_lists
        
        job = self.create_job("extract", DataSourceType.MARKET_SCREENERS, {
            "screener_lists": screener_lists
        })
        
        try:
            job.status = PipelineStatus.RUNNING
            job.started_at = datetime.utcnow()
            
            all_screeners = []
            raw_data = []
            
            for screener_type in screener_lists:
                logger.info("Extracting market screener", 
                           screener_type=screener_type, job_id=job.job_id)
                
                response = self.client.get_market_screener(screener_type)
                
                if response.success:
                    screener_data = response.data.get("body", [])
                    for i, record in enumerate(screener_data):
                        screener = MarketScreener(
                            symbol=record.get("symbol"),
                            name=record.get("longName") or record.get("shortName"),
                            price=record.get("regularMarketPrice"),
                            change=record.get("regularMarketChange"),
                            change_percent=record.get("regularMarketChangePercent"),
                            volume=record.get("regularMarketVolume"),
                            market_cap=record.get("marketCap"),
                            screener_type=screener_type,
                            rank=i + 1
                        )
                        all_screeners.append(screener)
                    
                    raw_data.append(response.data)
                    job.processed_items += len(screener_data)
                else:
                    job.failed_items += 1
                    logger.error("Failed to extract screener", 
                               screener_type=screener_type, error=response.error)
                
                # Rate limiting
                time.sleep(self.config.delay_between_requests)
            
            job.total_items = len(all_screeners)
            
            # Save data
            self.save_raw_data(raw_data, job, "market_screeners")
            self.save_processed_data(all_screeners, job, "market_screeners")
            
            job.status = PipelineStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Market screener extraction completed", 
                       job_id=job.job_id, total_items=job.total_items)
            
            return job
            
        except Exception as e:
            job.status = PipelineStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Market screener extraction failed", 
                        job_id=job.job_id, error=str(e))
            return job
    
    def save_processed_data(self, screeners: List[MarketScreener], job: PipelineJob, data_type: str):
        """Save processed screener data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = Path(self.config.processed_data_dir) / f"{data_type}_{timestamp}.json"
        
        data = [s.dict() for s in screeners]
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info("Processed data saved", file_path=str(file_path), job_id=job.job_id)


class StockNewsExtractor(BaseExtractor):
    """Extractor for stock news data."""
    
    def extract(self, symbols: List[str] = None) -> PipelineJob:
        """Extract stock news data."""
        if symbols is None:
            symbols = self.data_source_config.news_symbols
        
        job = self.create_job("extract", DataSourceType.STOCK_NEWS, {
            "symbols": symbols
        })
        
        try:
            job.status = PipelineStatus.RUNNING
            job.started_at = datetime.utcnow()
            
            all_news = []
            raw_data = []
            
            for symbol in symbols:
                logger.info("Extracting stock news", 
                           symbol=symbol, job_id=job.job_id)
                
                response = self.client.get_stock_news([symbol])
                
                if response.success:
                    news_data = response.data.get("body", [])
                    for record in news_data:
                        news = StockNews(
                            symbol=symbol,
                            title=record.get("title"),
                            url=record.get("url"),
                            text=record.get("text"),
                            source=record.get("source"),
                            news_type=record.get("type"),
                            image_url=record.get("img")
                        )
                        all_news.append(news)
                    
                    raw_data.append(response.data)
                    job.processed_items += len(news_data)
                else:
                    job.failed_items += 1
                    logger.error("Failed to extract news", 
                               symbol=symbol, error=response.error)
                
                # Rate limiting
                time.sleep(self.config.delay_between_requests)
            
            job.total_items = len(all_news)
            
            # Save data
            self.save_raw_data(raw_data, job, "stock_news")
            self.save_processed_data(all_news, job, "stock_news")
            
            job.status = PipelineStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Stock news extraction completed", 
                       job_id=job.job_id, total_items=job.total_items)
            
            return job
            
        except Exception as e:
            job.status = PipelineStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Stock news extraction failed", 
                        job_id=job.job_id, error=str(e))
            return job
    
    def save_processed_data(self, news: List[StockNews], job: PipelineJob, data_type: str):
        """Save processed news data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = Path(self.config.processed_data_dir) / f"{data_type}_{timestamp}.json"
        
        data = [n.dict() for n in news]
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info("Processed data saved", file_path=str(file_path), job_id=job.job_id)
