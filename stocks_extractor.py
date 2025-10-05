"""
Data extraction and transformation logic for Yahoo Finance stocks data.
"""
import asyncio
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

import pandas as pd
import structlog

from yahoo_client import YahooFinanceAPIClient, YahooFinanceDataParser
from stocks_models import Stock, StockQuote, StockHistory, MarketTickers, StockSearchResult, DataExtractionJob
from config import api_config

logger = structlog.get_logger()


class YahooFinanceDataExtractor:
    """Main class for extracting and transforming Yahoo Finance stocks data."""
    
    def __init__(self, rapidapi_key: str):
        self.client = YahooFinanceAPIClient(rapidapi_key)
        self.parser = YahooFinanceDataParser()
        self.jobs: Dict[str, DataExtractionJob] = {}
    
    def extract_market_tickers(
        self, 
        page: int = 1,
        type_filter: str = "STOCKS",
        job_id: Optional[str] = None,
        save_to_file: bool = True,
        output_format: str = "json"
    ) -> DataExtractionJob:
        """
        Extract market tickers from Yahoo Finance.
        
        Args:
            page: Page number for pagination
            type_filter: Type of securities to fetch (STOCKS, ETFs, etc.)
            job_id: Optional job ID for tracking
            save_to_file: Whether to save results to file
            output_format: Output format ('json', 'csv', 'parquet')
        
        Returns:
            DataExtractionJob with extraction results
        """
        if job_id is None:
            job_id = f"tickers_{int(time.time())}"
        
        job = DataExtractionJob(
            job_id=job_id,
            job_type="tickers",
            status="running",
            parameters={"page": page, "type_filter": type_filter, "output_format": output_format},
            started_at=datetime.utcnow()
        )
        self.jobs[job_id] = job
        
        try:
            logger.info("Starting market tickers extraction", job_id=job_id, page=page, type_filter=type_filter)
            
            # Fetch tickers from API
            api_response = self.client.get_market_tickers(page=page, type_filter=type_filter)
            
            if not api_response.success:
                job.status = "failed"
                job.error_message = api_response.error
                job.completed_at = datetime.utcnow()
                logger.error("Tickers extraction failed", job_id=job_id, error=api_response.error)
                return job
            
            # Parse the response
            stocks = self.parser.parse_tickers_response(api_response.data)
            job.total_items = len(stocks)
            job.processed_items = len(stocks)
            
            # Save to file if requested
            if save_to_file:
                self._save_tickers_data(stocks, job_id, output_format)
            
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Tickers extraction completed", 
                       job_id=job_id, 
                       total_items=job.total_items,
                       duration=job.duration_seconds)
            
            return job
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Tickers extraction failed with exception", 
                        job_id=job_id, 
                        error=str(e))
            return job
    
    def extract_stock_quotes(
        self,
        symbols: List[str],
        job_id: Optional[str] = None,
        save_to_file: bool = True,
        output_format: str = "json"
    ) -> DataExtractionJob:
        """
        Extract real-time quotes for multiple stocks.
        
        Args:
            symbols: List of stock symbols
            job_id: Optional job ID for tracking
            save_to_file: Whether to save results to file
            output_format: Output format ('json', 'csv', 'parquet')
        
        Returns:
            DataExtractionJob with extraction results
        """
        if job_id is None:
            job_id = f"quotes_{int(time.time())}"
        
        job = DataExtractionJob(
            job_id=job_id,
            job_type="quotes",
            status="running",
            parameters={"symbols": symbols, "output_format": output_format},
            started_at=datetime.utcnow()
        )
        self.jobs[job_id] = job
        
        try:
            logger.info("Starting stock quotes extraction", job_id=job_id, symbols=symbols)
            
            # Fetch quotes from API
            api_response = self.client.get_stock_quotes(symbols)
            
            if not api_response.success:
                job.status = "failed"
                job.error_message = api_response.error
                job.completed_at = datetime.utcnow()
                logger.error("Quotes extraction failed", job_id=job_id, error=api_response.error)
                return job
            
            # Parse the response
            quotes = self.parser.parse_quotes_response(api_response.data)
            job.total_items = len(quotes)
            job.processed_items = len(quotes)
            
            # Save to file if requested
            if save_to_file:
                self._save_quotes_data(quotes, job_id, output_format)
            
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Quotes extraction completed", 
                       job_id=job_id, 
                       total_items=job.total_items,
                       duration=job.duration_seconds)
            
            return job
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Quotes extraction failed with exception", 
                        job_id=job_id, 
                        error=str(e))
            return job
    
    def extract_stock_history(
        self,
        symbol: str,
        period: str = "1mo",
        interval: str = "1d",
        job_id: Optional[str] = None,
        save_to_file: bool = True,
        output_format: str = "json"
    ) -> DataExtractionJob:
        """
        Extract historical data for a stock.
        
        Args:
            symbol: Stock symbol
            period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
            job_id: Optional job ID for tracking
            save_to_file: Whether to save results to file
            output_format: Output format ('json', 'csv', 'parquet')
        
        Returns:
            DataExtractionJob with extraction results
        """
        if job_id is None:
            job_id = f"history_{symbol}_{int(time.time())}"
        
        job = DataExtractionJob(
            job_id=job_id,
            job_type="history",
            status="running",
            parameters={"symbol": symbol, "period": period, "interval": interval, "output_format": output_format},
            started_at=datetime.utcnow()
        )
        self.jobs[job_id] = job
        
        try:
            logger.info("Starting stock history extraction", job_id=job_id, symbol=symbol, period=period)
            
            # Fetch history from API
            api_response = self.client.get_stock_history(symbol=symbol, period=period, interval=interval)
            
            if not api_response.success:
                job.status = "failed"
                job.error_message = api_response.error
                job.completed_at = datetime.utcnow()
                logger.error("History extraction failed", job_id=job_id, error=api_response.error)
                return job
            
            # Parse the response
            history_data = api_response.data  # Raw data for now
            job.total_items = len(history_data.get("body", [])) if isinstance(history_data, dict) else 0
            job.processed_items = job.total_items
            
            # Save to file if requested
            if save_to_file:
                self._save_history_data(history_data, symbol, job_id, output_format)
            
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("History extraction completed", 
                       job_id=job_id, 
                       total_items=job.total_items,
                       duration=job.duration_seconds)
            
            return job
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("History extraction failed with exception", 
                        job_id=job_id, 
                        error=str(e))
            return job
    
    def search_stocks(
        self,
        query: str,
        job_id: Optional[str] = None,
        save_to_file: bool = True,
        output_format: str = "json"
    ) -> DataExtractionJob:
        """
        Search for stocks.
        
        Args:
            query: Search query string
            job_id: Optional job ID for tracking
            save_to_file: Whether to save results to file
            output_format: Output format ('json', 'csv', 'parquet')
        
        Returns:
            DataExtractionJob with extraction results
        """
        if job_id is None:
            job_id = f"search_{int(time.time())}"
        
        job = DataExtractionJob(
            job_id=job_id,
            job_type="search",
            status="running",
            parameters={"query": query, "output_format": output_format},
            started_at=datetime.utcnow()
        )
        self.jobs[job_id] = job
        
        try:
            logger.info("Starting stock search", job_id=job_id, query=query)
            
            # Fetch search results from API
            api_response = self.client.search_stocks(query)
            
            if not api_response.success:
                job.status = "failed"
                job.error_message = api_response.error
                job.completed_at = datetime.utcnow()
                logger.error("Stock search failed", job_id=job_id, error=api_response.error)
                return job
            
            # Parse the response
            search_data = api_response.data  # Raw data for now
            job.total_items = len(search_data.get("body", [])) if isinstance(search_data, dict) else 0
            job.processed_items = job.total_items
            
            # Save to file if requested
            if save_to_file:
                self._save_search_data(search_data, query, job_id, output_format)
            
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.info("Stock search completed", 
                       job_id=job_id, 
                       total_items=job.total_items,
                       duration=job.duration_seconds)
            
            return job
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            job.duration_seconds = (job.completed_at - job.started_at).total_seconds()
            
            logger.error("Stock search failed with exception", 
                        job_id=job_id, 
                        error=str(e))
            return job
    
    def _save_tickers_data(self, stocks: List[Stock], job_id: str, output_format: str):
        """Save tickers data to file."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == "json":
            file_path = output_dir / f"tickers_{job_id}_{timestamp}.json"
            with open(file_path, 'w') as f:
                json.dump([stock.dict() for stock in stocks], f, indent=2, default=str)
        
        elif output_format == "csv":
            file_path = output_dir / f"tickers_{job_id}_{timestamp}.csv"
            df = pd.DataFrame([stock.dict() for stock in stocks])
            df.to_csv(file_path, index=False)
        
        elif output_format == "parquet":
            file_path = output_dir / f"tickers_{job_id}_{timestamp}.parquet"
            df = pd.DataFrame([stock.dict() for stock in stocks])
            df.to_parquet(file_path, index=False)
        
        logger.info("Tickers data saved", job_id=job_id, file_path=str(file_path))
    
    def _save_quotes_data(self, quotes: List[StockQuote], job_id: str, output_format: str):
        """Save quotes data to file."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == "json":
            file_path = output_dir / f"quotes_{job_id}_{timestamp}.json"
            with open(file_path, 'w') as f:
                json.dump([quote.dict() for quote in quotes], f, indent=2, default=str)
        
        elif output_format == "csv":
            file_path = output_dir / f"quotes_{job_id}_{timestamp}.csv"
            df = pd.DataFrame([quote.dict() for quote in quotes])
            df.to_csv(file_path, index=False)
        
        elif output_format == "parquet":
            file_path = output_dir / f"quotes_{job_id}_{timestamp}.parquet"
            df = pd.DataFrame([quote.dict() for quote in quotes])
            df.to_parquet(file_path, index=False)
        
        logger.info("Quotes data saved", job_id=job_id, file_path=str(file_path))
    
    def _save_history_data(self, history_data: Dict[str, Any], symbol: str, job_id: str, output_format: str):
        """Save history data to file."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == "json":
            file_path = output_dir / f"history_{symbol}_{job_id}_{timestamp}.json"
            with open(file_path, 'w') as f:
                json.dump(history_data, f, indent=2, default=str)
        
        elif output_format == "csv":
            file_path = output_dir / f"history_{symbol}_{job_id}_{timestamp}.csv"
            if isinstance(history_data, dict) and "body" in history_data:
                df = pd.DataFrame(history_data["body"])
                df.to_csv(file_path, index=False)
            else:
                df = pd.DataFrame([history_data])
                df.to_csv(file_path, index=False)
        
        elif output_format == "parquet":
            file_path = output_dir / f"history_{symbol}_{job_id}_{timestamp}.parquet"
            if isinstance(history_data, dict) and "body" in history_data:
                df = pd.DataFrame(history_data["body"])
                df.to_parquet(file_path, index=False)
            else:
                df = pd.DataFrame([history_data])
                df.to_parquet(file_path, index=False)
        
        logger.info("History data saved", job_id=job_id, file_path=str(file_path))
    
    def _save_search_data(self, search_data: Dict[str, Any], query: str, job_id: str, output_format: str):
        """Save search data to file."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == "json":
            file_path = output_dir / f"search_{job_id}_{timestamp}.json"
            with open(file_path, 'w') as f:
                json.dump(search_data, f, indent=2, default=str)
        
        elif output_format == "csv":
            file_path = output_dir / f"search_{job_id}_{timestamp}.csv"
            if isinstance(search_data, dict) and "body" in search_data:
                df = pd.DataFrame(search_data["body"])
                df.to_csv(file_path, index=False)
            else:
                df = pd.DataFrame([search_data])
                df.to_csv(file_path, index=False)
        
        elif output_format == "parquet":
            file_path = output_dir / f"search_{job_id}_{timestamp}.parquet"
            if isinstance(search_data, dict) and "body" in search_data:
                df = pd.DataFrame(search_data["body"])
                df.to_parquet(file_path, index=False)
            else:
                df = pd.DataFrame([search_data])
                df.to_parquet(file_path, index=False)
        
        logger.info("Search data saved", job_id=job_id, file_path=str(file_path))
    
    def get_job_status(self, job_id: str) -> Optional[DataExtractionJob]:
        """Get the status of an extraction job."""
        return self.jobs.get(job_id)
    
    def list_jobs(self) -> List[DataExtractionJob]:
        """List all extraction jobs."""
        return list(self.jobs.values())
