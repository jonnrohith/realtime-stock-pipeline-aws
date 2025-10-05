"""
Yahoo Finance RapidAPI client for stocks data sourcing.
"""
import asyncio
import time
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta

import requests
import structlog
from pydantic import ValidationError

from api_client import BaseAPIClient, APIError, RateLimitError, AuthenticationError
from stocks_models import Stock, StockQuote, StockHistory, APIResponse
from config import api_config

logger = structlog.get_logger()


class YahooFinanceAPIClient(BaseAPIClient):
    """Yahoo Finance RapidAPI client for stocks data extraction."""
    
    def __init__(self, rapidapi_key: str):
        super().__init__(
            base_url=api_config.yahoo_api_base_url,
            api_key=rapidapi_key,
            api_secret=None
        )
        self.rapidapi_host = api_config.rapidapi_host
    
    def _get_headers(self) -> Dict[str, str]:
        """Get RapidAPI specific headers."""
        return {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.rapidapi_host,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'YahooFinanceDataSourcingClient/1.0'
        }
    
    def test_connection(self) -> bool:
        """Test API connection with a simple request."""
        try:
            # Test with a simple tickers request
            response = self.get_market_tickers()
            return response.success
        except Exception as e:
            logger.error("Connection test failed", error=str(e))
            return False
    
    def get_market_tickers(self, page: int = 1, type_filter: str = "STOCKS") -> APIResponse:
        """
        Get market tickers from Yahoo Finance.
        
        Args:
            page: Page number for pagination
            type_filter: Type of securities to fetch (STOCKS, ETFs, etc.)
        
        Returns:
            APIResponse containing market tickers data
        """
        start_time = time.time()
        
        try:
            logger.info("Fetching market tickers", page=page, type_filter=type_filter)
            
            response = self.get(
                endpoint="/api/v2/markets/tickers",
                params={"page": str(page), "type": type_filter}
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Market tickers fetched successfully", 
                           page=page, 
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v2/markets/tickers",
                    method="GET"
                )
            else:
                logger.error("Failed to fetch market tickers", 
                           status_code=response.status_code,
                           page=page)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v2/markets/tickers",
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error("Error fetching market tickers", 
                        error=str(e), 
                        page=page)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint="/api/v2/markets/tickers",
                method="GET"
            )
    
    def search_stocks(self, query: str) -> APIResponse:
        """
        Search for stocks using Yahoo Finance search API.
        
        Args:
            query: Search query string
        
        Returns:
            APIResponse containing search results
        """
        start_time = time.time()
        
        try:
            logger.info("Searching stocks", query=query)
            
            response = self.get(
                endpoint="/api/v1/search",
                params={"query": query}
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Stock search completed", 
                           query=query, 
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v1/search",
                    method="GET"
                )
            else:
                logger.error("Stock search failed", 
                           status_code=response.status_code,
                           query=query)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v1/search",
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error("Error searching stocks", 
                        error=str(e), 
                        query=query)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint="/api/v1/search",
                method="GET"
            )
    
    def get_stock_quotes(self, symbols: List[str]) -> APIResponse:
        """
        Get real-time quotes for multiple stocks.
        
        Args:
            symbols: List of stock symbols
        
        Returns:
            APIResponse containing stock quotes
        """
        start_time = time.time()
        
        try:
            logger.info("Fetching stock quotes", symbols=symbols)
            
            # Join symbols with comma for the API
            symbols_str = ",".join(symbols)
            response = self.get(
                endpoint="/api/v1/market/quotes",
                params={"symbol": symbols_str}
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Stock quotes fetched successfully", 
                           symbols=symbols, 
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v1/market/quotes",
                    method="GET"
                )
            else:
                logger.error("Failed to fetch stock quotes", 
                           status_code=response.status_code,
                           symbols=symbols)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v1/market/quotes",
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error("Error fetching stock quotes", 
                        error=str(e), 
                        symbols=symbols)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint="/api/v1/market/quotes",
                method="GET"
            )
    
    def get_stock_history(self, symbol: str, period: str = "1mo", interval: str = "1d") -> APIResponse:
        """
        Get historical data for a stock.
        
        Args:
            symbol: Stock symbol
            period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        
        Returns:
            APIResponse containing historical data
        """
        start_time = time.time()
        
        try:
            logger.info("Fetching stock history", symbol=symbol, period=period, interval=interval)
            
            response = self.get(
                endpoint="/api/v2/stock/history",
                params={
                    "symbol": symbol,
                    "period": period,
                    "interval": interval
                }
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Stock history fetched successfully", 
                           symbol=symbol, 
                           period=period,
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v2/stock/history",
                    method="GET"
                )
            else:
                logger.error("Failed to fetch stock history", 
                           status_code=response.status_code,
                           symbol=symbol)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint="/api/v2/stock/history",
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error("Error fetching stock history", 
                        error=str(e), 
                        symbol=symbol)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint="/api/v2/stock/history",
                method="GET"
            )


class YahooFinanceDataParser:
    """Parser for Yahoo Finance API responses."""
    
    @staticmethod
    def parse_tickers_response(response_data: Dict[str, Any]) -> List[Stock]:
        """Parse tickers API response into Stock models."""
        try:
            stocks = []
            if "body" in response_data and isinstance(response_data["body"], list):
                for ticker_data in response_data["body"]:
                    try:
                        stock = YahooFinanceDataParser.parse_ticker_data(ticker_data)
                        stocks.append(stock)
                    except ValidationError as e:
                        logger.warning("Failed to parse ticker", error=str(e), ticker_data=ticker_data)
                        continue
            
            return stocks
            
        except Exception as e:
            logger.error("Failed to parse tickers response", error=str(e), response_data=response_data)
            raise
    
    @staticmethod
    def parse_ticker_data(ticker_data: Dict[str, Any]) -> Stock:
        """Parse ticker data into Stock model."""
        try:
            # Parse market cap - handle comma-separated numbers
            market_cap = ticker_data.get("marketCap")
            if market_cap and isinstance(market_cap, str):
                try:
                    # Remove commas and convert to float
                    market_cap = float(market_cap.replace(",", ""))
                except (ValueError, TypeError):
                    market_cap = None
            
            # Map API response fields to our model
            parsed_data = {
                "symbol": ticker_data.get("symbol"),
                "name": ticker_data.get("name"),
                "exchange": ticker_data.get("exchange"),
                "sector": ticker_data.get("sector"),
                "industry": ticker_data.get("industry"),
                "market_cap": market_cap,
                "currency": ticker_data.get("currency"),
                "country": ticker_data.get("country"),
                "website": ticker_data.get("website"),
                "description": ticker_data.get("description")
            }
            
            # Remove None values
            parsed_data = {k: v for k, v in parsed_data.items() if v is not None}
            
            return Stock(**parsed_data)
            
        except Exception as e:
            logger.error("Failed to parse ticker data", error=str(e), ticker_data=ticker_data)
            raise
    
    @staticmethod
    def parse_quotes_response(response_data: Dict[str, Any]) -> List[StockQuote]:
        """Parse quotes API response into StockQuote models."""
        try:
            quotes = []
            if "body" in response_data and isinstance(response_data["body"], list):
                for quote_data in response_data["body"]:
                    try:
                        quote = YahooFinanceDataParser.parse_quote_data(quote_data)
                        quotes.append(quote)
                    except ValidationError as e:
                        logger.warning("Failed to parse quote", error=str(e), quote_data=quote_data)
                        continue
            
            return quotes
            
        except Exception as e:
            logger.error("Failed to parse quotes response", error=str(e), response_data=response_data)
            raise
    
    @staticmethod
    def parse_quote_data(quote_data: Dict[str, Any]) -> StockQuote:
        """Parse quote data into StockQuote model."""
        try:
            parsed_data = {
                "symbol": quote_data.get("symbol"),
                "name": quote_data.get("longName"),
                "price": quote_data.get("regularMarketPrice"),
                "previous_close": quote_data.get("regularMarketPreviousClose"),
                "open": quote_data.get("regularMarketOpen"),
                "high": quote_data.get("regularMarketDayHigh"),
                "low": quote_data.get("regularMarketDayLow"),
                "volume": quote_data.get("regularMarketVolume"),
                "market_cap": quote_data.get("marketCap"),
                "pe_ratio": quote_data.get("trailingPE"),
                "dividend_yield": quote_data.get("dividendYield"),
                "change": quote_data.get("regularMarketChange"),
                "change_percent": quote_data.get("regularMarketChangePercent"),
                "currency": quote_data.get("currency"),
                "exchange": quote_data.get("fullExchangeName"),
                "quote_type": quote_data.get("quoteType"),
                "timestamp": quote_data.get("regularMarketTime")
            }
            
            # Remove None values
            parsed_data = {k: v for k, v in parsed_data.items() if v is not None}
            
            return StockQuote(**parsed_data)
            
        except Exception as e:
            logger.error("Failed to parse quote data", error=str(e), quote_data=quote_data)
            raise
