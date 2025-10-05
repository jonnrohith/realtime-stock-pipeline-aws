"""
Updated Yahoo Finance RapidAPI client with all working endpoints.
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


class UpdatedYahooFinanceAPIClient(BaseAPIClient):
    """Updated Yahoo Finance RapidAPI client with all working endpoints."""
    
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
            'User-Agent': 'YahooFinanceDataSourcingClient/2.0'
        }
    
    def test_connection(self) -> bool:
        """Test API connection with a simple request."""
        try:
            # Test with market tickers
            response = self.get_market_tickers()
            return response.success
        except Exception as e:
            logger.error("Connection test failed", error=str(e))
            return False
    
    # Market Tickers
    def get_market_tickers(self, page: int = 1, type_filter: str = "STOCKS") -> APIResponse:
        """Get market tickers from Yahoo Finance."""
        return self._make_api_call(
            endpoint="/api/v2/markets/tickers",
            params={"page": str(page), "type": type_filter},
            description="market tickers"
        )
    
    # Search
    def search_stocks(self, query: str) -> APIResponse:
        """Search for stocks using Yahoo Finance search API."""
        return self._make_api_call(
            endpoint="/api/v1/markets/search",
            params={"search": query},
            description="stock search"
        )
    
    # Quotes
    def get_single_quote(self, ticker: str, quote_type: str = "STOCKS") -> APIResponse:
        """Get single stock quote."""
        return self._make_api_call(
            endpoint="/api/v1/markets/quote",
            params={"ticker": ticker, "type": quote_type},
            description="single quote"
        )
    
    def get_multiple_quotes(self, tickers: List[str]) -> APIResponse:
        """Get multiple stock quotes."""
        ticker_string = ",".join(tickers)
        return self._make_api_call(
            endpoint="/api/v1/markets/stock/quotes",
            params={"ticker": ticker_string},
            description="multiple quotes"
        )
    
    # Historical Data
    def get_stock_history(self, symbol: str, interval: str = "1d", limit: int = 30) -> APIResponse:
        """Get historical stock data."""
        return self._make_api_call(
            endpoint="/api/v2/markets/stock/history",
            params={"symbol": symbol, "interval": interval, "limit": str(limit)},
            description="stock history"
        )
    
    # Market Screener
    def get_market_screener(self, list_type: str = "day_gainers") -> APIResponse:
        """Get market screener data (day_gainers, day_losers, most_actives)."""
        return self._make_api_call(
            endpoint="/api/v1/markets/screener",
            params={"list": list_type},
            description="market screener"
        )
    
    def get_day_gainers(self) -> APIResponse:
        """Get day gainers."""
        return self.get_market_screener("day_gainers")
    
    def get_day_losers(self) -> APIResponse:
        """Get day losers."""
        return self.get_market_screener("day_losers")
    
    def get_most_actives(self) -> APIResponse:
        """Get most active stocks."""
        return self.get_market_screener("most_actives")
    
    # Stock Modules
    def get_stock_modules(self, ticker: str, module: str) -> APIResponse:
        """Get stock modules (asset-profile, financial-data, etc.)."""
        return self._make_api_call(
            endpoint="/api/v1/markets/stock/modules",
            params={"ticker": ticker, "module": module},
            description=f"stock module: {module}"
        )
    
    def get_asset_profile(self, ticker: str) -> APIResponse:
        """Get asset profile for a stock."""
        return self.get_stock_modules(ticker, "asset-profile")
    
    def get_financial_data(self, ticker: str) -> APIResponse:
        """Get financial data for a stock."""
        return self.get_stock_modules(ticker, "financial-data")
    
    # News
    def get_stock_news(self, tickers: List[str], news_type: str = "ALL") -> APIResponse:
        """Get news for stocks."""
        ticker_string = ",".join(tickers)
        return self._make_api_call(
            endpoint="/api/v2/markets/news",
            params={"tickers": ticker_string, "type": news_type},
            description="stock news"
        )
    
    # Helper method for making API calls
    def _make_api_call(self, endpoint: str, params: Dict[str, str], description: str) -> APIResponse:
        """Make API call with common error handling."""
        start_time = time.time()
        
        try:
            logger.info(f"Fetching {description}", endpoint=endpoint, params=params)
            
            response = self.get(
                endpoint=endpoint,
                params=params
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"{description} fetched successfully", 
                           endpoint=endpoint, 
                           response_time=response_time)
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint=endpoint,
                    method="GET"
                )
            else:
                logger.error(f"Failed to fetch {description}", 
                           status_code=response.status_code,
                           endpoint=endpoint)
                return APIResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    response_time=response_time,
                    endpoint=endpoint,
                    method="GET"
                )
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"Error fetching {description}", 
                        error=str(e), 
                        endpoint=endpoint)
            return APIResponse(
                success=False,
                error=str(e),
                response_time=response_time,
                endpoint=endpoint,
                method="GET"
            )


class UpdatedYahooFinanceDataParser:
    """Updated parser for Yahoo Finance API responses."""
    
    @staticmethod
    def parse_tickers_response(response_data: Dict[str, Any]) -> List[Stock]:
        """Parse tickers API response into Stock models."""
        try:
            stocks = []
            if "body" in response_data and isinstance(response_data["body"], list):
                for ticker_data in response_data["body"]:
                    try:
                        stock = UpdatedYahooFinanceDataParser.parse_ticker_data(ticker_data)
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
    def parse_search_response(response_data: Dict[str, Any]) -> List[Stock]:
        """Parse search API response into Stock models."""
        try:
            stocks = []
            if "body" in response_data and isinstance(response_data["body"], list):
                for search_result in response_data["body"]:
                    try:
                        stock = UpdatedYahooFinanceDataParser.parse_search_result(search_result)
                        stocks.append(stock)
                    except ValidationError as e:
                        logger.warning("Failed to parse search result", error=str(e), search_result=search_result)
                        continue
            
            return stocks
            
        except Exception as e:
            logger.error("Failed to parse search response", error=str(e), response_data=response_data)
            raise
    
    @staticmethod
    def parse_search_result(search_result: Dict[str, Any]) -> Stock:
        """Parse search result into Stock model."""
        try:
            parsed_data = {
                "symbol": search_result.get("symbol"),
                "name": search_result.get("longname") or search_result.get("shortname"),
                "exchange": search_result.get("exchDisp"),
                "sector": search_result.get("sectorDisp") or search_result.get("sector"),
                "industry": search_result.get("industryDisp") or search_result.get("industry"),
                "currency": "USD",  # Default to USD
                "country": "US"     # Default to US
            }
            
            # Remove None values
            parsed_data = {k: v for k, v in parsed_data.items() if v is not None}
            
            return Stock(**parsed_data)
            
        except Exception as e:
            logger.error("Failed to parse search result", error=str(e), search_result=search_result)
            raise
    
    @staticmethod
    def parse_quotes_response(response_data: Dict[str, Any]) -> List[StockQuote]:
        """Parse quotes API response into StockQuote models."""
        try:
            quotes = []
            if "body" in response_data and isinstance(response_data["body"], list):
                for quote_data in response_data["body"]:
                    try:
                        quote = UpdatedYahooFinanceDataParser.parse_quote_data(quote_data)
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
                "name": quote_data.get("longName") or quote_data.get("shortName"),
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
                "currency": quote_data.get("currency", "USD"),
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
