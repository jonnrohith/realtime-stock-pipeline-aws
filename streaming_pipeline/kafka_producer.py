"""
Kafka Producer for Real-time Yahoo Finance Data
"""
import json
import time
import asyncio
from datetime import datetime
from typing import Dict, List, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import structlog

from data_pipeline.extractors import (
    StockQuotesExtractor, MarketScreenerExtractor, StockNewsExtractor
)
from data_pipeline.models import DataSourceType

logger = structlog.get_logger()


class YahooFinanceKafkaProducer:
    """Kafka producer for streaming Yahoo Finance data."""
    
    def __init__(self, 
                 bootstrap_servers: str = "localhost:9092",
                 rapidapi_key: str = None,
                 topics: Dict[str, str] = None):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            rapidapi_key: RapidAPI key for Yahoo Finance
            topics: Dictionary mapping data types to Kafka topics
        """
        self.bootstrap_servers = bootstrap_servers
        self.rapidapi_key = rapidapi_key
        self.topics = topics or {
            "stock_quotes": "yahoo-stock-quotes",
            "market_screeners": "yahoo-market-screeners", 
            "stock_news": "yahoo-stock-news"
        }
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            retry_backoff_ms=100,
            request_timeout_ms=30000
        )
        
        # Initialize extractors
        self.extractors = {
            "stock_quotes": StockQuotesExtractor(rapidapi_key),
            "market_screeners": MarketScreenerExtractor(rapidapi_key),
            "stock_news": StockNewsExtractor(rapidapi_key)
        }
        
        logger.info("Kafka producer initialized", 
                   bootstrap_servers=bootstrap_servers,
                   topics=list(self.topics.values()))
    
    def produce_stock_quotes(self, symbols: List[str], interval: int = 30):
        """
        Continuously produce stock quotes to Kafka.
        
        Args:
            symbols: List of stock symbols to track
            interval: Production interval in seconds
        """
        logger.info("Starting stock quotes producer", 
                   symbols=symbols, interval=interval)
        
        while True:
            try:
                # Extract quotes
                job = self.extractors["stock_quotes"].extract(symbols=symbols)
                
                if job.status == "completed":
                    # Load processed data
                    quotes_data = self._load_processed_data("stock_quotes", job.job_id)
                    
                    # Send to Kafka
                    for quote in quotes_data:
                        message = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "data_type": "stock_quote",
                            "data": quote,
                            "metadata": {
                                "job_id": job.job_id,
                                "source": "yahoo_finance_api"
                            }
                        }
                        
                        self.producer.send(
                            self.topics["stock_quotes"],
                            key=quote.get("symbol"),
                            value=message
                        )
                    
                    logger.info("Stock quotes produced", 
                               count=len(quotes_data), 
                               symbols=symbols)
                else:
                    logger.error("Failed to extract quotes", 
                               error=job.error_message)
                
                # Wait for next interval
                time.sleep(interval)
                
            except Exception as e:
                logger.error("Error producing stock quotes", error=str(e))
                time.sleep(interval)
    
    def produce_market_screeners(self, screener_types: List[str] = None, interval: int = 300):
        """
        Continuously produce market screener data to Kafka.
        
        Args:
            screener_types: List of screener types (day_gainers, day_losers, etc.)
            interval: Production interval in seconds
        """
        if screener_types is None:
            screener_types = ["day_gainers", "day_losers", "most_actives"]
        
        logger.info("Starting market screeners producer", 
                   screener_types=screener_types, interval=interval)
        
        while True:
            try:
                # Extract screeners
                job = self.extractors["market_screeners"].extract(
                    screener_lists=screener_types
                )
                
                if job.status == "completed":
                    # Load processed data
                    screeners_data = self._load_processed_data("market_screeners", job.job_id)
                    
                    # Send to Kafka
                    for screener in screeners_data:
                        message = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "data_type": "market_screener",
                            "data": screener,
                            "metadata": {
                                "job_id": job.job_id,
                                "source": "yahoo_finance_api"
                            }
                        }
                        
                        self.producer.send(
                            self.topics["market_screeners"],
                            key=screener.get("screener_type"),
                            value=message
                        )
                    
                    logger.info("Market screeners produced", 
                               count=len(screeners_data), 
                               screener_types=screener_types)
                else:
                    logger.error("Failed to extract screeners", 
                               error=job.error_message)
                
                # Wait for next interval
                time.sleep(interval)
                
            except Exception as e:
                logger.error("Error producing market screeners", error=str(e))
                time.sleep(interval)
    
    def produce_stock_news(self, symbols: List[str], interval: int = 600):
        """
        Continuously produce stock news to Kafka.
        
        Args:
            symbols: List of stock symbols to track
            interval: Production interval in seconds
        """
        logger.info("Starting stock news producer", 
                   symbols=symbols, interval=interval)
        
        while True:
            try:
                # Extract news
                job = self.extractors["stock_news"].extract(symbols=symbols)
                
                if job.status == "completed":
                    # Load processed data
                    news_data = self._load_processed_data("stock_news", job.job_id)
                    
                    # Send to Kafka
                    for news in news_data:
                        message = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "data_type": "stock_news",
                            "data": news,
                            "metadata": {
                                "job_id": job.job_id,
                                "source": "yahoo_finance_api"
                            }
                        }
                        
                        self.producer.send(
                            self.topics["stock_news"],
                            key=news.get("symbol"),
                            value=message
                        )
                    
                    logger.info("Stock news produced", 
                               count=len(news_data), 
                               symbols=symbols)
                else:
                    logger.error("Failed to extract news", 
                               error=job.error_message)
                
                # Wait for next interval
                time.sleep(interval)
                
            except Exception as e:
                logger.error("Error producing stock news", error=str(e))
                time.sleep(interval)
    
    def produce_all_data(self, 
                        symbols: List[str],
                        quote_interval: int = 30,
                        screener_interval: int = 300,
                        news_interval: int = 600):
        """
        Produce all data types concurrently.
        
        Args:
            symbols: List of stock symbols to track
            quote_interval: Stock quotes production interval
            screener_interval: Market screeners production interval  
            news_interval: Stock news production interval
        """
        logger.info("Starting all data producers", symbols=symbols)
        
        # Create tasks for concurrent execution
        tasks = [
            asyncio.create_task(self._async_produce_quotes(symbols, quote_interval)),
            asyncio.create_task(self._async_produce_screeners(screener_interval)),
            asyncio.create_task(self._async_produce_news(symbols, news_interval))
        ]
        
        # Run all tasks concurrently
        asyncio.run(asyncio.gather(*tasks))
    
    async def _async_produce_quotes(self, symbols: List[str], interval: int):
        """Async wrapper for quotes production."""
        while True:
            try:
                job = self.extractors["stock_quotes"].extract(symbols=symbols)
                if job.status == "completed":
                    quotes_data = self._load_processed_data("stock_quotes", job.job_id)
                    for quote in quotes_data:
                        message = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "data_type": "stock_quote",
                            "data": quote,
                            "metadata": {"job_id": job.job_id, "source": "yahoo_finance_api"}
                        }
                        self.producer.send(self.topics["stock_quotes"], 
                                         key=quote.get("symbol"), value=message)
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error("Error in async quotes production", error=str(e))
                await asyncio.sleep(interval)
    
    async def _async_produce_screeners(self, interval: int):
        """Async wrapper for screeners production."""
        while True:
            try:
                job = self.extractors["market_screeners"].extract()
                if job.status == "completed":
                    screeners_data = self._load_processed_data("market_screeners", job.job_id)
                    for screener in screeners_data:
                        message = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "data_type": "market_screener",
                            "data": screener,
                            "metadata": {"job_id": job.job_id, "source": "yahoo_finance_api"}
                        }
                        self.producer.send(self.topics["market_screeners"], 
                                         key=screener.get("screener_type"), value=message)
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error("Error in async screeners production", error=str(e))
                await asyncio.sleep(interval)
    
    async def _async_produce_news(self, symbols: List[str], interval: int):
        """Async wrapper for news production."""
        while True:
            try:
                job = self.extractors["stock_news"].extract(symbols=symbols)
                if job.status == "completed":
                    news_data = self._load_processed_data("stock_news", job.job_id)
                    for news in news_data:
                        message = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "data_type": "stock_news",
                            "data": news,
                            "metadata": {"job_id": job.job_id, "source": "yahoo_finance_api"}
                        }
                        self.producer.send(self.topics["stock_news"], 
                                         key=news.get("symbol"), value=message)
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error("Error in async news production", error=str(e))
                await asyncio.sleep(interval)
    
    def _load_processed_data(self, data_type: str, job_id: str) -> List[Dict[str, Any]]:
        """Load processed data from files."""
        try:
            from pathlib import Path
            import json
            
            # Find the most recent file for this data type
            processed_dir = Path("data/processed")
            pattern = f"{data_type}_*.json"
            
            files = list(processed_dir.glob(pattern))
            if not files:
                return []
            
            # Get the most recent file
            latest_file = max(files, key=lambda x: x.stat().st_mtime)
            
            with open(latest_file, 'r') as f:
                data = json.load(f)
            
            return data if isinstance(data, list) else []
            
        except Exception as e:
            logger.error("Error loading processed data", 
                        data_type=data_type, error=str(e))
            return []
    
    def close(self):
        """Close the Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed")


def main():
    """Main function for running the producer."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("❌ Please set RAPIDAPI_KEY in your .env file")
        return
    
    # Initialize producer
    producer = YahooFinanceKafkaProducer(
        bootstrap_servers="localhost:9092",
        rapidapi_key=rapidapi_key
    )
    
    # Define symbols to track
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX"]
    
    try:
        print("🚀 Starting Yahoo Finance Kafka Producer")
        print(f"📊 Tracking symbols: {', '.join(symbols)}")
        print("⏹️  Press Ctrl+C to stop")
        
        # Start producing all data types
        producer.produce_all_data(
            symbols=symbols,
            quote_interval=30,    # Every 30 seconds
            screener_interval=300, # Every 5 minutes
            news_interval=600     # Every 10 minutes
        )
        
    except KeyboardInterrupt:
        print("\n⏹️  Stopping producer...")
    finally:
        producer.close()
        print("✅ Producer stopped")


if __name__ == "__main__":
    main()
