#!/usr/bin/env python3
"""
Simple Data Engineering Pipeline Example
"""
import os
import time
from datetime import datetime
from dotenv import load_dotenv

from data_pipeline.extractors import (
    MarketTickersExtractor, StockQuotesExtractor, StockHistoryExtractor,
    MarketScreenerExtractor, StockNewsExtractor
)
from data_pipeline.transformers import DataTransformer
from data_pipeline.models import DataSourceType

# Load environment variables
load_dotenv()


def main():
    """Main pipeline example."""
    print("🚀 Yahoo Finance Data Engineering Pipeline")
    print("=" * 60)
    
    # Initialize components
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("❌ Please set RAPIDAPI_KEY in your .env file")
        return
    
    transformer = DataTransformer()
    
    print("✅ Pipeline components initialized")
    
    # Example 1: Extract Market Tickers
    print("\n📊 Example 1: Extract Market Tickers")
    print("-" * 40)
    
    extractor = MarketTickersExtractor(rapidapi_key)
    job = extractor.extract(pages=2, types=["STOCKS"])
    
    if job.status == "completed":
        print(f"✅ Successfully extracted {job.processed_items} tickers")
        print(f"⏱️  Duration: {job.duration_seconds:.2f} seconds")
        print(f"📁 Data saved to: data/raw/ and data/processed/")
    else:
        print(f"❌ Failed: {job.error_message}")
    
    # Example 2: Extract Stock Quotes
    print("\n💰 Example 2: Extract Stock Quotes")
    print("-" * 40)
    
    extractor = StockQuotesExtractor(rapidapi_key)
    job = extractor.extract(symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"])
    
    if job.status == "completed":
        print(f"✅ Successfully extracted {job.processed_items} quotes")
        print(f"⏱️  Duration: {job.duration_seconds:.2f} seconds")
        print(f"📁 Data saved to: data/raw/ and data/processed/")
    else:
        print(f"❌ Failed: {job.error_message}")
    
    # Example 3: Extract Market Screeners
    print("\n📈 Example 3: Extract Market Screeners")
    print("-" * 40)
    
    extractor = MarketScreenerExtractor(rapidapi_key)
    job = extractor.extract(screener_lists=["day_gainers", "day_losers", "most_actives"])
    
    if job.status == "completed":
        print(f"✅ Successfully extracted {job.processed_items} screener results")
        print(f"⏱️  Duration: {job.duration_seconds:.2f} seconds")
        print(f"📁 Data saved to: data/raw/ and data/processed/")
    else:
        print(f"❌ Failed: {job.error_message}")
    
    # Example 4: Extract Stock History
    print("\n📊 Example 4: Extract Stock History")
    print("-" * 40)
    
    extractor = StockHistoryExtractor(rapidapi_key)
    job = extractor.extract(symbols=["AAPL", "MSFT"], intervals=["1d"])
    
    if job.status == "completed":
        print(f"✅ Successfully extracted {job.processed_items} history records")
        print(f"⏱️  Duration: {job.duration_seconds:.2f} seconds")
        print(f"📁 Data saved to: data/raw/ and data/processed/")
    else:
        print(f"❌ Failed: {job.error_message}")
    
    # Example 5: Extract Stock News
    print("\n📰 Example 5: Extract Stock News")
    print("-" * 40)
    
    extractor = StockNewsExtractor(rapidapi_key)
    job = extractor.extract(symbols=["AAPL", "MSFT"])
    
    if job.status == "completed":
        print(f"✅ Successfully extracted {job.processed_items} news items")
        print(f"⏱️  Duration: {job.duration_seconds:.2f} seconds")
        print(f"📁 Data saved to: data/raw/ and data/processed/")
    else:
        print(f"❌ Failed: {job.error_message}")
    
    # Example 6: Data Transformation
    print("\n🔄 Example 6: Data Transformation")
    print("-" * 40)
    
    # Load some sample data for transformation
    sample_data = [
        {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "price": 150.25,
            "change_percent": 1.5,
            "volume": 50000000,
            "market_cap": 3000000000000,
            "sector": "Technology"
        },
        {
            "symbol": "MSFT",
            "name": "Microsoft Corporation",
            "price": 300.50,
            "change_percent": -0.5,
            "volume": 30000000,
            "market_cap": 2500000000000,
            "sector": "Technology"
        }
    ]
    
    transformed = transformer.transform_data(sample_data, "stock_quotes")
    
    print(f"✅ Transformed {len(transformed['transformed_data'])} records")
    print(f"📊 Quality checks: {len(transformed['quality_checks'])}")
    print(f"📈 Market summary generated: {bool(transformed['aggregations'])}")
    
    # Show sample transformed data
    if transformed['transformed_data']:
        sample = transformed['transformed_data'][0]
        print(f"📋 Sample transformed record:")
        print(f"   Symbol: {sample.get('symbol')}")
        print(f"   Market Cap Category: {sample.get('market_cap_category')}")
        print(f"   Data Quality Score: {sample.get('data_quality_score', 0):.2f}")
    
    print("\n🎉 Pipeline examples completed!")
    print("\n📁 Check the following directories for data:")
    print("   - data/raw/     : Raw extracted data")
    print("   - data/processed/: Cleaned and transformed data")
    print("   - output/       : Final output files")
    
    print("\n🚀 Next steps for production:")
    print("1. Set up a cron job or scheduler")
    print("2. Add database storage (PostgreSQL, MongoDB)")
    print("3. Set up monitoring and alerting")
    print("4. Add data validation and quality checks")
    print("5. Implement data retention policies")


def batch_processing_example():
    """Example of batch processing multiple data sources."""
    print("\n🔄 Batch Processing Example")
    print("=" * 60)
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("❌ Please set RAPIDAPI_KEY in your .env file")
        return
    
    # Define data sources to process
    data_sources = [
        {
            "name": "Market Tickers",
            "extractor": MarketTickersExtractor(rapidapi_key),
            "params": {"pages": 1, "types": ["STOCKS"]}
        },
        {
            "name": "Stock Quotes",
            "extractor": StockQuotesExtractor(rapidapi_key),
            "params": {"symbols": ["AAPL", "MSFT", "GOOGL"]}
        },
        {
            "name": "Day Gainers",
            "extractor": MarketScreenerExtractor(rapidapi_key),
            "params": {"screener_lists": ["day_gainers"]}
        }
    ]
    
    results = []
    start_time = time.time()
    
    for source in data_sources:
        print(f"\n📊 Processing: {source['name']}")
        
        try:
            job = source['extractor'].extract(**source['params'])
            
            result = {
                "name": source['name'],
                "success": job.status == "completed",
                "items": job.processed_items,
                "duration": job.duration_seconds,
                "error": job.error_message
            }
            
            results.append(result)
            
            if result["success"]:
                print(f"✅ {result['items']} items in {result['duration']:.2f}s")
            else:
                print(f"❌ Failed: {result['error']}")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
            results.append({
                "name": source['name'],
                "success": False,
                "error": str(e)
            })
    
    # Summary
    total_time = time.time() - start_time
    successful = sum(1 for r in results if r["success"])
    total_items = sum(r.get("items", 0) for r in results if r["success"])
    
    print(f"\n📊 Batch Processing Summary:")
    print(f"   Total time: {total_time:.2f} seconds")
    print(f"   Successful sources: {successful}/{len(results)}")
    print(f"   Total items processed: {total_items}")
    
    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"   {status} {result['name']}: {result.get('items', 0)} items")


if __name__ == "__main__":
    try:
        # Run main examples
        main()
        
        # Run batch processing example
        batch_processing_example()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Examples interrupted by user.")
    except Exception as e:
        print(f"\n\n❌ Error running examples: {e}")
        print("Make sure you have set up your .env file with RAPIDAPI_KEY")

