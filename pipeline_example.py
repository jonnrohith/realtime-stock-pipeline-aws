#!/usr/bin/env python3
"""
Complete Data Engineering Pipeline Example
"""
import os
import time
from datetime import datetime
from dotenv import load_dotenv

from data_pipeline.pipeline import DataPipeline
from data_pipeline.models import DataSourceType

# Load environment variables
load_dotenv()


def main():
    """Main pipeline example."""
    print("🚀 Yahoo Finance Data Engineering Pipeline")
    print("=" * 60)
    
    # Initialize pipeline
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("❌ Please set RAPIDAPI_KEY in your .env file")
        return
    
    pipeline = DataPipeline(rapidapi_key)
    
    print("✅ Pipeline initialized")
    
    # Example 1: Run specific data source
    print("\n📊 Example 1: Extract Market Tickers")
    print("-" * 40)
    
    result = pipeline.run_data_source("market_tickers", pages=2)
    if result["success"]:
        print(f"✅ Successfully extracted {result['processed_items']} tickers")
        print(f"⏱️  Duration: {result['duration_seconds']:.2f} seconds")
    else:
        print(f"❌ Failed: {result.get('error', 'Unknown error')}")
    
    # Example 2: Run stock quotes
    print("\n💰 Example 2: Extract Stock Quotes")
    print("-" * 40)
    
    result = pipeline.run_data_source("stock_quotes", symbols=["AAPL", "MSFT", "GOOGL"])
    if result["success"]:
        print(f"✅ Successfully extracted {result['processed_items']} quotes")
        print(f"⏱️  Duration: {result['duration_seconds']:.2f} seconds")
    else:
        print(f"❌ Failed: {result.get('error', 'Unknown error')}")
    
    # Example 3: Run market screeners
    print("\n📈 Example 3: Extract Market Screeners")
    print("-" * 40)
    
    result = pipeline.run_data_source("market_screeners")
    if result["success"]:
        print(f"✅ Successfully extracted {result['processed_items']} screener results")
        print(f"⏱️  Duration: {result['duration_seconds']:.2f} seconds")
    else:
        print(f"❌ Failed: {result.get('error', 'Unknown error')}")
    
    # Example 4: Run full pipeline
    print("\n🔄 Example 4: Run Full Pipeline")
    print("-" * 40)
    
    start_time = time.time()
    result = pipeline.run_full_pipeline()
    total_time = time.time() - start_time
    
    if "error" not in result:
        print(f"✅ Full pipeline completed in {total_time:.2f} seconds")
        print(f"📊 Data sources processed: {result['data_sources_processed']}")
        print(f"✅ Successful: {result['successful_sources']}")
        print(f"❌ Failed: {result['failed_sources']}")
    else:
        print(f"❌ Full pipeline failed: {result['error']}")
    
    # Example 5: Check pipeline status
    print("\n📋 Example 5: Pipeline Status")
    print("-" * 40)
    
    status = pipeline.get_status()
    print(f"📊 Recent jobs: {len(status['recent_jobs'])}")
    print(f"🚨 Alerts: {status['alert_summary']['total_alerts']}")
    print(f"📈 Metrics available: {status['metrics_summary'].get('total_jobs', 0)}")
    
    # Example 6: List recent jobs
    print("\n📝 Example 6: Recent Jobs")
    print("-" * 40)
    
    jobs = pipeline.list_jobs()
    for job in jobs[:5]:  # Show last 5 jobs
        status_emoji = "✅" if job["status"] == "completed" else "❌"
        print(f"{status_emoji} {job['job_id']}: {job['data_source']} ({job['status']})")
    
    print("\n🎉 Pipeline examples completed!")
    print("\nNext steps:")
    print("1. Start the scheduler: pipeline.start()")
    print("2. Monitor in real-time: pipeline.get_status()")
    print("3. Run specific sources: pipeline.run_data_source('stock_quotes')")
    print("4. Check data in: data/raw/ and data/processed/ directories")


def scheduler_example():
    """Example of running the scheduler."""
    print("\n⏰ Scheduler Example")
    print("=" * 60)
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    pipeline = DataPipeline(rapidapi_key)
    
    # Start the pipeline (includes scheduler)
    pipeline.start()
    
    print("✅ Pipeline and scheduler started")
    print("📅 Schedules:")
    print("  - Market Tickers: Daily at 6 AM")
    print("  - Stock Quotes: Every 5 minutes (9 AM - 4 PM, Mon-Fri)")
    print("  - Stock History: Daily at 7 PM")
    print("  - Market Screeners: Every 15 minutes (9 AM - 4 PM, Mon-Fri)")
    print("  - Stock News: Every 30 minutes")
    
    print("\n⏳ Running for 2 minutes...")
    time.sleep(120)  # Run for 2 minutes
    
    # Check status
    status = pipeline.get_status()
    print(f"\n📊 Status after 2 minutes:")
    print(f"  - Recent jobs: {len(status['recent_jobs'])}")
    print(f"  - Alerts: {status['alert_summary']['total_alerts']}")
    
    # Stop the pipeline
    pipeline.stop()
    print("✅ Pipeline stopped")


def monitoring_example():
    """Example of monitoring and alerting."""
    print("\n🔍 Monitoring Example")
    print("=" * 60)
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    pipeline = DataPipeline(rapidapi_key)
    
    # Run a job
    result = pipeline.run_data_source("stock_quotes", symbols=["AAPL"])
    
    # Check for alerts
    if "alerts" in result:
        print(f"🚨 Alerts generated: {len(result['alerts'])}")
        for alert in result["alerts"]:
            print(f"  - {alert['severity'].upper()}: {alert['title']}")
    
    # Check metrics
    if "metrics" in result:
        metrics = result["metrics"]
        print(f"📊 Metrics:")
        print(f"  - Records processed: {metrics['records_processed']}")
        print(f"  - Processing time: {metrics['processing_time_seconds']:.2f}s")
        print(f"  - Memory usage: {metrics.get('memory_usage_mb', 0):.1f} MB")
        print(f"  - CPU usage: {metrics.get('cpu_usage_percent', 0):.1f}%")


if __name__ == "__main__":
    try:
        # Run main examples
        main()
        
        # Uncomment to run additional examples
        # scheduler_example()
        # monitoring_example()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Examples interrupted by user.")
    except Exception as e:
        print(f"\n\n❌ Error running examples: {e}")
        print("Make sure you have set up your .env file with RAPIDAPI_KEY")
