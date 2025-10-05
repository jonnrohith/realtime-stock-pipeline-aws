#!/usr/bin/env python3
"""
Example usage of the Yahoo Finance stocks data extraction system.
"""
import os
from dotenv import load_dotenv

from yahoo_client import YahooFinanceAPIClient
from stocks_extractor import YahooFinanceDataExtractor

# Load environment variables
load_dotenv()


def basic_stocks_example():
    """Basic example of using the Yahoo Finance API client."""
    print("=== Basic Yahoo Finance API Usage Example ===\n")
    
    # Initialize the client
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    client = YahooFinanceAPIClient(rapidapi_key)
    
    # Test connection
    print("Testing API connection...")
    if client.test_connection():
        print("✅ Connection successful!")
    else:
        print("❌ Connection failed!")
        return
    
    # Get market tickers
    print("\nFetching market tickers...")
    response = client.get_market_tickers(page=1, type_filter="STOCKS")
    
    if response.success:
        print(f"✅ Successfully fetched data!")
        print(f"Response time: {response.response_time:.2f} seconds")
        print(f"Status code: {response.status_code}")
        
        # Print some basic info about the response
        if response.data:
            print(f"Response keys: {list(response.data.keys())}")
            if "body" in response.data and len(response.data["body"]) > 0:
                print(f"Number of stocks: {len(response.data['body'])}")
                print(f"First stock: {response.data['body'][0]}")
    else:
        print(f"❌ Failed to fetch data: {response.error}")


def stocks_extraction_example():
    """Example of using the stocks data extractor for comprehensive data collection."""
    print("\n=== Stocks Data Extraction Example ===\n")
    
    # Initialize the data extractor
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    extractor = YahooFinanceDataExtractor(rapidapi_key)
    
    print("Extracting market tickers...")
    
    # Extract market tickers
    job = extractor.extract_market_tickers(
        page=1,
        type_filter="STOCKS",
        job_id="example_extraction",
        save_to_file=True,
        output_format="json"
    )
    
    # Print job results
    print(f"\nJob ID: {job.job_id}")
    print(f"Status: {job.status}")
    print(f"Total items: {job.total_items}")
    print(f"Processed items: {job.processed_items}")
    print(f"Failed items: {job.failed_items}")
    if job.duration_seconds:
        print(f"Duration: {job.duration_seconds:.2f} seconds")
    else:
        print("Duration: N/A")
    
    if job.error_message:
        print(f"Error: {job.error_message}")
    
    if job.status == "completed":
        print("✅ Data extraction completed successfully!")
        print("Check the 'output' directory for saved files.")


def search_stocks_example():
    """Example of searching for stocks."""
    print("\n=== Stock Search Example ===\n")
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    extractor = YahooFinanceDataExtractor(rapidapi_key)
    
    # Search query
    query = "Apple"
    
    print(f"Searching for: {query}")
    
    # Extract search results
    job = extractor.search_stocks(
        query=query,
        job_id="search_example",
        save_to_file=True,
        output_format="json"
    )
    
    # Print results
    print(f"\nJob ID: {job.job_id}")
    print(f"Status: {job.status}")
    print(f"Total items: {job.total_items}")
    if job.duration_seconds:
        print(f"Duration: {job.duration_seconds:.2f} seconds")
    else:
        print("Duration: N/A")
    
    if job.status == "completed":
        print("✅ Search completed successfully!")
        print("Check the 'output' directory for saved files.")
    else:
        print(f"❌ Search failed: {job.error_message}")


def job_monitoring_example():
    """Example of monitoring extraction jobs."""
    print("\n=== Job Monitoring Example ===\n")
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    extractor = YahooFinanceDataExtractor(rapidapi_key)
    
    # Start a job
    job = extractor.extract_market_tickers(
        page=1,
        type_filter="STOCKS",
        job_id="monitoring_example",
        save_to_file=True
    )
    
    # Monitor job status
    print(f"Job started: {job.job_id}")
    print(f"Initial status: {job.status}")
    
    # Get job status
    job_status = extractor.get_job_status(job.job_id)
    if job_status:
        print(f"Current status: {job_status.status}")
        print(f"Processed items: {job_status.processed_items}")
        print(f"Total items: {job_status.total_items}")
    
    # List all jobs
    all_jobs = extractor.list_jobs()
    print(f"\nTotal jobs: {len(all_jobs)}")
    for job in all_jobs:
        print(f"- {job.job_id}: {job.status} ({job.processed_items}/{job.total_items})")


if __name__ == "__main__":
    print("Yahoo Finance Stocks Data Extraction Examples")
    print("=" * 60)
    
    # Run examples
    try:
        basic_stocks_example()
        stocks_extraction_example()
        # Uncomment these to run additional examples
        # search_stocks_example()
        # job_monitoring_example()
        
    except KeyboardInterrupt:
        print("\n\nExamples interrupted by user.")
    except Exception as e:
        print(f"\n\nError running examples: {e}")
        print("Make sure you have set up your .env file with RAPIDAPI_KEY")
