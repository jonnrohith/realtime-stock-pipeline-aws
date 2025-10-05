"""
Example usage of the Walmart API data sourcing system.
"""
import os
from dotenv import load_dotenv

from walmart_client import WalmartAPIClient
from data_extractor import WalmartDataExtractor
from config import api_config

# Load environment variables
load_dotenv()


def basic_usage_example():
    """Basic example of using the Walmart API client."""
    print("=== Basic Walmart API Usage Example ===\n")
    
    # Initialize the client
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    client = WalmartAPIClient(rapidapi_key)
    
    # Test connection
    print("Testing API connection...")
    if client.test_connection():
        print("✅ Connection successful!")
    else:
        print("❌ Connection failed!")
        return
    
    # Example category URL (phone cases)
    category_url = "https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952"
    
    print(f"\nFetching data from category: {category_url}")
    
    # Get category data
    response = client.get_category_data(category_url)
    
    if response.success:
        print(f"✅ Successfully fetched data!")
        print(f"Response time: {response.response_time:.2f} seconds")
        print(f"Status code: {response.status_code}")
        
        # Print some basic info about the response
        if response.data:
            print(f"Response keys: {list(response.data.keys())}")
    else:
        print(f"❌ Failed to fetch data: {response.error}")


def data_extraction_example():
    """Example of using the data extractor for comprehensive data collection."""
    print("\n=== Data Extraction Example ===\n")
    
    # Initialize the data extractor
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    extractor = WalmartDataExtractor(rapidapi_key)
    
    # Example category URLs
    category_urls = [
        "https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952",
        "https://www.walmart.com/browse/electronics/laptops/3944_3951_1089430",
        "https://www.walmart.com/browse/home/home-decor/4044_1076885"
    ]
    
    print(f"Extracting data from {len(category_urls)} categories...")
    
    # Extract data from multiple categories
    job = extractor.extract_multiple_categories(
        category_urls=category_urls,
        job_id="example_extraction",
        save_to_file=True,
        output_format="json",
        delay_between_requests=2.0  # 2 second delay between requests
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


def single_category_example():
    """Example of extracting data from a single category."""
    print("\n=== Single Category Extraction Example ===\n")
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    extractor = WalmartDataExtractor(rapidapi_key)
    
    # Single category URL
    category_url = "https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952"
    
    print(f"Extracting data from: {category_url}")
    
    # Extract data
    job = extractor.extract_category_data(
        category_url=category_url,
        job_id="single_category_example",
        save_to_file=True,
        output_format="csv"  # Save as CSV
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
        print("✅ Data extraction completed successfully!")
        print("Check the 'output' directory for saved files.")
    else:
        print(f"❌ Data extraction failed: {job.error_message}")


def search_example():
    """Example of searching for products."""
    print("\n=== Product Search Example ===\n")
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    extractor = WalmartDataExtractor(rapidapi_key)
    
    # Search query
    query = "iPhone cases"
    
    print(f"Searching for: {query}")
    
    # Extract search results
    job = extractor.extract_search_results(
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
    
    extractor = WalmartDataExtractor(rapidapi_key)
    
    # Start a job
    category_url = "https://www.walmart.com/browse/cell-phones/phone-cases/1105910_133161_1997952"
    job = extractor.extract_category_data(
        category_url=category_url,
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
    print("Walmart API Data Sourcing Examples")
    print("=" * 50)
    
    # Run examples
    try:
        basic_usage_example()
        single_category_example()
        # Uncomment these to run additional examples
        # data_extraction_example()
        # search_example()
        # job_monitoring_example()
        
    except KeyboardInterrupt:
        print("\n\nExamples interrupted by user.")
    except Exception as e:
        print(f"\n\nError running examples: {e}")
        print("Make sure you have set up your .env file with RAPIDAPI_KEY")
