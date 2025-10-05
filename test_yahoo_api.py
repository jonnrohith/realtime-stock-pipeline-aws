#!/usr/bin/env python3
"""
Test script to verify Yahoo Finance API connection and data extraction.
"""
import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_direct_api_call():
    """Test the Yahoo Finance API directly using the provided code format."""
    print("=== Testing Direct Yahoo Finance API Call ===\n")
    
    url = "https://yahoo-finance15.p.rapidapi.com/api/v2/markets/tickers"
    querystring = {"page": "1", "type": "STOCKS"}
    headers = {
        "x-rapidapi-key": "71e112c20fmshc435cc02aea873ap14d266jsn27048b3027e6",
        "x-rapidapi-host": "yahoo-finance15.p.rapidapi.com"
    }
    
    try:
        print(f"Making request to: {url}")
        print(f"Query: {querystring}")
        print(f"Headers: {headers}")
        print()
        
        response = requests.get(url, headers=headers, params=querystring)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print()
        
        if response.status_code == 200:
            data = response.json()
            print("✅ API call successful!")
            print(f"Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            print(f"Response preview: {str(data)[:500]}...")
            return True
        else:
            print(f"❌ API call failed: {response.status_code}")
            print(f"Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        return False

def test_yahoo_client():
    """Test using the YahooFinanceAPIClient class."""
    print("\n=== Testing YahooFinanceAPIClient ===\n")
    
    try:
        from yahoo_client import YahooFinanceAPIClient
        
        # Initialize client
        client = YahooFinanceAPIClient("71e112c20fmshc435cc02aea873ap14d266jsn27048b3027e6")
        
        # Test connection
        print("Testing API connection...")
        if client.test_connection():
            print("✅ Connection test successful!")
        else:
            print("❌ Connection test failed!")
            return False
        
        # Test market tickers
        print("\nTesting market tickers...")
        response = client.get_market_tickers(page=1, type_filter="STOCKS")
        
        if response.success:
            print("✅ Market tickers successful!")
            print(f"Response time: {response.response_time:.2f} seconds")
            print(f"Data keys: {list(response.data.keys()) if response.data else 'No data'}")
            return True
        else:
            print(f"❌ Market tickers failed: {response.error}")
            return False
            
    except Exception as e:
        print(f"❌ Exception in client test: {e}")
        return False

def test_stocks_extractor():
    """Test using the YahooFinanceDataExtractor class."""
    print("\n=== Testing YahooFinanceDataExtractor ===\n")
    
    try:
        from stocks_extractor import YahooFinanceDataExtractor
        
        # Initialize extractor
        extractor = YahooFinanceDataExtractor("71e112c20fmshc435cc02aea873ap14d266jsn27048b3027e6")
        
        # Test market tickers extraction
        print("Testing market tickers extraction...")
        job = extractor.extract_market_tickers(
            page=1,
            type_filter="STOCKS",
            job_id="test_tickers",
            save_to_file=True,
            output_format="json"
        )
        
        print(f"Job ID: {job.job_id}")
        print(f"Status: {job.status}")
        print(f"Total items: {job.total_items}")
        print(f"Processed items: {job.processed_items}")
        if job.duration_seconds:
            print(f"Duration: {job.duration_seconds:.2f} seconds")
        
        if job.status == "completed":
            print("✅ Tickers extraction successful!")
            return True
        else:
            print(f"❌ Tickers extraction failed: {job.error_message}")
            return False
            
    except Exception as e:
        print(f"❌ Exception in extractor test: {e}")
        return False

def test_stock_quotes():
    """Test stock quotes extraction."""
    print("\n=== Testing Stock Quotes ===\n")
    
    try:
        from stocks_extractor import YahooFinanceDataExtractor
        
        # Initialize extractor
        extractor = YahooFinanceDataExtractor("71e112c20fmshc435cc02aea873ap14d266jsn27048b3027e6")
        
        # Test stock quotes extraction
        print("Testing stock quotes extraction...")
        job = extractor.extract_stock_quotes(
            symbols=["AAPL", "GOOGL", "MSFT"],
            job_id="test_quotes",
            save_to_file=True,
            output_format="json"
        )
        
        print(f"Job ID: {job.job_id}")
        print(f"Status: {job.status}")
        print(f"Total items: {job.total_items}")
        print(f"Processed items: {job.processed_items}")
        if job.duration_seconds:
            print(f"Duration: {job.duration_seconds:.2f} seconds")
        
        if job.status == "completed":
            print("✅ Quotes extraction successful!")
            return True
        else:
            print(f"❌ Quotes extraction failed: {job.error_message}")
            return False
            
    except Exception as e:
        print(f"❌ Exception in quotes test: {e}")
        return False

if __name__ == "__main__":
    print("Yahoo Finance API Test Script")
    print("=" * 50)
    
    # Test direct API call
    direct_success = test_direct_api_call()
    
    # Test client
    client_success = test_yahoo_client()
    
    # Test extractor
    extractor_success = test_stocks_extractor()
    
    # Test quotes
    quotes_success = test_stock_quotes()
    
    print("\n" + "=" * 50)
    print("SUMMARY:")
    print(f"Direct API call: {'✅ SUCCESS' if direct_success else '❌ FAILED'}")
    print(f"YahooFinanceAPIClient: {'✅ SUCCESS' if client_success else '❌ FAILED'}")
    print(f"YahooFinanceDataExtractor: {'✅ SUCCESS' if extractor_success else '❌ FAILED'}")
    print(f"Stock Quotes: {'✅ SUCCESS' if quotes_success else '❌ FAILED'}")
    
    if direct_success or client_success or extractor_success:
        print("\n🎉 Yahoo Finance API is working! You can now extract stocks data.")
    else:
        print("\n💥 Yahoo Finance API is not working. Check your credentials and endpoint.")
