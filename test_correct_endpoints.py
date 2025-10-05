#!/usr/bin/env python3
"""
Test script to verify the correct Yahoo Finance API endpoints.
"""
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_endpoint(endpoint, params=None, description=""):
    """Test a specific endpoint and return results."""
    url = f"https://yahoo-finance15.p.rapidapi.com{endpoint}"
    headers = {
        "x-rapidapi-key": "71e112c20fmshc435cc02aea873ap14d266jsn27048b3027e6",
        "x-rapidapi-host": "yahoo-finance15.p.rapidapi.com"
    }
    
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"Endpoint: {endpoint}")
    print(f"Params: {params}")
    print(f"{'='*60}")
    
    try:
        response = requests.get(url, headers=headers, params=params or {})
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ SUCCESS!")
            print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            # Show sample data
            if isinstance(data, dict):
                if "body" in data and isinstance(data["body"], list) and len(data["body"]) > 0:
                    print(f"Sample data (first item): {json.dumps(data['body'][0], indent=2)}")
                elif len(data) > 0:
                    first_key = list(data.keys())[0]
                    print(f"Sample data ({first_key}): {json.dumps(data[first_key], indent=2)[:500]}...")
            else:
                print(f"Sample data: {str(data)[:500]}...")
            
            return True
        else:
            print(f"❌ FAILED: {response.status_code}")
            print(f"Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False

def main():
    """Test the correct Yahoo Finance API endpoints."""
    print("Yahoo Finance API - Correct Endpoints Test")
    print("=" * 60)
    
    # List of correct endpoints to test
    endpoints_to_test = [
        # Market tickers (we know this works)
        ("/api/v2/markets/tickers", {"page": "1", "type": "STOCKS"}, "Market Tickers"),
        
        # Search endpoints
        ("/api/v1/markets/search", {"search": "AA"}, "Stock Search"),
        ("/api/v1/markets/search", {"search": "Apple"}, "Apple Search"),
        
        # Quote endpoints
        ("/api/v1/markets/quote", {"ticker": "AAPL", "type": "STOCKS"}, "Single Quote"),
        ("/api/v1/markets/stock/quotes", {"ticker": "AAPL,MSFT,GOOGL"}, "Multiple Quotes"),
        
        # Historical data
        ("/api/v2/markets/stock/history", {"symbol": "AAPL", "interval": "1d", "limit": "30"}, "Stock History"),
        ("/api/v2/markets/stock/history", {"symbol": "AAPL", "interval": "1m", "limit": "100"}, "Minute History"),
        
        # Market screener
        ("/api/v1/markets/screener", {"list": "day_gainers"}, "Day Gainers"),
        ("/api/v1/markets/screener", {"list": "day_losers"}, "Day Losers"),
        ("/api/v1/markets/screener", {"list": "most_actives"}, "Most Actives"),
        
        # Stock modules
        ("/api/v1/markets/stock/modules", {"ticker": "AAPL", "module": "asset-profile"}, "Asset Profile"),
        ("/api/v1/markets/stock/modules", {"ticker": "AAPL", "module": "financial-data"}, "Financial Data"),
        ("/api/v1/markets/stock/modules", {"ticker": "AAPL", "module": "price"}, "Price Data"),
        ("/api/v1/markets/stock/modules", {"ticker": "AAPL", "module": "summary-detail"}, "Summary Detail"),
        
        # News
        ("/api/v2/markets/news", {"tickers": "AAPL", "type": "ALL"}, "Stock News"),
        ("/api/v2/markets/news", {"tickers": "AAPL,MSFT", "type": "ALL"}, "Multiple Stock News"),
    ]
    
    successful_endpoints = []
    failed_endpoints = []
    
    for endpoint, params, description in endpoints_to_test:
        success = test_endpoint(endpoint, params, description)
        if success:
            successful_endpoints.append((endpoint, description))
        else:
            failed_endpoints.append((endpoint, description))
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Successful endpoints: {len(successful_endpoints)}")
    for endpoint, desc in successful_endpoints:
        print(f"  - {desc}: {endpoint}")
    
    print(f"\n❌ Failed endpoints: {len(failed_endpoints)}")
    for endpoint, desc in failed_endpoints:
        print(f"  - {desc}: {endpoint}")
    
    print(f"\n🎉 You can use {len(successful_endpoints)} different endpoints!")

if __name__ == "__main__":
    main()
