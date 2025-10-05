#!/usr/bin/env python3
"""
Test script to discover available Yahoo Finance API endpoints.
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
        print(f"Response Headers: {dict(response.headers)}")
        
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
    """Test various Yahoo Finance API endpoints."""
    print("Yahoo Finance API Endpoints Discovery")
    print("=" * 60)
    
    # List of endpoints to test
    endpoints_to_test = [
        # Market data endpoints
        ("/api/v2/markets/tickers", {"page": "1", "type": "STOCKS"}, "Market Tickers"),
        ("/api/v2/markets/tickers", {"page": "1", "type": "ETFS"}, "ETF Tickers"),
        ("/api/v2/markets/tickers", {"page": "1", "type": "CRYPTO"}, "Crypto Tickers"),
        
        # Search endpoints
        ("/api/v1/search", {"query": "Apple"}, "Stock Search"),
        ("/api/v1/search", {"query": "AAPL"}, "Symbol Search"),
        
        # Quote endpoints
        ("/api/v1/market/quotes", {"symbol": "AAPL"}, "Real-time Quotes"),
        ("/api/v1/market/quotes", {"symbol": "AAPL,GOOGL,MSFT"}, "Multiple Quotes"),
        
        # Historical data endpoints
        ("/api/v2/stock/history", {"symbol": "AAPL", "period": "1mo", "interval": "1d"}, "Stock History"),
        ("/api/v1/stock/history", {"symbol": "AAPL", "period": "1mo", "interval": "1d"}, "Stock History v1"),
        
        # Financial data endpoints
        ("/api/v1/stock/modules", {"symbol": "AAPL", "module": "financialData"}, "Financial Data"),
        ("/api/v1/stock/modules", {"symbol": "AAPL", "module": "summaryDetail"}, "Summary Detail"),
        ("/api/v1/stock/modules", {"symbol": "AAPL", "module": "price"}, "Price Data"),
        ("/api/v1/stock/modules", {"symbol": "AAPL", "module": "quoteSummary"}, "Quote Summary"),
        
        # Market screener
        ("/api/v1/market/screener", {"region": "US", "lang": "en"}, "Market Screener"),
        
        # News endpoints
        ("/api/v2/market/news", {"symbol": "AAPL"}, "Market News"),
        ("/api/v1/market/news", {"symbol": "AAPL"}, "Market News v1"),
        
        # Options data
        ("/api/v1/options", {"symbol": "AAPL"}, "Options Data"),
        
        # Insider trades
        ("/api/v1/insider-trades", {"symbol": "AAPL"}, "Insider Trades"),
        
        # Calendar events
        ("/api/v1/calendar", {"symbol": "AAPL"}, "Calendar Events"),
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
