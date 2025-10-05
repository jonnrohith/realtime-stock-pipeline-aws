#!/usr/bin/env python3
"""
Test script to verify Walmart API connection with new credentials.
"""
import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_direct_api_call():
    """Test the API directly using the provided code format."""
    print("=== Testing Direct API Call ===\n")
    
    url = "https://walmart-api4.p.rapidapi.com/walmart-serp.php"
    query = {"url": "https://www.walmart.com/search?q=samsung+galaxy"}
    headers = {
        "x-rapidapi-host": "walmart-api4.p.rapidapi.com",
        "x-rapidapi-key": "71e112c20fmshc435cc02aea873ap14d266jsn27048b3027e6"
    }
    
    try:
        print(f"Making request to: {url}")
        print(f"Query: {query}")
        print(f"Headers: {headers}")
        print()
        
        response = requests.get(url, headers=headers, params=query)
        
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

def test_walmart_client():
    """Test using the WalmartAPIClient class."""
    print("\n=== Testing WalmartAPIClient ===\n")
    
    try:
        from walmart_client import WalmartAPIClient
        
        # Initialize client
        client = WalmartAPIClient("71e112c20fmshc435cc02aea873ap14d266jsn27048b3027e6")
        
        # Test connection
        print("Testing API connection...")
        if client.test_connection():
            print("✅ Connection test successful!")
        else:
            print("❌ Connection test failed!")
            return False
        
        # Test search
        print("\nTesting product search...")
        response = client.search_products("samsung galaxy")
        
        if response.success:
            print("✅ Search successful!")
            print(f"Response time: {response.response_time:.2f} seconds")
            print(f"Data keys: {list(response.data.keys()) if response.data else 'No data'}")
            return True
        else:
            print(f"❌ Search failed: {response.error}")
            return False
            
    except Exception as e:
        print(f"❌ Exception in client test: {e}")
        return False

if __name__ == "__main__":
    print("Walmart API Test Script")
    print("=" * 50)
    
    # Test direct API call
    direct_success = test_direct_api_call()
    
    # Test client
    client_success = test_walmart_client()
    
    print("\n" + "=" * 50)
    print("SUMMARY:")
    print(f"Direct API call: {'✅ SUCCESS' if direct_success else '❌ FAILED'}")
    print(f"WalmartAPIClient: {'✅ SUCCESS' if client_success else '❌ FAILED'}")
    
    if direct_success or client_success:
        print("\n🎉 API is working! You can now extract data.")
    else:
        print("\n💥 API is not working. Check your credentials and endpoint.")
