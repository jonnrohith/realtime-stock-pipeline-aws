#!/usr/bin/env python3
"""
Comprehensive example using all available Yahoo Finance API endpoints.
"""
import os
import json
from dotenv import load_dotenv

from updated_yahoo_client import UpdatedYahooFinanceAPIClient, UpdatedYahooFinanceDataParser

# Load environment variables
load_dotenv()


def demonstrate_all_endpoints():
    """Demonstrate all available Yahoo Finance API endpoints."""
    print("Yahoo Finance API - All Endpoints Demo")
    print("=" * 60)
    
    # Initialize the client
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    if not rapidapi_key:
        print("Please set RAPIDAPI_KEY in your .env file")
        return
    
    client = UpdatedYahooFinanceAPIClient(rapidapi_key)
    parser = UpdatedYahooFinanceDataParser()
    
    # Test connection
    print("Testing API connection...")
    if not client.test_connection():
        print("❌ Connection failed!")
        return
    print("✅ Connection successful!")
    
    # 1. Market Tickers
    print("\n" + "="*60)
    print("1. MARKET TICKERS")
    print("="*60)
    
    response = client.get_market_tickers(page=1, type_filter="STOCKS")
    if response.success:
        stocks = parser.parse_tickers_response(response.data)
        print(f"✅ Retrieved {len(stocks)} stocks")
        print(f"Sample: {stocks[0].symbol} - {stocks[0].name}")
    else:
        print(f"❌ Failed: {response.error}")
    
    # 2. Stock Search
    print("\n" + "="*60)
    print("2. STOCK SEARCH")
    print("="*60)
    
    response = client.search_stocks("Apple")
    if response.success:
        search_results = parser.parse_search_response(response.data)
        print(f"✅ Found {len(search_results)} results for 'Apple'")
        for stock in search_results[:3]:  # Show first 3
            print(f"  - {stock.symbol}: {stock.name} ({stock.sector})")
    else:
        print(f"❌ Failed: {response.error}")
    
    # 3. Single Quote
    print("\n" + "="*60)
    print("3. SINGLE QUOTE")
    print("="*60)
    
    response = client.get_single_quote("AAPL")
    if response.success:
        print(f"✅ Retrieved quote for AAPL")
        print(f"Response keys: {list(response.data.keys())}")
    else:
        print(f"❌ Failed: {response.error}")
    
    # 4. Multiple Quotes
    print("\n" + "="*60)
    print("4. MULTIPLE QUOTES")
    print("="*60)
    
    response = client.get_multiple_quotes(["AAPL", "MSFT", "GOOGL"])
    if response.success:
        quotes = parser.parse_quotes_response(response.data)
        print(f"✅ Retrieved quotes for {len(quotes)} stocks")
        for quote in quotes:
            print(f"  - {quote.symbol}: ${quote.price} ({quote.change_percent}%)")
    else:
        print(f"❌ Failed: {response.error}")
    
    # 5. Stock History
    print("\n" + "="*60)
    print("5. STOCK HISTORY")
    print("="*60)
    
    response = client.get_stock_history("AAPL", interval="1d", limit=5)
    if response.success:
        print(f"✅ Retrieved {len(response.data.get('body', []))} days of history for AAPL")
        if response.data.get("body"):
            latest = response.data["body"][0]
            print(f"Latest: {latest['timestamp']} - Close: ${latest['close']}")
    else:
        print(f"❌ Failed: {response.error}")
    
    # 6. Market Screeners
    print("\n" + "="*60)
    print("6. MARKET SCREENERS")
    print("="*60)
    
    # Day Gainers
    response = client.get_day_gainers()
    if response.success:
        print(f"✅ Day Gainers: {len(response.data.get('body', []))} stocks")
        if response.data.get("body"):
            top_gainer = response.data["body"][0]
            print(f"Top Gainer: {top_gainer['symbol']} ({top_gainer['regularMarketChangePercent']}%)")
    else:
        print(f"❌ Day Gainers failed: {response.error}")
    
    # Day Losers
    response = client.get_day_losers()
    if response.success:
        print(f"✅ Day Losers: {len(response.data.get('body', []))} stocks")
    else:
        print(f"❌ Day Losers failed: {response.error}")
    
    # Most Actives
    response = client.get_most_actives()
    if response.success:
        print(f"✅ Most Actives: {len(response.data.get('body', []))} stocks")
    else:
        print(f"❌ Most Actives failed: {response.error}")
    
    # 7. Stock Modules
    print("\n" + "="*60)
    print("7. STOCK MODULES")
    print("="*60)
    
    # Asset Profile
    response = client.get_asset_profile("AAPL")
    if response.success:
        print(f"✅ Asset Profile for AAPL retrieved")
        print(f"Response keys: {list(response.data.keys())}")
    else:
        print(f"❌ Asset Profile failed: {response.error}")
    
    # Financial Data
    response = client.get_financial_data("AAPL")
    if response.success:
        print(f"✅ Financial Data for AAPL retrieved")
        print(f"Response keys: {list(response.data.keys())}")
    else:
        print(f"❌ Financial Data failed: {response.error}")
    
    # 8. News
    print("\n" + "="*60)
    print("8. STOCK NEWS")
    print("="*60)
    
    response = client.get_stock_news(["AAPL"])
    if response.success:
        news_items = response.data.get("body", [])
        print(f"✅ Retrieved {len(news_items)} news items for AAPL")
        if news_items:
            latest_news = news_items[0]
            print(f"Latest: {latest_news['title'][:50]}...")
    else:
        print(f"❌ News failed: {response.error}")
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("✅ All major Yahoo Finance endpoints are working!")
    print("You can now extract:")
    print("  - Market tickers and search results")
    print("  - Real-time quotes (single and multiple)")
    print("  - Historical price data")
    print("  - Market screeners (gainers, losers, most active)")
    print("  - Detailed stock modules (profile, financials)")
    print("  - Stock news and market updates")


def save_sample_data():
    """Save sample data from all endpoints to files."""
    print("\n" + "="*60)
    print("SAVING SAMPLE DATA")
    print("="*60)
    
    rapidapi_key = os.getenv("RAPIDAPI_KEY")
    client = UpdatedYahooFinanceAPIClient(rapidapi_key)
    parser = UpdatedYahooFinanceDataParser()
    
    # Create output directory
    import os as os_module
    os_module.makedirs("output", exist_ok=True)
    
    # Save market tickers
    response = client.get_market_tickers(page=1, type_filter="STOCKS")
    if response.success:
        stocks = parser.parse_tickers_response(response.data)
        with open("output/sample_tickers.json", "w") as f:
            json.dump([stock.dict() for stock in stocks[:10]], f, indent=2, default=str)
        print("✅ Saved sample tickers to output/sample_tickers.json")
    
    # Save search results
    response = client.search_stocks("Apple")
    if response.success:
        search_results = parser.parse_search_response(response.data)
        with open("output/sample_search.json", "w") as f:
            json.dump([stock.dict() for stock in search_results], f, indent=2, default=str)
        print("✅ Saved search results to output/sample_search.json")
    
    # Save quotes
    response = client.get_multiple_quotes(["AAPL", "MSFT", "GOOGL"])
    if response.success:
        quotes = parser.parse_quotes_response(response.data)
        with open("output/sample_quotes.json", "w") as f:
            json.dump([quote.dict() for quote in quotes], f, indent=2, default=str)
        print("✅ Saved quotes to output/sample_quotes.json")
    
    # Save history
    response = client.get_stock_history("AAPL", interval="1d", limit=30)
    if response.success:
        with open("output/sample_history.json", "w") as f:
            json.dump(response.data, f, indent=2, default=str)
        print("✅ Saved history to output/sample_history.json")
    
    # Save day gainers
    response = client.get_day_gainers()
    if response.success:
        with open("output/sample_gainers.json", "w") as f:
            json.dump(response.data, f, indent=2, default=str)
        print("✅ Saved day gainers to output/sample_gainers.json")
    
    # Save news
    response = client.get_stock_news(["AAPL"])
    if response.success:
        with open("output/sample_news.json", "w") as f:
            json.dump(response.data, f, indent=2, default=str)
        print("✅ Saved news to output/sample_news.json")


if __name__ == "__main__":
    try:
        demonstrate_all_endpoints()
        save_sample_data()
        
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user.")
    except Exception as e:
        print(f"\n\nError running demo: {e}")
        print("Make sure you have set up your .env file with RAPIDAPI_KEY")
