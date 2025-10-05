"""
Test script to verify the setup is working correctly.
"""
import sys

def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")
    
    try:
        from walmart_client import WalmartAPIClient
        print("✅ WalmartAPIClient imported successfully")
    except Exception as e:
        print(f"❌ Failed to import WalmartAPIClient: {e}")
        return False
    
    try:
        from data_extractor import WalmartDataExtractor
        print("✅ WalmartDataExtractor imported successfully")
    except Exception as e:
        print(f"❌ Failed to import WalmartDataExtractor: {e}")
        return False
    
    try:
        from models import WalmartProduct, WalmartCategory
        print("✅ Data models imported successfully")
    except Exception as e:
        print(f"❌ Failed to import data models: {e}")
        return False
    
    try:
        from config import api_config
        print("✅ Configuration loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        return False
    
    return True

def test_client_creation():
    """Test that clients can be created."""
    print("\nTesting client creation...")
    
    try:
        from walmart_client import WalmartAPIClient
        from data_extractor import WalmartDataExtractor
        
        # Test with dummy API key
        client = WalmartAPIClient("dummy_key")
        print("✅ WalmartAPIClient created successfully")
        
        extractor = WalmartDataExtractor("dummy_key")
        print("✅ WalmartDataExtractor created successfully")
        
        return True
    except Exception as e:
        print(f"❌ Failed to create clients: {e}")
        return False

def test_data_models():
    """Test that data models work correctly."""
    print("\nTesting data models...")
    
    try:
        from models import WalmartProduct, WalmartCategory
        from datetime import datetime
        
        # Test product model
        product = WalmartProduct(
            name="Test Product",
            price=29.99,
            brand="Test Brand",
            in_stock=True
        )
        print("✅ WalmartProduct model works")
        
        # Test category model
        category = WalmartCategory(
            name="Test Category",
            url="https://example.com",
            products=[product]
        )
        print("✅ WalmartCategory model works")
        
        return True
    except Exception as e:
        print(f"❌ Data models test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("Walmart API Data Sourcing - Setup Test")
    print("=" * 50)
    
    all_passed = True
    
    # Test imports
    if not test_imports():
        all_passed = False
    
    # Test client creation
    if not test_client_creation():
        all_passed = False
    
    # Test data models
    if not test_data_models():
        all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All tests passed! Your setup is working correctly.")
        print("\nNext steps:")
        print("1. Get your RapidAPI key from https://rapidapi.com")
        print("2. Update the .env file with your RAPIDAPI_KEY")
        print("3. Run: python3 example_usage.py")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()

