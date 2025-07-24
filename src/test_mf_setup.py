# Quick test script to verify the Indian MF data fetcher
import sys
import os

def test_imports():
    """Test if all required packages are available."""
    try:
        import pandas as pd
        import requests
        from datetime import datetime
        from dotenv import load_dotenv
        print("✅ All required packages are installed!")
        return True
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        return False

def test_data_fetch():
    """Test a simple data fetch operation."""
    try:
        # Add the src directory to Python path
        sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

        from get_indian_mutual_funds import IndianMutualFundDataFetcher

        print("🔄 Testing Indian Mutual Fund data fetcher...")
        fetcher = IndianMutualFundDataFetcher()

        # Test just the MFAPI endpoint (faster than AMFI for testing)
        schemes = fetcher.fetch_mfapi_schemes()

        if schemes and len(schemes) > 0:
            print(f"✅ Successfully connected! Found {len(schemes)} schemes")
            print("🔍 Sample schemes:")
            for i, scheme in enumerate(schemes[:5]):
                print(f"  {i+1}. {scheme.get('schemeName', 'Unknown')[:50]}")
            return True
        else:
            print("⚠️ Connected but no data returned")
            return False

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Indian Mutual Fund Data Fetcher Setup")
    print("=" * 50)

    # Test imports
    if test_imports():
        # Test data fetching
        test_data_fetch()

    print("=" * 50)
    print("🏁 Test complete!")
