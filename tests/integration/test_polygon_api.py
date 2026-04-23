# ====================================================================
# Polygon.io API Integration Test
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 19, 2026
# ====================================================================
# FILE: tests/integration/test_polygon_api.py
# Purpose: Verify Polygon API connectivity and document response structure
# ====================================================================
"""
Polygon.io API Integration Test

Purpose:
- Verify Polygon API connection and authentication
- Document actual API response structure for schema design
- Validate data fields returned by Polygon endpoints
- Serve as reference for future API integration work

Data Source:
- Polygon.io Stock Market API (https://polygon.io)
- Endpoints tested: Aggregate Bars, Previous Close

API Response Structure:
The Polygon API returns OHLCV data in the following format:
{
  "ticker": "AAPL",
  "queryCount": 1,
  "resultsCount": 1,
  "adjusted": true,
  "results": [
    {
      "v": 37838054.0,     // volume
      "vw": 271.9197,      // volume-weighted average price
      "o": 272.255,        // open
      "c": 271.01,         // close
      "h": 277.84,         // high
      "l": 269,            // low
      "t": 1767330000000,  // timestamp (Unix milliseconds)
      "n": 642187          // number of transactions
    }
  ],
  "status": "OK"
}

Field Mapping for Avro Schema:
- v (float) -> volume
- vw (float) -> vwap
- o (float) -> open
- c (float) -> close
- h (float) -> high
- l (float) -> low
- t (long) -> timestamp
- n (int) -> trade_count

Usage:
python3.11 tests/integration/test_polygon_api.py
"""

import json
import os
from polygon import StocksClient
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()


def test_polygon_connection():
    """Test basic Polygon API connection."""
    print("=" * 80)
    print("TEST 1: Polygon API Connection")
    print("=" * 80)
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("ERROR: POLYGON_API_KEY not found in .env")
        return False
    
    print(f"API Key found: {api_key[:10]}...")
    
    try:
        client = StocksClient(api_key=api_key)
        print("PASS: StocksClient initialized successfully")
        return True
    except Exception as e:
        print(f"FAIL: Failed to initialize StocksClient: {e}")
        return False


def test_daily_bars():
    """Test daily OHLCV bar retrieval."""
    print("\n" + "=" * 80)
    print("TEST 2: Daily OHLCV Bars (AAPL, 2026-01-02)")
    print("=" * 80)
    
    api_key = os.getenv('POLYGON_API_KEY')
    client = StocksClient(api_key=api_key)
    
    try:
        response = client.get_aggregate_bars(
            symbol='AAPL',
            from_date='2026-01-02',
            to_date='2026-01-02',
            timespan='day',
            multiplier=1,
            full_range=False,
            run_parallel=False
        )
        
        print("\nResponse Structure:")
        print(f"  Type: {type(response)}")
        print(f"  Keys: {list(response.keys())}")
        print(f"\nMetadata:")
        print(f"  Ticker: {response['ticker']}")
        print(f"  Status: {response['status']}")
        print(f"  Results Count: {response['resultsCount']}")
        print(f"  Adjusted: {response['adjusted']}")
        
        if response['results']:
            bar = response['results'][0]
            print(f"\nSingle Bar Structure:")
            print(f"  Keys: {list(bar.keys())}")
            print(f"\nBar Values:")
            print(f"  Open (o): {bar['o']}")
            print(f"  High (h): {bar['h']}")
            print(f"  Low (l): {bar['l']}")
            print(f"  Close (c): {bar['c']}")
            print(f"  Volume (v): {bar['v']}")
            print(f"  VWAP (vw): {bar['vw']}")
            print(f"  Timestamp (t): {bar['t']} (Unix ms)")
            print(f"  Transactions (n): {bar['n']}")
            
            # Convert timestamp to readable format
            timestamp_readable = datetime.fromtimestamp(bar['t'] / 1000.0)
            print(f"  Timestamp readable: {timestamp_readable}")
            
            print(f"\nFull JSON Response:")
            print(json.dumps(response, indent=2))
            
            print("\nPASS: Daily bars retrieved successfully")
            return True
        else:
            print("FAIL: No results returned")
            return False
            
    except Exception as e:
        print(f"FAIL: Failed to retrieve daily bars: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_intraday_bars():
    """Test intraday (5-minute) bar retrieval."""
    print("\n" + "=" * 80)
    print("TEST 3: Intraday 5-Minute Bars (AAPL, 2026-01-02)")
    print("=" * 80)
    
    api_key = os.getenv('POLYGON_API_KEY')
    client = StocksClient(api_key=api_key)
    
    try:
        response = client.get_aggregate_bars(
            symbol='AAPL',
            from_date='2026-01-02',
            to_date='2026-01-02',
            timespan='minute',
            multiplier=5,
            full_range=False,
            run_parallel=False
        )
        
        print(f"\nResults Count: {response['resultsCount']}")
        
        if response['results']:
            print(f"\nFirst 3 bars:")
            for i, bar in enumerate(response['results'][:3]):
                ts = datetime.fromtimestamp(bar['t'] / 1000.0)
                print(f"  Bar {i+1}: {ts} | o={bar['o']}, h={bar['h']}, l={bar['l']}, c={bar['c']}, v={bar['v']}")
            
            print("\nPASS: Intraday bars retrieved successfully")
            return True
        else:
            print("FAIL: No results returned")
            return False
            
    except Exception as e:
        print(f"FAIL: Failed to retrieve intraday bars: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_previous_close():
    """Test previous close endpoint."""
    print("\n" + "=" * 80)
    print("TEST 4: Previous Close (AAPL)")
    print("=" * 80)
    
    api_key = os.getenv('POLYGON_API_KEY')
    client = StocksClient(api_key=api_key)
    
    try:
        response = client.get_previous_close(symbol='AAPL')
        
        print(f"\nResponse:")
        print(json.dumps(response, indent=2, default=str))
        
        print("\nPASS: Previous close retrieved successfully")
        return True
            
    except Exception as e:
        print(f"FAIL: Failed to retrieve previous close: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_tickers():
    """Test retrieval for multiple tickers."""
    print("\n" + "=" * 80)
    print("TEST 5: Multiple Tickers (AAPL, MSFT, GOOGL)")
    print("=" * 80)
    
    api_key = os.getenv('POLYGON_API_KEY')
    client = StocksClient(api_key=api_key)
    
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    results = []
    
    for ticker in tickers:
        try:
            response = client.get_aggregate_bars(
                symbol=ticker,
                from_date='2026-01-02',
                to_date='2026-01-02',
                timespan='day',
                multiplier=1,
                full_range=False,
                run_parallel=False
            )
            
            if response['results']:
                bar = response['results'][0]
                results.append({
                    'ticker': ticker,
                    'close': bar['c'],
                    'volume': bar['v'],
                    'status': 'OK'
                })
                print(f"  {ticker}: close={bar['c']}, volume={bar['v']}")
            else:
                results.append({
                    'ticker': ticker,
                    'status': 'NO_DATA'
                })
                print(f"  {ticker}: No data")
        except Exception as e:
            results.append({
                'ticker': ticker,
                'status': 'ERROR',
                'error': str(e)
            })
            print(f"  {ticker}: ERROR - {e}")
    
    success_count = sum(1 for r in results if r['status'] == 'OK')
    
    if success_count == len(tickers):
        print(f"\nPASS: All {len(tickers)} tickers retrieved successfully")
        return True
    else:
        print(f"\nFAIL: Only {success_count}/{len(tickers)} tickers retrieved")
        return False


if __name__ == '__main__':
    print("\n" + "=" * 80)
    print("POLYGON.IO API INTEGRATION TESTS")
    print("Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 80)
    
    results = []
    
    # Run all tests
    results.append(("Connection", test_polygon_connection()))
    results.append(("Daily Bars", test_daily_bars()))
    results.append(("Intraday Bars", test_intraday_bars()))
    results.append(("Previous Close", test_previous_close()))
    results.append(("Multiple Tickers", test_multiple_tickers()))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {test_name}: {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\nAll tests passed. Polygon API is working correctly.")
    else:
        print(f"\n{total - passed} test(s) failed. Check errors above.")
    
    print("=" * 80)
