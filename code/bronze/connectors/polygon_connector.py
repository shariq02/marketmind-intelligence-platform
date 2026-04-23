# ====================================================================
# Polygon.io API Connector
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/connectors/polygon_connector.py
# Purpose: Fetch OHLCV bars and corporate actions from Polygon API
# ====================================================================
"""
Polygon.io Data Connector

Fetches:
- OHLCV bars (daily and intraday)
- Stock splits
- Dividends

Features:
- Rate limiting (5 calls/min free tier)
- Automatic retry with exponential backoff
- Pydantic validation
- Error handling

Usage:
    from code.bronze.connectors.polygon_connector import PolygonConnector
    
    connector = PolygonConnector()
    
    # Fetch daily bars
    bars = connector.fetch_daily_bars("AAPL", "2026-01-01", "2026-01-31")
    
    # Fetch intraday bars
    bars = connector.fetch_intraday_bars("AAPL", "2026-01-15", granularity="5min")
    
    # Fetch splits
    splits = connector.fetch_splits("AAPL", "2020-01-01", "2026-12-31")
    
    # Fetch dividends
    dividends = connector.fetch_dividends("AAPL", "2020-01-01", "2026-12-31")
"""

import time
from typing import List, Optional
from datetime import datetime, timedelta
from polygon import StocksClient, ReferenceClient

from config import (
    POLYGON_API_KEY,
    POLYGON_RATE_LIMIT,
    get_logger,
)

# Import Pydantic models
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'schemas'))

from market_bar import MarketBar
from corporate_action import CorporateAction, ActionType

logger = get_logger(__name__)


class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.call_times = []
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        now = time.time()
        
        # Remove calls older than 1 minute
        self.call_times = [t for t in self.call_times if now - t < 60]
        
        # If at limit, wait
        if len(self.call_times) >= self.calls_per_minute:
            sleep_time = 60 - (now - self.call_times[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached, sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        
        # Record this call
        self.call_times.append(time.time())


class PolygonConnector:
    """
    Connector for Polygon.io API.
    
    Handles data fetching, rate limiting, and validation.
    """
    
    def __init__(self):
        """Initialize Polygon clients and rate limiter."""
        self.stocks_client = StocksClient(api_key=POLYGON_API_KEY)
        self.reference_client = ReferenceClient(api_key=POLYGON_API_KEY)
        self.rate_limiter = RateLimiter(POLYGON_RATE_LIMIT['calls_per_minute'])
        
        logger.info("PolygonConnector initialized")
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry."""
        max_attempts = POLYGON_RATE_LIMIT['retry_attempts']
        delay = POLYGON_RATE_LIMIT['retry_delay_seconds']
        
        for attempt in range(max_attempts):
            try:
                self.rate_limiter.wait_if_needed()
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.error(f"Failed after {max_attempts} attempts: {e}")
                    raise
                
                wait_time = delay * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
    
    def fetch_daily_bars(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> List[MarketBar]:
        """
        Fetch daily OHLCV bars for a ticker.
        
        Args:
            ticker: Stock ticker symbol (e.g., AAPL)
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD
        
        Returns:
            List of validated MarketBar objects
        """
        logger.info(f"Fetching daily bars: {ticker} from {start_date} to {end_date}")
        
        def _fetch():
            response = self.stocks_client.get_aggregate_bars(
                symbol=ticker,
                from_date=start_date,
                to_date=end_date,
                timespan='day',
                multiplier=1,
                full_range=False,
                run_parallel=False
            )
            return response
        
        response = self._retry_with_backoff(_fetch)
        
        bars = []
        if response and response.get('results'):
            for bar_data in response['results']:
                try:
                    bar = MarketBar.from_polygon_response(
                        ticker=ticker,
                        bar_data=bar_data,
                        granularity='daily'
                    )
                    bars.append(bar)
                except Exception as e:
                    logger.error(f"Failed to validate bar for {ticker}: {e}")
                    continue
        
        logger.info(f"Fetched {len(bars)} daily bars for {ticker}")
        return bars
    
    def fetch_intraday_bars(
        self,
        ticker: str,
        date: str,
        granularity: str = '5min'
    ) -> List[MarketBar]:
        """
        Fetch intraday OHLCV bars for a ticker on a specific date.
        
        Args:
            ticker: Stock ticker symbol
            date: Date YYYY-MM-DD
            granularity: Bar size (1min, 5min, 15min, 30min, 1hour)
        
        Returns:
            List of validated MarketBar objects
        """
        logger.info(f"Fetching {granularity} bars: {ticker} on {date}")
        
        # Parse granularity
        multiplier_map = {
            '1min': (1, 'minute'),
            '5min': (5, 'minute'),
            '15min': (15, 'minute'),
            '30min': (30, 'minute'),
            '1hour': (1, 'hour'),
        }
        
        if granularity not in multiplier_map:
            raise ValueError(f"Invalid granularity: {granularity}")
        
        multiplier, timespan = multiplier_map[granularity]
        
        def _fetch():
            response = self.stocks_client.get_aggregate_bars(
                symbol=ticker,
                from_date=date,
                to_date=date,
                timespan=timespan,
                multiplier=multiplier,
                full_range=False,
                run_parallel=False
            )
            return response
        
        response = self._retry_with_backoff(_fetch)
        
        bars = []
        if response and response.get('results'):
            for bar_data in response['results']:
                try:
                    bar = MarketBar.from_polygon_response(
                        ticker=ticker,
                        bar_data=bar_data,
                        granularity=granularity
                    )
                    bars.append(bar)
                except Exception as e:
                    logger.error(f"Failed to validate bar for {ticker}: {e}")
                    continue
        
        logger.info(f"Fetched {len(bars)} {granularity} bars for {ticker}")
        return bars
    
    def fetch_splits(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> List[CorporateAction]:
        """
        Fetch stock splits for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD
        
        Returns:
            List of validated CorporateAction objects (SPLIT type)
        """
        logger.info(f"Fetching splits: {ticker} from {start_date} to {end_date}")
        
        def _fetch():
            response = self.reference_client.get_stock_splits(
                ticker=ticker,
                execution_date_gte=start_date,
                execution_date_lte=end_date
            )
            return response
        
        response = self._retry_with_backoff(_fetch)
        
        splits = []
        if response and isinstance(response, list):
            for split_data in response:
                try:
                    split = CorporateAction.from_polygon_split(ticker, split_data)
                    splits.append(split)
                except Exception as e:
                    logger.error(f"Failed to validate split for {ticker}: {e}")
                    continue
        
        logger.info(f"Fetched {len(splits)} splits for {ticker}")
        return splits
    
    def fetch_dividends(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> List[CorporateAction]:
        """
        Fetch dividends for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date YYYY-MM-DD
            end_date: End date YYYY-MM-DD
        
        Returns:
            List of validated CorporateAction objects (DIVIDEND type)
        """
        logger.info(f"Fetching dividends: {ticker} from {start_date} to {end_date}")
        
        def _fetch():
            response = self.reference_client.get_stock_dividends(
                ticker=ticker,
                ex_dividend_date_gte=start_date,
                ex_dividend_date_lte=end_date
            )
            return response
        
        response = self._retry_with_backoff(_fetch)
        
        dividends = []
        if response and isinstance(response, list):
            for dividend_data in response:
                try:
                    dividend = CorporateAction.from_polygon_dividend(ticker, dividend_data)
                    dividends.append(dividend)
                except Exception as e:
                    logger.error(f"Failed to validate dividend for {ticker}: {e}")
                    continue
        
        logger.info(f"Fetched {len(dividends)} dividends for {ticker}")
        return dividends


# Example usage
if __name__ == '__main__':
    connector = PolygonConnector()
    
    # Test daily bars
    print("Testing daily bars...")
    bars = connector.fetch_daily_bars("AAPL", "2026-01-02", "2026-01-05")
    print(f"Fetched {len(bars)} daily bars")
    if bars:
        print(f"Sample: {bars[0].ticker} {bars[0].close}")
    
    # Test intraday bars
    print("\nTesting intraday bars...")
    intraday = connector.fetch_intraday_bars("AAPL", "2026-01-02", "5min")
    print(f"Fetched {len(intraday)} 5min bars")
    
    # Test splits
    print("\nTesting splits...")
    splits = connector.fetch_splits("AAPL", "2020-01-01", "2026-12-31")
    print(f"Fetched {len(splits)} splits")
    
    # Test dividends
    print("\nTesting dividends...")
    dividends = connector.fetch_dividends("AAPL", "2020-01-01", "2026-12-31")
    print(f"Fetched {len(dividends)} dividends")
