# ====================================================================
# EdgarTools SEC Filings Connector
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/connectors/edgartools_connector.py
# Purpose: Fetch SEC filing metadata from EdgarTools library
# ====================================================================
"""
EdgarTools SEC Filings Connector

Fetches SEC filing metadata:
- 10-K (Annual Reports)
- 10-Q (Quarterly Reports)
- 8-K (Current Reports)

Features:
- User identity header (required by SEC)
- Rate limiting
- Pydantic validation
- Error handling

Usage:
    from code.bronze.connectors.edgartools_connector import EdgarToolsConnector
    
    connector = EdgarToolsConnector()
    
    # Fetch 10-K filings for AAPL
    filings = connector.fetch_filings("AAPL", form_type="10-K", limit=5)
    
    # Fetch recent 8-K filings
    filings = connector.fetch_filings("MSFT", form_type="8-K", limit=10)
"""

import time
from typing import List
from edgar import Company, set_identity

from config import (
    EDGAR_USER_IDENTITY,
    EDGAR_RATE_LIMIT,
    get_logger,
)

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'schemas'))

from filing_metadata import FilingMetadata

logger = get_logger(__name__)


class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.call_times = []
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        now = time.time()
        self.call_times = [t for t in self.call_times if now - t < 60]
        
        if len(self.call_times) >= self.calls_per_minute:
            sleep_time = 60 - (now - self.call_times[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached, sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        
        self.call_times.append(time.time())


class EdgarToolsConnector:
    """
    Connector for EdgarTools SEC filings.
    
    Fetches SEC filing metadata for US public companies.
    """
    
    def __init__(self):
        """Initialize EdgarTools connector and set SEC identity."""
        # Set required user identity for SEC requests
        set_identity(EDGAR_USER_IDENTITY)
        
        self.rate_limiter = RateLimiter(EDGAR_RATE_LIMIT['calls_per_minute'])
        
        logger.info(f"EdgarToolsConnector initialized with identity: {EDGAR_USER_IDENTITY}")
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry."""
        max_attempts = EDGAR_RATE_LIMIT['retry_attempts']
        delay = EDGAR_RATE_LIMIT['retry_delay_seconds']
        
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
    
    def fetch_filings(
        self,
        ticker: str,
        form_type: str = "10-K",
        limit: int = 10
    ) -> List[FilingMetadata]:
        """
        Fetch SEC filings for a ticker.
        
        Args:
            ticker: Stock ticker symbol (e.g., AAPL)
            form_type: SEC form type (10-K, 10-Q, 8-K, etc.)
            limit: Maximum number of filings to fetch
        
        Returns:
            List of validated FilingMetadata objects
        """
        logger.info(f"Fetching {form_type} filings for {ticker} (limit={limit})")
        
        def _fetch():
            company = Company(ticker)
            filings_collection = company.get_filings(form=form_type)
            return filings_collection
        
        filings_collection = self._retry_with_backoff(_fetch)
        
        # Iterate through the collection
        filings = []
        count = 0
        for filing in filings_collection:
            if count >= limit:
                break
            
            try:
                metadata = FilingMetadata.from_edgartools_filing(filing)
                filings.append(metadata)
                count += 1
            except Exception as e:
                logger.error(f"Failed to validate filing for {ticker}: {e}")
                continue
        
        logger.info(f"Fetched {len(filings)} {form_type} filings for {ticker}")
        return filings
    
    def fetch_10k_filings(self, ticker: str, limit: int = 5) -> List[FilingMetadata]:
        """
        Fetch 10-K annual report filings.
        
        Args:
            ticker: Stock ticker symbol
            limit: Maximum number of filings
        
        Returns:
            List of validated FilingMetadata objects
        """
        return self.fetch_filings(ticker, form_type="10-K", limit=limit)
    
    def fetch_10q_filings(self, ticker: str, limit: int = 10) -> List[FilingMetadata]:
        """
        Fetch 10-Q quarterly report filings.
        
        Args:
            ticker: Stock ticker symbol
            limit: Maximum number of filings
        
        Returns:
            List of validated FilingMetadata objects
        """
        return self.fetch_filings(ticker, form_type="10-Q", limit=limit)
    
    def fetch_8k_filings(self, ticker: str, limit: int = 20) -> List[FilingMetadata]:
        """
        Fetch 8-K current report filings.
        
        Args:
            ticker: Stock ticker symbol
            limit: Maximum number of filings
        
        Returns:
            List of validated FilingMetadata objects
        """
        return self.fetch_filings(ticker, form_type="8-K", limit=limit)


# Example usage
if __name__ == '__main__':
    connector = EdgarToolsConnector()
    
    # Test 10-K filings
    print("Testing 10-K filings...")
    filings_10k = connector.fetch_10k_filings("AAPL", limit=3)
    print(f"Fetched {len(filings_10k)} 10-K filings")
    if filings_10k:
        print(f"Sample: {filings_10k[0].company_name} {filings_10k[0].form_type} on {filings_10k[0].filing_date}")
    
    # Test 10-Q filings
    print("\nTesting 10-Q filings...")
    filings_10q = connector.fetch_10q_filings("AAPL", limit=3)
    print(f"Fetched {len(filings_10q)} 10-Q filings")
    if filings_10q:
        print(f"Sample: {filings_10q[0].company_name} {filings_10q[0].form_type} on {filings_10q[0].filing_date}")
    
    # Test 8-K filings
    print("\nTesting 8-K filings...")
    filings_8k = connector.fetch_8k_filings("AAPL", limit=5)
    print(f"Fetched {len(filings_8k)} 8-K filings")
    if filings_8k:
        print(f"Sample: {filings_8k[0].company_name} {filings_8k[0].form_type} on {filings_8k[0].filing_date}")
