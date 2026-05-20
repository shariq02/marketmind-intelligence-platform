# ====================================================================
# AkShare Macro Indicators Connector
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/connectors/akshare_connector.py
# Purpose: Fetch US macroeconomic indicators from AkShare library
# ====================================================================
"""
AkShare Macro Indicators Connector

Fetches US macroeconomic data:
- CPI (Consumer Price Index)
- GDP (Gross Domestic Product)
- Unemployment Rate
- Federal Funds Rate
- Other macro indicators

Features:
- Chinese to English column mapping
- Rate limiting
- Pydantic validation
- Error handling

Usage:
    from code.bronze.connectors.akshare_connector import AkShareConnector
    
    connector = AkShareConnector()
    
    # Fetch CPI data
    cpi_data = connector.fetch_cpi_monthly()
    
    # Fetch unemployment data
    unemployment = connector.fetch_unemployment_rate()
"""

import time
from typing import List
import akshare as ak
import pandas as pd

from config import (
    AKSHARE_RATE_LIMIT,
    get_logger,
)

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'schemas'))

from macro_indicator import MacroIndicator, Frequency

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


class AkShareConnector:
    """
    Connector for AkShare macro indicators.
    
    Fetches US macroeconomic data and maps Chinese columns to English.
    """
    
    def __init__(self):
        """Initialize AkShare connector and rate limiter."""
        self.rate_limiter = RateLimiter(AKSHARE_RATE_LIMIT['calls_per_minute'])
        logger.info("AkShareConnector initialized")
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry."""
        max_attempts = AKSHARE_RATE_LIMIT['retry_attempts']
        delay = AKSHARE_RATE_LIMIT['retry_delay_seconds']
        
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
    
    def _process_akshare_dataframe(
        self,
        df: pd.DataFrame,
        indicator_name: str,
        frequency: Frequency,
        unit: str = None,
        source_url: str = None
    ) -> List[MacroIndicator]:
        """
        Process AkShare DataFrame with Chinese columns to MacroIndicator objects.
        
        Maps Chinese columns:
        - 商品 (indicator) --> indicator_name (overridden by parameter)
        - 日期 (date) --> date
        - 今值 (current) --> value
        - 预测值 (forecast) --> forecast_value
        - 前值 (previous) --> previous_value
        
        Args:
            df: AkShare DataFrame with Chinese columns
            indicator_name: Standardized indicator name
            frequency: Publication frequency
            unit: Unit of measurement
            source_url: URL to original source
        
        Returns:
            List of validated MacroIndicator objects
        """
        indicators = []
        
        for _, row in df.iterrows():
            try:
                row_dict = row.to_dict()
                
                indicator = MacroIndicator.from_akshare_response(
                    indicator_name=indicator_name,
                    row=row_dict,
                    frequency=frequency,
                    unit=unit,
                    source_url=source_url
                )
                indicators.append(indicator)
            except Exception as e:
                logger.error(f"Failed to validate indicator {indicator_name}: {e}")
                continue
        
        return indicators
    
    def fetch_cpi_monthly(self) -> List[MacroIndicator]:
        """
        Fetch US CPI (Consumer Price Index) monthly data.
        
        Returns:
            List of validated MacroIndicator objects
        """
        logger.info("Fetching US CPI monthly data")
        
        def _fetch():
            df = ak.macro_usa_cpi_monthly()
            return df
        
        df = self._retry_with_backoff(_fetch)
        
        indicators = self._process_akshare_dataframe(
            df=df,
            indicator_name='US_CPI_MOM',
            frequency=Frequency.MONTHLY,
            unit='percent',
            source_url='https://www.bls.gov/cpi/'
        )
        
        logger.info(f"Fetched {len(indicators)} CPI monthly records")
        return indicators
    
    def fetch_unemployment_rate(self) -> List[MacroIndicator]:
        """
        Fetch US unemployment rate data.
        
        Returns:
            List of validated MacroIndicator objects
        """
        logger.info("Fetching US unemployment rate")
        
        def _fetch():
            df = ak.macro_usa_unemployment_rate()
            return df
        
        df = self._retry_with_backoff(_fetch)
        
        indicators = self._process_akshare_dataframe(
            df=df,
            indicator_name='US_UNEMPLOYMENT_RATE',
            frequency=Frequency.MONTHLY,
            unit='percent',
            source_url='https://www.bls.gov/cps/'
        )
        
        logger.info(f"Fetched {len(indicators)} unemployment rate records")
        return indicators
    
    def fetch_adp_employment(self) -> List[MacroIndicator]:
        """
        Fetch US ADP employment change data.
        
        Returns:
            List of validated MacroIndicator objects
        """
        logger.info("Fetching US ADP employment")
        
        def _fetch():
            df = ak.macro_usa_adp_employment()
            return df
        
        df = self._retry_with_backoff(_fetch)
        
        indicators = self._process_akshare_dataframe(
            df=df,
            indicator_name='US_ADP_EMPLOYMENT',
            frequency=Frequency.MONTHLY,
            unit='thousands',
            source_url='https://adpemploymentreport.com/'
        )
        
        logger.info(f"Fetched {len(indicators)} ADP employment records")
        return indicators
    
    def fetch_core_cpi_monthly(self) -> List[MacroIndicator]:
        """
        Fetch US Core CPI (excluding food and energy) monthly data.
        
        Returns:
            List of validated MacroIndicator objects
        """
        logger.info("Fetching US Core CPI monthly data")
        
        def _fetch():
            df = ak.macro_usa_core_cpi_monthly()
            return df
        
        df = self._retry_with_backoff(_fetch)
        
        indicators = self._process_akshare_dataframe(
            df=df,
            indicator_name='US_CORE_CPI_MOM',
            frequency=Frequency.MONTHLY,
            unit='percent',
            source_url='https://www.bls.gov/cpi/'
        )
        
        logger.info(f"Fetched {len(indicators)} Core CPI monthly records")
        return indicators
    
    def fetch_interest_rate(self) -> List[MacroIndicator]:
        """
        Fetch US Federal Funds interest rate data.
        
        Returns:
            List of validated MacroIndicator objects
        """
        logger.info("Fetching US interest rate")
        
        def _fetch():
            df = ak.macro_bank_usa_interest_rate()
            return df
        
        df = self._retry_with_backoff(_fetch)
        
        indicators = self._process_akshare_dataframe(
            df=df,
            indicator_name='US_FEDERAL_FUNDS_RATE',
            frequency=Frequency.MONTHLY,
            unit='percent',
            source_url='https://www.federalreserve.gov/'
        )
        
        logger.info(f"Fetched {len(indicators)} interest rate records")
        return indicators


# Example usage
if __name__ == '__main__':
    connector = AkShareConnector()
    
    # Test CPI
    print("Testing CPI monthly...")
    cpi = connector.fetch_cpi_monthly()
    print(f"Fetched {len(cpi)} CPI records")
    if cpi:
        print(f"Sample: {cpi[0].indicator_name} on {cpi[0].date} = {cpi[0].value}")
    
    # Test unemployment
    print("\nTesting unemployment rate...")
    unemployment = connector.fetch_unemployment_rate()
    print(f"Fetched {len(unemployment)} unemployment records")
    if unemployment:
        print(f"Sample: {unemployment[0].indicator_name} on {unemployment[0].date} = {unemployment[0].value}")
    
    # Test interest rate
    print("\nTesting interest rate...")
    interest = connector.fetch_interest_rate()
    print(f"Fetched {len(interest)} interest rate records")
    if interest:
        print(f"Sample: {interest[0].indicator_name} on {interest[0].date} = {interest[0].value}")
