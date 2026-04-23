# ====================================================================
# Pydantic Model for Macroeconomic Indicators
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/schemas/macro_indicator.py
# Purpose: Python validation model for macro indicators from AkShare
# ====================================================================
"""
Macro Indicator Pydantic Model

Validates macroeconomic indicator data before writing to Kafka topic
macro.indicators.v1.

Data Source: AkShare library (scrapes US government data)
Kafka Topic: macro.indicators.v1
Avro Schema: macro_indicators_v1.avsc

Chinese to English Field Mapping:
- 商品 (indicator) --> indicator_name
- 日期 (date) --> date
- 今值 (current value) --> value
- 预测值 (forecast) --> forecast_value
- 前值 (previous) --> previous_value

Field Validation:
- indicator_name: Non-empty string
- date: YYYY-MM-DD format
- value: Numeric (can be negative for rates)
- frequency: Must be valid (DAILY, WEEKLY, MONTHLY, QUARTERLY, ANNUALLY)

Usage:
    from code.bronze.schemas.macro_indicator import MacroIndicator, Frequency
    
    indicator = MacroIndicator(
        indicator_name="US_CPI_MOM",
        date="2026-01-15",
        value=0.3,
        forecast_value=0.2,
        previous_value=0.4,
        unit="percent",
        frequency=Frequency.MONTHLY,
        source="akshare",
        ingestion_timestamp=1713564000000
    )
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime
from enum import Enum


class Frequency(str, Enum):
    """Publication frequency of macroeconomic indicator."""
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"
    ANNUALLY = "ANNUALLY"


class MacroIndicator(BaseModel):
    """
    Macroeconomic indicator data model.
    
    Represents economic metrics like GDP, CPI, unemployment rate, interest rates.
    Data sourced from AkShare library which scrapes US government sources.
    """
    
    indicator_name: str = Field(
        ...,
        description="Name of the indicator (e.g., US_GDP_QOQ, US_CPI_MOM)",
        min_length=1,
        max_length=100
    )
    
    date: str = Field(
        ...,
        description="Date of the indicator value (YYYY-MM-DD)"
    )
    
    value: float = Field(
        ...,
        description="Current value of the indicator"
    )
    
    forecast_value: Optional[float] = Field(
        None,
        description="Forecasted/predicted value"
    )
    
    previous_value: Optional[float] = Field(
        None,
        description="Previous period value"
    )
    
    unit: Optional[str] = Field(
        None,
        description="Unit of measurement (e.g., percent, billions_usd, index)",
        max_length=50
    )
    
    frequency: Frequency = Field(
        ...,
        description="Publication frequency"
    )
    
    source: str = Field(
        "akshare",
        description="Data source identifier"
    )
    
    source_url: Optional[str] = Field(
        None,
        description="URL to original data source",
        max_length=500
    )
    
    ingestion_timestamp: int = Field(
        ...,
        description="Unix timestamp when record was ingested",
        gt=0
    )
    
    @field_validator('indicator_name')
    @classmethod
    def indicator_name_uppercase(cls, v: str) -> str:
        """Ensure indicator name is uppercase and standardized."""
        return v.upper().strip().replace(' ', '_')
    
    @field_validator('date')
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """Validate date is in YYYY-MM-DD format."""
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f'Date must be in YYYY-MM-DD format, got: {v}')
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return self.model_dump()
    
    @classmethod
    def from_akshare_response(
        cls,
        indicator_name: str,
        row: dict,
        frequency: Frequency,
        unit: Optional[str] = None,
        source_url: Optional[str] = None
    ) -> 'MacroIndicator':
        """
        Create MacroIndicator from AkShare response row.
        
        Maps Chinese column names to English fields:
        - 商品 --> indicator_name (overridden by parameter)
        - 日期 --> date
        - 今值 --> value
        - 预测值 --> forecast_value
        - 前值 --> previous_value
        
        Args:
            indicator_name: Standardized indicator name (e.g., US_CPI_MOM)
            row: Single row from AkShare DataFrame as dict
            frequency: Publication frequency
            unit: Unit of measurement
            source_url: URL to original source
        
        Returns:
            MacroIndicator instance
        
        Example:
            row = {
                '商品': '美国CPI月率',
                '日期': datetime.date(2026, 1, 15),
                '今值': 0.3,
                '预测值': 0.2,
                '前值': 0.4
            }
            
            indicator = MacroIndicator.from_akshare_response(
                indicator_name='US_CPI_MOM',
                row=row,
                frequency=Frequency.MONTHLY,
                unit='percent'
            )
        """
        # Extract date - could be datetime.date or string
        date_value = row.get('日期')
        if isinstance(date_value, datetime):
            date_str = date_value.strftime('%Y-%m-%d')
        elif hasattr(date_value, 'strftime'):  # datetime.date object
            date_str = date_value.strftime('%Y-%m-%d')
        else:
            date_str = str(date_value)
        
        # Extract values - handle NaN
        import math
        
        value = row.get('今值')
        forecast = row.get('预测值')
        previous = row.get('前值')
        
        # Convert NaN to None
        if isinstance(forecast, float) and math.isnan(forecast):
            forecast = None
        if isinstance(previous, float) and math.isnan(previous):
            previous = None
        
        return cls(
            indicator_name=indicator_name,
            date=date_str,
            value=value,
            forecast_value=forecast,
            previous_value=previous,
            unit=unit,
            frequency=frequency,
            source='akshare',
            source_url=source_url,
            ingestion_timestamp=int(datetime.now().timestamp() * 1000)
        )


# Example usage
if __name__ == '__main__':
    # Valid indicator
    cpi = MacroIndicator(
        indicator_name="us_cpi_mom",  # Will be converted to uppercase
        date="2026-01-15",
        value=0.3,
        forecast_value=0.2,
        previous_value=0.4,
        unit="percent",
        frequency=Frequency.MONTHLY,
        source="akshare",
        ingestion_timestamp=1713564000000
    )
    print(f"Valid indicator: {cpi.indicator_name} = {cpi.value}{cpi.unit}")
    
    # GDP indicator
    gdp = MacroIndicator(
        indicator_name="US_GDP_QOQ",
        date="2026-01-31",
        value=2.5,
        frequency=Frequency.QUARTERLY,
        unit="percent",
        source="akshare",
        ingestion_timestamp=1713564000000
    )
    print(f"Valid GDP: {gdp.indicator_name} = {gdp.value}{gdp.unit}")
    
    # Invalid date format
    try:
        invalid = MacroIndicator(
            indicator_name="US_CPI_MOM",
            date="2026/01/15",  # Invalid format
            value=0.3,
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1713564000000
        )
    except Exception as e:
        print(f"Validation error (expected): {e}")
