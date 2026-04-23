# ====================================================================
# Pydantic Model for Corporate Actions (Splits and Dividends)
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/schemas/corporate_action.py
# Purpose: Python validation model for stock splits and dividends from Polygon API
# ====================================================================
"""
Corporate Action Pydantic Model

Validates stock split and dividend data before writing to Kafka topic
market.corporate_actions.v1.

Data Source: Polygon.io Reference API
Kafka Topic: market.corporate_actions.v1
Avro Schema: market_corporate_actions_v1.avsc

Field Validation:
- ticker: Non-empty string, uppercase
- action_type: Must be SPLIT or DIVIDEND
- Split-specific fields validated only for SPLIT actions
- Dividend-specific fields validated only for DIVIDEND actions
- Dates in YYYY-MM-DD format

Usage:
    from code.bronze.schemas.corporate_action import CorporateAction, ActionType
    
    # Stock split
    split = CorporateAction(
        ticker="AAPL",
        action_type=ActionType.SPLIT,
        execution_date="2020-08-31",
        split_ratio=4.0,
        source="polygon",
        ingestion_timestamp=1713564000000
    )
    
    # Dividend
    dividend = CorporateAction(
        ticker="AAPL",
        action_type=ActionType.DIVIDEND,
        ex_dividend_date="2026-02-10",
        record_date="2026-02-12",
        pay_date="2026-02-20",
        cash_amount=0.25,
        dividend_type="CD",
        frequency=4,
        source="polygon",
        ingestion_timestamp=1713564000000
    )
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import datetime
from enum import Enum


class ActionType(str, Enum):
    """Type of corporate action."""
    SPLIT = "SPLIT"
    DIVIDEND = "DIVIDEND"


class CorporateAction(BaseModel):
    """
    Corporate action data model for stock splits and dividends.
    
    Represents corporate events that affect stock price or shareholder value.
    """
    
    ticker: str = Field(
        ...,
        description="Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)",
        min_length=1,
        max_length=10
    )
    
    action_type: ActionType = Field(
        ...,
        description="Type of corporate action: SPLIT or DIVIDEND"
    )
    
    # Split-specific fields
    execution_date: Optional[str] = Field(
        None,
        description="Date when split was executed (YYYY-MM-DD). Only for SPLIT."
    )
    
    split_ratio: Optional[float] = Field(
        None,
        description="Split ratio (4.0 for 4-for-1, 0.5 for 1-for-2 reverse). Only for SPLIT.",
        gt=0
    )
    
    # Dividend-specific fields
    ex_dividend_date: Optional[str] = Field(
        None,
        description="Ex-dividend date (YYYY-MM-DD). Only for DIVIDEND."
    )
    
    record_date: Optional[str] = Field(
        None,
        description="Record date (YYYY-MM-DD). Only for DIVIDEND."
    )
    
    declaration_date: Optional[str] = Field(
        None,
        description="Declaration date (YYYY-MM-DD). Only for DIVIDEND."
    )
    
    pay_date: Optional[str] = Field(
        None,
        description="Payment date (YYYY-MM-DD). Only for DIVIDEND."
    )
    
    cash_amount: Optional[float] = Field(
        None,
        description="Cash dividend per share in USD. Only for DIVIDEND.",
        gt=0
    )
    
    dividend_type: Optional[str] = Field(
        None,
        description="Dividend type (CD=cash, SC=stock). Only for DIVIDEND."
    )
    
    frequency: Optional[int] = Field(
        None,
        description="Dividend frequency per year (1=annual, 4=quarterly). Only for DIVIDEND.",
        ge=1,
        le=12
    )
    
    source: str = Field(
        "polygon",
        description="Data source identifier"
    )
    
    ingestion_timestamp: int = Field(
        ...,
        description="Unix timestamp when record was ingested",
        gt=0
    )
    
    @field_validator('ticker')
    @classmethod
    def ticker_uppercase(cls, v: str) -> str:
        """Ensure ticker is uppercase."""
        return v.upper().strip()
    
    @field_validator('execution_date', 'ex_dividend_date', 'record_date', 'declaration_date', 'pay_date')
    @classmethod
    def validate_date_format(cls, v: Optional[str]) -> Optional[str]:
        """Validate date is in YYYY-MM-DD format."""
        if v is None:
            return v
        
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f'Date must be in YYYY-MM-DD format, got: {v}')
    
    @model_validator(mode='after')
    def validate_action_specific_fields(self) -> 'CorporateAction':
        """Validate that required fields are present for each action type."""
        if self.action_type == ActionType.SPLIT:
            # Split must have execution_date and split_ratio
            if self.execution_date is None:
                raise ValueError('SPLIT action requires execution_date')
            if self.split_ratio is None:
                raise ValueError('SPLIT action requires split_ratio')
            
            # Dividend fields should be null for splits
            if any([self.ex_dividend_date, self.cash_amount]):
                raise ValueError('SPLIT action should not have dividend-specific fields')
        
        elif self.action_type == ActionType.DIVIDEND:
            # Dividend should have at least ex_dividend_date and cash_amount
            if self.ex_dividend_date is None:
                raise ValueError('DIVIDEND action requires ex_dividend_date')
            if self.cash_amount is None:
                raise ValueError('DIVIDEND action requires cash_amount')
            
            # Split fields should be null for dividends
            if any([self.execution_date, self.split_ratio]):
                raise ValueError('DIVIDEND action should not have split-specific fields')
        
        return self
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return self.model_dump()
    
    @classmethod
    def from_polygon_split(cls, ticker: str, split_data: dict) -> 'CorporateAction':
        """
        Create CorporateAction from Polygon stock split response.
        
        Args:
            ticker: Stock ticker symbol
            split_data: Split data from Polygon API
        
        Returns:
            CorporateAction instance
        """
        return cls(
            ticker=ticker,
            action_type=ActionType.SPLIT,
            execution_date=split_data.get('execution_date'),
            split_ratio=split_data.get('split_ratio'),
            source='polygon',
            ingestion_timestamp=int(datetime.now().timestamp() * 1000)
        )
    
    @classmethod
    def from_polygon_dividend(cls, ticker: str, dividend_data: dict) -> 'CorporateAction':
        """
        Create CorporateAction from Polygon dividend response.
        
        Args:
            ticker: Stock ticker symbol
            dividend_data: Dividend data from Polygon API
        
        Returns:
            CorporateAction instance
        """
        return cls(
            ticker=ticker,
            action_type=ActionType.DIVIDEND,
            ex_dividend_date=dividend_data.get('ex_dividend_date'),
            record_date=dividend_data.get('record_date'),
            declaration_date=dividend_data.get('declaration_date'),
            pay_date=dividend_data.get('pay_date'),
            cash_amount=dividend_data.get('cash_amount'),
            dividend_type=dividend_data.get('dividend_type'),
            frequency=dividend_data.get('frequency'),
            source='polygon',
            ingestion_timestamp=int(datetime.now().timestamp() * 1000)
        )


# Example usage
if __name__ == '__main__':
    # Valid split
    split = CorporateAction(
        ticker="aapl",
        action_type=ActionType.SPLIT,
        execution_date="2020-08-31",
        split_ratio=4.0,
        source="polygon",
        ingestion_timestamp=1713564000000
    )
    print(f"Valid split: {split.ticker} {split.split_ratio}-for-1")
    
    # Valid dividend
    dividend = CorporateAction(
        ticker="MSFT",
        action_type=ActionType.DIVIDEND,
        ex_dividend_date="2026-02-10",
        pay_date="2026-02-20",
        cash_amount=0.75,
        frequency=4,
        source="polygon",
        ingestion_timestamp=1713564000000
    )
    print(f"Valid dividend: {dividend.ticker} ${dividend.cash_amount}")
    
    # Invalid: split without required fields
    try:
        invalid = CorporateAction(
            ticker="AAPL",
            action_type=ActionType.SPLIT,
            source="polygon",
            ingestion_timestamp=1713564000000
        )
    except Exception as e:
        print(f"Validation error (expected): {e}")
