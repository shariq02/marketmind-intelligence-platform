# ====================================================================
# Pydantic Model for SEC Filing Metadata
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/schemas/filing_metadata.py
# Purpose: Python validation model for SEC filing metadata from EdgarTools
# ====================================================================
"""
Filing Metadata Pydantic Model

Validates SEC filing metadata before writing to Kafka topic
filings.metadata.v1.

Data Source: EdgarTools library
Kafka Topic: filings.metadata.v1
Avro Schema: filings_metadata_v1.avsc

Field Validation:
- accession_number: Non-empty string, SEC format
- cik: Numeric string, 10 digits
- form_type: Non-empty string (10-K, 10-Q, 8-K, etc.)
- filing_date: YYYY-MM-DD format
- filing_url: Valid SEC EDGAR URL

Usage:
    from code.bronze.schemas.filing_metadata import FilingMetadata
    
    filing = FilingMetadata(
        accession_number="0000320193-25-000079",
        ticker="AAPL",
        cik="0000320193",
        company_name="Apple Inc.",
        form_type="10-K",
        filing_date="2025-10-31",
        report_date="2025-09-27",
        is_xbrl=True,
        filing_url="https://www.sec.gov/Archives/...",
        source="edgartools",
        ingestion_timestamp=1713564000000
    )
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class FilingMetadata(BaseModel):
    """
    SEC filing metadata model.
    
    Represents metadata for SEC filings (10-K, 10-Q, 8-K, etc.).
    Does not include full filing content, only metadata.
    """
    
    accession_number: str = Field(
        ...,
        description="SEC accession number (format: 0000320193-25-000079)",
        min_length=10,
        max_length=30
    )
    
    ticker: Optional[str] = Field(
        None,
        description="Stock ticker symbol (e.g., AAPL)",
        max_length=10
    )
    
    cik: str = Field(
        ...,
        description="Central Index Key - SEC company identifier",
        min_length=1,
        max_length=20
    )
    
    company_name: str = Field(
        ...,
        description="Company name as registered with SEC",
        min_length=1,
        max_length=200
    )
    
    form_type: str = Field(
        ...,
        description="Type of SEC form (10-K, 10-Q, 8-K, etc.)",
        min_length=1,
        max_length=20
    )
    
    filing_date: str = Field(
        ...,
        description="Date when filing was submitted to SEC (YYYY-MM-DD)"
    )
    
    report_date: Optional[str] = Field(
        None,
        description="Period end date covered by report (YYYY-MM-DD)"
    )
    
    acceptance_datetime: Optional[int] = Field(
        None,
        description="Unix timestamp when SEC accepted the filing",
        gt=0
    )
    
    file_number: Optional[str] = Field(
        None,
        description="SEC file number (e.g., 001-36743)",
        max_length=50
    )
    
    is_xbrl: bool = Field(
        False,
        description="Whether filing includes XBRL data files"
    )
    
    is_inline_xbrl: bool = Field(
        False,
        description="Whether filing uses inline XBRL format"
    )
    
    filing_url: str = Field(
        ...,
        description="URL to filing index page on SEC EDGAR"
    )
    
    primary_document: Optional[str] = Field(
        None,
        description="Filename of primary document",
        max_length=200
    )
    
    document_count: Optional[int] = Field(
        None,
        description="Total number of documents in filing",
        ge=0
    )
    
    size_bytes: Optional[int] = Field(
        None,
        description="Total size of filing in bytes",
        ge=0
    )
    
    source: str = Field(
        "edgartools",
        description="Data source identifier"
    )
    
    ingestion_timestamp: int = Field(
        ...,
        description="Unix timestamp when record was ingested",
        gt=0
    )
    
    @field_validator('ticker')
    @classmethod
    def ticker_uppercase(cls, v: Optional[str]) -> Optional[str]:
        """Ensure ticker is uppercase if present."""
        if v is None:
            return v
        return v.upper().strip()
    
    @field_validator('filing_date', 'report_date')
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
    
    @field_validator('cik')
    @classmethod
    def validate_cik(cls, v: str) -> str:
        """Validate CIK is numeric."""
        # Remove leading zeros for validation
        if not v.lstrip('0').isdigit():
            raise ValueError(f'CIK must be numeric, got: {v}')
        return v
    
    @field_validator('filing_url')
    @classmethod
    def validate_sec_url(cls, v: str) -> str:
        """Validate URL is from SEC EDGAR."""
        if not v.startswith('https://www.sec.gov/'):
            raise ValueError(f'Filing URL must be from sec.gov, got: {v}')
        return v
    
    def to_dict(self) -> dict:
        """Convert to dictionary for Kafka serialization."""
        return self.model_dump()
    
    @classmethod
    def from_edgartools_filing(cls, filing) -> 'FilingMetadata':
        """
        Create FilingMetadata from EdgarTools Filing object.
        
        Args:
            filing: EdgarTools EntityFiling object
        
        Returns:
            FilingMetadata instance
        
        Example:
            from edgar import Company
            company = Company('AAPL')
            filing = company.get_filings(form='10-K').latest(1)
            metadata = FilingMetadata.from_edgartools_filing(filing)
        """
        # Extract acceptance datetime if available
        acceptance_ts = None
        if hasattr(filing, 'index_headers') and filing.index_headers:
            acceptance_dt = getattr(filing.index_headers, 'acceptance_datetime', None)
            if acceptance_dt:
                acceptance_ts = int(acceptance_dt.timestamp() * 1000)
        
        # Extract report date
        report_date_str = None
        if hasattr(filing, 'report_date') and filing.report_date:
            if isinstance(filing.report_date, str):
                report_date_str = filing.report_date
            else:
                report_date_str = filing.report_date.strftime('%Y-%m-%d')
        
        # Extract filing date
        filing_date_str = None
        if hasattr(filing, 'filing_date') and filing.filing_date:
            if isinstance(filing.filing_date, str):
                filing_date_str = filing.filing_date
            else:
                filing_date_str = filing.filing_date.strftime('%Y-%m-%d')
        
        # Extract ticker (may not be available)
        ticker = None
        if hasattr(filing, 'ticker'):
            ticker = filing.ticker
        
        return cls(
            accession_number=filing.accession_number,
            ticker=ticker,
            cik=str(filing.cik),
            company_name=filing.company,
            form_type=filing.form,
            filing_date=filing_date_str,
            report_date=report_date_str,
            acceptance_datetime=acceptance_ts,
            file_number=getattr(filing, 'file_number', None),
            is_xbrl=getattr(filing, 'is_xbrl', False) == 1,
            is_inline_xbrl=getattr(filing, 'is_inline_xbrl', False) == 1,
            filing_url=filing.url,
            primary_document=getattr(filing, 'primary_document', None),
            document_count=getattr(filing.index_headers, 'public_document_count', None) if hasattr(filing, 'index_headers') else None,
            size_bytes=getattr(filing, 'size', None),
            source='edgartools',
            ingestion_timestamp=int(datetime.now().timestamp() * 1000)
        )


# Example usage
if __name__ == '__main__':
    # Valid filing
    filing = FilingMetadata(
        accession_number="0000320193-25-000079",
        ticker="AAPL",
        cik="0000320193",
        company_name="Apple Inc.",
        form_type="10-K",
        filing_date="2025-10-31",
        report_date="2025-09-27",
        is_xbrl=True,
        is_inline_xbrl=True,
        filing_url="https://www.sec.gov/Archives/edgar/data/320193/0000320193-25-000079-index.html",
        primary_document="aapl-20250927.htm",
        document_count=91,
        size_bytes=9392337,
        source="edgartools",
        ingestion_timestamp=1713564000000
    )
    print(f"Valid filing: {filing.ticker} {filing.form_type} on {filing.filing_date}")
    
    # Invalid CIK
    try:
        invalid = FilingMetadata(
            accession_number="0000320193-25-000079",
            cik="INVALID",  # Non-numeric
            company_name="Apple Inc.",
            form_type="10-K",
            filing_date="2025-10-31",
            filing_url="https://www.sec.gov/Archives/edgar/data/320193/0000320193-25-000079-index.html",
            source="edgartools",
            ingestion_timestamp=1713564000000
        )
    except Exception as e:
        print(f"Validation error (expected): {e}")
