# MarketMind Intelligence Platform V1
# Unit Tests for FilingMetadata Schema
# Date: April 24, 2026

import pytest
from pydantic import ValidationError
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.bronze.schemas.filing_metadata import FilingMetadata


@pytest.mark.unit
@pytest.mark.schema
class TestFilingMetadataSchema:
    """Test suite for FilingMetadata Pydantic model"""
    
    def test_valid_filing(self):
        """Test creating valid filing metadata"""
        filing = FilingMetadata(
            accession_number="0000320193-25-000079",
            ticker="AAPL",
            cik="0000320193",
            company_name="Apple Inc.",
            form_type="10-K",
            filing_date="2025-10-31",
            report_date="2025-09-27",
            is_xbrl=True,
            filing_url="https://www.sec.gov/Archives/edgar/data/test.html",
            source="edgartools",
            ingestion_timestamp=1713564000000
        )
        
        assert filing.ticker == "AAPL"
        assert filing.form_type == "10-K"
    
    def test_ticker_uppercase_conversion(self):
        """Test ticker is converted to uppercase"""
        filing = FilingMetadata(
            accession_number="0000320193-25-000079",
            ticker="aapl",
            cik="0000320193",
            company_name="Apple Inc.",
            form_type="10-K",
            filing_date="2025-10-31",
            filing_url="https://www.sec.gov/Archives/edgar/data/test.html",
            source="edgartools",
            ingestion_timestamp=1713564000000
        )
        
        assert filing.ticker == "AAPL"
    
    def test_cik_validation_numeric(self):
        """Test CIK must be numeric"""
        with pytest.raises(ValidationError) as exc_info:
            FilingMetadata(
                accession_number="0000320193-25-000079",
                cik="INVALID",
                company_name="Apple Inc.",
                form_type="10-K",
                filing_date="2025-10-31",
                filing_url="https://www.sec.gov/Archives/edgar/data/test.html",
                source="edgartools",
                ingestion_timestamp=1713564000000
            )
        
        assert "must be numeric" in str(exc_info.value)
    
    def test_filing_url_must_be_sec_gov(self):
        """Test filing URL must be from sec.gov"""
        with pytest.raises(ValidationError) as exc_info:
            FilingMetadata(
                accession_number="0000320193-25-000079",
                cik="0000320193",
                company_name="Apple Inc.",
                form_type="10-K",
                filing_date="2025-10-31",
                filing_url="https://example.com/test.html",
                source="edgartools",
                ingestion_timestamp=1713564000000
            )
        
        assert "must be from sec.gov" in str(exc_info.value)
    
    def test_invalid_date_format_fails(self):
        """Test invalid date format fails"""
        with pytest.raises(ValidationError) as exc_info:
            FilingMetadata(
                accession_number="0000320193-25-000079",
                cik="0000320193",
                company_name="Apple Inc.",
                form_type="10-K",
                filing_date="2025/10/31",
                filing_url="https://www.sec.gov/Archives/edgar/data/test.html",
                source="edgartools",
                ingestion_timestamp=1713564000000
            )
        
        assert "YYYY-MM-DD format" in str(exc_info.value)
    
    def test_optional_ticker(self):
        """Test ticker is optional"""
        filing = FilingMetadata(
            accession_number="0000320193-25-000079",
            cik="0000320193",
            company_name="Apple Inc.",
            form_type="10-K",
            filing_date="2025-10-31",
            filing_url="https://www.sec.gov/Archives/edgar/data/test.html",
            source="edgartools",
            ingestion_timestamp=1713564000000
        )
        
        assert filing.ticker is None
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        filing = FilingMetadata(
            accession_number="0000320193-25-000079",
            ticker="AAPL",
            cik="0000320193",
            company_name="Apple Inc.",
            form_type="10-K",
            filing_date="2025-10-31",
            filing_url="https://www.sec.gov/Archives/edgar/data/test.html",
            source="edgartools",
            ingestion_timestamp=1713564000000
        )
        
        filing_dict = filing.to_dict()
        
        assert isinstance(filing_dict, dict)
        assert filing_dict['accession_number'] == '0000320193-25-000079'
