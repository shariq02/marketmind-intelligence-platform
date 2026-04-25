# MarketMind Intelligence Platform V1
# Unit Tests for EdgarTools Connector
# Date: April 24, 2026

import pytest
from unittest.mock import Mock, patch, MagicMock
from code.bronze.connectors.edgartools_connector import EdgarToolsConnector, RateLimiter
from code.bronze.schemas.filing_metadata import FilingMetadata


@pytest.mark.unit
@pytest.mark.connector
class TestEdgarToolsConnector:
    """Test suite for EdgarToolsConnector"""
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    def test_initialization(self, mock_set_identity):
        """Test connector initialization"""
        connector = EdgarToolsConnector()
        
        assert connector is not None
        assert hasattr(connector, 'rate_limiter')
        assert mock_set_identity.called
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_fetch_filings_success(self, mock_company, mock_set_identity):
        """Test fetching filings successfully"""
        # Mock filing object
        mock_filing = MagicMock()
        mock_filing.accession_number = "0000320193-25-000079"
        mock_filing.ticker = "AAPL"
        mock_filing.cik = "0000320193"
        mock_filing.company = "Apple Inc."
        mock_filing.form = "10-K"
        mock_filing.filing_date = "2025-10-31"
        mock_filing.report_date = "2025-09-27"
        mock_filing.url = "https://www.sec.gov/Archives/edgar/data/test.html"
        # Explicitly set optional fields to None
        mock_filing.file_number = None
        mock_filing.primary_document = None
        mock_filing.size = None
        mock_filing.is_xbrl = 0
        mock_filing.is_inline_xbrl = 0
        mock_filing.index_headers = None
        
        # Mock filings collection
        mock_filings_collection = [mock_filing]
        
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.return_value = mock_filings_collection
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        filings = connector.fetch_filings("AAPL", "10-K", limit=1)
        
        assert len(filings) == 1
        assert type(filings[0]).__name__ == "FilingMetadata"
        assert filings[0].ticker == "AAPL"
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_fetch_10k_filings(self, mock_company, mock_set_identity):
        """Test fetching 10-K filings"""
        mock_filing = MagicMock()
        mock_filing.accession_number = "0000320193-25-000079"
        mock_filing.cik = "0000320193"
        mock_filing.company = "Apple Inc."
        mock_filing.form = "10-K"
        mock_filing.filing_date = "2025-10-31"
        mock_filing.url = "https://www.sec.gov/Archives/edgar/data/test.html"
        # Explicitly set optional fields to None
        mock_filing.ticker = None
        mock_filing.report_date = None
        mock_filing.file_number = None
        mock_filing.primary_document = None
        mock_filing.size = None
        mock_filing.is_xbrl = 0
        mock_filing.is_inline_xbrl = 0
        mock_filing.index_headers = None
        
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.return_value = [mock_filing]
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        filings = connector.fetch_10k_filings("AAPL", limit=1)
        
        assert len(filings) == 1
        assert filings[0].form_type == "10-K"
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_fetch_10q_filings(self, mock_company, mock_set_identity):
        """Test fetching 10-Q filings"""
        mock_filing = MagicMock()
        mock_filing.accession_number = "0000320193-25-000080"
        mock_filing.cik = "0000320193"
        mock_filing.company = "Apple Inc."
        mock_filing.form = "10-Q"
        mock_filing.filing_date = "2025-07-31"
        mock_filing.url = "https://www.sec.gov/Archives/edgar/data/test.html"
        # Explicitly set optional fields to None
        mock_filing.ticker = None
        mock_filing.report_date = None
        mock_filing.file_number = None
        mock_filing.primary_document = None
        mock_filing.size = None
        mock_filing.is_xbrl = 0
        mock_filing.is_inline_xbrl = 0
        mock_filing.index_headers = None
        
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.return_value = [mock_filing]
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        filings = connector.fetch_10q_filings("AAPL", limit=1)
        
        assert len(filings) == 1
        assert filings[0].form_type == "10-Q"
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_fetch_8k_filings(self, mock_company, mock_set_identity):
        """Test fetching 8-K filings"""
        mock_filing = MagicMock()
        mock_filing.accession_number = "0000320193-25-000081"
        mock_filing.cik = "0000320193"
        mock_filing.company = "Apple Inc."
        mock_filing.form = "8-K"
        mock_filing.filing_date = "2025-06-15"
        mock_filing.url = "https://www.sec.gov/Archives/edgar/data/test.html"
        # Explicitly set optional fields to None
        mock_filing.ticker = None
        mock_filing.report_date = None
        mock_filing.file_number = None
        mock_filing.primary_document = None
        mock_filing.size = None
        mock_filing.is_xbrl = 0
        mock_filing.is_inline_xbrl = 0
        mock_filing.index_headers = None
        
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.return_value = [mock_filing]
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        filings = connector.fetch_8k_filings("AAPL", limit=1)
        
        assert len(filings) == 1
        assert filings[0].form_type == "8-K"
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_fetch_filings_respects_limit(self, mock_company, mock_set_identity):
        """Test limit parameter is respected"""
        # Create 5 mock filings
        mock_filings = []
        for i in range(5):
            mock_filing = MagicMock()
            mock_filing.accession_number = f"000032019{i}-25-000079"
            mock_filing.cik = "0000320193"
            mock_filing.company = "Apple Inc."
            mock_filing.form = "10-K"
            mock_filing.filing_date = "2025-10-31"
            mock_filing.url = "https://www.sec.gov/Archives/edgar/data/test.html"
            # Explicitly set optional fields to None
            mock_filing.ticker = None
            mock_filing.report_date = None
            mock_filing.file_number = None
            mock_filing.primary_document = None
            mock_filing.size = None
            mock_filing.is_xbrl = 0
            mock_filing.is_inline_xbrl = 0
            mock_filing.index_headers = None
            mock_filings.append(mock_filing)
        
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.return_value = mock_filings
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        filings = connector.fetch_filings("AAPL", "10-K", limit=3)
        
        assert len(filings) == 3
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_retry_with_backoff_success_after_retry(self, mock_company, mock_set_identity):
        """Test retry mechanism succeeds after failure"""
        mock_filing = MagicMock()
        mock_filing.accession_number = "0000320193-25-000079"
        mock_filing.cik = "0000320193"
        mock_filing.company = "Apple Inc."
        mock_filing.form = "10-K"
        mock_filing.filing_date = "2025-10-31"
        mock_filing.url = "https://www.sec.gov/Archives/edgar/data/test.html"
        # Explicitly set optional fields to None
        mock_filing.ticker = None
        mock_filing.report_date = None
        mock_filing.file_number = None
        mock_filing.primary_document = None
        mock_filing.size = None
        mock_filing.is_xbrl = 0
        mock_filing.is_inline_xbrl = 0
        mock_filing.index_headers = None
        
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.side_effect = [
            Exception("Temporary error"),
            [mock_filing]
        ]
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        filings = connector.fetch_filings("AAPL", "10-K", limit=1)
        
        assert len(filings) == 1
        assert mock_company_instance.get_filings.call_count == 2
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_retry_exhausted_raises_error(self, mock_company, mock_set_identity):
        """Test retry mechanism raises after max attempts"""
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.side_effect = Exception("Persistent error")
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        
        with pytest.raises(Exception) as exc_info:
            connector.fetch_filings("AAPL", "10-K", limit=1)
        
        assert "Persistent error" in str(exc_info.value)
    
    @patch('code.bronze.connectors.edgartools_connector.set_identity')
    @patch('code.bronze.connectors.edgartools_connector.Company')
    def test_fetch_filings_handles_validation_errors(self, mock_company, mock_set_identity):
        """Test connector handles invalid filing data gracefully"""
        # Create one invalid filing (missing required field)
        mock_filing_invalid = MagicMock()
        mock_filing_invalid.accession_number = "0000320193-25-000079"
        mock_filing_invalid.cik = None  # Invalid - will cause validation error
        
        # Create one valid filing
        mock_filing_valid = MagicMock()
        mock_filing_valid.accession_number = "0000320193-25-000080"
        mock_filing_valid.cik = "0000320193"
        mock_filing_valid.company = "Apple Inc."
        mock_filing_valid.form = "10-K"
        mock_filing_valid.filing_date = "2025-10-31"
        mock_filing_valid.url = "https://www.sec.gov/Archives/edgar/data/test.html"
        # Explicitly set optional fields to None
        mock_filing_valid.ticker = None
        mock_filing_valid.report_date = None
        mock_filing_valid.file_number = None
        mock_filing_valid.primary_document = None
        mock_filing_valid.size = None
        mock_filing_valid.is_xbrl = 0
        mock_filing_valid.is_inline_xbrl = 0
        mock_filing_valid.index_headers = None
        
        mock_company_instance = MagicMock()
        mock_company_instance.get_filings.return_value = [mock_filing_invalid, mock_filing_valid]
        mock_company.return_value = mock_company_instance
        
        connector = EdgarToolsConnector()
        filings = connector.fetch_filings("AAPL", "10-K", limit=10)
        
        # Should only return the valid filing
        assert len(filings) == 1
