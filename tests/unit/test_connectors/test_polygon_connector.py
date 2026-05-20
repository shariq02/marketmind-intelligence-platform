# MarketMind Intelligence Platform V1
# Unit Tests for Polygon Connector
# Author: Sharique Mohammad
# Date: April 24, 2026

import pytest
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.bronze.connectors.polygon_connector import PolygonConnector, RateLimiter
from code.bronze.schemas.corporate_action import ActionType


@pytest.mark.unit
@pytest.mark.connector
class TestRateLimiter:
    """Test suite for RateLimiter"""
    
    def test_initialization(self):
        """Test rate limiter initialization"""
        limiter = RateLimiter(calls_per_minute=5)
        assert limiter.calls_per_minute == 5
        assert limiter.call_times == []
    
    def test_first_call_no_wait(self):
        """Test first call does not wait"""
        limiter = RateLimiter(calls_per_minute=5)
        limiter.wait_if_needed()
        assert len(limiter.call_times) == 1


@pytest.mark.unit
@pytest.mark.connector
class TestPolygonConnector:
    """Test suite for PolygonConnector"""
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_initialization(self, mock_ref_client, mock_stocks_client):
        """Test connector initialization"""
        connector = PolygonConnector()
        
        assert connector is not None
        assert mock_stocks_client.called
        assert mock_ref_client.called
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_fetch_daily_bars_success(self, mock_ref_client, mock_stocks_client):
        """Test fetching daily bars successfully"""
        # Mock API response
        mock_response = {
            'results': [
                {
                    't': 1704153600000,
                    'o': 100.0,
                    'h': 105.0,
                    'l': 99.0,
                    'c': 103.0,
                    'v': 1000000.0,
                    'vw': 102.0,
                    'n': 5000
                }
            ]
        }
        
        mock_stocks_instance = MagicMock()
        mock_stocks_instance.get_aggregate_bars.return_value = mock_response
        mock_stocks_client.return_value = mock_stocks_instance
        
        connector = PolygonConnector()
        bars = connector.fetch_daily_bars("AAPL", "2024-01-02", "2024-01-02")
        
        assert len(bars) == 1
        assert type(bars[0]).__name__ == "MarketBar"
        assert bars[0].ticker == "AAPL"
        assert bars[0].close == 103.0
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_fetch_daily_bars_empty_response(self, mock_ref_client, mock_stocks_client):
        """Test handling empty response"""
        mock_response = {'results': []}
        
        mock_stocks_instance = MagicMock()
        mock_stocks_instance.get_aggregate_bars.return_value = mock_response
        mock_stocks_client.return_value = mock_stocks_instance
        
        connector = PolygonConnector()
        bars = connector.fetch_daily_bars("AAPL", "2024-01-02", "2024-01-02")
        
        assert bars == []
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_fetch_intraday_bars_success(self, mock_ref_client, mock_stocks_client):
        """Test fetching intraday bars"""
        mock_response = {
            'results': [
                {
                    't': 1704153600000,
                    'o': 100.0,
                    'h': 105.0,
                    'l': 99.0,
                    'c': 103.0,
                    'v': 100000.0,
                    'vw': 102.0,
                    'n': 500
                }
            ]
        }
        
        mock_stocks_instance = MagicMock()
        mock_stocks_instance.get_aggregate_bars.return_value = mock_response
        mock_stocks_client.return_value = mock_stocks_instance
        
        connector = PolygonConnector()
        bars = connector.fetch_intraday_bars("AAPL", "2024-01-02", "5min")
        
        assert len(bars) == 1
        assert bars[0].granularity == "5min"
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_fetch_intraday_bars_invalid_granularity(self, mock_ref_client, mock_stocks_client):
        """Test invalid granularity raises error"""
        connector = PolygonConnector()
        
        with pytest.raises(ValueError) as exc_info:
            connector.fetch_intraday_bars("AAPL", "2024-01-02", "invalid")
        
        assert "Invalid granularity" in str(exc_info.value)
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_fetch_splits_success(self, mock_ref_client, mock_stocks_client):
        """Test fetching stock splits"""
        mock_response = [
            {
                'execution_date': '2020-08-31',
                'split_ratio': 4.0
            }
        ]
        
        mock_ref_instance = MagicMock()
        mock_ref_instance.get_stock_splits.return_value = mock_response
        mock_ref_client.return_value = mock_ref_instance
        
        connector = PolygonConnector()
        splits = connector.fetch_splits("AAPL", "2020-01-01", "2020-12-31")
        
        assert len(splits) == 1
        assert type(splits[0]).__name__ == "CorporateAction"
        assert splits[0].action_type == ActionType.SPLIT
        assert splits[0].split_ratio == 4.0
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_fetch_dividends_success(self, mock_ref_client, mock_stocks_client):
        """Test fetching dividends"""
        mock_response = [
            {
                'ex_dividend_date': '2026-02-10',
                'cash_amount': 0.25,
                'frequency': 4
            }
        ]
        
        mock_ref_instance = MagicMock()
        mock_ref_instance.get_stock_dividends.return_value = mock_response
        mock_ref_client.return_value = mock_ref_instance
        
        connector = PolygonConnector()
        dividends = connector.fetch_dividends("AAPL", "2026-01-01", "2026-12-31")
        
        assert len(dividends) == 1
        assert type(dividends[0]).__name__ == "CorporateAction"
        assert dividends[0].action_type == ActionType.DIVIDEND
        assert dividends[0].cash_amount == 0.25
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_retry_with_backoff_success_after_retry(self, mock_ref_client, mock_stocks_client):
        """Test retry mechanism succeeds after failure"""
        mock_stocks_instance = MagicMock()
        mock_stocks_instance.get_aggregate_bars.side_effect = [
            Exception("Temporary error"),
            {'results': []}
        ]
        mock_stocks_client.return_value = mock_stocks_instance
        
        connector = PolygonConnector()
        bars = connector.fetch_daily_bars("AAPL", "2024-01-02", "2024-01-02")
        
        assert bars == []
        assert mock_stocks_instance.get_aggregate_bars.call_count == 2
    
    @patch('code.bronze.connectors.polygon_connector.StocksClient')
    @patch('code.bronze.connectors.polygon_connector.ReferenceClient')
    def test_retry_exhausted_raises_error(self, mock_ref_client, mock_stocks_client):
        """Test retry mechanism raises after max attempts"""
        mock_stocks_instance = MagicMock()
        mock_stocks_instance.get_aggregate_bars.side_effect = Exception("Persistent error")
        mock_stocks_client.return_value = mock_stocks_instance
        
        connector = PolygonConnector()
        
        with pytest.raises(Exception) as exc_info:
            connector.fetch_daily_bars("AAPL", "2024-01-02", "2024-01-02")
        
        assert "Persistent error" in str(exc_info.value)
