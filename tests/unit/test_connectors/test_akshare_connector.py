# MarketMind Intelligence Platform V1
# Unit Tests for AkShare Connector
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import patch
from code.bronze.connectors.akshare_connector import AkShareConnector


@pytest.mark.unit
@pytest.mark.connector
class TestAkShareConnector:
    """Test suite for AkShareConnector"""
    
    def test_initialization(self):
        """Test connector initialization"""
        connector = AkShareConnector()
        
        assert connector is not None
        assert hasattr(connector, 'rate_limiter')
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_usa_cpi_monthly')
    def test_fetch_cpi_monthly_success(self, mock_cpi):
        """Test fetching CPI monthly data"""
        # Mock AkShare response
        mock_df = pd.DataFrame({
            '商品': ['美国CPI月率', '美国CPI月率'],
            '日期': [pd.Timestamp('2026-01-15'), pd.Timestamp('2026-02-15')],
            '今值': [0.3, 0.4],
            '预测值': [0.2, 0.3],
            '前值': [0.4, 0.3]
        })
        
        mock_cpi.return_value = mock_df
        
        connector = AkShareConnector()
        indicators = connector.fetch_cpi_monthly()
        
        assert len(indicators) == 2
        assert type(indicators[0]).__name__ == "MacroIndicator"
        assert indicators[0].indicator_name == 'US_CPI_MOM'
        assert indicators[0].value == 0.3
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_usa_unemployment_rate')
    def test_fetch_unemployment_rate_success(self, mock_unemployment):
        """Test fetching unemployment rate"""
        mock_df = pd.DataFrame({
            '商品': ['美国失业率'],
            '日期': [pd.Timestamp('2026-01-15')],
            '今值': [3.7],
            '预测值': [3.8],
            '前值': [3.6]
        })
        
        mock_unemployment.return_value = mock_df
        
        connector = AkShareConnector()
        indicators = connector.fetch_unemployment_rate()
        
        assert len(indicators) == 1
        assert indicators[0].indicator_name == 'US_UNEMPLOYMENT_RATE'
        assert indicators[0].value == 3.7
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_usa_adp_employment')
    def test_fetch_adp_employment_success(self, mock_adp):
        """Test fetching ADP employment data"""
        mock_df = pd.DataFrame({
            '商品': ['美国ADP就业人数'],
            '日期': [pd.Timestamp('2026-01-15')],
            '今值': [150.0],
            '预测值': [140.0],
            '前值': [130.0]
        })
        
        mock_adp.return_value = mock_df
        
        connector = AkShareConnector()
        indicators = connector.fetch_adp_employment()
        
        assert len(indicators) == 1
        assert indicators[0].indicator_name == 'US_ADP_EMPLOYMENT'
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_usa_core_cpi_monthly')
    def test_fetch_core_cpi_monthly_success(self, mock_core_cpi):
        """Test fetching Core CPI monthly data"""
        mock_df = pd.DataFrame({
            '商品': ['美国核心CPI月率'],
            '日期': [pd.Timestamp('2026-01-15')],
            '今值': [0.2],
            '预测值': [0.2],
            '前值': [0.3]
        })
        
        mock_core_cpi.return_value = mock_df
        
        connector = AkShareConnector()
        indicators = connector.fetch_core_cpi_monthly()
        
        assert len(indicators) == 1
        assert indicators[0].indicator_name == 'US_CORE_CPI_MOM'
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_bank_usa_interest_rate')
    def test_fetch_interest_rate_success(self, mock_interest):
        """Test fetching interest rate data"""
        mock_df = pd.DataFrame({
            '商品': ['美国联邦基金利率'],
            '日期': [pd.Timestamp('2026-01-15')],
            '今值': [5.5],
            '预测值': [5.5],
            '前值': [5.25]
        })
        
        mock_interest.return_value = mock_df
        
        connector = AkShareConnector()
        indicators = connector.fetch_interest_rate()
        
        assert len(indicators) == 1
        assert indicators[0].indicator_name == 'US_FEDERAL_FUNDS_RATE'
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_usa_cpi_monthly')
    def test_retry_with_backoff_success_after_retry(self, mock_cpi):
        """Test retry mechanism succeeds after failure"""
        mock_cpi.side_effect = [
            Exception("Temporary error"),
            pd.DataFrame({
                '商品': ['美国CPI月率'],
                '日期': [pd.Timestamp('2026-01-15')],
                '今值': [0.3],
                '预测值': [0.2],
                '前值': [0.4]
            })
        ]
        
        connector = AkShareConnector()
        indicators = connector.fetch_cpi_monthly()
        
        assert len(indicators) == 1
        assert mock_cpi.call_count == 2
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_usa_cpi_monthly')
    def test_retry_exhausted_raises_error(self, mock_cpi):
        """Test retry mechanism raises after max attempts"""
        mock_cpi.side_effect = Exception("Persistent error")
        
        connector = AkShareConnector()
        
        with pytest.raises(Exception) as exc_info:
            connector.fetch_cpi_monthly()
        
        assert "Persistent error" in str(exc_info.value)
    
    @patch('code.bronze.connectors.akshare_connector.ak.macro_usa_cpi_monthly')
    def test_process_dataframe_with_nan_values(self, mock_cpi):
        """Test handling NaN values in response"""
        import numpy as np
        
        mock_df = pd.DataFrame({
            '商品': ['美国CPI月率'],
            '日期': [pd.Timestamp('2026-01-15')],
            '今值': [0.3],
            '预测值': [np.nan],
            '前值': [np.nan]
        })
        
        mock_cpi.return_value = mock_df
        
        connector = AkShareConnector()
        indicators = connector.fetch_cpi_monthly()
        
        assert len(indicators) == 1
        assert indicators[0].forecast_value is None
        assert indicators[0].previous_value is None
