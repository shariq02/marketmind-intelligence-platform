# MarketMind Intelligence Platform V1
# Unit Tests for All Gold Loaders
# Date: April 24, 2026

import pytest
import pandas as pd
from unittest.mock import patch
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import all loaders
from code.gold.loaders.market_bars_loader import MarketBarsLoader
from code.gold.loaders.macro_indicators_loader import MacroIndicatorsLoader
from code.gold.loaders.filings_metadata_loader import FilingsMetadataLoader
from code.gold.loaders.corporate_actions_loader import CorporateActionsLoader
from code.gold.loaders.pipeline_audit_loader import PipelineAuditLoader
from code.gold.loaders.quality_alerts_loader import QualityAlertsLoader


@pytest.mark.unit
@pytest.mark.loader
class TestMarketBarsLoader:
    """Test suite for MarketBarsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = MarketBarsLoader()
        assert loader is not None
        assert hasattr(loader, 'silver_base_path')
        assert loader.table_name == 'gold.ohlcv_bars'
    
    @patch('code.gold.loaders.market_bars_loader.psycopg2.connect')
    def test_get_connection(self, mock_connect):
        """Test database connection"""
        loader = MarketBarsLoader()
        loader.get_connection()
        assert mock_connect.called
    
    def test_read_silver_partition_path_not_exists(self, tmp_path):
        """Test reading non-existent partition"""
        with patch('code.gold.loaders.market_bars_loader.DATA_DIR', tmp_path):
            loader = MarketBarsLoader()
            result = loader.read_silver_partition('2026-01-02')
            assert result.empty
    
    def test_prepare_for_gold(self):
        """Test prepare_for_gold transformation"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'granularity': 'day',
             'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 1000000,
             'vwap': 102, 'trade_count': 5000, 'adjusted': True, 'date': '2024-01-02',
             'year': 2024, 'month': 1, 'day': 2, 'is_trading_day': True, 'is_valid_ohlc': True},
        ])
        
        loader = MarketBarsLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert 'ticker' in result.columns
        assert 'open' in result.columns


@pytest.mark.unit
@pytest.mark.loader
class TestMacroIndicatorsLoader:
    """Test suite for MacroIndicatorsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = MacroIndicatorsLoader()
        assert loader is not None
        assert loader.table_name == 'gold.macro_indicators'
    
    def test_read_silver_data_path_not_exists(self, tmp_path):
        """Test reading with non-existent path"""
        with patch('code.gold.loaders.macro_indicators_loader.DATA_DIR', tmp_path):
            loader = MacroIndicatorsLoader()
            result = loader.read_silver_data()
            assert result.empty
    
    def test_prepare_for_gold(self):
        """Test prepare_for_gold transformation"""
        df = pd.DataFrame([
            {'indicator_name': 'US_CPI_MOM', 'date': '2024-01-01', 'value': 0.3,
             'unit': 'percent', 'frequency': 'monthly', 'forecast_value': 0.2,
             'previous_value': 0.1, 'year': 2024, 'month': 1, 'quarter': 1,
             'value_change': 0.2, 'value_pct_change': 200.0, 'indicator_category': 'Inflation'},
        ])
        
        loader = MacroIndicatorsLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert 'indicator_name' in result.columns


@pytest.mark.unit
@pytest.mark.loader
class TestFilingsMetadataLoader:
    """Test suite for FilingsMetadataLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = FilingsMetadataLoader()
        assert loader is not None
        assert loader.table_name == 'gold.sec_filings'
    
    def test_prepare_for_gold(self):
        """Test prepare_for_gold transformation"""
        df = pd.DataFrame([
            {'accession_number': '0001628280-24-000001', 'ticker': 'AAPL', 'cik': '0000320193',
             'company_name': 'Apple Inc.', 'form_type': '10-K', 'filing_date': '2024-01-15',
             'report_date': '2023-12-31', 'filing_year': 2024, 'filing_quarter': 1,
             'filing_category': 'Annual Report', 'is_periodic_report': True, 'is_amended': False,
             'is_xbrl': True, 'is_inline_xbrl': False, 'has_structured_data': True,
             'filing_lag_days': 15},
        ])
        
        loader = FilingsMetadataLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert 'accession_number' in result.columns


@pytest.mark.unit
@pytest.mark.loader
class TestCorporateActionsLoader:
    """Test suite for CorporateActionsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = CorporateActionsLoader()
        assert loader is not None
        assert loader.table_name == 'gold.corporate_actions'
    
    def test_prepare_for_gold_splits(self):
        """Test prepare_for_gold with splits"""
        df = pd.DataFrame([
            {'ticker': 'AAPL', 'action_type': 'SPLIT', 'execution_date': '2024-01-15',
             'split_ratio': 2.0, 'ex_dividend_date': None, 'payment_date': None,
             'record_date': None, 'cash_amount': None, 'declaration_date': None,
             'frequency': None, 'year': 2024, 'month': 1, 'is_forward_split': True,
             'is_reverse_split': False, 'is_valid_split': True},
        ])
        
        loader = CorporateActionsLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert result.iloc[0]['action_type'] == 'SPLIT'


@pytest.mark.unit
@pytest.mark.loader
class TestPipelineAuditLoader:
    """Test suite for PipelineAuditLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = PipelineAuditLoader()
        assert loader is not None
        assert loader.table_name == 'gold.pipeline_audit'
    
    def test_prepare_for_gold(self):
        """Test prepare_for_gold transformation"""
        df = pd.DataFrame([
            {'audit_id': '001', 'connector': 'polygon', 'execution_mode': 'full',
             'status': 'SUCCESS', 'start_datetime': pd.Timestamp('2024-01-01'),
             'end_datetime': pd.Timestamp('2024-01-01 00:10:00'), 'duration_seconds': 600,
             'duration_minutes': 10, 'records_retrieved': 1000, 'records_written': 1000,
             'bytes_written': 1048576, 'megabytes_written': 1.0, 'api_calls_made': 10,
             'rate_limited': False, 'write_success_rate': 100.0, 'records_per_second': 1.67,
             'records_per_api_call': 100.0, 'is_success': True, 'is_failure': False,
             'is_slow': False, 'error_message': None},
        ])
        
        loader = PipelineAuditLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert 'audit_id' in result.columns


@pytest.mark.unit
@pytest.mark.loader
class TestQualityAlertsLoader:
    """Test suite for QualityAlertsLoader"""
    
    def test_initialization(self):
        """Test loader initialization"""
        loader = QualityAlertsLoader()
        assert loader is not None
        assert loader.table_name == 'gold.quality_alerts'
    
    def test_prepare_for_gold(self):
        """Test prepare_for_gold transformation"""
        df = pd.DataFrame([
            {'alert_id': '001', 'layer': 'BRONZE', 'table_name': 'market_bars',
             'check_type': 'COMPLETENESS', 'severity': 'HIGH', 'check_result': 'FAIL',
             'check_datetime': pd.Timestamp('2024-01-01'), 'resolved': False,
             'resolution_datetime': None, 'time_to_resolution_hours': None,
             'failure_description': 'Test failure', 'row_count_checked': 1000,
             'failure_count': 100, 'failure_rate': 10.0, 'threshold_value': 95.0,
             'actual_value': 90.0, 'pipeline_blocked': False, 'severity_score': 3,
             'impact_score': 0.3, 'is_critical': False, 'is_unresolved': True},
        ])
        
        loader = QualityAlertsLoader()
        result = loader.prepare_for_gold(df)
        
        assert len(result) == 1
        assert 'alert_id' in result.columns
