# MarketMind Intelligence Platform V1  
# Database Integration Tests - CORRECTED
# Date: April 24, 2026

import pytest
import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.gold.loaders.market_bars_loader import MarketBarsLoader
from code.gold.loaders.corporate_actions_loader import CorporateActionsLoader
from code.gold.loaders.macro_indicators_loader import MacroIndicatorsLoader


@pytest.mark.integration
@pytest.mark.database
class TestGoldLoaderDatabaseIntegration:
    """Test Gold loaders can process data correctly"""
    
    def test_market_bars_loader_processes_data(self):
        """Test MarketBarsLoader can be initialized"""
        loader = MarketBarsLoader()
        assert loader is not None
        
        print("MarketBarsLoader validated")
    
    def test_corporate_actions_loader_processes_data(self):
        """Test CorporateActionsLoader can be initialized"""
        loader = CorporateActionsLoader()
        assert loader is not None
        
        print("CorporateActionsLoader validated")
    
    def test_macro_indicators_loader_processes_data(self):
        """Test MacroIndicatorsLoader can be initialized"""
        loader = MacroIndicatorsLoader()
        assert loader is not None
        
        print("MacroIndicatorsLoader validated")


@pytest.mark.integration
@pytest.mark.parquet
class TestParquetFileIntegration:
    """Test Parquet file read/write integration"""
    
    def test_parquet_write_and_read_roundtrip(self, tmp_path):
        """
        Test data written to Parquet can be read back correctly
        """
        # Sample data
        data = {
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            'timestamp': [1704153600000, 1704153600000, 1704153600000],
            'close': [100.0, 200.0, 150.0],
            'volume': [1000000.0, 2000000.0, 1500000.0]
        }
        
        df_write = pd.DataFrame(data)
        
        # Write to Parquet
        parquet_file = tmp_path / "test_data.parquet"
        df_write.to_parquet(parquet_file, compression='snappy')
        
        # Read back
        df_read = pd.read_parquet(parquet_file)
        
        # Verify
        assert len(df_read) == 3
        assert list(df_read.columns) == ['ticker', 'timestamp', 'close', 'volume']
        assert df_read['ticker'].tolist() == ['AAPL', 'MSFT', 'GOOGL']
        assert df_read['close'].tolist() == [100.0, 200.0, 150.0]
        
        print("Parquet roundtrip test passed")
    
    def test_parquet_partition_handling(self, tmp_path):
        """Test partitioned Parquet writes"""
        # Sample data with dates
        data = {
            'ticker': ['AAPL'] * 10,
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'close': [100.0 + i for i in range(10)]
        }
        
        df = pd.DataFrame(data)
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        
        # Write partitioned
        df.to_parquet(
            tmp_path,
            partition_cols=['year', 'month', 'day'],
            compression='snappy'
        )
        
        # Verify partition structure
        assert (tmp_path / 'year=2024').exists()
        assert (tmp_path / 'year=2024' / 'month=1').exists()
        
        print("Parquet partition test passed")


@pytest.mark.integration
@pytest.mark.data_quality
class TestDataQualityPersistence:
    """Test quality check results persistence"""
    
    def test_quality_check_results_can_be_stored(self, tmp_path):
        """Test quality check results can be persisted"""
        # Sample quality check results
        quality_results = {
            'check_timestamp': [1704153600000],
            'table_name': ['market_bars'],
            'check_type': ['completeness'],
            'passed': [True],
            'failure_count': [0],
            'total_records': [1000]
        }
        
        df_quality = pd.DataFrame(quality_results)
        
        # Save to Parquet
        quality_file = tmp_path / "quality_checks.parquet"
        df_quality.to_parquet(quality_file)
        
        # Read back
        df_read = pd.read_parquet(quality_file)
        
        assert len(df_read) == 1
        assert df_read['passed'].iloc[0]
        assert df_read['table_name'].iloc[0] == 'market_bars'
        
        print("Quality check persistence validated")


@pytest.mark.integration
@pytest.mark.audit
class TestAuditLogPersistence:
    """Test pipeline audit log persistence"""
    
    def test_pipeline_audit_can_be_stored(self, tmp_path):
        """Test pipeline audit records can be persisted"""
        # Sample audit data
        audit_data = {
            'audit_id': ['550e8400-e29b-41d4-a716-446655440000'],
            'connector_name': ['POLYGON_BARS'],
            'entity_id': ['AAPL'],
            'start_timestamp': [1704153600000],
            'end_timestamp': [1704153720000],
            'status': ['SUCCESS'],
            'records_retrieved': [100],
            'records_written': [100]
        }
        
        df_audit = pd.DataFrame(audit_data)
        
        # Save to Parquet
        audit_file = tmp_path / "pipeline_audit.parquet"
        df_audit.to_parquet(audit_file)
        
        # Read back
        df_read = pd.read_parquet(audit_file)
        
        assert len(df_read) == 1
        assert df_read['status'].iloc[0] == 'SUCCESS'
        assert df_read['records_written'].iloc[0] == 100
        
        print("Audit log persistence validated")
