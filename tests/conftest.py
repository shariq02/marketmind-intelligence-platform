# MarketMind Intelligence Platform V1
# Test Fixtures and Configuration
# Date: April 23, 2026

import sys
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
import psycopg2


@pytest.fixture
def sample_market_bar():
    """Sample market bar data for testing"""
    return {
        'ticker': 'AAPL',
        'timestamp': 1704153600,  # 2024-01-02 00:00:00 UTC
        'granularity': 'day',
        'open': 185.50,
        'high': 187.25,
        'low': 184.75,
        'close': 186.80,
        'volume': 52000000,
        'vwap': 186.32,
        'trade_count': 125000,
        'adjusted': True
    }


@pytest.fixture
def sample_market_bars_df():
    """Sample DataFrame with multiple market bars"""
    data = []
    base_timestamp = 1704153600
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    
    for i in range(5):  # 5 days
        for ticker in tickers:
            data.append({
                'ticker': ticker,
                'timestamp': base_timestamp + (i * 86400),
                'granularity': 'day',
                'open': 100.0 + i,
                'high': 105.0 + i,
                'low': 99.0 + i,
                'close': 103.0 + i,
                'volume': 10000000 + (i * 1000000),
                'vwap': 102.0 + i,
                'trade_count': 50000 + i,
                'adjusted': True
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_macro_indicator():
    """Sample macro indicator data"""
    return {
        'indicator_name': 'US_CPI_MOM',
        'date': '2024-01-15',
        'value': 0.3,
        'unit': 'percent',
        'frequency': 'monthly',
        'forecast_value': 0.2,
        'previous_value': 0.1
    }


@pytest.fixture
def sample_macro_indicators_df():
    """Sample DataFrame with macro indicators"""
    data = []
    indicators = ['US_CPI_MOM', 'US_UNEMPLOYMENT_RATE', 'US_FEDERAL_FUNDS_RATE']
    base_date = datetime(2024, 1, 1)
    
    for i in range(10):  # 10 months
        for indicator in indicators:
            data.append({
                'indicator_name': indicator,
                'date': (base_date + timedelta(days=30*i)).strftime('%Y-%m-%d'),
                'value': 2.0 + (i * 0.1),
                'unit': 'percent',
                'frequency': 'monthly',
                'forecast_value': 2.1 + (i * 0.1),
                'previous_value': 1.9 + (i * 0.1)
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_sec_filing():
    """Sample SEC filing data"""
    return {
        'accession_number': '0001628280-24-000001',
        'ticker': 'AAPL',
        'cik': '0000320193',
        'company_name': 'Apple Inc.',
        'form_type': '10-K',
        'filing_date': '2024-01-15',
        'report_date': '2023-12-31'
    }


@pytest.fixture
def sample_sec_filings_df():
    """Sample DataFrame with SEC filings"""
    data = []
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    form_types = ['10-K', '10-Q', '8-K']
    base_date = datetime(2024, 1, 1)
    
    for i, ticker in enumerate(tickers):
        for j, form_type in enumerate(form_types):
            data.append({
                'accession_number': f'0001628280-24-{i:06d}{j:02d}',
                'ticker': ticker,
                'cik': f'000032019{i}',
                'company_name': f'{ticker} Inc.',
                'form_type': form_type,
                'filing_date': (base_date + timedelta(days=30*j)).strftime('%Y-%m-%d'),
                'report_date': (base_date + timedelta(days=30*j - 15)).strftime('%Y-%m-%d')
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_corporate_action():
    """Sample corporate action data"""
    return {
        'ticker': 'AAPL',
        'action_type': 'dividend',
        'execution_date': '2024-02-15',
        'ex_dividend_date': '2024-02-10',
        'payment_date': '2024-02-15',
        'record_date': '2024-02-12',
        'cash_amount': 0.24,
        'declaration_date': '2024-01-25',
        'frequency': 4
    }


@pytest.fixture
def mock_db_connection():
    """Mock database connection for testing"""
    mock_conn = MagicMock(spec=psycopg2.extensions.connection)
    mock_cursor = MagicMock(spec=psycopg2.extensions.cursor)
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing"""
    mock_producer = Mock()
    mock_producer.send.return_value = None
    mock_producer.flush.return_value = None
    return mock_producer


@pytest.fixture
def invalid_ohlcv_data():
    """Invalid OHLCV data for testing quality checks"""
    return pd.DataFrame([
        # High < Low (invalid)
        {'ticker': 'AAPL', 'timestamp': 1704153600, 'open': 100, 'high': 95, 'low': 105, 'close': 102, 'volume': 1000000},
        # Negative price (invalid)
        {'ticker': 'MSFT', 'timestamp': 1704240000, 'open': -50, 'high': 100, 'low': 90, 'close': 95, 'volume': 1000000},
        # Zero volume (invalid)
        {'ticker': 'GOOGL', 'timestamp': 1704326400, 'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 0},
    ])


@pytest.fixture
def valid_ohlcv_data():
    """Valid OHLCV data for testing"""
    return pd.DataFrame([
        {'ticker': 'AAPL', 'timestamp': 1704153600, 'open': 100, 'high': 105, 'low': 99, 'close': 103, 'volume': 1000000},
        {'ticker': 'MSFT', 'timestamp': 1704240000, 'open': 200, 'high': 205, 'low': 198, 'close': 202, 'volume': 2000000},
        {'ticker': 'GOOGL', 'timestamp': 1704326400, 'open': 150, 'high': 155, 'low': 148, 'close': 153, 'volume': 1500000},
    ])


@pytest.fixture
def test_config():
    """Test configuration settings"""
    return {
        'db_host': 'localhost',
        'db_port': 5432,
        'db_name': 'test_marketmind',
        'db_user': 'test_user',
        'db_password': 'test_password',
        'kafka_bootstrap_servers': 'localhost:9092',
        'bronze_dir': '/tmp/test_bronze',
        'silver_dir': '/tmp/test_silver',
    }


# Pytest hooks
def pytest_configure(config):
    """Configure pytest with custom settings"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection behavior"""
    for item in items:
        # Auto-mark tests based on path
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
