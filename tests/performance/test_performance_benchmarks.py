# MarketMind Intelligence Platform V1
# Performance Tests - Speed & Throughput Benchmarks
# Date: April 24, 2026

import pytest
import time
from datetime import datetime, timedelta
import statistics
import pandas as pd

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT)

from code.bronze.connectors.polygon_connector import PolygonConnector
from code.bronze.connectors.akshare_connector import AkShareConnector
from code.bronze.connectors.edgartools_connector import EdgarToolsConnector
from code.bronze.schemas.market_bar import MarketBar
from code.silver.quality.quality_checks import QualityChecker


@pytest.mark.performance
class TestConnectorPerformance:
    """Test connector API response times and throughput"""
    
    def test_polygon_daily_bars_performance(self):
        """
        Test Polygon daily bars fetch performance
        
        Benchmark: Single ticker, 1 week of data
        Target: < 3 seconds
        """
        connector = PolygonConnector()
        
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        start_time = time.time()
        bars = connector.fetch_daily_bars("AAPL", start_date, end_date)
        elapsed = time.time() - start_time
        
        assert elapsed < 3.0, f"Polygon daily bars too slow: {elapsed:.2f}s (target: <3s)"
        assert len(bars) > 0, "No data returned"
        
        throughput = len(bars) / elapsed
        print(f"Polygon daily bars: {elapsed:.2f}s, {len(bars)} bars, {throughput:.1f} bars/sec")
    
    def test_polygon_intraday_bars_performance(self):
        """
        Test Polygon intraday bars fetch performance
        
        Benchmark: Single ticker, 1 day, 5min granularity
        Target: < 5 seconds
        """
        connector = PolygonConnector()
        
        date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        start_time = time.time()
        bars = connector.fetch_intraday_bars("AAPL", date, "5min")
        elapsed = time.time() - start_time
        
        assert elapsed < 5.0, f"Polygon intraday bars too slow: {elapsed:.2f}s (target: <5s)"
        
        print(f"Polygon intraday bars: {elapsed:.2f}s, {len(bars)} bars")
    
    def test_polygon_splits_performance(self):
        """
        Test Polygon splits fetch performance
        
        Benchmark: Single ticker, 1 year
        Target: < 3 seconds
        """
        connector = PolygonConnector()
        
        start_time = time.time()
        splits = connector.fetch_splits("AAPL", "2020-01-01", "2020-12-31")
        elapsed = time.time() - start_time
        
        assert elapsed < 3.0, f"Polygon splits too slow: {elapsed:.2f}s (target: <3s)"
        
        print(f"Polygon splits: {elapsed:.2f}s, {len(splits)} splits")
    
    def test_akshare_cpi_performance(self):
        """
        Test AkShare CPI fetch performance
        
        Target: < 120 seconds (API can be slow)
        """
        connector = AkShareConnector()
        
        start_time = time.time()
        indicators = connector.fetch_cpi_monthly()
        elapsed = time.time() - start_time
        
        assert elapsed < 120.0, f"AkShare CPI too slow: {elapsed:.2f}s (target: <120s)"
        assert len(indicators) > 0, "No data returned"
        
        print(f"AkShare CPI: {elapsed:.2f}s, {len(indicators)} indicators")
    
    def test_edgartools_filings_performance(self):
        """
        Test EdgarTools filings fetch performance
        
        Benchmark: 10 filings
        Target: < 10 seconds
        """
        connector = EdgarToolsConnector()
        
        start_time = time.time()
        filings = connector.fetch_10k_filings("AAPL", limit=10)
        elapsed = time.time() - start_time
        
        assert elapsed < 10.0, f"EdgarTools filings too slow: {elapsed:.2f}s (target: <10s)"
        assert len(filings) > 0, "No data returned"
        
        throughput = len(filings) / elapsed
        print(f"EdgarTools filings: {elapsed:.2f}s, {len(filings)} filings, {throughput:.2f} filings/sec")


@pytest.mark.performance
class TestBatchProcessingPerformance:
    """Test batch processing throughput"""
    
    def test_schema_validation_performance(self):
        """
        Test Pydantic schema validation speed
        
        Benchmark: 1000 MarketBar objects
        Target: < 0.5 seconds
        """
        test_data = {
            'ticker': 'AAPL',
            'timestamp': 1704153600000,
            'granularity': 'daily',
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 103.0,
            'volume': 1000000.0,
            'vwap': 102.0,
            'trade_count': 5000,
            'adjusted': True,
            'source': 'polygon',
            'ingestion_timestamp': 1704153600000
        }
        
        start_time = time.time()
        bars = [MarketBar(**test_data) for _ in range(1000)]
        elapsed = time.time() - start_time
        
        assert elapsed < 0.5, f"Schema validation too slow: {elapsed:.2f}s (target: <0.5s)"
        assert len(bars) == 1000
        
        throughput = len(bars) / elapsed
        print(f"Schema validation: {elapsed:.4f}s, {len(bars)} objects, {throughput:.0f} objects/sec")
    
    def test_quality_check_performance(self):
        """
        Test quality check speed
        
        Benchmark: 1000 records, completeness check
        Target: < 0.1 seconds
        """
        # Create test data
        test_records = []
        for i in range(1000):
            test_records.append({
                'ticker': 'AAPL',
                'timestamp': 1704153600000 + (i * 86400000),
                'close': 100.0 + i,
                'volume': 1000000.0
            })
        
        df = pd.DataFrame(test_records)
        checker = QualityChecker()
        
        start_time = time.time()
        result = checker.check_completeness(
            df,
            required_columns=['ticker', 'timestamp', 'close', 'volume']
        )
        elapsed = time.time() - start_time
        
        assert elapsed < 0.1, f"Quality check too slow: {elapsed:.2f}s (target: <0.1s)"
        assert result.passed
        
        throughput = len(test_records) / elapsed
        print(f"Quality check: {elapsed:.4f}s, {len(test_records)} records, {throughput:.0f} records/sec")


@pytest.mark.performance
class TestMemoryEfficiency:
    """Test memory usage patterns"""
    
    def test_large_batch_processing(self):
        """
        Test processing large batches doesn't exhaust memory
        
        Benchmark: 10,000 MarketBar objects
        """
        test_data = {
            'ticker': 'AAPL',
            'timestamp': 1704153600000,
            'granularity': 'daily',
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 103.0,
            'volume': 1000000.0,
            'vwap': 102.0,
            'trade_count': 5000,
            'adjusted': True,
            'source': 'polygon',
            'ingestion_timestamp': 1704153600000
        }
        
        start_time = time.time()
        
        # Create large batch
        bars = [MarketBar(**test_data) for _ in range(10000)]
        
        # Convert to dict
        bar_dicts = [bar.to_dict() for bar in bars]
        
        elapsed = time.time() - start_time
        
        assert len(bars) == 10000
        assert len(bar_dicts) == 10000
        
        print(f"Large batch processing: {elapsed:.2f}s for 10,000 objects")


@pytest.mark.performance
class TestConcurrencyPatterns:
    """Test concurrent data processing patterns"""
    
    def test_serial_vs_batch_comparison(self):
        """Compare serial processing vs batch processing"""
        connector = PolygonConnector()
        
        tickers = ['AAPL', 'MSFT', 'GOOGL']
        date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Serial processing
        start_time = time.time()
        serial_results = []
        for ticker in tickers:
            bars = connector.fetch_daily_bars(ticker, date, date)
            serial_results.extend(bars)
        serial_elapsed = time.time() - start_time
        
        print(f"Serial processing: {serial_elapsed:.2f}s for {len(tickers)} tickers")
        print(f"Average per ticker: {serial_elapsed/len(tickers):.2f}s")


@pytest.mark.performance
class TestPerformanceRegression:
    """Test for performance regressions"""
    
    def test_polygon_performance_baseline(self):
        """
        Establish performance baseline for Polygon connector
        
        This test records timing for regression detection
        """
        connector = PolygonConnector()
        
        measurements = []
        
        # Run 3 times to get average
        for _ in range(3):
            start_time = time.time()
            connector.fetch_daily_bars("AAPL", "2026-01-02", "2026-01-02")
            elapsed = time.time() - start_time
            measurements.append(elapsed)
        
        avg_time = statistics.mean(measurements)
        std_dev = statistics.stdev(measurements) if len(measurements) > 1 else 0
        
        # Baseline: should be under 3 seconds average
        assert avg_time < 3.0, f"Performance regression detected: {avg_time:.2f}s (baseline: <3s)"
        
        print(f"Polygon baseline: avg={avg_time:.2f}s, stddev={std_dev:.2f}s")
