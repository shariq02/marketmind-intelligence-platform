# MarketMind Intelligence Platform V1
# Unit Tests for Kafka Producer
# Date: April 24, 2026

import pytest
from unittest.mock import patch, MagicMock, mock_open
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.bronze.producers.kafka_producer import KafkaProducer
from code.bronze.schemas.market_bar import MarketBar
from code.bronze.schemas.corporate_action import CorporateAction, ActionType
from code.bronze.schemas.macro_indicator import MacroIndicator, Frequency
from code.bronze.schemas.filing_metadata import FilingMetadata
from code.bronze.schemas.pipeline_audit import PipelineAudit, ConnectorName, ExecutionMode
from code.bronze.schemas.quality_alert import QualityAlert, Layer, CheckType, Severity, CheckResult


@pytest.mark.unit
class TestKafkaProducer:
    """Test suite for KafkaProducer"""
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_initialization(self, mock_file, mock_schema_path, mock_producer_class):
        """Test producer initialization"""
        mock_schema_path.return_value = '/fake/path.avsc'
        
        producer = KafkaProducer()
        
        assert producer is not None
        assert mock_producer_class.called
        assert producer.sent_count == 0
        assert producer.error_count == 0
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    @patch('code.bronze.producers.kafka_producer.fastavro.schemaless_writer')
    def test_send_market_bar_success(self, mock_writer, mock_file, mock_schema_path, mock_producer_class):
        """Test sending a single market bar"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        
        bar = MarketBar(
            ticker="AAPL",
            timestamp=1704153600000,
            granularity="daily",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            vwap=102.0,
            trade_count=5000,
            adjusted=True,
            source="polygon",
            ingestion_timestamp=1704153600000
        )
        
        producer.send_market_bar(bar)
        
        assert mock_producer_instance.produce.called
        assert mock_producer_instance.poll.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_send_market_bars_batch(self, mock_file, mock_schema_path, mock_producer_class):
        """Test sending batch of market bars"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        mock_producer_instance.flush.return_value = 0
        mock_producer_instance.flush.return_value = 0
        
        producer = KafkaProducer()
        
        bars = [
            MarketBar(
                ticker="AAPL",
                timestamp=1704153600000,
                granularity="daily",
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000000.0,
                vwap=102.0,
                trade_count=5000,
                adjusted=True,
                source="polygon",
            ingestion_timestamp=1704153600000
            ),
            MarketBar(
                ticker="MSFT",
                timestamp=1704153600000,
                granularity="daily",
                open=200.0,
                high=205.0,
                low=199.0,
                close=203.0,
                volume=2000000.0,
                vwap=202.0,
                trade_count=6000,
                adjusted=True,
                source="polygon",
            ingestion_timestamp=1704153600000
            )
        ]
        
        producer.send_market_bars_batch(bars)
        
        assert mock_producer_instance.produce.call_count == 2
        assert mock_producer_instance.flush.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_send_corporate_action(self, mock_file, mock_schema_path, mock_producer_class):
        """Test sending corporate action"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        
        action = CorporateAction(
            ticker="AAPL",
            action_type=ActionType.SPLIT,
            execution_date="2020-08-31",
            split_ratio=4.0,
            source="polygon",
            ingestion_timestamp=1704153600000
        )
        
        producer.send_corporate_action(action)
        
        assert mock_producer_instance.produce.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_send_macro_indicator(self, mock_file, mock_schema_path, mock_producer_class):
        """Test sending macro indicator"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        
        indicator = MacroIndicator(
            indicator_name="US_CPI_MOM",
            date="2026-01-15",
            value=0.3,
            forecast_value=0.2,
            previous_value=0.4,
            unit="percent",
            frequency=Frequency.MONTHLY,
            source="akshare",
            ingestion_timestamp=1704153600000
        )
        
        producer.send_macro_indicator(indicator)
        
        assert mock_producer_instance.produce.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_send_filing_metadata(self, mock_file, mock_schema_path, mock_producer_class):
        """Test sending filing metadata"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        
        filing = FilingMetadata(
            accession_number="0000320193-25-000079",
            cik="0000320193",
            company_name="Apple Inc.",
            form_type="10-K",
            filing_date="2025-10-31",
            filing_url="https://www.sec.gov/test.html",
            source="edgartools",
            ingestion_timestamp=1704153600000
        )
        
        producer.send_filing_metadata(filing)
        
        assert mock_producer_instance.produce.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_send_pipeline_audit(self, mock_file, mock_schema_path, mock_producer_class):
        """Test sending pipeline audit"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        
        audit = PipelineAudit.create_new(
            connector_name=ConnectorName.POLYGON_BARS,
            entity_type="ticker",
            entity_id="AAPL",
            execution_mode=ExecutionMode.INCREMENTAL
        )
        
        producer.send_pipeline_audit(audit)
        
        assert mock_producer_instance.produce.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_send_quality_alert(self, mock_file, mock_schema_path, mock_producer_class):
        """Test sending quality alert"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        
        alert = QualityAlert.create_new(
            layer=Layer.BRONZE,
            table_name="market_bars",
            check_type=CheckType.COMPLETENESS,
            severity=Severity.HIGH,
            check_result=CheckResult.FAIL,
            failure_description="Missing data",
            row_count_checked=1000,
            failure_count=100
        )
        
        producer.send_quality_alert(alert)
        
        assert mock_producer_instance.produce.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_delivery_callback_success(self, mock_file, mock_schema_path, mock_producer_class):
        """Test delivery callback on success"""
        mock_schema_path.return_value = '/fake/path.avsc'
        
        producer = KafkaProducer()
        
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test_topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 123
        
        producer._delivery_callback(None, mock_msg)
        
        assert producer.sent_count == 1
        assert producer.error_count == 0
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_delivery_callback_error(self, mock_file, mock_schema_path, mock_producer_class):
        """Test delivery callback on error"""
        mock_schema_path.return_value = '/fake/path.avsc'
        
        producer = KafkaProducer()
        
        mock_error = Exception("Delivery failed")
        producer._delivery_callback(mock_error, None)
        
        assert producer.sent_count == 0
        assert producer.error_count == 1
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_flush(self, mock_file, mock_schema_path, mock_producer_class):
        """Test flushing messages"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_instance.flush.return_value = 0
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        producer.flush()
        
        assert mock_producer_instance.flush.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_close(self, mock_file, mock_schema_path, mock_producer_class):
        """Test closing producer"""
        mock_schema_path.return_value = '/fake/path.avsc'
        mock_producer_instance = MagicMock()
        mock_producer_instance.flush.return_value = 0
        mock_producer_class.return_value = mock_producer_instance
        
        producer = KafkaProducer()
        producer.close()
        
        assert mock_producer_instance.flush.called
    
    @patch('code.bronze.producers.kafka_producer.Producer')
    @patch('code.bronze.producers.kafka_producer.get_avro_schema_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_get_stats(self, mock_file, mock_schema_path, mock_producer_class):
        """Test getting statistics"""
        mock_schema_path.return_value = '/fake/path.avsc'
        
        producer = KafkaProducer()
        producer.sent_count = 100
        producer.error_count = 5
        
        stats = producer.get_stats()
        
        assert stats['sent'] == 100
        assert stats['errors'] == 5
