# MarketMind Intelligence Platform V1
# Unit Tests for Bronze Writer
# Date: April 24, 2026

import pytest
from unittest.mock import patch, MagicMock, mock_open

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from code.bronze.writers.bronze_writer import BronzeWriter


@pytest.mark.unit
class TestBronzeWriter:
    """Test suite for BronzeWriter"""
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('code.bronze.writers.bronze_writer.get_bronze_data_path')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_initialization(self, mock_file, mock_path, mock_consumer_class):
        """Test writer initialization"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        mock_path.return_value = '/fake/path'
        
        writer = BronzeWriter()
        
        assert writer is not None
        assert mock_consumer_instance.subscribe.called
        assert writer.consumed_count == 0
        assert writer.written_count == 0
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_get_topic_key(self, mock_file, mock_consumer_class):
        """Test mapping topic name to key"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        with patch('code.bronze.writers.bronze_writer.KAFKA_TOPICS', {
            'market_bars': 'market-bars-topic',
            'corporate_actions': 'corporate-actions-topic'
        }):
            writer = BronzeWriter()
            
            assert writer._get_topic_key('market-bars-topic') == 'market_bars'
            assert writer._get_topic_key('corporate-actions-topic') == 'corporate_actions'
            assert writer._get_topic_key('unknown-topic') is None
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_extract_date_market_bars(self, mock_file, mock_consumer_class):
        """Test date extraction from market bars"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        writer = BronzeWriter()
        
        record = {
            'timestamp': 1704153600000  # 2024-01-02
        }
        
        year, month, day = writer._extract_date_from_record(record, 'market_bars')
        
        assert year == 2024
        assert month == 1
        assert day == 2
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_extract_date_corporate_actions(self, mock_file, mock_consumer_class):
        """Test date extraction from corporate actions"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        writer = BronzeWriter()
        
        record = {
            'execution_date': '2020-08-31'
        }
        
        year, month, day = writer._extract_date_from_record(record, 'corporate_actions')
        
        assert year == 2020
        assert month == 8
        assert day == 31
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_extract_date_macro_indicators(self, mock_file, mock_consumer_class):
        """Test date extraction from macro indicators"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        writer = BronzeWriter()
        
        record = {
            'date': '2026-01-15'
        }
        
        year, month, day = writer._extract_date_from_record(record, 'macro_indicators')
        
        assert year == 2026
        assert month == 1
        assert day == 15
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_extract_date_filings(self, mock_file, mock_consumer_class):
        """Test date extraction from filings"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        writer = BronzeWriter()
        
        record = {
            'filing_date': '2025-10-31'
        }
        
        year, month, day = writer._extract_date_from_record(record, 'filings_metadata')
        
        assert year == 2025
        assert month == 10
        assert day == 31
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('code.bronze.writers.bronze_writer.get_bronze_data_path')
    @patch('code.bronze.writers.bronze_writer.pq.write_table')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_write_parquet(self, mock_file, mock_write_table, mock_path, mock_consumer_class):
        """Test writing records to Parquet"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_partition_dir = MagicMock()
        mock_partition_dir.mkdir = MagicMock()
        mock_path.return_value = mock_partition_dir
        
        writer = BronzeWriter()
        
        records = [
            {'ticker': 'AAPL', 'timestamp': 1704153600000, 'close': 100.0},
            {'ticker': 'MSFT', 'timestamp': 1704153600000, 'close': 200.0}
        ]
        
        writer._write_parquet('market_bars', records)
        
        assert mock_partition_dir.mkdir.called
        assert mock_write_table.called
        assert writer.written_count == 2
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('code.bronze.writers.bronze_writer.fastavro.schemaless_reader')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_deserialize_avro(self, mock_file, mock_reader, mock_consumer_class):
        """Test Avro deserialization"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_reader.return_value = {'ticker': 'AAPL', 'close': 100.0}
        
        writer = BronzeWriter()
        
        avro_bytes = b'fake_avro_data'
        schema = {'type': 'record', 'name': 'test'}
        
        result = writer._deserialize_avro(avro_bytes, schema)
        
        assert result == {'ticker': 'AAPL', 'close': 100.0}
        assert mock_reader.called
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('code.bronze.writers.bronze_writer.get_bronze_data_path')
    @patch('code.bronze.writers.bronze_writer.pq.write_table')
    @patch('code.bronze.writers.bronze_writer.fastavro.schemaless_reader')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_consume_once_success(self, mock_file, mock_reader, mock_write_table, mock_path, mock_consumer_class):
        """Test consuming messages in batch mode"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        # Create mock messages
        mock_msg1 = MagicMock()
        mock_msg1.error.return_value = None
        mock_msg1.topic.return_value = 'market-bars-topic'
        mock_msg1.value.return_value = b'fake_avro_data'
        
        mock_msg2 = MagicMock()
        mock_msg2.error.return_value = None
        mock_msg2.topic.return_value = 'market-bars-topic'
        mock_msg2.value.return_value = b'fake_avro_data'
        
        # Mock poll to return messages then None
        mock_consumer_instance.poll.side_effect = [
            mock_msg1,
            mock_msg2,
            None,
            None,
            None
        ]
        
        # Mock deserialization
        mock_reader.return_value = {'ticker': 'AAPL', 'timestamp': 1704153600000, 'close': 100.0}
        
        # Mock partition directory
        mock_partition_dir = MagicMock()
        mock_partition_dir.mkdir = MagicMock()
        mock_path.return_value = mock_partition_dir
        
        with patch('code.bronze.writers.bronze_writer.KAFKA_TOPICS', {
            'market_bars': 'market-bars-topic'
        }):
            writer = BronzeWriter()
            writer.consume_once()
        
        assert mock_consumer_instance.commit.called
        assert writer.consumed_count == 2
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_consume_once_handles_errors(self, mock_file, mock_consumer_class):
        """Test handling consumer errors"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        # Mock error message
        mock_msg = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = 1  # Not PARTITION_EOF
        mock_msg.error.return_value = mock_error
        
        mock_consumer_instance.poll.side_effect = [
            mock_msg,
            None,
            None,
            None
        ]
        
        writer = BronzeWriter()
        writer.consume_once()
        
        # Should handle error and continue
        assert mock_consumer_instance.commit.called
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_signal_handler(self, mock_file, mock_consumer_class):
        """Test shutdown signal handling"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        writer = BronzeWriter()
        
        assert not writer.shutdown
        
        writer._signal_handler(2, None)
        
        assert writer.shutdown
    
    @patch('code.bronze.writers.bronze_writer.Consumer')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "record", "name": "test"}')
    def test_close(self, mock_file, mock_consumer_class):
        """Test closing consumer"""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        writer = BronzeWriter()
        writer.close()
        
        assert mock_consumer_instance.close.called
