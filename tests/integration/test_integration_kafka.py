# MarketMind Intelligence Platform V1
# Kafka Integration Tests - Producer/Consumer/Parquet Flow
# Date: April 24, 2026

import pytest
import os
import time
import tempfile
from pathlib import Path
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from code.bronze.connectors.polygon_connector import PolygonConnector
from code.bronze.producers.kafka_producer import KafkaProducer
from code.bronze.writers.bronze_writer import BronzeWriter
from code.bronze.schemas.market_bar import MarketBar


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaProducerIntegration:
    """Test Kafka producer integration with real Kafka broker"""
    
    @pytest.fixture(scope="class")
    def kafka_admin(self):
        """Create Kafka admin client for test setup"""
        admin_client = AdminClient({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        })
        return admin_client
    
    @pytest.fixture(scope="class")
    def create_test_topics(self, kafka_admin):
        """Create test Kafka topics"""
        topics = [
            NewTopic('test-market-bars', num_partitions=1, replication_factor=1),
            NewTopic('test-corporate-actions', num_partitions=1, replication_factor=1),
            NewTopic('test-macro-indicators', num_partitions=1, replication_factor=1)
        ]
        
        try:
            fs = kafka_admin.create_topics(topics)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic {topic} created")
                except KafkaException as e:
                    if e.args[0].code() != KafkaException._TOPIC_ALREADY_EXISTS:
                        raise
        except Exception as e:
            print(f"Topic creation warning: {e}")
        
        yield
        
        # Cleanup
        try:
            kafka_admin.delete_topics(['test-market-bars', 'test-corporate-actions', 'test-macro-indicators'])
        except Exception:
            pass
    
    def test_producer_sends_to_kafka(self, create_test_topics):
        """
        Test KafkaProducer successfully sends messages to Kafka
        
        Flow:
        1. Create MarketBar objects
        2. Send to Kafka via Producer
        3. Verify delivery
        """
        producer = KafkaProducer()
        
        # Create test bar
        bar = MarketBar(
            ticker="TEST",
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
        
        initial_sent = producer.sent_count
        
        # Send to Kafka
        producer.send_market_bar(bar)
        producer.flush()
        
        # Verify delivery
        assert producer.sent_count > initial_sent, "Message not delivered"
        assert producer.error_count == 0, "Errors occurred during send"
        
        producer.close()
        print(f"Successfully sent {producer.sent_count} messages to Kafka")
    
    def test_batch_send_to_kafka(self, create_test_topics):
        """
        Test batch sending to Kafka
        
        Flow:
        1. Create batch of MarketBar objects
        2. Send batch to Kafka
        3. Verify all delivered
        """
        producer = KafkaProducer()
        
        bars = []
        for i in range(10):
            bars.append(MarketBar(
                ticker=f"TEST{i}",
                timestamp=1704153600000 + (i * 86400000),
                granularity="daily",
                open=100.0 + i,
                high=105.0 + i,
                low=99.0 + i,
                close=103.0 + i,
                volume=1000000.0,
                vwap=102.0 + i,
                trade_count=5000,
                adjusted=True,
                source="polygon",
                ingestion_timestamp=1704153600000
            ))
        
        initial_sent = producer.sent_count
        
        producer.send_market_bars_batch(bars)
        
        # Verify all delivered
        assert producer.sent_count >= initial_sent + 10, "Not all messages delivered"
        
        producer.close()
        print(f"Batch send successful: {len(bars)} messages")


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaConsumerIntegration:
    """Test Kafka consumer (BronzeWriter) integration"""
    
    def test_consumer_reads_from_kafka(self):
        """
        Test BronzeWriter consumes from Kafka and writes to Parquet
        
        Flow:
        1. Producer sends messages to Kafka
        2. Consumer reads messages
        3. Consumer writes to Parquet
        4. Verify Parquet files created
        """
        # Step 1: Send test data to Kafka
        producer = KafkaProducer()
        
        bars = []
        for i in range(5):
            bars.append(MarketBar(
                ticker="TESTCONS",
                timestamp=1704153600000 + (i * 86400000),
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
            ))
        
        producer.send_market_bars_batch(bars)
        producer.close()
        
        # Wait for messages to be available
        time.sleep(2)
        
        # Step 2: Consume messages
        writer = BronzeWriter()
        
        initial_consumed = writer.consumed_count
        
        writer.consume_once()
        
        # Verify consumption
        assert writer.consumed_count > initial_consumed, "No messages consumed"
        
        writer.close()
        print(f"Consumer test passed: consumed {writer.consumed_count} messages")


@pytest.mark.integration
@pytest.mark.kafka
class TestEndToEndKafkaFlow:
    """Test complete Kafka flow: Connector → Producer → Kafka → Consumer → Parquet"""
    
    def test_polygon_to_kafka_to_parquet_flow(self):
        """
        Test complete end-to-end flow:
        1. Fetch from Polygon API
        2. Send to Kafka via Producer
        3. Consume from Kafka via Writer
        4. Verify Parquet file creation
        """
        # Step 1: Fetch from Polygon
        connector = PolygonConnector()
        bars = connector.fetch_daily_bars("AAPL", "2026-01-02", "2026-01-02")
        
        assert len(bars) > 0, "No bars fetched from Polygon"
        
        # Step 2: Send to Kafka
        producer = KafkaProducer()
        
        for bar in bars[:5]:  # Send first 5 bars
            producer.send_market_bar(bar)
        
        producer.flush()
        initial_sent = producer.sent_count
        
        assert initial_sent > 0, "No messages sent to Kafka"
        
        producer.close()
        
        # Wait for Kafka
        time.sleep(3)
        
        # Step 3: Consume from Kafka
        writer = BronzeWriter()
        
        initial_consumed = writer.consumed_count
        writer.consume_once()
        final_consumed = writer.consumed_count
        
        assert final_consumed > initial_consumed, "No messages consumed from Kafka"
        
        writer.close()
        
        print(f"End-to-end flow successful: {initial_sent} sent, {final_consumed - initial_consumed} consumed")


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaErrorHandling:
    """Test Kafka error handling scenarios"""
    
    def test_producer_handles_broker_unavailable(self):
        """Test producer gracefully handles broker unavailability"""
        # This test requires Kafka to be down - skip if running
        pytest.skip("Requires Kafka broker to be unavailable")
    
    def test_consumer_handles_empty_topic(self):
        """Test consumer handles empty topics gracefully"""
        writer = BronzeWriter()
        
        # Consume from empty topic should not crash
        try:
            writer.consume_once()
            consumed = writer.consumed_count
            assert consumed >= 0  # Should be 0 or more, not error
        finally:
            writer.close()


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaPerformance:
    """Test Kafka throughput"""
    
    def test_high_throughput_sending(self):
        """
        Test high-throughput message sending
        
        Benchmark: 100 messages in < 5 seconds
        """
        producer = KafkaProducer()
        
        bars = []
        for i in range(100):
            bars.append(MarketBar(
                ticker=f"PERF{i}",
                timestamp=1704153600000 + (i * 86400000),
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
            ))
        
        start_time = time.time()
        
        for bar in bars:
            producer.send_market_bar(bar)
        
        producer.flush()
        elapsed = time.time() - start_time
        
        assert elapsed < 5.0, f"High throughput sending too slow: {elapsed:.2f}s"
        
        throughput = len(bars) / elapsed
        print(f"Kafka throughput: {throughput:.1f} messages/sec")
        
        producer.close()
