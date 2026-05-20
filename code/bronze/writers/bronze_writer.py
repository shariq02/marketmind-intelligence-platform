# ====================================================================
# Bronze Writer - Kafka to Parquet
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/writers/bronze_writer.py
# Purpose: Consume from Kafka topics and write to Parquet files
# ====================================================================
"""
Bronze Writer

Consumes messages from Kafka topics and writes to Parquet files:
- Deserializes Avro messages
- Batches writes for efficiency
- Partitions by date
- Updates checkpoints

Features:
- Multi-topic consumption
- Automatic partitioning (year/month/day)
- Checkpoint tracking
- Graceful shutdown

Usage:
    from code.bronze.writers.bronze_writer import BronzeWriter
    
    # Batch mode (consume all available, then stop)
    writer = BronzeWriter()
    writer.consume_once()
    
    # Streaming mode (run continuously)
    writer = BronzeWriter()
    writer.run()
"""

import json
import signal
import fastavro
from io import BytesIO
from typing import Dict, List, Any
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    KAFKA_CONFIG,
    KAFKA_TOPICS,
    KAFKA_CONSUMER_GROUPS,
    BRONZE_CONFIG,
    get_avro_schema_path,
    get_bronze_data_path,
    get_logger,
)

logger = get_logger(__name__)


class BronzeWriter:
    """
    Bronze Writer - Consumes from Kafka and writes to Parquet.
    
    Handles all 6 Kafka topics and writes to partitioned Parquet files.
    """
    
    def __init__(self):
        """Initialize consumer and load schemas."""
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONSUMER_GROUPS['bronze_writer'],
            'auto.offset.reset': BRONZE_CONFIG['consumer_auto_offset_reset'],
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
        })
        
        # Subscribe to all topics
        topics = list(KAFKA_TOPICS.values())
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
        
        # Load Avro schemas
        self.schemas = self._load_avro_schemas()
        
        # Message buffers (topic -> list of messages)
        self.buffers = {topic: [] for topic in KAFKA_TOPICS.keys()}
        
        # Stats
        self.consumed_count = 0
        self.written_count = 0
        
        # Shutdown flag
        self.shutdown = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("BronzeWriter initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown = True
    
    def _load_avro_schemas(self) -> Dict[str, Any]:
        """Load all Avro schemas."""
        schemas = {}
        
        schema_files = {
            'market_bars': 'market_bars_v1.avsc',
            'corporate_actions': 'market_corporate_actions_v1.avsc',
            'macro_indicators': 'macro_indicators_v1.avsc',
            'filings_metadata': 'filings_metadata_v1.avsc',
            'pipeline_audit': 'pipeline_audit_v1.avsc',
            'quality_alerts': 'quality_alerts_v1.avsc',
        }
        
        for key, filename in schema_files.items():
            schema_path = get_avro_schema_path(filename.replace('.avsc', ''))
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            schemas[key] = schema
        
        return schemas
    
    def _deserialize_avro(self, avro_bytes: bytes, schema: dict) -> dict:
        """
        Deserialize Avro bytes to dictionary.
        
        Args:
            avro_bytes: Avro-encoded bytes
            schema: Avro schema
        
        Returns:
            Dictionary representation
        """
        input_stream = BytesIO(avro_bytes)
        return fastavro.schemaless_reader(input_stream, schema)
    
    def _get_topic_key(self, topic: str) -> str:
        """Map Kafka topic name to buffer key."""
        for key, topic_name in KAFKA_TOPICS.items():
            if topic_name == topic:
                return key
        return None
    
    def _extract_date_from_record(self, record: dict, topic_key: str) -> tuple:
        """
        Extract year, month, day from record for partitioning.
        
        Args:
            record: Deserialized message
            topic_key: Topic identifier
        
        Returns:
            Tuple of (year, month, day)
        """
        # Extract timestamp based on topic
        if topic_key == 'market_bars':
            ts = record['timestamp']
            dt = datetime.fromtimestamp(ts / 1000.0)
        elif topic_key == 'corporate_actions':
            date_str = record.get('execution_date') or record.get('ex_dividend_date')
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        elif topic_key == 'macro_indicators':
            date_str = record['date']
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        elif topic_key == 'filings_metadata':
            date_str = record['filing_date']
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        elif topic_key in ['pipeline_audit', 'quality_alerts']:
            ts = record['start_timestamp'] if topic_key == 'pipeline_audit' else record['check_timestamp']
            dt = datetime.fromtimestamp(ts / 1000.0)
        else:
            dt = datetime.now()
        
        return (dt.year, dt.month, dt.day)
    
    def _write_parquet(self, topic_key: str, records: List[dict]):
        """
        Write records to Parquet file.
        
        Args:
            topic_key: Topic identifier (e.g., 'market_bars')
            records: List of deserialized messages
        """
        if not records:
            return
        
        # Group by date partition
        partitions = {}
        for record in records:
            year, month, day = self._extract_date_from_record(record, topic_key)
            partition_key = (year, month, day)
            
            if partition_key not in partitions:
                partitions[partition_key] = []
            partitions[partition_key].append(record)
        
        # Write each partition
        for (year, month, day), partition_records in partitions.items():
            # Get partition directory
            partition_dir = get_bronze_data_path(topic_key, year, month, day)
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            # Convert to DataFrame
            df = pd.DataFrame(partition_records)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"part_{timestamp}.parquet"
            filepath = partition_dir / filename
            
            # Write to Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                filepath,
                compression=BRONZE_CONFIG['parquet_compression']
            )
            
            logger.info(
                f"Wrote {len(partition_records)} records to "
                f"{topic_key}/year={year}/month={month:02d}/day={day:02d}/{filename}"
            )
            
            self.written_count += len(partition_records)
    
    def consume_once(self):
        """
        Consume all available messages and write to Parquet.
        Stops when no more messages available.
        """
        logger.info("Starting batch consumption...")
        
        timeout = BRONZE_CONFIG['consumer_poll_timeout_ms'] / 1000.0
        empty_polls = 0
        max_empty_polls = 3
        
        while empty_polls < max_empty_polls:
            msg = self.consumer.poll(timeout=timeout)
            
            if msg is None:
                empty_polls += 1
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            
            # Reset empty poll counter
            empty_polls = 0
            
            # Get topic key
            topic_key = self._get_topic_key(msg.topic())
            if not topic_key:
                logger.warning(f"Unknown topic: {msg.topic()}")
                continue
            
            # Deserialize
            try:
                record = self._deserialize_avro(msg.value(), self.schemas[topic_key])
                self.buffers[topic_key].append(record)
                self.consumed_count += 1
            except Exception as e:
                logger.error(f"Failed to deserialize message: {e}")
                continue
            
            # Check if buffer needs flushing
            if len(self.buffers[topic_key]) >= BRONZE_CONFIG['parquet_batch_size']:
                self._write_parquet(topic_key, self.buffers[topic_key])
                self.buffers[topic_key] = []
        
        # Flush remaining buffers
        for topic_key, records in self.buffers.items():
            if records:
                self._write_parquet(topic_key, records)
        
        # Commit offsets
        self.consumer.commit()
        
        logger.info(
            f"Batch consumption complete: "
            f"consumed={self.consumed_count}, written={self.written_count}"
        )
    
    def run(self):
        """
        Run continuously, consuming and writing messages.
        For production use with Airflow.
        """
        logger.info("Starting streaming consumption...")
        
        timeout = BRONZE_CONFIG['consumer_poll_timeout_ms'] / 1000.0
        last_write_time = datetime.now()
        write_interval = BRONZE_CONFIG['parquet_write_interval_seconds']
        
        while not self.shutdown:
            msg = self.consumer.poll(timeout=timeout)
            
            if msg is None:
                # Check if we should flush based on time
                if (datetime.now() - last_write_time).seconds >= write_interval:
                    for topic_key, records in self.buffers.items():
                        if records:
                            self._write_parquet(topic_key, records)
                            self.buffers[topic_key] = []
                    last_write_time = datetime.now()
                    self.consumer.commit()
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            
            # Get topic key
            topic_key = self._get_topic_key(msg.topic())
            if not topic_key:
                continue
            
            # Deserialize
            try:
                record = self._deserialize_avro(msg.value(), self.schemas[topic_key])
                self.buffers[topic_key].append(record)
                self.consumed_count += 1
            except Exception as e:
                logger.error(f"Failed to deserialize message: {e}")
                continue
            
            # Flush if buffer full
            if len(self.buffers[topic_key]) >= BRONZE_CONFIG['parquet_batch_size']:
                self._write_parquet(topic_key, self.buffers[topic_key])
                self.buffers[topic_key] = []
                last_write_time = datetime.now()
                self.consumer.commit()
        
        # Shutdown: flush remaining buffers
        logger.info("Shutting down, flushing buffers...")
        for topic_key, records in self.buffers.items():
            if records:
                self._write_parquet(topic_key, records)
        
        self.consumer.commit()
        self.consumer.close()
        
        logger.info(
            f"BronzeWriter shutdown complete: "
            f"consumed={self.consumed_count}, written={self.written_count}"
        )
    
    def close(self):
        """Close consumer."""
        self.consumer.close()


# Example usage
if __name__ == '__main__':
    writer = BronzeWriter()
    
    print("Consuming messages from Kafka and writing to Parquet...")
    writer.consume_once()
    
    print(f"Consumed: {writer.consumed_count}")
    print(f"Written: {writer.written_count}")
    
    writer.close()
    print("Writer closed")
