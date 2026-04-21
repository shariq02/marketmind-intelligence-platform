# ====================================================================
# Kafka Producer for MarketMind V1
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 20, 2026
# ====================================================================
# FILE: code/bronze/producers/kafka_producer.py
# Purpose: Send validated Pydantic models to Kafka topics as Avro
# ====================================================================
"""
Kafka Producer

Sends validated data to Kafka topics:
- Serializes Pydantic models to Avro format
- Routes to correct topic based on data type
- Handles delivery confirmation
- Batch sending for efficiency

Features:
- Automatic Avro serialization
- Delivery callbacks
- Error handling
- Batch flushing

Usage:
    from code.bronze.producers.kafka_producer import KafkaProducer
    from code.bronze.schemas.market_bar import MarketBar
    
    producer = KafkaProducer()
    
    # Send single message
    bar = MarketBar(...)
    producer.send_market_bar(bar)
    
    # Send batch
    bars = [MarketBar(...), MarketBar(...)]
    producer.send_market_bars_batch(bars)
    
    # Flush and close
    producer.flush()
    producer.close()
"""

import json
import fastavro
from io import BytesIO
from typing import List, Dict, Any
from confluent_kafka import Producer

from config import (
    KAFKA_CONFIG,
    KAFKA_TOPICS,
    get_avro_schema_path,
    get_logger,
)

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'schemas'))

from market_bar import MarketBar
from corporate_action import CorporateAction
from macro_indicator import MacroIndicator
from filing_metadata import FilingMetadata
from pipeline_audit import PipelineAudit
from quality_alert import QualityAlert

logger = get_logger(__name__)


class KafkaProducer:
    """
    Kafka producer for MarketMind V1 data.
    
    Handles serialization and delivery of validated Pydantic models
    to Kafka topics in Avro format.
    """
    
    def __init__(self):
        """Initialize Kafka producer and load Avro schemas."""
        self.producer = Producer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'client.id': KAFKA_CONFIG['client_id'],
            'compression.type': KAFKA_CONFIG['compression_type'],
            'acks': KAFKA_CONFIG['acks'],
            'retries': KAFKA_CONFIG['retries'],
            'batch.size': KAFKA_CONFIG['batch_size'],
            'linger.ms': KAFKA_CONFIG['linger_ms'],
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 32768,
            'max.in.flight.requests.per.connection': KAFKA_CONFIG['max_in_flight_requests'],
        })
        
        # Load Avro schemas
        self.schemas = self._load_avro_schemas()
        
        # Message counters
        self.sent_count = 0
        self.error_count = 0
        
        logger.info("KafkaProducer initialized")
    
    def _load_avro_schemas(self) -> Dict[str, Any]:
        """Load all Avro schemas from disk."""
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
            try:
                with open(schema_path, 'r') as f:
                    schema = json.load(f)
                schemas[key] = schema
                logger.info(f"Loaded Avro schema: {filename}")
            except Exception as e:
                logger.error(f"Failed to load schema {filename}: {e}")
                raise
        
        return schemas
    
    def _serialize_avro(self, data: dict, schema: dict) -> bytes:
        """
        Serialize dictionary to Avro binary format.
        
        Args:
            data: Dictionary representation of Pydantic model
            schema: Avro schema
        
        Returns:
            Avro-encoded bytes
        """
        output = BytesIO()
        fastavro.schemaless_writer(output, schema, data)
        return output.getvalue()
    
    def _delivery_callback(self, err, msg):
        """
        Callback for message delivery confirmation.
        
        Args:
            err: Error if delivery failed
            msg: Message metadata
        """
        if err:
            self.error_count += 1
            logger.error(f"Message delivery failed: {err}")
        else:
            self.sent_count += 1
            logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )
    
    def send_market_bar(self, bar: MarketBar):
        """
        Send a single market bar to Kafka.
        
        Args:
            bar: Validated MarketBar object
        """
        try:
            # Convert to dict
            data = bar.to_dict()
            
            # Serialize to Avro
            avro_bytes = self._serialize_avro(data, self.schemas['market_bars'])
            
            # Send to Kafka
            self.producer.produce(
                topic=KAFKA_TOPICS['market_bars'],
                value=avro_bytes,
                key=bar.ticker.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Poll for events (non-blocking)
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send market bar: {e}")
            raise
    
    def send_market_bars_batch(self, bars: List[MarketBar]):
        """
        Send multiple market bars to Kafka.
        
        Args:
            bars: List of validated MarketBar objects
        """
        logger.info(f"Sending batch of {len(bars)} market bars")
        
        for bar in bars:
            self.send_market_bar(bar)
        
        # Flush after batch
        self.flush()
        
        logger.info(f"Batch sent: {len(bars)} bars")
    
    def send_corporate_action(self, action: CorporateAction):
        """
        Send a corporate action to Kafka.
        
        Args:
            action: Validated CorporateAction object
        """
        try:
            data = action.to_dict()
            avro_bytes = self._serialize_avro(data, self.schemas['corporate_actions'])
            
            self.producer.produce(
                topic=KAFKA_TOPICS['corporate_actions'],
                value=avro_bytes,
                key=action.ticker.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send corporate action: {e}")
            raise
    
    def send_corporate_actions_batch(self, actions: List[CorporateAction]):
        """Send multiple corporate actions to Kafka."""
        logger.info(f"Sending batch of {len(actions)} corporate actions")
        
        for action in actions:
            self.send_corporate_action(action)
        
        self.flush()
        logger.info(f"Batch sent: {len(actions)} corporate actions")
    
    def send_macro_indicator(self, indicator: MacroIndicator):
        """
        Send a macro indicator to Kafka.
        
        Args:
            indicator: Validated MacroIndicator object
        """
        try:
            data = indicator.to_dict()
            avro_bytes = self._serialize_avro(data, self.schemas['macro_indicators'])
            
            self.producer.produce(
                topic=KAFKA_TOPICS['macro_indicators'],
                value=avro_bytes,
                key=indicator.indicator_name.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send macro indicator: {e}")
            raise
    
    def send_macro_indicators_batch(self, indicators: List[MacroIndicator]):
        """Send multiple macro indicators to Kafka."""
        logger.info(f"Sending batch of {len(indicators)} macro indicators")
        
        for indicator in indicators:
            self.send_macro_indicator(indicator)
        
        self.flush()
        logger.info(f"Batch sent: {len(indicators)} macro indicators")
    
    def send_filing_metadata(self, filing: FilingMetadata):
        """
        Send filing metadata to Kafka.
        
        Args:
            filing: Validated FilingMetadata object
        """
        try:
            data = filing.to_dict()
            avro_bytes = self._serialize_avro(data, self.schemas['filings_metadata'])
            
            self.producer.produce(
                topic=KAFKA_TOPICS['filings_metadata'],
                value=avro_bytes,
                key=filing.accession_number.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send filing metadata: {e}")
            raise
    
    def send_filing_metadata_batch(self, filings: List[FilingMetadata]):
        """Send multiple filing metadata records to Kafka."""
        logger.info(f"Sending batch of {len(filings)} filing metadata")
        
        for filing in filings:
            self.send_filing_metadata(filing)
        
        self.flush()
        logger.info(f"Batch sent: {len(filings)} filing metadata")
    
    def send_pipeline_audit(self, audit: PipelineAudit):
        """
        Send pipeline audit record to Kafka.
        
        Args:
            audit: Validated PipelineAudit object
        """
        try:
            data = audit.to_dict()
            avro_bytes = self._serialize_avro(data, self.schemas['pipeline_audit'])
            
            self.producer.produce(
                topic=KAFKA_TOPICS['pipeline_audit'],
                value=avro_bytes,
                key=audit.audit_id.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send pipeline audit: {e}")
            raise
    
    def send_quality_alert(self, alert: QualityAlert):
        """
        Send quality alert to Kafka.
        
        Args:
            alert: Validated QualityAlert object
        """
        try:
            data = alert.to_dict()
            avro_bytes = self._serialize_avro(data, self.schemas['quality_alerts'])
            
            self.producer.produce(
                topic=KAFKA_TOPICS['quality_alerts'],
                value=avro_bytes,
                key=alert.alert_id.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send quality alert: {e}")
            raise
    
    def flush(self):
        """Flush pending messages to Kafka."""
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush")
        else:
            logger.debug("All messages flushed successfully")
    
    def close(self):
        """Close producer and log statistics."""
        self.flush()
        
        logger.info(
            f"KafkaProducer closing: "
            f"sent={self.sent_count}, errors={self.error_count}"
        )
    
    def get_stats(self) -> Dict[str, int]:
        """Get producer statistics."""
        return {
            'sent': self.sent_count,
            'errors': self.error_count,
        }


# Example usage
if __name__ == '__main__':
    from code.bronze.connectors.polygon_connector import PolygonConnector
    
    producer = KafkaProducer()
    connector = PolygonConnector()
    
    # Test with real data
    print("Fetching AAPL daily bars...")
    bars = connector.fetch_daily_bars("AAPL", "2026-01-02", "2026-01-02")
    
    print(f"Sending {len(bars)} bars to Kafka...")
    producer.send_market_bars_batch(bars)
    
    # Get stats
    stats = producer.get_stats()
    print(f"Producer stats: {stats}")
    
    # Close
    producer.close()
    print("Producer closed")
