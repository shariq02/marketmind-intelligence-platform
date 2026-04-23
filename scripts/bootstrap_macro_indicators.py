# ====================================================================
# Cold Bootstrap - Macro Indicators
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: scripts/bootstrap_macro_indicators.py
# Purpose: Fetch US macroeconomic indicators
# ====================================================================
"""
Cold Bootstrap - Macro Indicators

Fetches US macroeconomic indicators from AkShare:
- CPI (Consumer Price Index)
- Unemployment Rate
- ADP Employment
- Core CPI
- Federal Funds Rate

Features:
- Rate limiting (10 calls/min for AkShare)
- Progress tracking
- Error recovery

Usage:
    python3.11 scripts/bootstrap_macro_indicators.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime

from config import get_logger
from code.bronze.connectors.akshare_connector import AkShareConnector
from code.bronze.producers.kafka_producer import KafkaProducer

logger = get_logger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Macro indicators to fetch
INDICATORS = [
    ('cpi_monthly', 'US CPI Monthly'),
    ('unemployment_rate', 'US Unemployment Rate'),
    ('adp_employment', 'US ADP Employment'),
    ('core_cpi_monthly', 'US Core CPI Monthly'),
    ('interest_rate', 'US Federal Funds Rate'),
]

# ============================================================================
# BOOTSTRAP LOGIC
# ============================================================================

def bootstrap_macro_indicators():
    """
    Bootstrap macro indicators.
    
    Fetches all configured US macroeconomic indicators.
    """
    logger.info("=" * 70)
    logger.info("Cold Bootstrap - Macro Indicators")
    logger.info(f"Indicators: {len(INDICATORS)}")
    logger.info("=" * 70)
    
    # Initialize connectors
    akshare = AkShareConnector()
    producer = KafkaProducer()
    
    # Track stats
    start_time = datetime.now()
    total_records = 0
    total_errors = 0
    
    try:
        for idx, (indicator_type, indicator_name) in enumerate(INDICATORS, 1):
            logger.info(f"\n[{idx}/{len(INDICATORS)}] Fetching {indicator_name}...")
            
            try:
                # Fetch indicator based on type
                if indicator_type == 'cpi_monthly':
                    data = akshare.fetch_cpi_monthly()
                elif indicator_type == 'unemployment_rate':
                    data = akshare.fetch_unemployment_rate()
                elif indicator_type == 'adp_employment':
                    data = akshare.fetch_adp_employment()
                elif indicator_type == 'core_cpi_monthly':
                    data = akshare.fetch_core_cpi_monthly()
                elif indicator_type == 'interest_rate':
                    data = akshare.fetch_interest_rate()
                else:
                    logger.warning(f"  Unknown indicator type: {indicator_type}")
                    continue
                
                if not data:
                    logger.warning(f"  No data returned for {indicator_name}")
                    continue
                
                logger.info(f"  Fetched {len(data)} records")
                
                # Send to Kafka
                for record in data:
                    producer.send_macro_indicator(record)
                
                producer.flush()
                logger.info(f"  Sent {len(data)} records to Kafka")
                
                total_records += len(data)
                
                logger.info(f"  Progress: {idx}/{len(INDICATORS)} indicators, {total_records} total records")
                
            except Exception as e:
                logger.error(f"  Failed to fetch {indicator_name}: {e}")
                total_errors += 1
                continue
        
        # Final summary
        elapsed = (datetime.now() - start_time).seconds
        logger.info("\n" + "=" * 70)
        logger.info("Cold Bootstrap - Complete")
        logger.info(f"  Completed: {len(INDICATORS) - total_errors} indicators")
        logger.info(f"  Failed:    {total_errors} indicators")
        logger.info(f"  Total records: {total_records}")
        logger.info(f"  Elapsed:   {elapsed // 60} minutes {elapsed % 60} seconds")
        logger.info("=" * 70)
    
    finally:
        producer.close()


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point."""
    print("\nBootstrapping macro indicators...")
    print("This will fetch US economic data from AkShare")
    print("\nPress Ctrl+C to stop\n")
    
    try:
        bootstrap_macro_indicators()
    except KeyboardInterrupt:
        print("\n\nBootstrap interrupted by user.")
        sys.exit(0)


if __name__ == '__main__':
    main()
