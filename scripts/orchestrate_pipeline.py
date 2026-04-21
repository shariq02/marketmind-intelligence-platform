# ====================================================================
# Pipeline Orchestration - Bronze to Gold
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: scripts/orchestrate_pipeline.py
# Purpose: Orchestrate Bronze --> Silver --> Gold data flow
# ====================================================================
"""
Pipeline Orchestration

Orchestrates the full data pipeline:
1. Bronze Writer: Consume from Kafka --> Write to Parquet
2. Silver Transformers: Transform all partitions
3. Gold Loaders: Load all partitions to PostgreSQL

Usage:
    # Run full pipeline
    python3.11 scripts/orchestrate_pipeline.py
    
    # Run specific layer
    python3.11 scripts/orchestrate_pipeline.py --layer bronze
    python3.11 scripts/orchestrate_pipeline.py --layer silver
    python3.11 scripts/orchestrate_pipeline.py --layer gold
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from datetime import datetime, timedelta

from config import get_logger, DATA_DIR
from code.bronze.writers.bronze_writer import BronzeWriter
from code.silver.transformations.market_bars_transformer import MarketBarsTransformer
from code.gold.loaders.market_bars_loader import MarketBarsLoader

logger = get_logger(__name__)

# ============================================================================
# ORCHESTRATION LOGIC
# ============================================================================

def run_bronze_writer():
    """Run Bronze Writer to consume from Kafka."""
    logger.info("=" * 70)
    logger.info("BRONZE LAYER - Kafka to Parquet")
    logger.info("=" * 70)
    
    writer = BronzeWriter()
    
    try:
        logger.info("Starting Bronze Writer (batch mode)...")
        writer.consume_once()
        logger.info(f"Bronze Writer complete: {writer.consumed_count} consumed, {writer.written_count} written")
        return True
    except Exception as e:
        logger.error(f"Bronze Writer failed: {e}")
        return False
    finally:
        writer.close()


def run_silver_transformers():
    """Run Silver Transformers for all partitions."""
    logger.info("=" * 70)
    logger.info("SILVER LAYER - Bronze to Silver Transformation")
    logger.info("=" * 70)
    
    # Find all Bronze partitions
    bronze_base = DATA_DIR / 'bronze' / 'market_bars'
    
    if not bronze_base.exists():
        logger.warning("No Bronze data found")
        return False
    
    # Find all year/month/day partitions
    partitions = []
    for year_dir in bronze_base.glob('year=*'):
        year = int(year_dir.name.split('=')[1])
        for month_dir in year_dir.glob('month=*'):
            month = int(month_dir.name.split('=')[1])
            for day_dir in month_dir.glob('day=*'):
                day = int(day_dir.name.split('=')[1])
                date_str = f"{year}-{month:02d}-{day:02d}"
                partitions.append(date_str)
    
    if not partitions:
        logger.warning("No Bronze partitions found")
        return False
    
    logger.info(f"Found {len(partitions)} Bronze partitions to transform")
    
    # Transform each partition
    transformer = MarketBarsTransformer()
    success_count = 0
    fail_count = 0
    
    for idx, date_str in enumerate(sorted(partitions), 1):
        logger.info(f"\n[{idx}/{len(partitions)}] Transforming {date_str}...")
        
        try:
            if transformer.transform_partition(date_str):
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            logger.error(f"Failed to transform {date_str}: {e}")
            fail_count += 1
    
    logger.info(f"\nSilver Transformation complete: {success_count} success, {fail_count} failed")
    return fail_count == 0


def run_gold_loaders():
    """Run Gold Loaders for all partitions."""
    logger.info("=" * 70)
    logger.info("GOLD LAYER - Silver to PostgreSQL")
    logger.info("=" * 70)
    
    # Find all Silver partitions
    silver_base = DATA_DIR / 'silver' / 'market_bars'
    
    if not silver_base.exists():
        logger.warning("No Silver data found")
        return False
    
    # Find all year/month/day partitions
    partitions = []
    for year_dir in silver_base.glob('year=*'):
        year = int(year_dir.name.split('=')[1])
        for month_dir in year_dir.glob('month=*'):
            month = int(month_dir.name.split('=')[1])
            for day_dir in month_dir.glob('day=*'):
                day = int(day_dir.name.split('=')[1])
                date_str = f"{year}-{month:02d}-{day:02d}"
                partitions.append(date_str)
    
    if not partitions:
        logger.warning("No Silver partitions found")
        return False
    
    logger.info(f"Found {len(partitions)} Silver partitions to load")
    
    # Load each partition
    loader = MarketBarsLoader()
    success_count = 0
    fail_count = 0
    
    for idx, date_str in enumerate(sorted(partitions), 1):
        logger.info(f"\n[{idx}/{len(partitions)}] Loading {date_str}...")
        
        try:
            if loader.load_partition(date_str, mode='upsert'):
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            logger.error(f"Failed to load {date_str}: {e}")
            fail_count += 1
    
    # Get final count
    total_count = loader.get_record_count()
    
    logger.info(f"\nGold Load complete: {success_count} partitions loaded, {fail_count} failed")
    logger.info(f"Total records in gold.ohlcv_bars: {total_count}")
    return fail_count == 0


def run_full_pipeline():
    """Run full Bronze --> Silver --> Gold pipeline."""
    logger.info("=" * 70)
    logger.info("FULL PIPELINE ORCHESTRATION")
    logger.info("=" * 70)
    
    start_time = datetime.now()
    
    # Step 1: Bronze Writer
    logger.info("\n" + "=" * 70)
    logger.info("Step 1/3: Bronze Layer")
    logger.info("=" * 70)
    if not run_bronze_writer():
        logger.error("Bronze layer failed - stopping pipeline")
        return False
    
    # Step 2: Silver Transformers
    logger.info("\n" + "=" * 70)
    logger.info("Step 2/3: Silver Layer")
    logger.info("=" * 70)
    if not run_silver_transformers():
        logger.error("Silver layer failed - stopping pipeline")
        return False
    
    # Step 3: Gold Loaders
    logger.info("\n" + "=" * 70)
    logger.info("Step 3/3: Gold Layer")
    logger.info("=" * 70)
    if not run_gold_loaders():
        logger.error("Gold layer failed - stopping pipeline")
        return False
    
    # Summary
    elapsed = (datetime.now() - start_time).seconds
    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Elapsed: {elapsed // 60} minutes {elapsed % 60} seconds")
    logger.info("=" * 70)
    
    return True


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Orchestrate Bronze --> Silver --> Gold pipeline'
    )
    
    parser.add_argument(
        '--layer',
        type=str,
        choices=['bronze', 'silver', 'gold', 'full'],
        default='full',
        help='Layer to run (default: full)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    print(f"\nOrchestrating pipeline: {args.layer}")
    print("\n")
    
    try:
        if args.layer == 'bronze':
            success = run_bronze_writer()
        elif args.layer == 'silver':
            success = run_silver_transformers()
        elif args.layer == 'gold':
            success = run_gold_loaders()
        else:  # full
            success = run_full_pipeline()
        
        if success:
            print("\nPipeline completed successfully")
            sys.exit(0)
        else:
            print("\nPipeline failed")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(0)


if __name__ == '__main__':
    main()
