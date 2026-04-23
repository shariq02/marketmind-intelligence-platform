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

logger = get_logger(__name__)

# ============================================================================
# DATA TYPE CONFIGURATION
# ============================================================================

DATA_TYPE_CONFIG = {
    'market_bars': {
        'bronze_dir': 'market_bars',
        'silver_dir': 'market_bars',
        'transformer_module': 'code.silver.transformations.market_bars_transformer',
        'transformer_class': 'MarketBarsTransformer',
        'loader_module': 'code.gold.loaders.market_bars_loader',
        'loader_class': 'MarketBarsLoader',
    },
    'corporate_actions': {
        'bronze_dir': 'corporate_actions',
        'silver_dir': 'corporate_actions',
        'transformer_module': 'code.silver.transformations.corporate_actions_transformer',
        'transformer_class': 'CorporateActionsTransformer',
        'loader_module': 'code.gold.loaders.corporate_actions_loader',
        'loader_class': 'CorporateActionsLoader',
    },
    'macro_indicators': {
        'bronze_dir': 'macro_indicators',
        'silver_dir': 'macro_indicators',
        'transformer_module': 'code.silver.transformations.macro_indicators_transformer',
        'transformer_class': 'MacroIndicatorsTransformer',
        'loader_module': 'code.gold.loaders.macro_indicators_loader',
        'loader_class': 'MacroIndicatorsLoader',
    },
    'sec_filings': {
        'bronze_dir': 'filings_metadata',  # FIXED: was 'sec_filings'
        'silver_dir': 'filings_metadata',  # FIXED: was 'sec_filings'
        'transformer_module': 'code.silver.transformations.filings_metadata_transformer',
        'transformer_class': 'FilingsMetadataTransformer',
        'loader_module': 'code.gold.loaders.filings_metadata_loader',
        'loader_class': 'FilingsMetadataLoader',
    },
}


def get_transformer_class(data_type):
    """Dynamically import and return transformer class."""
    import importlib
    config = DATA_TYPE_CONFIG[data_type]
    module = importlib.import_module(config['transformer_module'])
    return getattr(module, config['transformer_class'])


def get_loader_class(data_type):
    """Dynamically import and return loader class."""
    import importlib
    config = DATA_TYPE_CONFIG[data_type]
    module = importlib.import_module(config['loader_module'])
    return getattr(module, config['loader_class'])

# ============================================================================
# ORCHESTRATION LOGIC
# ============================================================================

def run_bronze_writer(data_type='market_bars'):
    """Run Bronze Writer to consume from Kafka."""
    from code.bronze.writers.bronze_writer import BronzeWriter
    
    logger.info("=" * 70)
    logger.info(f"BRONZE LAYER - Kafka to Parquet ({data_type})")
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


def run_silver_transformers(data_type='market_bars'):
    """Run Silver Transformers for all partitions."""
    logger.info("=" * 70)
    logger.info(f"SILVER LAYER - Bronze to Silver Transformation ({data_type})")
    logger.info("=" * 70)
    
    config = DATA_TYPE_CONFIG[data_type]
    
    # Find all Bronze partitions
    bronze_base = DATA_DIR / 'bronze' / config['bronze_dir']
    
    if not bronze_base.exists():
        logger.warning(f"No Bronze data found for {data_type}")
        return False
    
    # Get transformer class dynamically
    TransformerClass = get_transformer_class(data_type)
    transformer = TransformerClass()
    
    # Check which transformation method the transformer uses
    if hasattr(transformer, 'transform_partition'):
        # Partition-based transformation (market_bars)
        logger.info(f"Using partition-based transformation for {data_type}")
        
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
            logger.warning(f"No Bronze partitions found for {data_type}")
            return False
        
        logger.info(f"Found {len(partitions)} Bronze partitions to transform")
        
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
    
    elif hasattr(transformer, 'transform_all'):
        # Bulk transformation (macro_indicators, corporate_actions, sec_filings)
        logger.info(f"Using bulk transformation for {data_type}")
        
        try:
            success = transformer.transform_all()
            if success:
                logger.info("Silver transformation complete: SUCCESS")
                return True
            else:
                logger.warning("Silver transformation complete: No data transformed")
                return False
        except Exception as e:
            logger.error(f"Silver transformation failed: {e}")
            return False
    
    else:
        logger.error(f"Transformer {TransformerClass.__name__} has no transform_partition or transform_all method")
        return False


def run_gold_loaders(data_type='market_bars'):
    """Run Gold Loaders for all partitions."""
    logger.info("=" * 70)
    logger.info(f"GOLD LAYER - Silver to PostgreSQL ({data_type})")
    logger.info("=" * 70)
    
    config = DATA_TYPE_CONFIG[data_type]
    
    # Find all Silver data
    silver_base = DATA_DIR / 'silver' / config['silver_dir']
    
    if not silver_base.exists():
        logger.warning(f"No Silver data found for {data_type}")
        return False
    
    # Get loader class dynamically
    LoaderClass = get_loader_class(data_type)
    loader = LoaderClass()
    
    # Check which loading method the loader uses
    if hasattr(loader, 'load_partition'):
        # Partition-based loading (market_bars)
        logger.info(f"Using partition-based loading for {data_type}")
        
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
            logger.warning(f"No Silver partitions found for {data_type}")
            return False
        
        logger.info(f"Found {len(partitions)} Silver partitions to load")
        
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
        
        # Get final count if method exists
        if hasattr(loader, 'get_record_count'):
            total_count = loader.get_record_count()
            logger.info(f"\nGold Load complete: {success_count} partitions loaded, {fail_count} failed")
            logger.info(f"Total records in database: {total_count}")
        else:
            logger.info(f"\nGold Load complete: {success_count} partitions loaded, {fail_count} failed")
        
        return fail_count == 0
    
    elif hasattr(loader, 'load_all'):
        # Bulk loading (macro_indicators, corporate_actions, sec_filings)
        logger.info(f"Using bulk loading for {data_type}")
        
        try:
            success = loader.load_all(mode='replace')
            
            if success:
                # Get record count if method exists
                if hasattr(loader, 'get_record_count'):
                    total_count = loader.get_record_count()
                    logger.info(f"Gold load complete: SUCCESS")
                    logger.info(f"Total records in database: {total_count}")
                else:
                    logger.info("Gold load complete: SUCCESS")
                return True
            else:
                logger.warning("Gold load complete: No data loaded")
                return False
        except Exception as e:
            logger.error(f"Gold load failed: {e}")
            return False
    
    else:
        logger.error(f"Loader {LoaderClass.__name__} has no load_partition or load_all method")
        return False


def run_full_pipeline(data_type='market_bars'):
    """Run full Bronze --> Silver --> Gold pipeline."""
    logger.info("=" * 70)
    logger.info(f"FULL PIPELINE ORCHESTRATION ({data_type})")
    logger.info("=" * 70)
    
    start_time = datetime.now()
    
    # Step 1: Bronze Writer
    logger.info("\n" + "=" * 70)
    logger.info("Step 1/3: Bronze Layer")
    logger.info("=" * 70)
    if not run_bronze_writer(data_type):
        logger.error("Bronze layer failed - stopping pipeline")
        return False
    
    # Step 2: Silver Transformers
    logger.info("\n" + "=" * 70)
    logger.info("Step 2/3: Silver Layer")
    logger.info("=" * 70)
    if not run_silver_transformers(data_type):
        logger.error("Silver layer failed - stopping pipeline")
        return False
    
    # Step 3: Gold Loaders
    logger.info("\n" + "=" * 70)
    logger.info("Step 3/3: Gold Layer")
    logger.info("=" * 70)
    if not run_gold_loaders(data_type):
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
    
    parser.add_argument(
        '--data-type',
        type=str,
        choices=['market_bars', 'corporate_actions', 'macro_indicators', 'sec_filings'],
        default='market_bars',
        help='Data type to process (default: market_bars)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    print(f"\nOrchestrating pipeline: {args.layer}")
    print(f"Data type: {args.data_type}")
    print("\n")
    
    try:
        if args.layer == 'bronze':
            success = run_bronze_writer(args.data_type)
        elif args.layer == 'silver':
            success = run_silver_transformers(args.data_type)
        elif args.layer == 'gold':
            success = run_gold_loaders(args.data_type)
        else:  # full
            success = run_full_pipeline(args.data_type)
        
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
