# ====================================================================
# Cold Bootstrap - Corporate Actions
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: scripts/bootstrap_corporate_actions.py
# Purpose: Fetch historical corporate actions for ticker universe
# ====================================================================
"""
Cold Bootstrap - Corporate Actions

Fetches historical splits and dividends for ticker universe.

Features:
- Checkpoint/resume capability
- Rate limiting (5 calls/min for Polygon free tier)
- Progress tracking
- Error recovery

Usage:
    # Full S&P 500
    python3.11 scripts/bootstrap_corporate_actions.py --tickers sp500
    
    # Test with 10 tickers
    python3.11 scripts/bootstrap_corporate_actions.py --tickers AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,JPM,V,WMT
    
    # Resume from checkpoint
    python3.11 scripts/bootstrap_corporate_actions.py --resume
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import time
import json
import argparse
from datetime import datetime

from config import get_logger, DATA_DIR
from code.bronze.connectors.polygon_connector import PolygonConnector
from code.bronze.producers.kafka_producer import KafkaProducer

logger = get_logger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Historical window for corporate actions (2020-2026)
HISTORICAL_START = '2020-01-01'
HISTORICAL_END = '2026-12-31'

# S&P 500 top 20 tickers
SP500_TOP_20 = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'BRK.B', 'UNH', 'XOM',
    'JNJ', 'JPM', 'V', 'PG', 'MA',
    'HD', 'CVX', 'MRK', 'ABBV', 'LLY'
]

SP500_FULL = SP500_TOP_20  # TODO: Replace with full S&P 500 list

CHECKPOINT_FILE = DATA_DIR / 'checkpoints' / 'bootstrap_corporate_actions.json'

# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================

class Checkpoint:
    """Manage bootstrap progress checkpoints."""
    
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()
    
    def _load(self) -> dict:
        """Load checkpoint from disk."""
        if self.filepath.exists():
            with open(self.filepath, 'r') as f:
                return json.load(f)
        return {
            'started_at': None,
            'completed_tickers': [],
            'failed_tickers': [],
            'total_splits': 0,
            'total_dividends': 0,
            'last_updated': None
        }
    
    def save(self):
        """Save checkpoint to disk."""
        self.data['last_updated'] = datetime.now().isoformat()
        with open(self.filepath, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def mark_completed(self, ticker: str, splits_count: int, dividends_count: int):
        """Mark ticker as completed."""
        if ticker not in self.data['completed_tickers']:
            self.data['completed_tickers'].append(ticker)
        self.data['total_splits'] += splits_count
        self.data['total_dividends'] += dividends_count
        self.save()
    
    def mark_failed(self, ticker: str):
        """Mark ticker as failed."""
        if ticker not in self.data['failed_tickers']:
            self.data['failed_tickers'].append(ticker)
        self.save()
    
    def is_completed(self, ticker: str) -> bool:
        """Check if ticker already completed."""
        return ticker in self.data['completed_tickers']
    
    def get_pending_tickers(self, all_tickers: list) -> list:
        """Get list of tickers not yet completed."""
        completed = set(self.data['completed_tickers'])
        return [t for t in all_tickers if t not in completed]


# ============================================================================
# BOOTSTRAP LOGIC
# ============================================================================

def bootstrap_corporate_actions(
    tickers: list,
    start_date: str,
    end_date: str,
    resume: bool = False
):
    """
    Bootstrap corporate actions for ticker universe.
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        resume: Resume from checkpoint if True
    """
    logger.info("=" * 70)
    logger.info("Cold Bootstrap - Corporate Actions")
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"Tickers: {len(tickers)}")
    logger.info("=" * 70)
    
    # Initialize checkpoint
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    
    if not resume:
        checkpoint.data = {
            'started_at': datetime.now().isoformat(),
            'completed_tickers': [],
            'failed_tickers': [],
            'total_splits': 0,
            'total_dividends': 0,
            'last_updated': None
        }
        checkpoint.save()
        logger.info("Fresh bootstrap - checkpoint reset")
    else:
        logger.info(
            f"Resuming from checkpoint: "
            f"{len(checkpoint.data['completed_tickers'])} tickers already completed"
        )
    
    # Get pending tickers
    pending_tickers = checkpoint.get_pending_tickers(tickers)
    
    if not pending_tickers:
        logger.info("All tickers already completed!")
        return
    
    logger.info(f"Pending tickers: {len(pending_tickers)}")
    
    # Initialize connectors
    polygon = PolygonConnector()
    producer = KafkaProducer()
    
    # Track stats
    start_time = datetime.now()
    total_errors = 0
    
    try:
        for idx, ticker in enumerate(pending_tickers, 1):
            logger.info(f"\n[{idx}/{len(pending_tickers)}] Fetching {ticker}...")
            
            splits_count = 0
            dividends_count = 0
            
            try:
                # Fetch splits
                logger.info(f"  Fetching splits for {ticker}...")
                splits = polygon.fetch_splits(ticker, start_date, end_date)
                splits_count = len(splits)
                
                if splits:
                    logger.info(f"  Found {splits_count} splits")
                    for split in splits:
                        producer.send_corporate_action(split)
                else:
                    logger.info(f"  No splits found")
                
                # Fetch dividends
                logger.info(f"  Fetching dividends for {ticker}...")
                dividends = polygon.fetch_dividends(ticker, start_date, end_date)
                dividends_count = len(dividends)
                
                if dividends:
                    logger.info(f"  Found {dividends_count} dividends")
                    for dividend in dividends:
                        producer.send_corporate_action(dividend)
                else:
                    logger.info(f"  No dividends found")
                
                # Flush producer
                producer.flush()
                
                # Update checkpoint
                checkpoint.mark_completed(ticker, splits_count, dividends_count)
                
                logger.info(
                    f"  Progress: {idx}/{len(pending_tickers)} tickers, "
                    f"{checkpoint.data['total_splits']} splits, "
                    f"{checkpoint.data['total_dividends']} dividends"
                )
                
            except Exception as e:
                logger.error(f"  Failed to fetch {ticker}: {e}")
                checkpoint.mark_failed(ticker)
                total_errors += 1
                continue
        
        # Final summary
        elapsed = (datetime.now() - start_time).seconds
        logger.info("\n" + "=" * 70)
        logger.info("Cold Bootstrap - Complete")
        logger.info(f"  Completed: {len(checkpoint.data['completed_tickers'])} tickers")
        logger.info(f"  Failed:    {len(checkpoint.data['failed_tickers'])} tickers")
        logger.info(f"  Total splits:    {checkpoint.data['total_splits']}")
        logger.info(f"  Total dividends: {checkpoint.data['total_dividends']}")
        logger.info(f"  Elapsed:   {elapsed // 60} minutes {elapsed % 60} seconds")
        logger.info("=" * 70)
        
        if checkpoint.data['failed_tickers']:
            logger.warning(f"Failed tickers: {checkpoint.data['failed_tickers']}")
    
    finally:
        producer.close()


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Bootstrap corporate actions'
    )
    
    parser.add_argument(
        '--tickers',
        type=str,
        default='sp500_top20',
        help='Ticker list: sp500_top20, sp500, or comma-separated (AAPL,MSFT,GOOGL)'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        default=HISTORICAL_START,
        help='Start date YYYY-MM-DD (default: 2020-01-01)'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        default=HISTORICAL_END,
        help='End date YYYY-MM-DD (default: 2026-12-31)'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from checkpoint'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Parse ticker list
    if args.tickers == 'sp500_top20':
        tickers = SP500_TOP_20
    elif args.tickers == 'sp500':
        tickers = SP500_FULL
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(',')]
    
    print(f"\nBootstrapping corporate actions for {len(tickers)} tickers")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Resume mode: {args.resume}")
    print("\nPress Ctrl+C to stop (progress will be saved)\n")
    
    try:
        bootstrap_corporate_actions(
            tickers=tickers,
            start_date=args.start_date,
            end_date=args.end_date,
            resume=args.resume
        )
    except KeyboardInterrupt:
        print("\n\nBootstrap interrupted by user. Progress saved to checkpoint.")
        print("Run with --resume flag to continue.")
        sys.exit(0)


if __name__ == '__main__':
    main()
