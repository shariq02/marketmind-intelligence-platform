# ====================================================================
# Cold Bootstrap - Market Bars
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: scripts/bootstrap_market_bars.py
# Purpose: Fetch Q1 2026 historical OHLCV data for ticker universe
# ====================================================================
"""
Cold Bootstrap - Market Bars

Fetches historical daily OHLCV bars for Q1 2026 (Jan 1 - Mar 31).

Features:
- Checkpoint/resume capability
- Rate limiting (5 calls/min for Polygon free tier)
- Progress tracking
- Error recovery

Usage:
    # Full S&P 500
    python3.11 scripts/bootstrap_market_bars.py --tickers sp500
    
    # Test with 10 tickers
    python3.11 scripts/bootstrap_market_bars.py --tickers AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,JPM,V,WMT
    
    # Resume from checkpoint
    python3.11 scripts/bootstrap_market_bars.py --resume
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import time
import json
import argparse
from datetime import datetime, date
from typing import List

from config import get_logger, DATA_DIR
from code.bronze.connectors.polygon_connector import PolygonConnector
from code.bronze.producers.kafka_producer import KafkaProducer

logger = get_logger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

Q1_2026_START = '2026-01-01'
Q1_2026_END = '2026-03-31'

# S&P 500 top 20 tickers (for initial test)
SP500_TOP_20 = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'BRK.B', 'UNH', 'XOM',
    'JNJ', 'JPM', 'V', 'PG', 'MA',
    'HD', 'CVX', 'MRK', 'ABBV', 'LLY'
]

# Full S&P 500 (sample - you'd need complete list)
SP500_FULL = SP500_TOP_20  # TODO: Replace with full S&P 500 list

CHECKPOINT_FILE = DATA_DIR / 'checkpoints' / 'bootstrap_market_bars.json'

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
            'total_bars_fetched': 0,
            'last_updated': None
        }
    
    def save(self):
        """Save checkpoint to disk."""
        self.data['last_updated'] = datetime.now().isoformat()
        with open(self.filepath, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def mark_completed(self, ticker: str, bars_count: int):
        """Mark ticker as completed."""
        if ticker not in self.data['completed_tickers']:
            self.data['completed_tickers'].append(ticker)
        self.data['total_bars_fetched'] += bars_count
        self.save()
    
    def mark_failed(self, ticker: str):
        """Mark ticker as failed."""
        if ticker not in self.data['failed_tickers']:
            self.data['failed_tickers'].append(ticker)
        self.save()
    
    def is_completed(self, ticker: str) -> bool:
        """Check if ticker already completed."""
        return ticker in self.data['completed_tickers']
    
    def get_pending_tickers(self, all_tickers: List[str]) -> List[str]:
        """Get list of tickers not yet completed."""
        completed = set(self.data['completed_tickers'])
        return [t for t in all_tickers if t not in completed]


# ============================================================================
# BOOTSTRAP LOGIC
# ============================================================================

def bootstrap_market_bars(
    tickers: List[str],
    start_date: str,
    end_date: str,
    resume: bool = False
):
    """
    Bootstrap market bars for ticker universe.
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        resume: Resume from checkpoint if True
    """
    logger.info("=" * 70)
    logger.info("Cold Bootstrap - Market Bars")
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"Tickers: {len(tickers)}")
    logger.info("=" * 70)
    
    # Initialize checkpoint
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    
    if not resume:
        # Fresh start - reset checkpoint
        checkpoint.data = {
            'started_at': datetime.now().isoformat(),
            'completed_tickers': [],
            'failed_tickers': [],
            'total_bars_fetched': 0,
            'last_updated': None
        }
        checkpoint.save()
        logger.info("Fresh bootstrap - checkpoint reset")
    else:
        logger.info(f"Resuming from checkpoint: {len(checkpoint.data['completed_tickers'])} tickers already completed")
    
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
    total_bars = 0
    total_errors = 0
    
    try:
        for idx, ticker in enumerate(pending_tickers, 1):
            logger.info(f"\n[{idx}/{len(pending_tickers)}] Fetching {ticker}...")
            
            try:
                # Fetch daily bars
                bars = polygon.fetch_daily_bars(ticker, start_date, end_date)
                
                if not bars:
                    logger.warning(f"  No data returned for {ticker}")
                    checkpoint.mark_failed(ticker)
                    continue
                
                logger.info(f"  Fetched {len(bars)} bars for {ticker}")
                
                # Send to Kafka
                producer.send_market_bars_batch(bars)
                logger.info(f"  Sent {len(bars)} bars to Kafka")
                
                # Update checkpoint
                checkpoint.mark_completed(ticker, len(bars))
                total_bars += len(bars)
                
                # Progress update
                logger.info(f"  Progress: {idx}/{len(pending_tickers)} tickers, {total_bars} total bars")
                
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
        logger.info(f"  Total bars: {checkpoint.data['total_bars_fetched']}")
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
        description='Bootstrap Q1 2026 market bars'
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
        default=Q1_2026_START,
        help='Start date YYYY-MM-DD (default: 2026-01-01)'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        default=Q1_2026_END,
        help='End date YYYY-MM-DD (default: 2026-03-31)'
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
    
    print(f"\nBootstrapping {len(tickers)} tickers from {args.start_date} to {args.end_date}")
    print(f"Resume mode: {args.resume}")
    print("\nPress Ctrl+C to stop (progress will be saved)\n")
    
    try:
        bootstrap_market_bars(
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
