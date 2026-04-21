# ====================================================================
# Cold Bootstrap - SEC Filings
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: scripts/bootstrap_sec_filings.py
# Purpose: Fetch SEC filing metadata for ticker universe
# ====================================================================
"""
Cold Bootstrap - SEC Filings

Fetches SEC filing metadata (10-K, 10-Q, 8-K) for ticker universe.

Features:
- Checkpoint/resume capability
- Rate limiting (10 calls/min for EdgarTools)
- Progress tracking
- Error recovery

Usage:
    # Full S&P 500
    python3.11 scripts/bootstrap_sec_filings.py --tickers sp500
    
    # Test with 10 tickers
    python3.11 scripts/bootstrap_sec_filings.py --tickers AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,JPM,V,WMT
    
    # Resume from checkpoint
    python3.11 scripts/bootstrap_sec_filings.py --resume
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import argparse
from datetime import datetime

from config import get_logger, DATA_DIR
from code.bronze.connectors.edgartools_connector import EdgarToolsConnector
from code.bronze.producers.kafka_producer import KafkaProducer

logger = get_logger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Filing types to fetch
FILING_TYPES = [
    ('10-K', 3),   # Annual reports - last 3 years
    ('10-Q', 12),  # Quarterly reports - last 12 quarters
    ('8-K', 20),   # Current reports - last 20 filings
]

# S&P 500 top 20 tickers
SP500_TOP_20 = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'BRK.B', 'UNH', 'XOM',
    'JNJ', 'JPM', 'V', 'PG', 'MA',
    'HD', 'CVX', 'MRK', 'ABBV', 'LLY'
]

SP500_FULL = SP500_TOP_20  # TODO: Replace with full S&P 500 list

CHECKPOINT_FILE = DATA_DIR / 'checkpoints' / 'bootstrap_sec_filings.json'

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
            'total_filings': 0,
            'filings_by_type': {},
            'last_updated': None
        }
    
    def save(self):
        """Save checkpoint to disk."""
        self.data['last_updated'] = datetime.now().isoformat()
        with open(self.filepath, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def mark_completed(self, ticker: str, filings_count: dict):
        """Mark ticker as completed."""
        if ticker not in self.data['completed_tickers']:
            self.data['completed_tickers'].append(ticker)
        
        for form_type, count in filings_count.items():
            self.data['total_filings'] += count
            if form_type not in self.data['filings_by_type']:
                self.data['filings_by_type'][form_type] = 0
            self.data['filings_by_type'][form_type] += count
        
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

def bootstrap_sec_filings(
    tickers: list,
    resume: bool = False
):
    """
    Bootstrap SEC filings for ticker universe.
    
    Args:
        tickers: List of ticker symbols
        resume: Resume from checkpoint if True
    """
    logger.info("=" * 70)
    logger.info("Cold Bootstrap - SEC Filings")
    logger.info(f"Tickers: {len(tickers)}")
    logger.info(f"Filing types: {[ft[0] for ft in FILING_TYPES]}")
    logger.info("=" * 70)
    
    # Initialize checkpoint
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    
    if not resume:
        checkpoint.data = {
            'started_at': datetime.now().isoformat(),
            'completed_tickers': [],
            'failed_tickers': [],
            'total_filings': 0,
            'filings_by_type': {},
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
    edgar = EdgarToolsConnector()
    producer = KafkaProducer()
    
    # Track stats
    start_time = datetime.now()
    total_errors = 0
    
    try:
        for idx, ticker in enumerate(pending_tickers, 1):
            logger.info(f"\n[{idx}/{len(pending_tickers)}] Fetching {ticker}...")
            
            filings_count = {}
            
            try:
                # Fetch each filing type
                for form_type, limit in FILING_TYPES:
                    logger.info(f"  Fetching {form_type} filings...")
                    
                    filings = edgar.fetch_filings(ticker, form_type, limit)
                    
                    if filings:
                        logger.info(f"  Found {len(filings)} {form_type} filings")
                        for filing in filings:
                            producer.send_filing_metadata(filing)
                        filings_count[form_type] = len(filings)
                    else:
                        logger.info(f"  No {form_type} filings found")
                        filings_count[form_type] = 0
                
                # Flush producer
                producer.flush()
                
                # Update checkpoint
                checkpoint.mark_completed(ticker, filings_count)
                
                total_filings = sum(filings_count.values())
                logger.info(
                    f"  Total filings for {ticker}: {total_filings}"
                )
                logger.info(
                    f"  Progress: {idx}/{len(pending_tickers)} tickers, "
                    f"{checkpoint.data['total_filings']} total filings"
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
        logger.info(f"  Total filings: {checkpoint.data['total_filings']}")
        logger.info(f"  By type: {checkpoint.data['filings_by_type']}")
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
        description='Bootstrap SEC filings'
    )
    
    parser.add_argument(
        '--tickers',
        type=str,
        default='sp500_top20',
        help='Ticker list: sp500_top20, sp500, or comma-separated (AAPL,MSFT,GOOGL)'
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
    
    print(f"\nBootstrapping SEC filings for {len(tickers)} tickers")
    print(f"Resume mode: {args.resume}")
    print("\nPress Ctrl+C to stop (progress will be saved)\n")
    
    try:
        bootstrap_sec_filings(
            tickers=tickers,
            resume=args.resume
        )
    except KeyboardInterrupt:
        print("\n\nBootstrap interrupted by user. Progress saved to checkpoint.")
        print("Run with --resume flag to continue.")
        sys.exit(0)


if __name__ == '__main__':
    main()
