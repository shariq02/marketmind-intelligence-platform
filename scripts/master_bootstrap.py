# ====================================================================
# Master Pipeline Orchestration
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: scripts/master_bootstrap.py
# Purpose: Master orchestration - Bootstrap + Process all data types
# ====================================================================
"""
Master Pipeline Orchestration

Orchestrates complete data ingestion and processing:
1. Market Bars: Bootstrap --> Bronze --> Silver --> Gold
2. Corporate Actions: Bootstrap --> Bronze --> Silver --> Gold
3. Macro Indicators: Bootstrap --> Bronze --> Silver --> Gold
4. SEC Filings: Bootstrap --> Bronze --> Silver --> Gold

Features:
- Checkpoint/resume capability
- Pause/continue prompts
- Automatic pipeline execution after each bootstrap
- Error recovery

Usage:
    # Fresh run
    python3.11 scripts/master_bootstrap.py --tickers AAPL,MSFT,GOOGL
    
    # Resume from checkpoint
    python3.11 scripts/master_bootstrap.py --resume
    
    # Skip prompts (run everything automatically)
    python3.11 scripts/master_bootstrap.py --auto
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import argparse
import subprocess
from datetime import datetime

from config import get_logger, DATA_DIR

logger = get_logger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

CHECKPOINT_FILE = DATA_DIR / 'checkpoints' / 'master_bootstrap.json'

# Pipeline stages in order
STAGES = [
    {
        'name': 'market_bars',
        'bootstrap_script': 'scripts/bootstrap_market_bars.py',
        'bootstrap_args': ['--tickers', '{tickers}'],
        'description': 'Market Bars (OHLCV data)',
    },
    {
        'name': 'corporate_actions',
        'bootstrap_script': 'scripts/bootstrap_corporate_actions.py',
        'bootstrap_args': ['--tickers', '{tickers}'],
        'description': 'Corporate Actions (splits/dividends)',
    },
    {
        'name': 'macro_indicators',
        'bootstrap_script': 'scripts/bootstrap_macro_indicators.py',
        'bootstrap_args': [],
        'description': 'Macro Indicators (economic data)',
    },
    {
        'name': 'sec_filings',
        'bootstrap_script': 'scripts/bootstrap_sec_filings.py',
        'bootstrap_args': ['--tickers', '{tickers}'],
        'description': 'SEC Filings (10-K, 10-Q, 8-K)',
    },
]

# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================

class Checkpoint:
    """Manage master pipeline checkpoints."""
    
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
            'completed_stages': [],
            'failed_stages': [],
            'current_stage': None,
            'tickers': None,
            'last_updated': None
        }
    
    def save(self):
        """Save checkpoint to disk."""
        self.data['last_updated'] = datetime.now().isoformat()
        with open(self.filepath, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def mark_stage_started(self, stage_name: str):
        """Mark stage as started."""
        self.data['current_stage'] = stage_name
        self.save()
    
    def mark_stage_completed(self, stage_name: str):
        """Mark stage as completed."""
        if stage_name not in self.data['completed_stages']:
            self.data['completed_stages'].append(stage_name)
        self.data['current_stage'] = None
        self.save()
    
    def mark_stage_failed(self, stage_name: str):
        """Mark stage as failed."""
        if stage_name not in self.data['failed_stages']:
            self.data['failed_stages'].append(stage_name)
        self.data['current_stage'] = None
        self.save()
    
    def is_stage_completed(self, stage_name: str) -> bool:
        """Check if stage already completed."""
        return stage_name in self.data['completed_stages']
    
    def get_pending_stages(self) -> list:
        """Get list of stages not yet completed."""
        completed = set(self.data['completed_stages'])
        return [s for s in STAGES if s['name'] not in completed]


# ============================================================================
# PIPELINE EXECUTION
# ============================================================================

def run_bootstrap(stage: dict, tickers: str) -> bool:
    """
    Run bootstrap script for a stage.
    
    Args:
        stage: Stage configuration
        tickers: Ticker list
    
    Returns:
        True if successful, False otherwise
    """
    script = stage['bootstrap_script']
    args = [arg.format(tickers=tickers) for arg in stage['bootstrap_args']]
    
    logger.info(f"Running bootstrap: {script} {' '.join(args)}")
    
    try:
        cmd = ['python3.11', script] + args
        result = subprocess.run(cmd, check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        logger.error(f"Bootstrap failed with exit code {e.returncode}")
        return False
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        return False


def run_pipeline() -> bool:
    """
    Run full Bronze --> Silver --> Gold pipeline.
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("Running full pipeline orchestration...")
    
    try:
        cmd = ['python3.11', 'scripts/orchestrate_pipeline.py']
        result = subprocess.run(cmd, check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        logger.error(f"Pipeline failed with exit code {e.returncode}")
        return False
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


def prompt_continue(stage_name: str) -> bool:
    """
    Prompt user to continue to next stage.
    
    Args:
        stage_name: Name of next stage
    
    Returns:
        True to continue, False to stop
    """
    print(f"\n{'='*70}")
    print(f"Ready to proceed to: {stage_name}")
    print("Options:")
    print("  [c] Continue to next stage")
    print("  [s] Stop here (progress saved, resume with --resume)")
    print("  [q] Quit without saving")
    print("="*70)
    
    while True:
        choice = input("\nChoice [c/s/q]: ").lower().strip()
        if choice in ['c', 'continue', '']:
            return True
        elif choice in ['s', 'stop']:
            print("\nStopping. Run with --resume to continue from here.")
            return False
        elif choice in ['q', 'quit']:
            print("\nQuitting without saving progress.")
            sys.exit(0)
        else:
            print("Invalid choice. Please enter c, s, or q.")


# ============================================================================
# MASTER ORCHESTRATION
# ============================================================================

def run_master_bootstrap(
    tickers: str,
    resume: bool = False,
    auto: bool = False
):
    """
    Run master bootstrap and pipeline orchestration.
    
    Args:
        tickers: Ticker list (comma-separated)
        resume: Resume from checkpoint
        auto: Skip all prompts
    """
    logger.info("=" * 70)
    logger.info("MASTER BOOTSTRAP AND PIPELINE ORCHESTRATION")
    logger.info("=" * 70)
    
    # Initialize checkpoint
    checkpoint = Checkpoint(CHECKPOINT_FILE)
    
    if not resume:
        checkpoint.data = {
            'started_at': datetime.now().isoformat(),
            'completed_stages': [],
            'failed_stages': [],
            'current_stage': None,
            'tickers': tickers,
            'last_updated': None
        }
        checkpoint.save()
        logger.info("Fresh run - checkpoint reset")
    else:
        logger.info(
            f"Resuming from checkpoint: "
            f"{len(checkpoint.data['completed_stages'])} stages completed"
        )
        tickers = checkpoint.data.get('tickers', tickers)
    
    # Get pending stages
    pending_stages = checkpoint.get_pending_stages()
    
    if not pending_stages:
        logger.info("All stages already completed!")
        return
    
    logger.info(f"\nPending stages: {len(pending_stages)}")
    for stage in pending_stages:
        logger.info(f"  - {stage['name']}: {stage['description']}")
    
    print(f"\nTickers: {tickers}")
    print(f"Auto mode: {auto}")
    print()
    
    # Track overall stats
    start_time = datetime.now()
    
    # Execute each stage
    for idx, stage in enumerate(pending_stages, 1):
        stage_name = stage['name']
        stage_desc = stage['description']
        
        logger.info("\n" + "=" * 70)
        logger.info(f"STAGE {idx}/{len(pending_stages)}: {stage_desc}")
        logger.info("=" * 70)
        
        # Prompt to continue (unless auto mode)
        if not auto and idx > 1:
            if not prompt_continue(stage_desc):
                logger.info("Stopped by user")
                return
        
        # Mark stage as started
        checkpoint.mark_stage_started(stage_name)
        
        # Step 1: Run bootstrap
        logger.info(f"\nStep 1/2: Bootstrap {stage_name}...")
        bootstrap_success = run_bootstrap(stage, tickers)
        
        if not bootstrap_success:
            logger.error(f"Bootstrap failed for {stage_name}")
            checkpoint.mark_stage_failed(stage_name)
            
            if not auto:
                print(f"\n{stage_name} bootstrap failed. Continue to next stage? [y/n]: ", end='')
                choice = input().lower().strip()
                if choice != 'y':
                    return
            continue
        
        logger.info(f"Bootstrap complete for {stage_name}")
        
        # Step 2: Run pipeline
        logger.info("\nStep 2/2: Running pipeline orchestration...")
        pipeline_success = run_pipeline()
        
        if not pipeline_success:
            logger.error(f"Pipeline failed after {stage_name} bootstrap")
            checkpoint.mark_stage_failed(stage_name)
            
            if not auto:
                print("\nPipeline failed. Continue to next stage? [y/n]: ", end='')
                choice = input().lower().strip()
                if choice != 'y':
                    return
            continue
        
        logger.info(f"Pipeline complete for {stage_name}")
        
        # Mark stage as completed
        checkpoint.mark_stage_completed(stage_name)
        logger.info(f"Stage {stage_name} completed successfully")
    
    # Final summary
    elapsed = (datetime.now() - start_time).seconds
    logger.info("\n" + "=" * 70)
    logger.info("MASTER BOOTSTRAP COMPLETE")
    logger.info(f"  Completed: {len(checkpoint.data['completed_stages'])} stages")
    logger.info(f"  Failed:    {len(checkpoint.data['failed_stages'])} stages")
    logger.info(f"  Elapsed:   {elapsed // 60} minutes {elapsed % 60} seconds")
    logger.info("=" * 70)
    
    if checkpoint.data['failed_stages']:
        logger.warning(f"Failed stages: {checkpoint.data['failed_stages']}")


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Master bootstrap and pipeline orchestration'
    )
    
    parser.add_argument(
        '--tickers',
        type=str,
        default='AAPL,MSFT,GOOGL',
        help='Ticker list (comma-separated, default: AAPL,MSFT,GOOGL)'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from checkpoint'
    )
    
    parser.add_argument(
        '--auto',
        action='store_true',
        help='Auto mode - skip all prompts'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    print("\n" + "=" * 70)
    print("MASTER BOOTSTRAP AND PIPELINE ORCHESTRATION")
    print("=" * 70)
    print(f"\nTickers: {args.tickers}")
    print(f"Resume mode: {args.resume}")
    print(f"Auto mode: {args.auto}")
    print("\nThis will:")
    print("1. Bootstrap Market Bars --> Run Pipeline")
    print("2. Bootstrap Corporate Actions --> Run Pipeline")
    print("3. Bootstrap Macro Indicators --> Run Pipeline")
    print("4. Bootstrap SEC Filings --> Run Pipeline")
    print("\nPress Ctrl+C to stop (progress will be saved)")
    print("=" * 70)
    
    if not args.auto and not args.resume:
        input("\nPress Enter to begin...")
    
    try:
        run_master_bootstrap(
            tickers=args.tickers,
            resume=args.resume,
            auto=args.auto
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Progress saved to checkpoint.")
        print("Run with --resume flag to continue.")
        sys.exit(0)


if __name__ == '__main__':
    main()
