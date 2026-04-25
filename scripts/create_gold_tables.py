# ====================================================================
# SQL Schema Executor
# MarketMind Intelligence Platform V1
# Author: Sharique Mohammad
# Date: April 21, 2026
# ====================================================================
# FILE: scripts/create_gold_tables.py
# Purpose: Execute Gold schema DDL on PostgreSQL
# ====================================================================
"""
Create Gold Tables

Executes the Gold schema DDL to create tables in PostgreSQL.

Usage:
    python3.11 scripts/create_gold_tables.py
"""

import psycopg2
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_database_url, get_logger

logger = get_logger(__name__)


def create_gold_tables():
    """Execute Gold schema DDL."""
    
    # Read SQL file
    sql_file = Path(__file__).parent.parent / 'code' / 'gold' / 'schemas' / 'gold_tables.sql'
    
    if not sql_file.exists():
        logger.error(f"SQL file not found: {sql_file}")
        return False
    
    logger.info(f"Reading SQL from: {sql_file}")
    sql_script = sql_file.read_text()
    
    # Connect to database
    try:
        from sqlalchemy.engine.url import make_url
        
        # Parse connection URL properly
        db_url = get_database_url()
        url_obj = make_url(db_url)
        
        conn = psycopg2.connect(
            host=url_obj.host,
            port=url_obj.port,
            database=url_obj.database,
            user=url_obj.username,
            password=url_obj.password
        )
        
        logger.info("Connected to PostgreSQL")
        
        # Execute SQL
        cursor = conn.cursor()
        cursor.execute(sql_script)
        conn.commit()
        
        logger.info("Gold tables created successfully!")
        
        # Verify tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gold'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        logger.info(f"Created {len(tables)} tables:")
        for table in tables:
            logger.info(f"  - gold.{table[0]}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        return False


if __name__ == '__main__':
    print("Creating Gold layer tables in PostgreSQL...")
    success = create_gold_tables()
    
    if success:
        print("\nSuccess! Gold tables created.")
    else:
        print("\nFailed to create tables.")
        sys.exit(1)
