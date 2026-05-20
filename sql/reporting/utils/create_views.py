#!/usr/bin/env python3
"""
Create reporting views in PostgreSQL database.
Reads and executes sql/reporting/create_reporting_views.sql
"""

import psycopg2
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG

# Use config from config.py (reads from .env)
DB_CONFIG = DATABASE_CONFIG

def main():
    # Read SQL file
    sql_file = Path(__file__).parent.parent / 'create_reporting_views.sql'
    
    if not sql_file.exists():
        print(f"ERROR: SQL file not found at {sql_file}")
        return
    
    print(f"Reading SQL file: {sql_file}")
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    # Connect to database
    print(f"\nConnecting to database: {DB_CONFIG['database']} at {DB_CONFIG['host']}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Important for CREATE VIEW statements
        cur = conn.cursor()
        
        # Execute SQL file
        print("Executing SQL to create reporting views...")
        cur.execute(sql_content)
        
        # Verify views were created
        print("\n" + "=" * 70)
        print("VERIFYING VIEWS CREATED")
        print("=" * 70)
        
        cur.execute("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'gold' 
              AND table_name LIKE 'vw_%'
            ORDER BY table_name
        """)
        
        views = cur.fetchall()
        
        if views:
            print(f"\nSuccessfully created {len(views)} reporting views:\n")
            for i, (view_name,) in enumerate(views, 1):
                # Get row count for each view
                cur.execute(f"SELECT COUNT(*) FROM gold.{view_name}")
                row_count = cur.fetchone()[0]
                print(f"  {i:2d}. gold.{view_name:35s} ({row_count:,} rows)")
        else:
            print("WARNING: No views found with 'vw_' prefix in gold schema")
        
        print("\n" + "=" * 70)
        print("REPORTING VIEWS CREATION COMPLETE")
        print("=" * 70)
        
        cur.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"\nERROR: Database error occurred")
        print(f"Error code: {e.pgcode}")
        print(f"Error message: {e.pgerror}")
        return
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        return

if __name__ == '__main__':
    main()
