#!/usr/bin/env python3
"""
Run sample analytics queries for portfolio screenshots.
Executes key queries from sql/reporting/sample_analytics_queries.sql
"""

import psycopg2
from tabulate import tabulate
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG

# Use config from config.py (reads from .env)
DB_CONFIG = DATABASE_CONFIG

def run_query(cur, title, query, description):
    """Run a query and display formatted results"""
    print("\n" + "=" * 80)
    print(f"QUERY: {title}")
    print("=" * 80)
    print(f"Description: {description}\n")
    
    try:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        if rows:
            print(tabulate(rows, headers=columns, tablefmt='grid'))
            print(f"\nRows returned: {len(rows)}")
        else:
            print("No rows returned")
    
    except Exception as e:
        print(f"ERROR: {str(e)}")

def main():
    print("=" * 80)
    print("MARKETMIND V1 - SAMPLE ANALYTICS QUERIES")
    print("=" * 80)
    print(f"Database: {DB_CONFIG['database']} at {DB_CONFIG['host']}\n")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # ====================================================================
        # QUERY 1: SYSTEM HEALTH OVERVIEW
        # ====================================================================
        query1 = """
        SELECT 
            'Total Tickers' AS metric,
            COUNT(DISTINCT ticker)::text AS value
        FROM gold.ohlcv_bars
        
        UNION ALL
        
        SELECT 
            'Total Records',
            COUNT(*)::text
        FROM gold.ohlcv_bars
        
        UNION ALL
        
        SELECT 
            'Date Range',
            MIN(date)::text || ' to ' || MAX(date)::text
        FROM gold.ohlcv_bars
        
        UNION ALL
        
        SELECT 
            'Avg Completeness',
            ROUND(AVG(completeness_pct), 2)::text || '%'
        FROM gold.vw_ticker_coverage
        
        UNION ALL
        
        SELECT 
            'Fresh Data Tickers',
            COUNT(*)::text
        FROM gold.vw_data_freshness
        WHERE freshness_status = 'FRESH'
        
        UNION ALL
        
        SELECT 
            'Corporate Actions',
            COUNT(*)::text
        FROM gold.corporate_actions
        
        UNION ALL
        
        SELECT 
            'SEC Filings',
            COUNT(*)::text
        FROM gold.sec_filings
        
        UNION ALL
        
        SELECT 
            'Macro Indicators',
            COUNT(DISTINCT indicator_name)::text
        FROM gold.macro_indicators;
        """
        
        run_query(
            cur, 
            "System Health Overview (multiple views)",
            query1,
            "Comprehensive system metrics for executive dashboard"
        )
        
        # ====================================================================
        # QUERY 2: DAILY PRICE MOVEMENTS
        # ====================================================================
        query2 = """
        SELECT 
            ticker,
            date,
            open,
            high,
            low,
            close,
            volume,
            daily_change_pct,
            direction
        FROM gold.vw_daily_price_summary
        ORDER BY date DESC, ticker
        LIMIT 15;
        """
        
        run_query(
            cur,
            "Daily Price Movements (vw_daily_price_summary)",
            query2,
            "Recent price movements with calculated metrics"
        )
        
        # ====================================================================
        # QUERY 3: TICKER COVERAGE REPORT
        # ====================================================================
        query3 = """
        SELECT 
            ticker,
            records_count,
            first_date,
            last_date,
            completeness_pct,
            coverage_rating
        FROM gold.vw_ticker_coverage
        ORDER BY completeness_pct DESC;
        """
        
        run_query(
            cur,
            "Ticker Coverage Report (vw_ticker_coverage)",
            query3,
            "Data completeness metrics by ticker"
        )
        
        # ====================================================================
        # QUERY 4: PRICE TRENDS WITH MOVING AVERAGES
        # ====================================================================
        query4 = """
        SELECT 
            ticker,
            date,
            close,
            ROUND(sma_5::numeric, 2) AS sma_5_day,
            ROUND(sma_20::numeric, 2) AS sma_20_day,
            CASE 
                WHEN sma_5 > sma_20 THEN 'BULLISH'
                WHEN sma_5 < sma_20 THEN 'BEARISH'
                ELSE 'NEUTRAL'
            END AS trend_signal
        FROM gold.vw_ticker_price_trends
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY ticker, date DESC
        LIMIT 20;
        """
        
        run_query(
            cur,
            "Price Trends with Moving Averages (vw_ticker_price_trends)",
            query4,
            "Technical analysis with SMA-based trend signals"
        )
        
        # ====================================================================
        # QUERY 5: DATA FRESHNESS CHECK
        # ====================================================================
        query5 = """
        SELECT 
            ticker,
            latest_data_date,
            total_records,
            trading_days_count,
            days_since_last_update,
            freshness_status
        FROM gold.vw_data_freshness
        ORDER BY freshness_status, latest_data_date DESC;
        """
        
        run_query(
            cur,
            "Data Freshness Check (vw_data_freshness)",
            query5,
            "SLA monitoring and data currency tracking"
        )
        
        print("\n" + "=" * 80)
        print("ALL QUERIES COMPLETED")
        print("=" * 80)
        print("\nNEXT STEPS:")
        print("1. Review query results above")
        print("2. Capture screenshots of key queries for portfolio")
        print("3. Recommended screenshots:")
        print("   - System Health Overview (comprehensive metrics)")
        print("   - Ticker Coverage Report (data completeness)")
        print("   - Price Trends with Moving Averages (technical analysis)")
        print("   - Data Freshness Check (operational monitoring)")
        
        cur.close()
        conn.close()
        
    except psycopg2.Error as e:
        print("\nERROR: Database error occurred")
        print(f"Error code: {e.pgcode}")
        print(f"Error message: {e.pgerror}")
    except Exception as e:
        print(f"\nERROR: {str(e)}")

if __name__ == '__main__':
    main()
