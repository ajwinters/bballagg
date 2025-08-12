#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from allintwo import connect_to_rds
import pandas as pd

def check_tables():
    conn = connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')
    
    print("=== NBA Tables Created ===")
    # List all NBA tables
    df = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'nba_%' ORDER BY table_name", conn)
    print(f"Total NBA tables: {len(df)}")
    for table in df['table_name']:
        print(f"- {table}")
    
    print("\n=== Failed Calls Table ===")
    try:
        df_failed = pd.read_sql("SELECT * FROM nba_failed_calls", conn)
        print(f"Failed calls records: {len(df_failed)}")
        if len(df_failed) > 0:
            print(df_failed.head())
        else:
            print("No failed calls recorded (which is good!)")
    except Exception as e:
        print(f"Error checking failed calls table: {e}")
    
    print("\n=== Sample Table Content ===")
    try:
        df_sample = pd.read_sql("SELECT game_id, COUNT(*) as records FROM nba_boxscoreadvancedv3_playerstats GROUP BY game_id LIMIT 5", conn)
        print(f"Sample from nba_boxscoreadvancedv3_playerstats:")
        print(df_sample)
    except Exception as e:
        print(f"Error checking sample table: {e}")
    
    conn.close()

if __name__ == "__main__":
    check_tables()
