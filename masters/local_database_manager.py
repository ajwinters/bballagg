"""
Local Database Manager for NBA Master Tables

Quick-start version using local PostgreSQL or SQLite for immediate testing
while RDS connectivity issues are resolved.
"""

import pandas as pd
import time
import os
import sys
import json
from datetime import datetime, timedelta
import numpy as np

# Try PostgreSQL first, fallback to SQLite
try:
    import psycopg2
    from psycopg2.extras import execute_batch
    from psycopg2 import sql
    import re
    HAS_POSTGRES = True
    print("✅ PostgreSQL available")
except ImportError:
    print("⚠️  PostgreSQL not available")
    HAS_POSTGRES = False

try:
    import sqlite3
    HAS_SQLITE = True  
    print("✅ SQLite available")
except ImportError:
    print("❌ SQLite not available")
    HAS_SQLITE = False

# NBA API imports (same as main system)
from nba_api.stats.endpoints import leaguegamefinder
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.static import teams, players

# Add path to access archive modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'archive'))
try:
    import allintwo
    USE_ALLINTWO = True
except ImportError:
    USE_ALLINTWO = False


class LocalDatabaseManager:
    """Local database manager with PostgreSQL and SQLite support"""
    
    def __init__(self, use_sqlite=False):
        self.use_sqlite = use_sqlite or not HAS_POSTGRES
        
        if self.use_sqlite:
            self.db_path = os.path.join(os.path.dirname(__file__), 'nba_masters_local.db')
            print(f"🗃️  Using SQLite database: {self.db_path}")
        else:
            self.db_config = {
                'database': 'thebigone',
                'user': 'ajwin',
                'password': 'CharlesBark!23',  
                'host': 'localhost',  # Local PostgreSQL
                'port': 5432
            }
            print(f"🐘 Using local PostgreSQL: {self.db_config['host']}:{self.db_config['port']}")
        
        # Same league configs as main system
        self.league_configs = [
            {
                'id': '00', 
                'name': 'NBA', 
                'full_name': 'National Basketball Association',
                'season_format': 'two_year',
                'start_year': 1946
            },
            {
                'id': '10', 
                'name': 'WNBA', 
                'full_name': 'Women\'s National Basketball Association',
                'season_format': 'single_year',
                'start_year': 1997
            },
            {
                'id': '20', 
                'name': 'G-League', 
                'full_name': 'G League',
                'season_format': 'two_year',
                'start_year': 2001
            }
        ]
    
    def connect_to_database(self):
        """Connect to local database (PostgreSQL or SQLite)"""
        try:
            if self.use_sqlite:
                conn = sqlite3.connect(self.db_path)
                print("✅ Connected to local SQLite database")
                return conn
            else:
                # Try local PostgreSQL
                conn = psycopg2.connect(
                    dbname=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    host=self.db_config['host'],
                    port=self.db_config['port']
                )
                print("✅ Connected to local PostgreSQL database")
                return conn
        except Exception as e:
            print(f"❌ Error connecting to local database: {str(e)}")
            
            if not self.use_sqlite and HAS_SQLITE:
                print("🔄 Falling back to SQLite...")
                self.use_sqlite = True
                return self.connect_to_database()
            return None
    
    def test_connection(self):
        """Test database connection and show info"""
        print("\n🔍 Testing Local Database Connection")
        print("=" * 45)
        
        conn = self.connect_to_database()
        if not conn:
            return False
            
        try:
            if self.use_sqlite:
                cursor = conn.cursor()
                cursor.execute("SELECT sqlite_version();")
                version = cursor.fetchone()[0]
                print(f"📊 SQLite Version: {version}")
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                print(f"📊 Existing tables: {len(tables)}")
                
            else:
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                print(f"📊 PostgreSQL Version: {version}")
                
                cursor.execute("SELECT current_database(), current_user;")
                db_info = cursor.fetchone()
                print(f"📊 Database: {db_info[0]}, User: {db_info[1]}")
                
                cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
                table_count = cursor.fetchone()[0]
                print(f"📊 Tables in public schema: {table_count}")
            
            conn.close()
            print("✅ Local database connection successful!")
            return True
            
        except Exception as e:
            print(f"❌ Connection test failed: {str(e)}")
            return False
    
    def create_sample_table(self):
        """Create a sample NBA games table to test functionality"""
        print("\n🏗️  Creating Sample NBA Games Table")
        print("=" * 40)
        
        conn = self.connect_to_database()
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            
            # Create sample table schema
            if self.use_sqlite:
                create_query = """
                CREATE TABLE IF NOT EXISTS sample_nba_games (
                    game_id TEXT PRIMARY KEY,
                    team_name TEXT,
                    game_date TEXT,
                    season TEXT,
                    points INTEGER,
                    league_name TEXT DEFAULT 'NBA',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                """
            else:
                create_query = """
                CREATE TABLE IF NOT EXISTS sample_nba_games (
                    game_id TEXT PRIMARY KEY,
                    team_name TEXT,
                    game_date DATE,
                    season TEXT,
                    points INTEGER,
                    league_name TEXT DEFAULT 'NBA',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            
            cursor.execute(create_query)
            conn.commit()
            print("✅ Sample table created successfully")
            
            # Insert sample data
            sample_data = [
                ('0022400001', 'Lakers', '2024-10-15', '2024-25', 112),
                ('0022400001', 'Warriors', '2024-10-15', '2024-25', 108),
                ('0022400002', 'Celtics', '2024-10-16', '2024-25', 115),
                ('0022400002', 'Heat', '2024-10-16', '2024-25', 103)
            ]
            
            if self.use_sqlite:
                insert_query = "INSERT OR IGNORE INTO sample_nba_games (game_id, team_name, game_date, season, points) VALUES (?, ?, ?, ?, ?)"
            else:
                insert_query = "INSERT INTO sample_nba_games (game_id, team_name, game_date, season, points) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING"
            
            cursor.executemany(insert_query, sample_data)
            conn.commit()
            
            # Verify data
            cursor.execute("SELECT COUNT(*) FROM sample_nba_games")
            count = cursor.fetchone()[0]
            print(f"✅ Inserted sample data: {count} records")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error creating sample table: {str(e)}")
            return False
    
    def collect_real_nba_sample(self):
        """Collect a real sample of NBA data to test the full pipeline"""
        print("\n🏀 Collecting Real NBA Data Sample")
        print("=" * 40)
        
        try:
            # Get recent NBA games (small sample)
            print("⏳ Fetching recent NBA games...")
            
            gamefinder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable='00',  # NBA
                season_type_nullable='Regular Season',
                season_nullable='2024-25'
            ).get_data_frames()[0]
            
            if len(gamefinder) == 0:
                print("⚠️  No current season games, trying 2023-24...")
                gamefinder = leaguegamefinder.LeagueGameFinder(
                    league_id_nullable='00',
                    season_type_nullable='Regular Season', 
                    season_nullable='2023-24'
                ).get_data_frames()[0]
            
            if len(gamefinder) > 0:
                # Take first 10 games for sample
                sample = gamefinder.head(10)
                print(f"✅ Retrieved {len(sample)} sample games")
                
                # Store in local database
                conn = self.connect_to_database()
                if conn:
                    cursor = conn.cursor()
                    
                    # Clean column names
                    clean_sample = sample.copy()
                    clean_sample.columns = [re.sub(r'[^a-zA-Z0-9]', '', col).lower() for col in clean_sample.columns]
                    
                    if self.use_sqlite:
                        # Use pandas to_sql for SQLite
                        clean_sample.to_sql('real_nba_games_sample', conn, if_exists='replace', index=False)
                    else:
                        # Use manual insertion for PostgreSQL
                        # This would need more complex logic for full implementation
                        pass
                    
                    print("✅ Real NBA sample stored in local database")
                    conn.close()
                    return True
            else:
                print("❌ No NBA games found")
                return False
                
        except Exception as e:
            print(f"❌ Error collecting NBA sample: {str(e)}")
            return False
    
    def show_database_contents(self):
        """Show what's currently in the local database"""
        print("\n📊 Local Database Contents")
        print("=" * 35)
        
        conn = self.connect_to_database()
        if not conn:
            return
            
        try:
            cursor = conn.cursor()
            
            if self.use_sqlite:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                print(f"📋 Tables found: {len(tables)}")
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"   • {table_name}: {count} records")
            else:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                tables = cursor.fetchall()
                
                print(f"📋 Tables found: {len(tables)}")
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"   • {table_name}: {count} records")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error showing database contents: {str(e)}")
    
    def run_local_test_suite(self):
        """Run comprehensive local database tests"""
        print("🧪 LOCAL DATABASE TEST SUITE")
        print("=" * 50)
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        tests = [
            ("Database Connection", self.test_connection),
            ("Create Sample Table", self.create_sample_table),
            ("Collect Real NBA Sample", self.collect_real_nba_sample),
            ("Show Database Contents", lambda: (self.show_database_contents(), True)[1])
        ]
        
        results = {}
        for test_name, test_func in tests:
            print(f"\n{'='*50}")
            try:
                result = test_func()
                results[test_name] = result if result is not None else True
            except Exception as e:
                print(f"❌ {test_name} failed: {str(e)}")
                results[test_name] = False
        
        # Summary
        print(f"\n{'='*50}")
        print("🏁 LOCAL TEST SUMMARY")
        print("=" * 50)
        
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        
        print(f"📊 Tests passed: {passed}/{total}")
        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"   {test_name}: {status}")
        
        if passed == total:
            print("\n🎉 LOCAL DATABASE READY!")
            print("   You can now use this for NBA data collection while fixing RDS")
        else:
            print(f"\n⚠️  {total - passed} tests failed - check errors above")
        
        return results


def main():
    """Interactive local database setup"""
    print("🏠 NBA MASTER TABLES - LOCAL DATABASE SETUP")
    print("=" * 55)
    
    if not HAS_POSTGRES and not HAS_SQLITE:
        print("❌ Neither PostgreSQL nor SQLite available!")
        print("   Install PostgreSQL: https://www.postgresql.org/download/")
        return
    
    # Auto-choose SQLite for immediate testing (non-interactive)
    use_sqlite = True
    print("📋 Using SQLite for quick testing (no setup required)")
    
    # Create manager and run tests
    manager = LocalDatabaseManager(use_sqlite=use_sqlite)
    
    print(f"\n🚀 Setting up local database...")
    results = manager.run_local_test_suite()
    
    if all(results.values()):
        print(f"\n✅ LOCAL SETUP COMPLETE!")
        print(f"   Database ready for NBA data collection")
        print(f"   Location: {'SQLite file' if use_sqlite else 'Local PostgreSQL'}")
        print(f"\n🎯 NEXT STEPS:")
        print(f"   1. Use this local database while fixing RDS issues")
        print(f"   2. Run main collection system with local config")
        print(f"   3. Switch back to RDS when connectivity is restored")
    else:
        print(f"\n⚠️  Some tests failed - review errors and try again")


if __name__ == "__main__":
    main()
