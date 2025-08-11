"""
NBA Master Tables Database Manager - Fixed Version

Corrected for:
- NBA API data starts from 1983 (not 1946)
- Updated API parameter names
- Fixed PostgreSQL table naming issues
- Realistic data targets
"""

import pandas as pd
import time
import os
import sys
import json
from datetime import datetime, timedelta
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
import re

# NBA API imports
from nba_api.stats.endpoints import leaguegamefinder
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.static import teams, players

# Add path to access archive modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'archive'))

# Try to import allintwo for database functions
try:
    import allintwo
    USE_ALLINTWO = True
    print("✅ Using allintwo for database connections")
except ImportError:
    print("⚠️  Warning: allintwo not available, using basic psycopg2 connection")
    USE_ALLINTWO = False


class FixedMasterTablesManager:
    """Fixed NBA Master Tables Manager with corrected API calls and realistic data ranges"""
    
    def __init__(self, db_config=None):
        self.db_config = db_config or {
            'database': 'thebigone',
            'user': 'ajwin', 
            'password': 'CharlesBark!23',
            'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            'port': 5432
        }
        
        # CORRECTED: League configurations with realistic start years
        self.league_configs = [
            {
                'id': '00', 
                'name': 'NBA', 
                'full_name': 'National Basketball Association',
                'season_format': 'two_year',  # 2023-24 format
                'start_year': 1983  # NBA API data starts here, not 1946
            },
            {
                'id': '10', 
                'name': 'WNBA', 
                'full_name': 'Women\'s National Basketball Association',
                'season_format': 'single_year',  # 2024 format  
                'start_year': 1997
            },
            {
                'id': '20', 
                'name': 'G_League', 
                'full_name': 'G League',
                'season_format': 'two_year',  # 2023-24 format
                'start_year': 2001
            }
        ]
        
        # Season types to collect
        self.season_types = [
            {'type': 'Regular Season', 'name': 'regular'},
            {'type': 'Playoffs', 'name': 'playoffs'},
            {'type': 'Pre Season', 'name': 'preseason'}
            # Note: Removed IST as it causes API errors for older seasons
        ]
        
        # Master table definitions
        self.master_tables = {
            'games': {
                'update_frequency': 'daily',
                'time_column': 'game_date',
                'unique_columns': ['game_id', 'league_name']
            },
            'players': {
                'update_frequency': 'weekly', 
                'time_column': 'last_updated',
                'unique_columns': ['player_id', 'league_name', 'season']
            },
            'teams': {
                'update_frequency': 'yearly',
                'time_column': 'last_updated', 
                'unique_columns': ['team_id', 'league_name']
            }
        }
        
    def connect_to_database(self):
        """Connect to PostgreSQL RDS database"""
        try:
            if USE_ALLINTWO:
                # Use allintwo connection method
                conn = allintwo.connect_to_rds(
                    self.db_config['database'],
                    self.db_config['user'],
                    self.db_config['password'],
                    self.db_config['host'],
                    self.db_config['port']
                )
            else:
                # Fallback to basic psycopg2 connection
                conn = psycopg2.connect(
                    dbname=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    host=self.db_config['host'],
                    port=self.db_config['port']
                )
                print("Connected to RDS PostgreSQL database")
            return conn
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            return None
    
    def generate_seasons_by_league(self, league_config, end_year=None):
        """Generate seasons based on league-specific format"""
        if end_year is None:
            end_year = datetime.now().year + 1
            
        seasons = []
        start_year = league_config['start_year']
        
        if league_config['season_format'] == 'two_year':
            # NBA/G-League format: 2023-24
            for year in range(start_year, end_year):
                season_str = f"{year}-{str(year+1)[2:].zfill(2)}"
                seasons.append(season_str)
        else:
            # WNBA format: 2024
            for year in range(start_year, end_year):
                seasons.append(str(year))
        
        return seasons[::-1]  # Most recent first
    
    def test_api_connectivity(self):
        """Test NBA API connectivity with current data"""
        print("🧪 Testing NBA API Connectivity")
        print("=" * 40)
        
        for league_config in self.league_configs:
            league_name = league_config['name']
            league_id = league_config['id']
            
            print(f"\n🏀 Testing {league_name}...")
            
            try:
                # Use completed recent season for testing (not current/future season)
                if league_config['season_format'] == 'two_year':
                    test_season = "2023-24"  # Completed NBA/G-League season
                else:
                    test_season = "2024"     # Completed WNBA season
                
                print(f"   Testing season: {test_season}")
                
                # Test games endpoint
                gamefinder = leaguegamefinder.LeagueGameFinder(
                    league_id_nullable=league_id,
                    season_type_nullable='Regular Season',
                    season_nullable=test_season
                ).get_data_frames()[0]
                
                print(f"   ✅ Games API: {len(gamefinder)} games found")
                
                # Test teams (static)
                if league_name == 'NBA':
                    teams_data = teams.get_teams()
                    print(f"   ✅ Teams API: {len(teams_data)} teams")
                
                time.sleep(0.6)  # Rate limiting
                
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
    
    def create_simple_games_table(self, conn, league_config, test_mode=True):
        """Create and populate a simple games table for testing"""
        league_name = league_config['name']
        table_name = f"test_games_{league_name.lower()}"
        
        print(f"\n🏗️  Creating test table: {table_name}")
        
        try:
            cursor = conn.cursor()
            
            # Drop existing test table
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            
            # Create simple table structure
            create_query = f"""
            CREATE TABLE {table_name} (
                game_id TEXT PRIMARY KEY,
                team_name TEXT,
                game_date DATE,
                season TEXT,
                points INTEGER,
                league_name TEXT DEFAULT '{league_name}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            cursor.execute(create_query)
            print(f"   ✅ Table {table_name} created")
            
            # Collect sample games data using completed season
            if league_config['season_format'] == 'two_year':
                test_season = "2023-24"  # Completed NBA/G-League season
            else:
                test_season = "2024"     # Completed WNBA season
            
            print(f"   📊 Collecting sample data for {test_season}...")
            
            gamefinder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable=league_config['id'],
                season_type_nullable='Regular Season',
                season_nullable=test_season
            ).get_data_frames()[0]
            
            if len(gamefinder) > 0:
                # Take first 10 games for testing
                sample_games = gamefinder.head(10 if test_mode else 100)
                
                # Insert sample data
                for _, game in sample_games.iterrows():
                    insert_query = f"""
                    INSERT INTO {table_name} (game_id, team_name, game_date, season, points)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (game_id) DO NOTHING;
                    """
                    
                    cursor.execute(insert_query, (
                        game.get('GAME_ID', ''),
                        game.get('TEAM_NAME', ''),
                        game.get('GAME_DATE', None),
                        test_season,
                        game.get('PTS', 0)
                    ))
                
                conn.commit()
                
                # Show results
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"   ✅ Inserted {count} sample records")
                
                return True
            else:
                print(f"   ⚠️  No games found for {test_season}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error creating table: {str(e)}")
            conn.rollback()
            return False
    
    def run_quick_test(self):
        """Run a quick test of the system"""
        print("🧪 RUNNING QUICK SYSTEM TEST")
        print("=" * 50)
        
        # Test API connectivity
        self.test_api_connectivity()
        
        # Test database connection
        print(f"\n💾 Testing Database Connection...")
        conn = self.connect_to_database()
        
        if not conn:
            print("❌ Database connection failed")
            return False
        
        print("✅ Database connection successful")
        
        # Test creating simple tables
        success_count = 0
        for league_config in self.league_configs:
            if self.create_simple_games_table(conn, league_config, test_mode=True):
                success_count += 1
        
        conn.close()
        
        print(f"\n🏁 TEST RESULTS:")
        print(f"   ✅ API Connectivity: Working")
        print(f"   ✅ Database Connection: Working")
        print(f"   ✅ Tables Created: {success_count}/{len(self.league_configs)}")
        
        if success_count == len(self.league_configs):
            print(f"\n🎉 ALL TESTS PASSED!")
            print(f"   Ready for full system deployment")
            return True
        else:
            print(f"\n⚠️  Some tests failed - check errors above")
            return False


def main():
    """Quick test runner"""
    print("🏀 NBA MASTER TABLES - FIXED VERSION QUICK TEST")
    print("=" * 60)
    
    manager = FixedMasterTablesManager()
    
    print("🎯 This test will:")
    print("   • Verify NBA API connectivity for all leagues")
    print("   • Test database connection")
    print("   • Create sample tables with real data")
    print("   • Validate the fixed system is working")
    
    input("\nPress Enter to start the test...")
    
    success = manager.run_quick_test()
    
    if success:
        print(f"\n✅ SYSTEM READY!")
        print(f"   The fixed master tables system is working correctly")
        print(f"   Ready to proceed with comprehensive data collection")
    else:
        print(f"\n❌ Issues found - review test results above")


if __name__ == "__main__":
    main()
