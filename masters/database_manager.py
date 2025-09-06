"""
NBA Master Tables Database Manager

This system creates and maintains master tables in PostgreSQL RDS for all leagues:
- NBA, WNBA, G-League
- Games (daily updates), Players (weekly updates), Teams (yearly updates)
- Incremental updates with timestamp tracking
- 9 total master tables

Based on league_separated_collection.py with RDS integration.
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
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'archive'))

# Try to import allintwo for database functions
try:
    import allintwo
    USE_ALLINTWO = True
    print("✅ Using allintwo for database connections")
except ImportError:
    print("⚠️  Warning: allintwo not available, using basic psycopg2 connection")
    USE_ALLINTWO = False


class MasterTablesManager:
    """Manages NBA master tables in PostgreSQL RDS with incremental updates"""
    
    def __init__(self, db_config=None):
        self.db_config = db_config or {
            'database': 'thebigone',
            'user': 'ajwin', 
            'password': 'CharlesBark!23',
            'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            'port': 5432
        }
        
        # League configurations with proper season formatting and realistic start years
        self.league_configs = [
            {
                'id': '00', 
                'name': 'NBA', 
                'full_name': 'National Basketball Association',
                'season_format': 'two_year',  # 2023-24 format
                'start_year': 1983  # NBA API data availability starts here
            },
            {
                'id': '10', 
                'name': 'WNBA', 
                'full_name': 'Women\'s National Basketball Association',
                'season_format': 'single_year',  # 2024 format  
                'start_year': 1997  # WNBA founded in 1997
            },
            {
                'id': '20', 
                'name': 'G-League', 
                'full_name': 'G League',
                'season_format': 'two_year',  # 2023-24 format
                'start_year': 2001  # G-League data available from 2001
            }
        ]
        
        # Season types to collect
        self.season_types = [
            {'type': 'Regular Season', 'name': 'regular'},
            {'type': 'Playoffs', 'name': 'playoffs'},
            {'type': 'Pre Season', 'name': 'preseason'},
            {'type': 'IST', 'name': 'in_season_tournament'}
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
    
    def clean_column_names(self, df):
        """Clean column names for PostgreSQL compatibility"""
        df.columns = [re.sub(r'[^a-zA-Z0-9]', '', col).lower() for col in df.columns]
        return df
    
    def map_dtype_to_postgresql(self, dtype):
        """Map pandas dtypes to PostgreSQL types"""
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
    
    def drop_table(self, conn, table_name):
        """Drop a table if it exists"""
        try:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            conn.commit()
            print(f"✓ Table {table_name} dropped successfully")
            return True
        except Exception as e:
            print(f"❌ Error dropping table {table_name}: {str(e)}")
            conn.rollback()
            return False
    
    def create_players_table(self, conn, table_name, league_name='NBA'):
        """Create a properly structured players table for the collection system"""
        try:
            cursor = conn.cursor()
            
            create_table_sql = f"""
                CREATE TABLE {table_name} (
                    playerid VARCHAR(50) PRIMARY KEY,
                    playername VARCHAR(200) NOT NULL,
                    firstname VARCHAR(100),
                    lastname VARCHAR(100),
                    birthdate VARCHAR(50),
                    college VARCHAR(200),
                    country VARCHAR(100),
                    height VARCHAR(20),
                    weight VARCHAR(20),
                    position VARCHAR(20),
                    draftyear INTEGER,
                    draftround INTEGER,
                    draftnumber INTEGER,
                    isactive BOOLEAN DEFAULT TRUE,
                    league VARCHAR(20) DEFAULT '{league_name}',
                    createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            
            cursor.execute(create_table_sql)
            
            # Create indexes for performance
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_league ON {table_name} (league);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_active ON {table_name} (isactive);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (playername);")
            
            conn.commit()
            print(f"✓ Players table {table_name} created successfully with proper structure")
            return True
            
        except Exception as e:
            print(f"❌ Error creating players table {table_name}: {str(e)}")
            conn.rollback()
            return False
    
    def recreate_players_tables(self):
        """Drop and recreate all player tables with correct structure"""
        conn = self.connect_to_database()
        if not conn:
            print("❌ Could not connect to database")
            return False
        
        league_tables = [
            ('nba_players', 'NBA'),
            ('wnba_players', 'WNBA'), 
            ('gleague_players', 'G-League')
        ]
        
        success_count = 0
        
        try:
            for table_name, league_name in league_tables:
                print(f"\n🔄 Recreating {table_name} table...")
                
                # Drop existing table
                if self.drop_table(conn, table_name):
                    # Create new table with correct structure
                    if self.create_players_table(conn, table_name, league_name):
                        success_count += 1
                        print(f"✅ {table_name} recreated successfully")
                    else:
                        print(f"❌ Failed to create {table_name}")
                else:
                    print(f"❌ Failed to drop {table_name}")
            
            print(f"\n🎉 Successfully recreated {success_count}/{len(league_tables)} player tables!")
            return success_count == len(league_tables)
            
        except Exception as e:
            print(f"❌ Error during table recreation: {str(e)}")
            return False
        finally:
            conn.close()

    def create_master_table_schema(self, conn, table_name, sample_df, table_type):
        """Create master table with proper schema and indexes"""
        cursor = conn.cursor()
        
        # Clean the sample dataframe
        clean_df = self.clean_column_names(sample_df.copy())
        
        # Base columns from sample data
        columns = []
        for col, dtype in zip(clean_df.columns, clean_df.dtypes):
            pg_type = self.map_dtype_to_postgresql(dtype)
            columns.append(f"{col} {pg_type}")
        
        # Add standard tracking columns
        tracking_columns = [
            "league_name TEXT NOT NULL",
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP", 
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "data_source TEXT DEFAULT 'nba_api'",
            "collection_run_id TEXT"
        ]
        
        all_columns = columns + tracking_columns
        columns_sql = ', '.join(all_columns)
        
        # Create table
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        cursor.execute(create_query)
        
        # Create indexes for performance
        unique_cols = self.master_tables[table_type]['unique_columns']
        if unique_cols:
            index_cols = ', '.join(unique_cols)
            index_name = f"idx_{table_name}_unique"
            index_query = f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({index_cols});"
            cursor.execute(index_query)
        
        # Create time-based index for incremental updates
        time_col = self.master_tables[table_type]['time_column']
        if time_col in clean_df.columns:
            time_index_name = f"idx_{table_name}_time"
            time_index_query = f"CREATE INDEX IF NOT EXISTS {time_index_name} ON {table_name} ({time_col});"
            cursor.execute(time_index_query)
        
        conn.commit()
        print(f"✓ Table {table_name} created with indexes")
        
    def get_last_update_time(self, conn, table_name, table_type):
        """Get the last update timestamp for incremental updates"""
        try:
            cursor = conn.cursor()
            time_column = self.master_tables[table_type]['time_column']
            
            query = f"SELECT MAX({time_column}) FROM {table_name};"
            cursor.execute(query)
            result = cursor.fetchone()[0]
            
            if result:
                print(f"Last update for {table_name}: {result}")
                return result
            else:
                print(f"No previous data found for {table_name}")
                return None
                
        except Exception as e:
            print(f"Error getting last update time for {table_name}: {str(e)}")
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
    
    def collect_games_for_league(self, conn, league_config, last_update_date=None, test_mode=False):
        """Collect games for a specific league with incremental update support"""
        league_name = league_config['name']
        league_id = league_config['id']
        
        print(f"\\n🏀 Collecting {league_name} games...")
        
        # Generate seasons
        seasons = self.generate_seasons_by_league(league_config)
        if test_mode:
            seasons = seasons[:2]  # Only recent seasons for testing
            
        games_collected = []
        collection_run_id = datetime.now().isoformat()
        
        for season in seasons:
            for season_type_config in self.season_types:
                try:
                    season_type = season_type_config['type']
                    combo_name = f"{season} {league_name} {season_type}"
                    
                    print(f"  Fetching {combo_name}...", end=" ")
                    
                    # Get games from NBA API
                    gamefinder = leaguegamefinder.LeagueGameFinder(
                        league_id_nullable=league_id,
                        season_type_nullable=season_type,
                        season_nullable=season
                    ).get_data_frames()[0]
                    
                    if len(gamefinder) > 0:
                        # Filter by date if doing incremental update
                        if last_update_date:
                            gamefinder['GAME_DATE'] = pd.to_datetime(gamefinder['GAME_DATE'])
                            gamefinder = gamefinder[gamefinder['GAME_DATE'] > last_update_date]
                        
                        if len(gamefinder) > 0:
                            # Add metadata
                            gamefinder['league_name'] = league_name
                            gamefinder['season_type'] = season_type
                            gamefinder['collection_run_id'] = collection_run_id
                            
                            games_collected.append(gamefinder)
                            print(f"✓ {len(gamefinder)} games")
                        else:
                            print("○ No new games")
                    else:
                        print("○ No games")
                        
                    time.sleep(0.6)  # Rate limiting
                    
                except Exception as e:
                    print(f"✗ Error: {str(e)}")
                    time.sleep(2)
        
        # Combine and return results
        if games_collected:
            all_games = pd.concat(games_collected, ignore_index=True)
            print(f"✓ Total new {league_name} games: {len(all_games)}")
            return all_games
        else:
            print(f"○ No new {league_name} games to collect")
            return None
    
    def collect_players_for_league(self, conn, league_config, last_update_date=None, test_mode=False):
        """Collect players for a specific league"""
        league_name = league_config['name']
        league_id = league_config['id']
        
        print(f"\\n👥 Collecting {league_name} players...")
        
        # For players, we typically collect by season
        seasons = self.generate_seasons_by_league(league_config)
        if test_mode:
            seasons = seasons[:2]  # Only recent seasons for testing
            
        players_collected = []
        collection_run_id = datetime.now().isoformat()
        
        for season in seasons:
            try:
                print(f"  Fetching {league_name} players for {season}...", end=" ")
                
                # Use LeagueDashPlayerBioStats for comprehensive player data
                player_endpoint = nbaapi.leaguedashplayerbiostats.LeagueDashPlayerBioStats(
                    league_id_nullable=league_id,
                    season=season
                )
                players_df = player_endpoint.get_data_frames()[0]
                
                if len(players_df) > 0:
                    # Add metadata
                    players_df['league_name'] = league_name
                    players_df['season'] = season
                    players_df['collection_run_id'] = collection_run_id
                    players_df['last_updated'] = datetime.now()
                    
                    players_collected.append(players_df)
                    print(f"✓ {len(players_df)} players")
                else:
                    print("○ No players")
                    
                time.sleep(0.6)
                
            except Exception as e:
                print(f"✗ Error: {str(e)}")
                time.sleep(2)
        
        if players_collected:
            all_players = pd.concat(players_collected, ignore_index=True)
            print(f"✓ Total {league_name} players: {len(all_players)}")
            return all_players
        else:
            print(f"○ No {league_name} players collected")
            return None
    
    def collect_teams_for_league(self, conn, league_config):
        """Collect teams data (this is simpler since teams don't change often)"""
        league_name = league_config['name']
        
        print(f"\\n🏟️ Collecting {league_name} teams...")
        
        # Get teams from static data
        teams_data = teams.get_teams()
        teams_df = pd.DataFrame(teams_data)
        
        # Add metadata
        teams_df['league_name'] = league_name
        teams_df['last_updated'] = datetime.now()
        teams_df['collection_run_id'] = datetime.now().isoformat()
        
        print(f"✓ {len(teams_df)} teams collected")
        return teams_df
    
    def upsert_data_to_table(self, conn, df, table_name, table_type):
        """Insert or update data in master table"""
        if df is None or len(df) == 0:
            print(f"No data to upsert for {table_name}")
            return 0
            
        # Clean the dataframe
        clean_df = self.clean_column_names(df.copy())
        
        cursor = conn.cursor()
        
        # Get unique columns for conflict resolution
        unique_cols = self.master_tables[table_type]['unique_columns']
        unique_cols_clean = [re.sub(r'[^a-zA-Z0-9]', '', col).lower() for col in unique_cols]
        
        # Prepare columns and values
        columns = clean_df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        columns_sql = ', '.join(columns)
        
        # Create ON CONFLICT clause for upsert
        conflict_cols = ', '.join(unique_cols_clean)
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_cols_clean])
        
        upsert_query = f"""
            INSERT INTO {table_name} ({columns_sql}) 
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols}) 
            DO UPDATE SET {update_set}, updated_at = CURRENT_TIMESTAMP;
        """
        
        # Convert dataframe to tuples
        data_tuples = [tuple(row) for row in clean_df.to_numpy()]
        
        try:
            execute_batch(cursor, upsert_query, data_tuples)
            conn.commit()
            print(f"✓ Upserted {len(data_tuples)} records to {table_name}")
            return len(data_tuples)
            
        except Exception as e:
            conn.rollback()
            print(f"✗ Error upserting to {table_name}: {str(e)}")
            return 0
    
    def update_master_games(self, test_mode=False):
        """Update all league master games tables"""
        print("\\n" + "="*60)
        print("🎯 UPDATING MASTER GAMES TABLES (Daily Process)")
        print("="*60)
        
        conn = self.connect_to_database()
        if not conn:
            return False
            
        total_records = 0
        
        try:
            for league_config in self.league_configs:
                league_name = league_config['name']
                table_name = f"master_games_{league_name.lower()}"
                
                print(f"\\n📊 Processing {table_name}...")
                
                # Check if table exists and get last update
                last_update = self.get_last_update_time(conn, table_name, 'games')
                
                # Collect games data
                games_df = self.collect_games_for_league(conn, league_config, last_update, test_mode)
                
                if games_df is not None:
                    # Create table if needed (using first batch as schema sample)
                    self.create_master_table_schema(conn, table_name, games_df, 'games')
                    
                    # Upsert data
                    records_added = self.upsert_data_to_table(conn, games_df, table_name, 'games')
                    total_records += records_added
                
            print(f"\\n✅ Games update complete: {total_records} total records processed")
            return True
            
        except Exception as e:
            print(f"\\n❌ Games update failed: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def update_master_players(self, test_mode=False):
        """Update all league master players tables"""
        print("\\n" + "="*60)
        print("👥 UPDATING MASTER PLAYERS TABLES (Weekly Process)")
        print("="*60)
        
        conn = self.connect_to_database()
        if not conn:
            return False
            
        total_records = 0
        
        try:
            for league_config in self.league_configs:
                league_name = league_config['name']
                table_name = f"master_players_{league_name.lower()}"
                
                print(f"\\n📊 Processing {table_name}...")
                
                # Get last update time
                last_update = self.get_last_update_time(conn, table_name, 'players')
                
                # Collect players data
                players_df = self.collect_players_for_league(conn, league_config, last_update, test_mode)
                
                if players_df is not None:
                    # Create table if needed
                    self.create_master_table_schema(conn, table_name, players_df, 'players')
                    
                    # Upsert data
                    records_added = self.upsert_data_to_table(conn, players_df, table_name, 'players')
                    total_records += records_added
                
            print(f"\\n✅ Players update complete: {total_records} total records processed")
            return True
            
        except Exception as e:
            print(f"\\n❌ Players update failed: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def update_master_teams(self, test_mode=False):
        """Update all league master teams tables"""
        print("\\n" + "="*60)
        print("🏟️ UPDATING MASTER TEAMS TABLES (Yearly Process)")
        print("="*60)
        
        conn = self.connect_to_database()
        if not conn:
            return False
            
        total_records = 0
        
        try:
            for league_config in self.league_configs:
                league_name = league_config['name']
                table_name = f"master_teams_{league_name.lower()}"
                
                print(f"\\n📊 Processing {table_name}...")
                
                # Collect teams data
                teams_df = self.collect_teams_for_league(conn, league_config)
                
                if teams_df is not None:
                    # Create table if needed
                    self.create_master_table_schema(conn, table_name, teams_df, 'teams')
                    
                    # Upsert data
                    records_added = self.upsert_data_to_table(conn, teams_df, table_name, 'teams')
                    total_records += records_added
                
            print(f"\\n✅ Teams update complete: {total_records} total records processed")
            return True
            
        except Exception as e:
            print(f"\\n❌ Teams update failed: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def run_full_backfill(self, test_mode=True):
        """Run complete backfill of all master tables"""
        print("\\n" + "="*80)
        print("🚀 RUNNING COMPLETE NBA MASTER TABLES BACKFILL")
        print("="*80)
        
        start_time = time.time()
        
        if test_mode:
            print("🧪 TEST MODE: Limited data collection for validation")
        else:
            print("🏭 FULL MODE: Complete historical data collection")
            print("⚠️  This will take several hours due to API rate limits!")
        
        # Run all updates
        results = {
            'teams': self.update_master_teams(test_mode),
            'players': self.update_master_players(test_mode), 
            'games': self.update_master_games(test_mode)
        }
        
        # Summary
        elapsed_time = time.time() - start_time
        
        print("\\n" + "="*80)
        print("🏁 BACKFILL COMPLETE!")
        print(f"⏱️ Total time: {elapsed_time/60:.1f} minutes")
        
        print("\\n📊 RESULTS:")
        for process, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {process.upper()}: {status}")
        
        all_success = all(results.values())
        final_status = "🎉 ALL PROCESSES COMPLETED" if all_success else "⚠️ SOME PROCESSES FAILED"
        print(f"\\n{final_status}")
        
        return results
    
    def show_database_summary(self):
        """Show summary of master tables in database"""
        print("\\n" + "="*60)
        print("📋 DATABASE MASTER TABLES SUMMARY")
        print("="*60)
        
        conn = self.connect_to_database()
        if not conn:
            return
            
        try:
            cursor = conn.cursor()
            
            # Get all master tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'master_%'
                ORDER BY table_name;
            """)
            
            tables = cursor.fetchall()
            
            if not tables:
                print("No master tables found in database")
                return
            
            print(f"Found {len(tables)} master tables:\\n")
            
            for (table_name,) in tables:
                # Get row count and last update
                cursor.execute(f"SELECT COUNT(*), MAX(updated_at) FROM {table_name};")
                count, last_update = cursor.fetchone()
                
                print(f"📊 {table_name}:")
                print(f"   Records: {count:,}")
                print(f"   Last updated: {last_update or 'Never'}")
                print()
                
        except Exception as e:
            print(f"Error getting database summary: {str(e)}")
        finally:
            conn.close()


def main():
    """Main execution with interactive menu"""
    manager = MasterTablesManager()
    
    print("🏀 NBA MASTER TABLES DATABASE MANAGER")
    print("="*50)
    
    while True:
        print("\\nChoose an operation:")
        print("1. 🚀 Run full backfill (test mode)")
        print("2. 🏭 Run full backfill (production mode)")  
        print("3. 🎯 Update games only (daily)")
        print("4. 👥 Update players only (weekly)")
        print("5. 🏟️ Update teams only (yearly)")
        print("6. 📋 Show database summary")
        print("7. 🚪 Exit")
        
        choice = input("\\nEnter choice (1-7): ").strip()
        
        if choice == '1':
            manager.run_full_backfill(test_mode=True)
        elif choice == '2':
            confirm = input("This will take hours! Continue? (y/N): ").strip().lower()
            if confirm == 'y':
                manager.run_full_backfill(test_mode=False)
        elif choice == '3':
            manager.update_master_games(test_mode=False)
        elif choice == '4':
            manager.update_master_players(test_mode=False)
        elif choice == '5':
            manager.update_master_teams(test_mode=False)
        elif choice == '6':
            manager.show_database_summary()
        elif choice == '7':
            print("\\n👋 Goodbye!")
            break
        else:
            print("Invalid choice, please try again.")


if __name__ == "__main__":
    main()
