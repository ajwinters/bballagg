"""
NBA Master Tables Collection Test

This script tests the complete master table collection process:
1. Master Teams (all current NBA teams)
2. Master Seasons (all NBA seasons from 1946-47 to current)
3. Master Games (all regular season games across all seasons)
4. Master Players (all players across all seasons)

It validates data completeness and saves results locally for testing.
"""

import pandas as pd
import time
import os
import sys
from datetime import datetime
import numpy as np

# NBA API imports
from nba_api.stats.endpoints import leaguegamefinder
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.static import teams, players

# Add src directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    import allintwo
except ImportError:
    print("Warning: Could not import allintwo module")
    allintwo = None

class MasterTableCollector:
    """Collects and validates NBA master tables"""
    
    def __init__(self, data_dir='../data'):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.failed_seasons_games = []
        self.failed_seasons_players = []
        
    def generate_seasons(self, start_year=1946, end_year=2025):
        """Generate NBA seasons in proper format"""
        seasons = []
        for i in range(start_year, end_year):
            seasons.append(f"{i}-{str(i+1)[2:].zfill(2)}")
        return seasons[::-1]  # Most recent first for faster testing
    
    def create_master_teams(self):
        """Create master teams table"""
        print("\n=== CREATING MASTER TEAMS ===")
        teams_data = teams.get_teams()
        master_teams = pd.DataFrame(teams_data)
        
        # Save to CSV
        teams_path = os.path.join(self.data_dir, 'master_teams.csv')
        master_teams.to_csv(teams_path, index=False)
        
        print(f"✓ Retrieved {len(master_teams)} teams")
        print("Teams sample:")
        print(master_teams[['full_name', 'abbreviation', 'city']].head())
        
        return master_teams
    
    def create_master_seasons(self):
        """Create master seasons table"""
        print("\n=== CREATING MASTER SEASONS ===")
        seasons = self.generate_seasons()
        
        master_seasons = pd.DataFrame({
            'season': seasons,
            'start_year': [int(s.split('-')[0]) for s in seasons],
            'end_year': [int(s.split('-')[0]) + 1 for s in seasons],  # Much simpler - just add 1 to start year
            'created_date': pd.Timestamp.now()
        })
        
        # Save to CSV
        seasons_path = os.path.join(self.data_dir, 'master_seasons.csv')
        master_seasons.to_csv(seasons_path, index=False)
        
        print(f"✓ Created {len(master_seasons)} seasons ({seasons[0]} to {seasons[-1]})")
        
        return master_seasons, seasons
    
    def create_master_games(self, seasons, test_mode=True):
        """Create comprehensive master games table across all leagues and season types"""
        print("\n=== CREATING COMPREHENSIVE MASTER GAMES ===")
        
        # Define all possible league IDs and season types
        league_configs = [
            {'id': '00', 'name': 'NBA'},
            {'id': '01', 'name': 'ABA'},  # Historical ABA games
            {'id': '10', 'name': 'WNBA'},
            {'id': '20', 'name': 'G-League'}
        ]
        
        season_type_configs = [
            {'type': 'Regular Season', 'name': 'Regular'},
            {'type': 'Pre Season', 'name': 'Preseason'},
            {'type': 'Playoffs', 'name': 'Playoffs'},
            {'type': 'IST', 'name': 'InSeasonTournament'}  # In-Season Tournament (newer)
        ]
        
        # In test mode, only collect recent seasons and main NBA
        if test_mode:
            test_seasons = seasons[:3]  # Last 3 seasons for speed
            test_leagues = [league_configs[0]]  # NBA only
            test_season_types = season_type_configs[:3]  # Regular, Pre, Playoffs
            print(f"TEST MODE: Collecting {len(test_seasons)} recent seasons")
            print(f"  - Leagues: {[l['name'] for l in test_leagues]}")
            print(f"  - Season Types: {[s['name'] for s in test_season_types]}")
        else:
            test_seasons = seasons
            test_leagues = league_configs
            test_season_types = season_type_configs
            print(f"FULL MODE: Collecting ALL {len(test_seasons)} seasons")
            print(f"  - Leagues: {[l['name'] for l in test_leagues]}")
            print(f"  - Season Types: {[s['name'] for s in test_season_types]}")
        
        gamesl = []
        total_combinations = len(test_seasons) * len(test_leagues) * len(test_season_types)
        current_combo = 0
        
        for season in test_seasons:
            for league in test_leagues:
                for season_type in test_season_types:
                    current_combo += 1
                    try:
                        combo_name = f"{season} {league['name']} {season_type['name']}"
                        print(f"[{current_combo:3d}/{total_combinations}] {combo_name}")
                        
                        gamefinder = leaguegamefinder.LeagueGameFinder(
                            league_id_nullable=league['id'], 
                            season_type_nullable=season_type['type'],
                            season_nullable=season
                        ).get_data_frames()[0]
                        
                        if len(gamefinder) > 0:
                            # Add metadata for tracking
                            gamefinder['league_id'] = league['id']
                            gamefinder['league_name'] = league['name']
                            gamefinder['season_type'] = season_type['type']
                            gamefinder['season_type_name'] = season_type['name']
                            gamesl.append(gamefinder)
                            print(f"    ✓ Retrieved {len(gamefinder)} games")
                        else:
                            print(f"    ○ No games found")
                            
                        time.sleep(0.7)  # Rate limiting - slightly longer for comprehensive collection
                        
                    except Exception as e:
                        print(f"    ✗ Failed: {str(e)}")
                        self.failed_seasons_games.append(combo_name)
                        time.sleep(2)  # Longer wait after error
        
        # Combine all games
        if gamesl:
            gamehistory = pd.concat(gamesl, axis=0, ignore_index=True)
            gamehistory['GAME_DATE'] = pd.to_datetime(gamehistory['GAME_DATE'])
            
            # Save to CSV
            games_path = os.path.join(self.data_dir, 'master_games.csv')
            gamehistory.to_csv(games_path, index=False)
            
            print(f"✓ Total games collected: {len(gamehistory):,}")
            print(f"✓ Unique games: {gamehistory['GAME_ID'].nunique():,}")
            print(f"✓ Date range: {gamehistory['GAME_DATE'].min().strftime('%Y-%m-%d')} to {gamehistory['GAME_DATE'].max().strftime('%Y-%m-%d')}")
            
            # Show breakdown by league and season type
            print("\n📊 Games Summary by League:")
            league_summary = gamehistory.groupby(['league_name']).size().reset_index(name='games_count')
            for _, row in league_summary.iterrows():
                print(f"  {row['league_name']}: {row['games_count']:,} games")
            
            print("\n📊 Games Summary by Season Type:")
            season_type_summary = gamehistory.groupby(['season_type_name']).size().reset_index(name='games_count')
            for _, row in season_type_summary.iterrows():
                print(f"  {row['season_type_name']}: {row['games_count']:,} games")
            
            return gamehistory
        else:
            print("❌ No games data collected!")
            return None
    
    def create_master_players(self, seasons, test_mode=True):
        """Create master players table"""
        print("\n=== CREATING MASTER PLAYERS ===")
        
        # In test mode, only collect recent seasons for speed
        if test_mode:
            test_seasons = seasons[:5]  # Last 5 seasons
            print(f"TEST MODE: Collecting {len(test_seasons)} recent seasons: {test_seasons}")
        else:
            test_seasons = seasons
            print(f"FULL MODE: Collecting ALL {len(test_seasons)} seasons")
        
        playersl = []
        
        for i, season in enumerate(test_seasons):
            try:
                print(f"Fetching players for season {season} ({i+1}/{len(test_seasons)})")
                
                playerfinder = nbaapi.leaguedashplayerbiostats.LeagueDashPlayerBioStats(
                    season=season
                ).get_data_frames()[0]
                
                if len(playerfinder) > 0:
                    playerfinder['season'] = season
                    playersl.append(playerfinder)
                    print(f"  ✓ Retrieved {len(playerfinder)} players")
                else:
                    print(f"  ⚠ No players found for {season}")
                    
                time.sleep(0.4)  # Slightly faster for players
                
            except Exception as e:
                print(f"  ✗ Failed to fetch players for {season}: {str(e)}")
                self.failed_seasons_players.append(season)
                time.sleep(2)  # Longer wait after error
        
        # Combine all players
        if playersl:
            allplayers = pd.concat(playersl, axis=0, ignore_index=True)
            
            # Save to CSV
            players_path = os.path.join(self.data_dir, 'master_players.csv')
            allplayers.to_csv(players_path, index=False)
            
            print(f"✓ Total player records collected: {len(allplayers):,}")
            print(f"✓ Unique players: {allplayers['PLAYER_ID'].nunique():,}")
            print(f"✓ Seasons covered: {allplayers['season'].nunique()}")
            
            return allplayers
        else:
            print("❌ No players data collected!")
            return None
    
    def validate_master_tables(self):
        """Validate all master tables"""
        print("\n=== VALIDATING MASTER TABLES ===")
        
        tables = {
            'master_teams.csv': ['id', 'full_name', 'abbreviation'],
            'master_seasons.csv': ['season', 'start_year', 'end_year'],
            'master_games.csv': ['GAME_ID', 'GAME_DATE', 'HOME_TEAM_ID'],
            'master_players.csv': ['PLAYER_ID', 'PLAYER_NAME', 'season']
        }
        
        validation_results = {}
        
        for filename, expected_cols in tables.items():
            filepath = os.path.join(self.data_dir, filename)
            
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    missing_cols = [col for col in expected_cols if col not in df.columns]
                    
                    validation_results[filename] = {
                        'exists': True,
                        'rows': len(df),
                        'columns': len(df.columns),
                        'missing_columns': missing_cols,
                        'valid': len(missing_cols) == 0
                    }
                    
                    status = "✅" if len(missing_cols) == 0 else "⚠"
                    print(f"{status} {filename}: {len(df):,} rows, {len(df.columns)} columns")
                    if missing_cols:
                        print(f"   Missing columns: {missing_cols}")
                    
                except Exception as e:
                    validation_results[filename] = {
                        'exists': True,
                        'error': str(e),
                        'valid': False
                    }
                    print(f"❌ {filename}: Error reading file - {str(e)}")
            else:
                validation_results[filename] = {
                    'exists': False,
                    'valid': False
                }
                print(f"❌ {filename}: File not found")
        
        return validation_results
    
    def run_full_collection(self, test_mode=True):
        """Run complete master table collection"""
        print("🚀 STARTING NBA MASTER TABLES COLLECTION")
        print("=" * 50)
        
        start_time = time.time()
        
        # Step 1: Create teams
        master_teams = self.create_master_teams()
        
        # Step 2: Create seasons
        master_seasons, seasons = self.create_master_seasons()
        
        # Step 3: Create games
        master_games = self.create_master_games(seasons, test_mode)
        
        # Step 4: Create players
        master_players = self.create_master_players(seasons, test_mode)
        
        # Step 5: Validate
        validation_results = self.validate_master_tables()
        
        # Summary
        elapsed_time = time.time() - start_time
        print("\n" + "=" * 50)
        print("🏁 COLLECTION COMPLETE!")
        print(f"⏱ Total time: {elapsed_time:.1f} seconds")
        
        if self.failed_seasons_games:
            print(f"⚠ Failed game seasons: {len(self.failed_seasons_games)}")
        if self.failed_seasons_players:
            print(f"⚠ Failed player seasons: {len(self.failed_seasons_players)}")
        
        # Show summary
        all_valid = all(v.get('valid', False) for v in validation_results.values())
        status = "✅ ALL TABLES VALID" if all_valid else "⚠ SOME ISSUES FOUND"
        print(f"\n{status}")
        
        return {
            'teams': master_teams,
            'seasons': master_seasons,
            'games': master_games,
            'players': master_players,
            'validation': validation_results
        }


def main():
    """Main execution"""
    collector = MasterTableCollector()
    
    print("Choose collection mode:")
    print("1. Test mode (last 5 seasons only - faster)")
    print("2. Full mode (all seasons from 1946 - slower)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    test_mode = choice != '2'
    
    if test_mode:
        print("Running in TEST MODE - collecting recent seasons only")
    else:
        print("Running in FULL MODE - collecting ALL historical data")
        print("This will take a long time due to API rate limits!")
        
    results = collector.run_full_collection(test_mode=test_mode)
    
    print("\n📊 FINAL SUMMARY:")
    for table_type, data in results.items():
        if table_type == 'validation':
            continue
        if data is not None:
            print(f"  {table_type.upper()}: {len(data):,} records")
        else:
            print(f"  {table_type.upper()}: FAILED")


if __name__ == "__main__":
    main()
