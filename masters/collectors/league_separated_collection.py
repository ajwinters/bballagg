"""
Updated NBA Master Tables Collection with League Separation

This enhanced script collects master tables and automatically separates them by league:
1. Creates comprehensive master tables (all leagues combined)
2. Automatically creates league-separated tables during collection
3. Ensures proper data formatting (string IDs, leading zeros, etc.)
4. Provides organized output structure

Key improvements:
- League separation built into collection process
- Proper ID formatting from the start
- Better organization and validation
- Comprehensive error handling
"""

import pandas as pd
import time
import os
import sys
import json
from datetime import datetime
import numpy as np

# NBA API imports
from nba_api.stats.endpoints import leaguegamefinder
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.static import teams, players

class LeagueSeparatedMasterCollector:
    """Collects NBA master tables with automatic league separation"""
    
    def __init__(self, data_dir='data', create_league_dirs=True):
        self.data_dir = data_dir
        self.leagues_dir = os.path.join(data_dir, 'leagues')
        
        # Create directories
        os.makedirs(data_dir, exist_ok=True)
        if create_league_dirs:
            os.makedirs(self.leagues_dir, exist_ok=True)
        
        self.failed_seasons_games = []
        self.failed_seasons_players = []
        
        # League configurations
        self.league_configs = [
            {'id': '00', 'name': 'NBA', 'full_name': 'National Basketball Association'},
            {'id': '10', 'name': 'WNBA', 'full_name': 'Women\'s National Basketball Association'},
            {'id': '20', 'name': 'G-League', 'full_name': 'G League'}
        ]
        
        # Season type configurations  
        self.season_type_configs = [
            {'type': 'Regular Season', 'name': 'Regular'},
            {'type': 'Pre Season', 'name': 'Preseason'},
            {'type': 'Playoffs', 'name': 'Playoffs'},
            {'type': 'IST', 'name': 'InSeasonTournament'}
        ]
        
        self.collection_timestamp = datetime.now().isoformat()
        
    def generate_seasons_by_league(self, league_name, start_year=1984, end_year=2025):
        """Generate seasons in proper format for each league"""
        seasons = []
        
        if league_name in ['NBA', 'G-League']:
            # NBA and G-League use two-year format: 2023-24, 2024-25, etc.
            for i in range(start_year, end_year):
                season_str = f"{i}-{str(i+1)[2:].zfill(2)}"
                seasons.append(season_str)
        elif league_name == 'WNBA':
            # WNBA uses single year format: 2024, 2025, etc.
            # WNBA started in 1997
            wnba_start = max(start_year, 1997)
            for i in range(wnba_start, end_year):
                seasons.append(str(i))
        
        return seasons[::-1]  # Most recent first for faster testing
    
    def generate_all_seasons(self, start_year=1984, end_year=2025):
        """Generate master seasons table (NBA format for reference)"""
        seasons = []
        for i in range(start_year, end_year):
            season_str = f"{i}-{str(i+1)[2:].zfill(2)}"
            seasons.append(season_str)
        return seasons[::-1]
    
    def format_game_ids(self, df):
        """Ensure GAME_IDs are properly formatted as 10-digit strings with leading zeros"""
        if 'GAME_ID' in df.columns:
            df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
        return df
    
    def format_player_ids(self, df):
        """Ensure PLAYER_IDs and TEAM_IDs are properly formatted as strings"""
        if 'PLAYER_ID' in df.columns:
            df['PLAYER_ID'] = df['PLAYER_ID'].astype(str)
        if 'TEAM_ID' in df.columns:
            df['TEAM_ID'] = df['TEAM_ID'].astype(str)
        return df
    
    def create_master_teams(self):
        """Create master teams table (NBA only since teams are consistent)"""
        print("\\n=== CREATING MASTER TEAMS ===")
        teams_data = teams.get_teams()
        master_teams = pd.DataFrame(teams_data)
        
        # Ensure ID is string format
        master_teams['id'] = master_teams['id'].astype(str)
        
        # Save to CSV
        teams_path = os.path.join(self.data_dir, 'master_teams.csv')
        master_teams.to_csv(teams_path, index=False)
        
        print(f"✓ Retrieved {len(master_teams)} teams")
        print(f"✓ Saved to: {teams_path}")
        
        return master_teams
    
    def create_master_seasons(self):
        """Create master seasons table"""
        print("\\n=== CREATING MASTER SEASONS ===")
        seasons = self.generate_all_seasons()
        
        master_seasons = pd.DataFrame({
            'season': seasons,
            'start_year': [int(s.split('-')[0]) for s in seasons],
            'end_year': [int(s.split('-')[0]) + 1 for s in seasons],
            'created_date': pd.Timestamp.now()
        })
        
        # Save to CSV
        seasons_path = os.path.join(self.data_dir, 'master_seasons.csv')
        master_seasons.to_csv(seasons_path, index=False)
        
        print(f"✓ Created {len(master_seasons)} seasons ({seasons[0]} to {seasons[-1]})")
        print(f"✓ Saved to: {seasons_path}")
        
        return master_seasons, seasons
    
    def collect_games_by_league(self, master_seasons, test_mode=True):
        """Collect games and separate by league during collection"""
        print("\\n=== COLLECTING GAMES BY LEAGUE ===")
        
        # In test mode, limit scope for faster testing
        if test_mode:
            leagues_to_test = [self.league_configs[0]]  # NBA only for testing
            seasons_limit = 3  # Last 3 seasons
            test_season_types = self.season_type_configs[:2]  # Regular + Playoffs
            print(f"🧪 TEST MODE: {len(leagues_to_test)} leagues, {seasons_limit} seasons each")
        else:
            leagues_to_test = self.league_configs
            seasons_limit = None  # All seasons
            test_season_types = self.season_type_configs
            print(f"🚀 FULL MODE: {len(leagues_to_test)} leagues, all historical seasons")
        
        # Initialize storage for each league
        league_games = {league['name']: [] for league in leagues_to_test}
        all_games = []
        
        total_combinations = 0
        current_combo = 0
        
        # Calculate total combinations for progress tracking
        for league in leagues_to_test:
            league_seasons = self.generate_seasons_by_league(league['name'])
            if seasons_limit:
                league_seasons = league_seasons[:seasons_limit]
            total_combinations += len(league_seasons) * len(test_season_types)
        
        print(f"\\n📊 Processing {total_combinations} combinations...")
        
        for league in leagues_to_test:
            print(f"\\n🏀 Collecting {league['name']} games...")
            
            # Get league-specific seasons
            league_seasons = self.generate_seasons_by_league(league['name'])
            if seasons_limit:
                league_seasons = league_seasons[:seasons_limit]
            
            print(f"   Seasons format for {league['name']}: {league_seasons[:3]}...")
            
            for season in league_seasons:
                for season_type in test_season_types:
                    current_combo += 1
                    combo_name = f"{season} {league['name']} {season_type['name']}"
                    
                    try:
                        print(f"  [{current_combo:3d}/{total_combinations}] {combo_name}", end=" ")
                        
                        gamefinder = leaguegamefinder.LeagueGameFinder(
                            league_id_nullable=league['id'], 
                            season_type_nullable=season_type['type'],
                            season_nullable=season
                        ).get_data_frames()[0]
                        
                        if len(gamefinder) > 0:
                            # Add metadata
                            gamefinder['league_id'] = league['id']
                            gamefinder['league_name'] = league['name']
                            gamefinder['season_type'] = season_type['type']
                            gamefinder['season_type_name'] = season_type['name']
                            gamefinder['collection_timestamp'] = self.collection_timestamp
                            
                            # Format IDs properly
                            gamefinder = self.format_game_ids(gamefinder)
                            
                            # Add to league-specific storage
                            league_games[league['name']].append(gamefinder)
                            all_games.append(gamefinder)
                            
                            print(f"→ {len(gamefinder)} games")
                        else:
                            print("→ No games")
                        
                        time.sleep(0.6)  # Rate limiting
                        
                    except Exception as e:
                        print(f"→ ERROR: {str(e)}")
                        self.failed_seasons_games.append(combo_name)
                        time.sleep(2)
        
        # Process and save results
        return self._save_games_by_league(league_games, all_games, leagues_to_test)
    
    def _save_games_by_league(self, league_games, all_games, leagues):
        """Save games data both comprehensively and by league"""
        
        print("\\n💾 SAVING GAMES DATA...")
        
        results = {}
        
        # Create comprehensive table (all leagues combined)
        if all_games:
            comprehensive_games = pd.concat(all_games, axis=0, ignore_index=True)
            comprehensive_games['GAME_DATE'] = pd.to_datetime(comprehensive_games['GAME_DATE'])
            
            # Save comprehensive table
            comprehensive_path = os.path.join(self.data_dir, 'comprehensive_master_games.csv')
            comprehensive_games.to_csv(comprehensive_path, index=False)
            
            print(f"✅ Comprehensive games: {len(comprehensive_games):,} → {comprehensive_path}")
            
            results['comprehensive'] = {
                'data': comprehensive_games,
                'file': comprehensive_path,
                'count': len(comprehensive_games)
            }
        
        # Create league-specific tables
        for league in leagues:
            league_name = league['name']
            
            if league_games[league_name]:
                league_df = pd.concat(league_games[league_name], axis=0, ignore_index=True)
                league_df['GAME_DATE'] = pd.to_datetime(league_df['GAME_DATE'])
                
                # Save league-specific table
                league_filename = f"{league_name.lower().replace('-', '_')}_master_games.csv"
                league_path = os.path.join(self.leagues_dir, league_filename)
                league_df.to_csv(league_path, index=False)
                
                print(f"✅ {league_name} games: {len(league_df):,} → {league_path}")
                
                results[league_name] = {
                    'data': league_df,
                    'file': league_path,
                    'count': len(league_df)
                }
            else:
                print(f"⚠️  {league_name}: No games collected")
                results[league_name] = {'count': 0}
        
        return results
    
    def collect_players_by_league(self, master_seasons, test_mode=True):
        """Collect players and separate by league"""
        print("\\n=== COLLECTING PLAYERS BY LEAGUE ===")
        
        # Use same test parameters as games
        if test_mode:
            leagues_to_test = [self.league_configs[0]]  # NBA only for testing
            seasons_limit = 3  # Last 3 seasons
            print(f"🧪 TEST MODE: {len(leagues_to_test)} leagues, {seasons_limit} seasons each")
        else:
            leagues_to_test = self.league_configs
            seasons_limit = None  # All seasons
            print(f"🚀 FULL MODE: {len(leagues_to_test)} leagues, all historical seasons")
        
        # Initialize storage for each league
        league_players = {league['name']: [] for league in leagues_to_test}
        all_players = []
        
        for league in leagues_to_test:
            print(f"\\n👥 Collecting {league['name']} players...")
            
            # Get league-specific seasons
            league_seasons = self.generate_seasons_by_league(league['name'])
            if seasons_limit:
                league_seasons = league_seasons[:seasons_limit]
            
            print(f"   Seasons format for {league['name']}: {league_seasons[:3]}...")
            
            for i, season in enumerate(league_seasons):
                try:
                    print(f"  [{i+1:2d}/{len(league_seasons)}] Season {season}", end=" ")
                    
                    # Get players for this season and league
                    players_endpoint = nbaapi.leaguedashplayerstats.LeagueDashPlayerStats(
                        season=season,
                        league_id_nullable=league['id']
                    )
                    players_df = players_endpoint.get_data_frames()[0]
                    
                    if len(players_df) > 0:
                        # Add metadata
                        players_df['league_id'] = league['id']
                        players_df['league_name'] = league['name']
                        players_df['season'] = season
                        players_df['collection_timestamp'] = self.collection_timestamp
                        
                        # Format IDs properly
                        players_df = self.format_player_ids(players_df)
                        
                        # Add to storage
                        league_players[league['name']].append(players_df)
                        all_players.append(players_df)
                        
                        print(f"→ {len(players_df)} players")
                    else:
                        print("→ No players")
                    
                    time.sleep(0.6)  # Rate limiting
                    
                except Exception as e:
                    print(f"→ ERROR: {str(e)}")
                    self.failed_seasons_players.append(f"{season} {league['name']}")
                    time.sleep(2)
        
        # Process and save results
        return self._save_players_by_league(league_players, all_players, leagues_to_test)
    
    def _save_players_by_league(self, league_players, all_players, leagues):
        """Save players data both comprehensively and by league"""
        
        print("\\n💾 SAVING PLAYERS DATA...")
        
        results = {}
        
        # Create comprehensive table (all leagues combined)
        if all_players:
            comprehensive_players = pd.concat(all_players, axis=0, ignore_index=True)
            
            # Save comprehensive table
            comprehensive_path = os.path.join(self.data_dir, 'comprehensive_master_players.csv')
            comprehensive_players.to_csv(comprehensive_path, index=False)
            
            print(f"✅ Comprehensive players: {len(comprehensive_players):,} → {comprehensive_path}")
            
            results['comprehensive'] = {
                'data': comprehensive_players,
                'file': comprehensive_path,
                'count': len(comprehensive_players)
            }
        
        # Create league-specific tables
        for league in leagues:
            league_name = league['name']
            
            if league_players[league_name]:
                league_df = pd.concat(league_players[league_name], axis=0, ignore_index=True)
                
                # Save league-specific table
                league_filename = f"{league_name.lower().replace('-', '_')}_master_players.csv"
                league_path = os.path.join(self.leagues_dir, league_filename)
                league_df.to_csv(league_path, index=False)
                
                print(f"✅ {league_name} players: {len(league_df):,} → {league_path}")
                
                results[league_name] = {
                    'data': league_df,
                    'file': league_path,
                    'count': len(league_df)
                }
            else:
                print(f"⚠️  {league_name}: No players collected")
                results[league_name] = {'count': 0}
        
        return results
    
    def create_collection_summary(self, games_results, players_results):
        """Create a comprehensive summary of the collection"""
        
        summary = {
            'collection_info': {
                'timestamp': self.collection_timestamp,
                'data_directory': self.data_dir,
                'leagues_directory': self.leagues_dir,
                'total_leagues': len(self.league_configs)
            },
            'league_configs': self.league_configs,
            'collection_results': {
                'games': {},
                'players': {}
            },
            'file_structure': {
                'comprehensive_tables': [],
                'league_separated_tables': []
            },
            'errors': {
                'failed_games': self.failed_seasons_games,
                'failed_players': self.failed_seasons_players
            }
        }
        
        # Process games results
        total_games = 0
        for league_name, result in games_results.items():
            count = result.get('count', 0)
            summary['collection_results']['games'][league_name] = {
                'count': count,
                'file': result.get('file', '')
            }
            if league_name != 'comprehensive':
                total_games += count
        
        # Process players results
        total_players = 0
        for league_name, result in players_results.items():
            count = result.get('count', 0)
            summary['collection_results']['players'][league_name] = {
                'count': count,
                'file': result.get('file', '')
            }
            if league_name != 'comprehensive':
                total_players += count
        
        # Add totals
        summary['totals'] = {
            'total_games': total_games,
            'total_players': total_players,
            'total_errors': len(self.failed_seasons_games) + len(self.failed_seasons_players)
        }
        
        # Save summary
        summary_path = os.path.join(self.data_dir, 'master_collection_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\\n📋 Collection summary saved: {summary_path}")
        
        return summary
    
    def run_league_separated_collection(self, test_mode=True):
        """Run the complete league-separated master table collection"""
        
        print("🚀 STARTING LEAGUE-SEPARATED MASTER COLLECTION")
        print("=" * 60)
        
        if test_mode:
            print("🧪 TEST MODE - Collecting recent data for validation")
        else:
            print("🏭 FULL MODE - Collecting comprehensive historical data")
            print("   This will take significant time due to API rate limits!")
        
        start_time = time.time()
        
        # Step 1: Create teams (shared across leagues)
        master_teams = self.create_master_teams()
        
        # Step 2: Create seasons
        master_seasons, seasons = self.create_master_seasons()
        
        # Step 3: Collect games by league
        games_results = self.collect_games_by_league(master_seasons, test_mode)
        
        # Step 4: Collect players by league
        players_results = self.collect_players_by_league(master_seasons, test_mode)
        
        # Step 5: Create summary
        summary = self.create_collection_summary(games_results, players_results)
        
        # Final summary
        elapsed_time = time.time() - start_time
        
        print("\\n" + "=" * 60)
        print("🏁 LEAGUE-SEPARATED COLLECTION COMPLETE!")
        print(f"⏱️  Total time: {elapsed_time/60:.1f} minutes")
        
        print("\\n📊 FINAL RESULTS:")
        print(f"🏀 Games collected:")
        for league_name, info in summary['collection_results']['games'].items():
            if league_name != 'comprehensive':
                print(f"   {league_name}: {info['count']:,} games")
        
        print(f"\\n👥 Players collected:")
        for league_name, info in summary['collection_results']['players'].items():
            if league_name != 'comprehensive':
                print(f"   {league_name}: {info['count']:,} players")
        
        if summary['totals']['total_errors'] > 0:
            print(f"\\n⚠️  Errors encountered: {summary['totals']['total_errors']}")
            if self.failed_seasons_games:
                print(f"   Failed game collections: {len(self.failed_seasons_games)}")
            if self.failed_seasons_players:
                print(f"   Failed player collections: {len(self.failed_seasons_players)}")
        
        print(f"\\n📁 Files created:")
        print(f"   📊 Comprehensive tables: {self.data_dir}/")
        print(f"   🏀 League-separated tables: {self.leagues_dir}/")
        
        return {
            'teams': master_teams,
            'seasons': master_seasons,
            'games': games_results,
            'players': players_results,
            'summary': summary
        }


def main():
    """Main execution with interactive options"""
    
    print("🏀 NBA Master Tables Collection - League Separated Edition")
    print("=" * 60)
    
    # Get user preferences
    print("\\nChoose collection mode:")
    print("1. 🧪 Test mode (recent seasons, NBA only - fast)")
    print("2. 🏭 Full mode (all seasons, all leagues - comprehensive)")
    
    choice = input("\\nEnter choice (1 or 2): ").strip()
    test_mode = choice != '2'
    
    # Initialize collector
    collector = LeagueSeparatedMasterCollector()
    
    # Run collection
    results = collector.run_league_separated_collection(test_mode=test_mode)
    
    # Show what was created
    print("\\n🎯 NEXT STEPS:")
    print("✅ Master tables created with league separation")
    print("✅ All ID formatting issues prevented")
    print("✅ Ready for endpoint data collection")
    print("\\nUse the league-specific tables for targeted data collection!")


if __name__ == "__main__":
    main()
