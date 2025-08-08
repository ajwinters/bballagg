"""
Comprehensive NBA Games Collection
Collects games from ALL leagues and season types across ALL time periods.

This script is designed to collect the most complete dataset possible from the NBA API.
"""

import pandas as pd
import time
import os
from datetime import datetime
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.static import teams
import json


class ComprehensiveGamesCollector:
    """Collects ALL NBA games across all leagues and season types"""
    
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        
        # Track progress and errors
        self.successful_collections = []
        self.failed_collections = []
        self.total_games = 0
        
        # Define comprehensive configuration
        self.league_configs = [
            {'id': '00', 'name': 'NBA', 'active': True},
            {'id': '10', 'name': 'WNBA', 'active': True},
            {'id': '20', 'name': 'G-League', 'active': True}
        ]
        
        self.season_type_configs = [
            {'type': 'Regular Season', 'name': 'Regular', 'active': True},
            {'type': 'Pre Season', 'name': 'Preseason', 'active': True},
            {'type': 'Playoffs', 'name': 'Playoffs', 'active': True},
            {'type': 'All-Star', 'name': 'AllStar', 'active': True},
            {'type': 'IST', 'name': 'InSeasonTournament', 'active': False}  # May not work for all seasons
        ]
    
    def generate_seasons(self, league_id, start_year=1946, end_year=2025):
        """Generate seasons based on league naming conventions"""
        seasons = []
        
        # Different leagues use different season naming conventions
        if league_id == '10':  # WNBA - single year seasons
            for year in range(1997, end_year):  # WNBA started in 1997
                seasons.append(str(year))
        else:  # NBA ('00') and G-League ('20') - two-year seasons
            for year in range(start_year, end_year):
                next_year = str(year + 1)[2:]  # Get last 2 digits
                seasons.append(f"{year}-{next_year}")
                
        return seasons
    
    def test_single_combination(self, league, season_type, season):
        """Test a single league/season_type/season combination with robust error handling"""
        combo_name = f"{season} {league['name']} {season_type['name']}"
        print(f"Testing: {combo_name}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                gamefinder = leaguegamefinder.LeagueGameFinder(
                    league_id_nullable=league['id'],
                    season_type_nullable=season_type['type'],
                    season_nullable=season
                ).get_data_frames()[0]
                
                if len(gamefinder) > 0:
                    print(f"  ✓ SUCCESS: {len(gamefinder)} games found")
                    return True, len(gamefinder)
                else:
                    print(f"  ○ No games (empty result)")
                    return True, 0
                    
            except KeyboardInterrupt:
                print(f"  ⚠ User interrupted")
                raise
            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1:
                    print(f"  ⚠ Attempt {attempt + 1} failed: {error_msg}")
                    print(f"    Retrying in {2 * (attempt + 1)} seconds...")
                    time.sleep(2 * (attempt + 1))
                else:
                    print(f"  ✗ ERROR (after {max_retries} attempts): {error_msg}")
                    return False, 0
        
        return False, 0
    
    def run_api_exploration(self):
        """Test different combinations to understand what works"""
        print("=== API EXPLORATION MODE ===")
        print("Testing various combinations to understand API capabilities...")
        
        results = []
        
        for league in self.league_configs:
            if not league['active']:
                continue
            
            # Generate appropriate test seasons for each league
            if league['id'] == '10':  # WNBA - single year
                test_seasons = ['2023', '2022', '2021']
            else:  # NBA and G-League - two year
                test_seasons = ['2023-24', '2022-23', '2021-22']
            
            print(f"\n--- Testing {league['name']} with seasons: {test_seasons} ---")
                
            for season_type in self.season_type_configs:
                if not season_type['active']:
                    continue
                    
                for season in test_seasons:
                    success, count = self.test_single_combination(league, season_type, season)
                    
                    results.append({
                        'league_id': league['id'],
                        'league_name': league['name'],
                        'season_type': season_type['type'],
                        'season_type_name': season_type['name'],
                        'season': season,
                        'success': success,
                        'games_count': count
                    })
                    
                    time.sleep(0.5)  # Rate limiting
        
        # Save exploration results
        results_df = pd.DataFrame(results)
        results_path = os.path.join(self.data_dir, 'api_exploration_results.csv')
        results_df.to_csv(results_path, index=False)
        
        print(f"\n=== EXPLORATION RESULTS ===")
        print(f"Results saved to: {results_path}")
        
        # Summary
        successful = results_df[results_df['success'] == True]
        print(f"Successful combinations: {len(successful)}/{len(results)}")
        print(f"Total games found in test: {successful['games_count'].sum():,}")
        
        # Show working combinations
        working = successful[successful['games_count'] > 0]
        if len(working) > 0:
            print(f"\nWorking combinations with games:")
            for _, row in working.iterrows():
                print(f"  {row['league_name']} {row['season_type_name']} {row['season']}: {row['games_count']:,} games")
        
        return results_df
    
    def collect_comprehensive_games(self, start_year=1946, test_mode=True):
        """Collect ALL games from ALL working combinations"""
        print("=== COMPREHENSIVE GAMES COLLECTION ===")
        
        all_games = []
        current_combo = 0
        
        # Calculate total combinations across all leagues
        total_combinations = 0
        for league in self.league_configs:
            if league['active']:
                league_seasons = self.generate_seasons(league['id'], start_year)
                if test_mode:
                    league_seasons = league_seasons[-5:]  # Last 5 seasons for testing
                total_combinations += len(league_seasons) * len([s for s in self.season_type_configs if s['active']])
        
        for league in self.league_configs:
            if not league['active']:
                continue
            
            # Generate seasons specific to this league
            seasons = self.generate_seasons(league['id'], start_year)
            
            if test_mode:
                seasons = seasons[-5:]  # Last 5 seasons for testing
            
            print(f"\n--- {league['name']} ({league['id']}) ---")
            print(f"Seasons to collect: {seasons}")
            
            for season in seasons:
                for season_type in self.season_type_configs:
                    if not season_type['active']:
                        continue
                
                    current_combo += 1
                    combo_name = f"{season} {league['name']} {season_type['name']}"
                    
                    try:
                        print(f"[{current_combo:4d}/{total_combinations}] {combo_name}")
                        
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
                            gamefinder['collection_timestamp'] = datetime.now()
                            
                            all_games.append(gamefinder)
                            self.total_games += len(gamefinder)
                            
                            print(f"    ✓ {len(gamefinder):,} games")
                            
                            self.successful_collections.append({
                                'combo': combo_name,
                                'games': len(gamefinder)
                            })
                        else:
                            print(f"    ○ No games")
                            
                        time.sleep(0.8)  # Generous rate limiting for comprehensive collection
                        
                    except Exception as e:
                        error_msg = str(e)
                        print(f"    ✗ ERROR: {error_msg}")
                        
                        self.failed_collections.append({
                            'combo': combo_name,
                            'error': error_msg
                        })
                        
                        time.sleep(2)  # Longer wait after error
        
        return self.save_comprehensive_results(all_games)
    
    def save_comprehensive_results(self, all_games):
        """Save comprehensive results and generate summary"""
        if not all_games:
            print("❌ No games collected!")
            return None
        
        print(f"\n=== COMBINING AND SAVING RESULTS ===")
        
        # Combine all games
        master_games = pd.concat(all_games, axis=0, ignore_index=True)
        master_games['GAME_DATE'] = pd.to_datetime(master_games['GAME_DATE'])
        
        # Save main file
        games_path = os.path.join(self.data_dir, 'comprehensive_master_games.csv')
        master_games.to_csv(games_path, index=False)
        
        print(f"✓ Comprehensive games saved to: {games_path}")
        print(f"✓ Total games: {len(master_games):,}")
        print(f"✓ Unique games: {master_games['GAME_ID'].nunique():,}")
        print(f"✓ Date range: {master_games['GAME_DATE'].min().strftime('%Y-%m-%d')} to {master_games['GAME_DATE'].max().strftime('%Y-%m-%d')}")
        
        # Generate detailed summary
        self.generate_summary_report(master_games)
        
        return master_games
    
    def generate_summary_report(self, games_df):
        """Generate comprehensive summary report"""
        print(f"\n=== COMPREHENSIVE SUMMARY REPORT ===")
        
        # League breakdown
        print(f"\n📊 Games by League:")
        league_summary = games_df.groupby('league_name').agg({
            'GAME_ID': 'nunique'
        }).reset_index()
        league_summary.columns = ['League', 'Unique_Games']
        
        for _, row in league_summary.iterrows():
            print(f"  {row['League']}: {row['Unique_Games']:,} games")
        
        # Season type breakdown
        print(f"\n📊 Games by Season Type:")
        season_type_summary = games_df.groupby('season_type_name').agg({
            'GAME_ID': 'nunique'
        }).reset_index()
        
        for _, row in season_type_summary.iterrows():
            print(f"  {row['season_type_name']}: {row['GAME_ID']:,} games")
        
        # Year-over-year trends
        games_df['year'] = games_df['GAME_DATE'].dt.year
        yearly_summary = games_df.groupby(['year', 'league_name']).agg({
            'GAME_ID': 'nunique'
        }).reset_index()
        
        # Save detailed summaries
        summary_data = {
            'league_summary': league_summary.to_dict('records'),
            'season_type_summary': season_type_summary.to_dict('records'),
            'yearly_summary': yearly_summary.to_dict('records'),
            'successful_collections': self.successful_collections,
            'failed_collections': self.failed_collections,
            'collection_metadata': {
                'total_combinations_attempted': len(self.successful_collections) + len(self.failed_collections),
                'successful_combinations': len(self.successful_collections),
                'failed_combinations': len(self.failed_collections),
                'total_games_collected': self.total_games,
                'collection_timestamp': datetime.now().isoformat()
            }
        }
        
        summary_path = os.path.join(self.data_dir, 'comprehensive_games_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary_data, f, indent=2, default=str)
        
        print(f"✓ Detailed summary saved to: {summary_path}")
        
        # Show failure summary
        if self.failed_collections:
            print(f"\n⚠ Failed Collections: {len(self.failed_collections)}")
            error_types = {}
            for failure in self.failed_collections:
                error_key = failure['error'][:50]  # First 50 chars
                error_types[error_key] = error_types.get(error_key, 0) + 1
            
            for error, count in error_types.items():
                print(f"  {error}...: {count} occurrences")


def main():
    """Main execution"""
    collector = ComprehensiveGamesCollector()
    
    print("NBA Comprehensive Games Collection")
    print("=" * 50)
    
    # First, run exploration to understand API capabilities
    print("STEP 1: API Exploration")
    exploration_results = collector.run_api_exploration()
    
    print("\n" + "="*50)
    print("STEP 2: Comprehensive Collection")
    
    # Enable all leagues (excluding ABA)
    collector.league_configs = [
        {'id': '00', 'name': 'NBA', 'active': True},
        {'id': '10', 'name': 'WNBA', 'active': True},
        {'id': '20', 'name': 'G-League', 'active': True}
    ]
    
    # All season types
    collector.season_type_configs = [
        {'type': 'Regular Season', 'name': 'Regular', 'active': True},
        {'type': 'Pre Season', 'name': 'Preseason', 'active': True},
        {'type': 'Playoffs', 'name': 'Playoffs', 'active': True},
        {'type': 'All-Star', 'name': 'AllStar', 'active': True}
    ]
    
    comprehensive_games = collector.collect_comprehensive_games(
        start_year=1946,
        test_mode=True  # Set to False for full collection
    )
    
    if comprehensive_games is not None:
        print("\n🎉 COMPREHENSIVE GAMES COLLECTION COMPLETE!")
        print(f"Final dataset: {len(comprehensive_games):,} total games")
        print(f"Unique games: {comprehensive_games['GAME_ID'].nunique():,}")
        print(f"Data files saved in: {collector.data_dir}")
    else:
        print("\n❌ No games were collected. Check the error logs above.")


if __name__ == "__main__":
    main()
