"""
Comprehensive NBA Players Collection
Collects players from ALL leagues (NBA, WNBA, G-League) across ALL seasons.

This script builds upon the successful games collection to create a complete player dataset.
"""

import pandas as pd
import time
import os
import json
from datetime import datetime
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.static import players


class ComprehensivePlayersCollector:
    """Collects ALL players across all leagues and seasons"""
    
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        
        # Track progress and errors
        self.successful_collections = []
        self.failed_collections = []
        self.total_players = 0
        
        # Define league configurations (same as games)
        self.league_configs = [
            {'id': '00', 'name': 'NBA', 'active': True},
            {'id': '10', 'name': 'WNBA', 'active': True},
            {'id': '20', 'name': 'G-League', 'active': True}
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
    
    def test_single_league_season(self, league, season):
        """Test a single league/season combination for players"""
        combo_name = f"{season} {league['name']} Players"
        print(f"Testing: {combo_name}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Try different player endpoints based on league
                if league['id'] == '10':  # WNBA
                    # WNBA might need different approach
                    playerfinder = nbaapi.leaguedashplayerbiostats.LeagueDashPlayerBioStats(
                        season=season,
                        league_id=league['id']
                    ).get_data_frames()[0]
                else:  # NBA and G-League
                    playerfinder = nbaapi.leaguedashplayerbiostats.LeagueDashPlayerBioStats(
                        season=season,
                        league_id=league['id']
                    ).get_data_frames()[0]
                
                if len(playerfinder) > 0:
                    print(f"  ✓ SUCCESS: {len(playerfinder)} players found")
                    return True, len(playerfinder)
                else:
                    print(f"  ○ No players (empty result)")
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
    
    def run_players_exploration(self):
        """Test different combinations to understand what works for players"""
        print("=== PLAYERS API EXPLORATION MODE ===")
        print("Testing various combinations to understand player API capabilities...")
        
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
                
            for season in test_seasons:
                success, count = self.test_single_league_season(league, season)
                
                results.append({
                    'league_id': league['id'],
                    'league_name': league['name'],
                    'season': season,
                    'success': success,
                    'players_count': count
                })
                
                time.sleep(0.5)  # Rate limiting
        
        # Save exploration results
        results_df = pd.DataFrame(results)
        results_path = os.path.join(self.data_dir, 'players_api_exploration_results.csv')
        results_df.to_csv(results_path, index=False)
        
        print(f"\n=== PLAYERS EXPLORATION RESULTS ===")
        print(f"Results saved to: {results_path}")
        
        # Summary
        successful = results_df[results_df['success'] == True]
        print(f"Successful combinations: {len(successful)}/{len(results)}")
        print(f"Total players found in test: {successful['players_count'].sum():,}")
        
        # Show working combinations
        working = successful[successful['players_count'] > 0]
        if len(working) > 0:
            print(f"\nWorking combinations with players:")
            for _, row in working.iterrows():
                print(f"  {row['league_name']} {row['season']}: {row['players_count']:,} players")
        
        return results_df
    
    def collect_comprehensive_players(self, start_year=1946, test_mode=True):
        """Collect ALL players from ALL working combinations"""
        print("=== COMPREHENSIVE PLAYERS COLLECTION ===")
        
        all_players = []
        current_combo = 0
        
        # Calculate total combinations across all leagues
        total_combinations = 0
        for league in self.league_configs:
            if league['active']:
                league_seasons = self.generate_seasons(league['id'], start_year)
                if test_mode:
                    league_seasons = league_seasons[-5:]  # Last 5 seasons for testing
                total_combinations += len(league_seasons)
        
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
                current_combo += 1
                combo_name = f"{season} {league['name']} Players"
                
                try:
                    print(f"[{current_combo:4d}/{total_combinations}] {combo_name}")
                    
                    playerfinder = nbaapi.leaguedashplayerbiostats.LeagueDashPlayerBioStats(
                        season=season,
                        league_id=league['id']
                    ).get_data_frames()[0]
                    
                    if len(playerfinder) > 0:
                        # Add metadata
                        playerfinder['league_id'] = league['id']
                        playerfinder['league_name'] = league['name']
                        playerfinder['season'] = season
                        playerfinder['collection_timestamp'] = datetime.now()
                        
                        all_players.append(playerfinder)
                        self.total_players += len(playerfinder)
                        
                        print(f"    ✓ {len(playerfinder):,} players")
                        
                        self.successful_collections.append({
                            'combo': combo_name,
                            'players': len(playerfinder)
                        })
                    else:
                        print(f"    ○ No players")
                        
                    time.sleep(0.6)  # Generous rate limiting for comprehensive collection
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"    ✗ ERROR: {error_msg}")
                    
                    self.failed_collections.append({
                        'combo': combo_name,
                        'error': error_msg
                    })
                    
                    time.sleep(2)  # Longer wait after error
        
        return self.save_comprehensive_results(all_players)
    
    def save_comprehensive_results(self, all_players):
        """Save comprehensive results and generate summary"""
        if not all_players:
            print("❌ No players collected!")
            return None
        
        print(f"\n=== COMBINING AND SAVING RESULTS ===")
        
        # Combine all players
        master_players = pd.concat(all_players, axis=0, ignore_index=True)
        
        # Save main file
        players_path = os.path.join(self.data_dir, 'comprehensive_master_players.csv')
        master_players.to_csv(players_path, index=False)
        
        print(f"✓ Comprehensive players saved to: {players_path}")
        print(f"✓ Total player records: {len(master_players):,}")
        print(f"✓ Unique players: {master_players['PLAYER_ID'].nunique():,}")
        
        # Generate detailed summary
        self.generate_summary_report(master_players)
        
        return master_players
    
    def generate_summary_report(self, players_df):
        """Generate comprehensive summary report"""
        print(f"\n=== COMPREHENSIVE PLAYERS SUMMARY REPORT ===")
        
        # League breakdown
        print(f"\n📊 Players by League:")
        league_summary = players_df.groupby('league_name').agg({
            'PLAYER_ID': 'nunique'
        }).reset_index()
        league_summary.columns = ['League', 'Unique_Players']
        
        for _, row in league_summary.iterrows():
            print(f"  {row['League']}: {row['Unique_Players']:,} players")
        
        # Season breakdown
        season_summary = players_df.groupby(['league_name', 'season']).agg({
            'PLAYER_ID': 'nunique'
        }).reset_index()
        
        # Save detailed summaries
        summary_data = {
            'league_summary': league_summary.to_dict('records'),
            'season_summary': season_summary.to_dict('records'),
            'successful_collections': self.successful_collections,
            'failed_collections': self.failed_collections,
            'collection_metadata': {
                'total_combinations_attempted': len(self.successful_collections) + len(self.failed_collections),
                'successful_combinations': len(self.successful_collections),
                'failed_combinations': len(self.failed_collections),
                'total_player_records_collected': self.total_players,
                'collection_timestamp': datetime.now().isoformat()
            }
        }
        
        summary_path = os.path.join(self.data_dir, 'comprehensive_players_summary.json')
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
    collector = ComprehensivePlayersCollector()
    
    print("NBA Comprehensive Players Collection")
    print("=" * 50)
    
    # First, run exploration to understand API capabilities
    print("STEP 1: Players API Exploration")
    exploration_results = collector.run_players_exploration()
    
    print("\n" + "="*50)
    print("STEP 2: Comprehensive Players Collection")
    
    # Enable all leagues
    collector.league_configs = [
        {'id': '00', 'name': 'NBA', 'active': True},
        {'id': '10', 'name': 'WNBA', 'active': True},
        {'id': '20', 'name': 'G-League', 'active': True}
    ]
    
    comprehensive_players = collector.collect_comprehensive_players(
        start_year=1946,
        test_mode=True  # Set to False for full collection
    )
    
    if comprehensive_players is not None:
        print("\n🎉 COMPREHENSIVE PLAYERS COLLECTION COMPLETE!")
        print(f"Final dataset: {len(comprehensive_players):,} total player records")
        print(f"Unique players: {comprehensive_players['PLAYER_ID'].nunique():,}")
        print(f"Data files saved in: {collector.data_dir}")
    else:
        print("\n❌ No players were collected. Check the error logs above.")


if __name__ == "__main__":
    main()
