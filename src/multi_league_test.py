"""
Comprehensive Multi-League Test

This script validates the complete master data collection process across all leagues:
- NBA, WNBA, G-League
- Games, Players, Teams, Seasons
- Proper season formatting for each league
- Data integrity validation
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from league_separated_master_collection import LeagueSeparatedMasterCollector
import pandas as pd
import time

class MultiLeagueValidator:
    """Validates master data collection across all leagues"""
    
    def __init__(self):
        self.collector = LeagueSeparatedMasterCollector()
        self.validation_results = {}
    
    def run_multi_league_test(self):
        """Run comprehensive multi-league test"""
        
        print("🏀 COMPREHENSIVE MULTI-LEAGUE VALIDATION TEST")
        print("=" * 60)
        
        print("🧪 Test Configuration:")
        print("   Leagues: NBA, WNBA, G-League")
        print("   Data Types: Games, Players, Teams, Seasons")
        print("   Scope: Recent seasons for validation")
        print("   Expected Duration: 10-15 minutes")
        
        # Temporarily modify collector for multi-league testing
        original_configs = self.collector.league_configs.copy()
        
        # Enable all leagues but limit seasons for testing
        print(f"\\n📊 Testing with all {len(self.collector.league_configs)} leagues...")
        for league in self.collector.league_configs:
            print(f"   • {league['name']} (ID: {league['id']}) - {league['full_name']}")
        
        start_time = time.time()
        
        # Run collection with multi-league test mode
        results = self._run_test_collection()
        
        elapsed_time = time.time() - start_time
        
        # Validate results
        validation_summary = self._validate_results(results)
        
        # Show final summary
        self._show_test_summary(validation_summary, elapsed_time)
        
        return validation_summary
    
    def _run_test_collection(self):
        """Run the actual collection test"""
        
        print("\\n🚀 STARTING MULTI-LEAGUE COLLECTION TEST...")
        
        # Step 1: Teams
        print("\\n=== TESTING TEAMS COLLECTION ===")
        teams = self.collector.create_master_teams()
        
        # Step 2: Seasons  
        print("\\n=== TESTING SEASONS COLLECTION ===")
        seasons_df, seasons_list = self.collector.create_master_seasons()
        
        # Step 3: Games (limited test mode)
        print("\\n=== TESTING GAMES COLLECTION ===")
        games_results = self._test_games_collection()
        
        # Step 4: Players (limited test mode)
        print("\\n=== TESTING PLAYERS COLLECTION ===")
        players_results = self._test_players_collection()
        
        return {
            'teams': teams,
            'seasons': {'data': seasons_df, 'list': seasons_list},
            'games': games_results,
            'players': players_results
        }
    
    def _test_games_collection(self):
        """Test games collection across leagues"""
        
        # Override test mode to include all leagues but limit seasons
        test_leagues = self.collector.league_configs  # All leagues
        test_seasons_per_league = 2  # Just 2 recent seasons per league
        test_season_types = self.collector.season_type_configs[:2]  # Regular + Playoffs
        
        league_games = {league['name']: [] for league in test_leagues}
        all_games = []
        
        print(f"   Testing {len(test_leagues)} leagues × {test_seasons_per_league} seasons × {len(test_season_types)} types")
        
        for league in test_leagues:
            print(f"\\n   🏀 Testing {league['name']} games...")
            
            # Get league-specific seasons
            league_seasons = self.collector.generate_seasons_by_league(league['name'])[:test_seasons_per_league]
            print(f"      Season format: {league_seasons}")
            
            for season in league_seasons:
                for season_type in test_season_types:
                    try:
                        print(f"      {season} {season_type['name']}", end=" ")
                        
                        from nba_api.stats.endpoints import leaguegamefinder
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
                            
                            # Format IDs
                            gamefinder = self.collector.format_game_ids(gamefinder)
                            
                            league_games[league['name']].append(gamefinder)
                            all_games.append(gamefinder)
                            
                            print(f"→ {len(gamefinder)} games")
                        else:
                            print("→ No games")
                        
                        time.sleep(0.8)  # Rate limiting
                        
                    except Exception as e:
                        print(f"→ ERROR: {str(e)}")
                        time.sleep(2)
        
        # Save test results
        return self.collector._save_games_by_league(league_games, all_games, test_leagues)
    
    def _test_players_collection(self):
        """Test players collection across leagues"""
        
        test_leagues = self.collector.league_configs  # All leagues
        test_seasons_per_league = 2  # Just 2 recent seasons per league
        
        league_players = {league['name']: [] for league in test_leagues}
        all_players = []
        
        print(f"   Testing {len(test_leagues)} leagues × {test_seasons_per_league} seasons")
        
        for league in test_leagues:
            print(f"\\n   👥 Testing {league['name']} players...")
            
            # Get league-specific seasons
            league_seasons = self.collector.generate_seasons_by_league(league['name'])[:test_seasons_per_league]
            print(f"      Season format: {league_seasons}")
            
            for season in league_seasons:
                try:
                    print(f"      Season {season}", end=" ")
                    
                    import nba_api.stats.endpoints as nbaapi
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
                        
                        # Format IDs
                        players_df = self.collector.format_player_ids(players_df)
                        
                        league_players[league['name']].append(players_df)
                        all_players.append(players_df)
                        
                        print(f"→ {len(players_df)} players")
                    else:
                        print("→ No players")
                    
                    time.sleep(0.8)  # Rate limiting
                    
                except Exception as e:
                    print(f"→ ERROR: {str(e)}")
                    time.sleep(2)
        
        # Save test results
        return self.collector._save_players_by_league(league_players, all_players, test_leagues)
    
    def _validate_results(self, results):
        """Validate the test results"""
        
        print("\\n🔍 VALIDATING TEST RESULTS...")
        
        validation = {
            'teams': {'status': 'unknown', 'count': 0, 'issues': []},
            'seasons': {'status': 'unknown', 'count': 0, 'issues': []},
            'games': {'status': 'unknown', 'leagues': {}, 'total': 0, 'issues': []},
            'players': {'status': 'unknown', 'leagues': {}, 'total': 0, 'issues': []},
            'overall_status': 'unknown'
        }
        
        # Validate Teams
        if results['teams'] is not None:
            validation['teams']['count'] = len(results['teams'])
            validation['teams']['status'] = 'success' if len(results['teams']) >= 30 else 'warning'
            print(f"   ✅ Teams: {validation['teams']['count']} teams collected")
        else:
            validation['teams']['status'] = 'failed'
            validation['teams']['issues'].append('No teams data collected')
            print(f"   ❌ Teams: Collection failed")
        
        # Validate Seasons
        if results['seasons']['data'] is not None:
            validation['seasons']['count'] = len(results['seasons']['data'])
            validation['seasons']['status'] = 'success' if len(results['seasons']['data']) >= 70 else 'warning'
            print(f"   ✅ Seasons: {validation['seasons']['count']} seasons collected")
        else:
            validation['seasons']['status'] = 'failed'
            validation['seasons']['issues'].append('No seasons data collected')
            print(f"   ❌ Seasons: Collection failed")
        
        # Validate Games
        total_games = 0
        games_success = 0
        for league_name, result in results['games'].items():
            if league_name != 'comprehensive':
                count = result.get('count', 0)
                validation['games']['leagues'][league_name] = {
                    'count': count,
                    'status': 'success' if count > 0 else 'failed'
                }
                total_games += count
                if count > 0:
                    games_success += 1
                
                print(f"   {'✅' if count > 0 else '❌'} {league_name} Games: {count:,}")
        
        validation['games']['total'] = total_games
        validation['games']['status'] = 'success' if games_success >= 2 else ('partial' if games_success >= 1 else 'failed')
        
        # Validate Players
        total_players = 0
        players_success = 0
        for league_name, result in results['players'].items():
            if league_name != 'comprehensive':
                count = result.get('count', 0)
                validation['players']['leagues'][league_name] = {
                    'count': count,
                    'status': 'success' if count > 0 else 'failed'
                }
                total_players += count
                if count > 0:
                    players_success += 1
                
                print(f"   {'✅' if count > 0 else '❌'} {league_name} Players: {count:,}")
        
        validation['players']['total'] = total_players
        validation['players']['status'] = 'success' if players_success >= 2 else ('partial' if players_success >= 1 else 'failed')
        
        # Overall Status
        statuses = [validation['teams']['status'], validation['seasons']['status'], 
                   validation['games']['status'], validation['players']['status']]
        
        if all(s == 'success' for s in statuses):
            validation['overall_status'] = 'success'
        elif 'failed' not in statuses:
            validation['overall_status'] = 'partial_success'
        else:
            validation['overall_status'] = 'failed'
        
        return validation
    
    def _show_test_summary(self, validation, elapsed_time):
        """Show final test summary"""
        
        print(f"\\n📋 MULTI-LEAGUE TEST SUMMARY")
        print("=" * 50)
        
        print(f"⏱️  Test Duration: {elapsed_time/60:.1f} minutes")
        
        # Overall Status
        status_emoji = {
            'success': '✅',
            'partial_success': '⚠️',
            'failed': '❌'
        }
        
        overall_status = validation['overall_status']
        print(f"{status_emoji.get(overall_status, '❓')} Overall Status: {overall_status.upper()}")
        
        print(f"\\n📊 Data Collection Results:")
        print(f"   Teams: {validation['teams']['count']} ({validation['teams']['status']})")
        print(f"   Seasons: {validation['seasons']['count']} ({validation['seasons']['status']})")
        print(f"   Total Games: {validation['games']['total']:,} ({validation['games']['status']})")
        print(f"   Total Players: {validation['players']['total']:,} ({validation['players']['status']})")
        
        print(f"\\n🏀 League Breakdown:")
        for league in ['NBA', 'WNBA', 'G-League']:
            games_info = validation['games']['leagues'].get(league, {'count': 0, 'status': 'not_tested'})
            players_info = validation['players']['leagues'].get(league, {'count': 0, 'status': 'not_tested'})
            
            print(f"   {league}:")
            print(f"     Games: {games_info['count']:,} ({games_info['status']})")
            print(f"     Players: {players_info['count']:,} ({players_info['status']})")
        
        # Success determination for next steps
        if overall_status == 'success':
            print(f"\\n🎉 TEST PASSED - Ready to proceed with file reorganization!")
        elif overall_status == 'partial_success':
            print(f"\\n⚠️  TEST PARTIALLY SUCCESSFUL - Can proceed with caution")
        else:
            print(f"\\n❌ TEST FAILED - Need to address issues before proceeding")
        
        return overall_status


def main():
    """Main test execution"""
    
    validator = MultiLeagueValidator()
    result = validator.run_multi_league_test()
    
    return result == 'success' or result == 'partial_success'


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
