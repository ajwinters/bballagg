"""
Production NBA Data Collection System

This system uses the validated master tables to systematically collect data from
high-priority NBA API endpoints. It includes error handling, rate limiting, and
incremental updates.
"""

import pandas as pd
import time
import os
import json
import sys
from datetime import datetime, timedelta
from nba_api.stats import endpoints

# Add parent directory to path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.nba_endpoints_config import ALL_ENDPOINTS


class ProductionDataCollector:
    """Production-ready NBA data collection system"""
    
    def __init__(self, data_dir='data', output_dir='production_data'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Load master tables
        self.master_games = pd.read_csv(f'{data_dir}/comprehensive_master_games.csv')
        self.master_players = pd.read_csv(f'{data_dir}/comprehensive_master_players.csv') 
        self.master_teams = pd.read_csv(f'{data_dir}/master_teams.csv')
        
        # Collection tracking
        self.collection_log = []
        self.successful_collections = 0
        self.failed_collections = 0
        
        print(f"🏗️ Production NBA Data Collection System Initialized")
        print(f"📊 Master Tables: {len(self.master_games):,} games, {len(self.master_players):,} players, {len(self.master_teams)} teams")
    
    def get_endpoint_class(self, endpoint_name):
        """Get the endpoint class from nba_api with proper error handling"""
        endpoint_variations = [
            endpoint_name,
            endpoint_name.lower(),
            ''.join(word.capitalize() for word in endpoint_name.split('_')),
        ]
        
        for variation in endpoint_variations:
            try:
                endpoint_class = getattr(endpoints, variation)
                if callable(endpoint_class):
                    return endpoint_class
            except AttributeError:
                continue
        
        raise AttributeError(f"Endpoint class {endpoint_name} not found in nba_api")
    
    def collect_game_based_data(self, game_ids, endpoint_name, priority='high', max_games=None):
        """Collect data for game-based endpoints"""
        print(f"\n🎯 Collecting {endpoint_name} data for {len(game_ids)} games...")
        
        if max_games:
            game_ids = game_ids[:max_games]
            print(f"   Limited to {max_games} games for testing")
        
        try:
            endpoint_class = self.get_endpoint_class(endpoint_name)
        except AttributeError as e:
            print(f"❌ {e}")
            return None
        
        all_dataframes = []
        failed_games = []
        
        for i, game_id in enumerate(game_ids):
            try:
                print(f"   Processing game {i+1}/{len(game_ids)}: {game_id}", end="")
                
                # Make API call
                instance = endpoint_class(game_id=str(game_id))
                dataframes = instance.get_data_frames()
                
                # Add game_id to all dataframes
                for df in dataframes:
                    if not df.empty:
                        df['source_game_id'] = game_id
                        df['collection_timestamp'] = datetime.now().isoformat()
                        all_dataframes.append(df)
                
                total_rows = sum(len(df) for df in dataframes if not df.empty)
                print(f" ✅ {len(dataframes)} tables, {total_rows} rows")
                
                self.successful_collections += 1
                
                # Rate limiting
                time.sleep(0.6)
                
            except Exception as e:
                print(f" ❌ {str(e)}")
                failed_games.append({'game_id': game_id, 'error': str(e)})
                self.failed_collections += 1
                time.sleep(1)  # Longer wait after errors
        
        # Combine all data
        if all_dataframes:
            combined_data = pd.concat(all_dataframes, ignore_index=True)
            
            # Save to file
            output_file = f"{self.output_dir}/{endpoint_name.lower()}_games_data.csv"
            combined_data.to_csv(output_file, index=False)
            
            print(f"💾 Saved {len(combined_data):,} rows to {output_file}")
            
            return {
                'endpoint': endpoint_name,
                'total_rows': len(combined_data),
                'successful_games': len(game_ids) - len(failed_games),
                'failed_games': len(failed_games),
                'output_file': output_file,
                'failed_game_details': failed_games
            }
        else:
            print(f"❌ No data collected for {endpoint_name}")
            return None
    
    def collect_player_based_data(self, player_ids, endpoint_name, max_players=None):
        """Collect data for player-based endpoints"""
        print(f"\n🎯 Collecting {endpoint_name} data for {len(player_ids)} players...")
        
        if max_players:
            player_ids = player_ids[:max_players]
            print(f"   Limited to {max_players} players for testing")
        
        try:
            endpoint_class = self.get_endpoint_class(endpoint_name)
        except AttributeError as e:
            print(f"❌ {e}")
            return None
        
        all_dataframes = []
        failed_players = []
        
        for i, player_id in enumerate(player_ids):
            try:
                print(f"   Processing player {i+1}/{len(player_ids)}: {player_id}", end="")
                
                # Prepare parameters
                params = {'player_id': str(player_id)}
                
                # Handle special endpoints that need additional parameters
                if 'ByClutch' in endpoint_name:
                    params['last_n_games'] = 30
                
                # Make API call
                instance = endpoint_class(**params)
                dataframes = instance.get_data_frames()
                
                # Add player_id to all dataframes  
                for df in dataframes:
                    if not df.empty:
                        df['source_player_id'] = player_id
                        df['collection_timestamp'] = datetime.now().isoformat()
                        all_dataframes.append(df)
                
                total_rows = sum(len(df) for df in dataframes if not df.empty)
                print(f" ✅ {len(dataframes)} tables, {total_rows} rows")
                
                self.successful_collections += 1
                
                # Rate limiting
                time.sleep(0.6)
                
            except Exception as e:
                print(f" ❌ {str(e)}")
                failed_players.append({'player_id': player_id, 'error': str(e)})
                self.failed_collections += 1
                time.sleep(1)
        
        # Combine all data
        if all_dataframes:
            combined_data = pd.concat(all_dataframes, ignore_index=True)
            
            # Save to file
            output_file = f"{self.output_dir}/{endpoint_name.lower()}_players_data.csv"
            combined_data.to_csv(output_file, index=False)
            
            print(f"💾 Saved {len(combined_data):,} rows to {output_file}")
            
            return {
                'endpoint': endpoint_name,
                'total_rows': len(combined_data),
                'successful_players': len(player_ids) - len(failed_players),
                'failed_players': len(failed_players),
                'output_file': output_file,
                'failed_player_details': failed_players
            }
        else:
            print(f"❌ No data collected for {endpoint_name}")
            return None
    
    def collect_league_data(self, endpoint_name, season='2023-24', date_range=None):
        """Collect data for league-based endpoints"""
        print(f"\n🎯 Collecting {endpoint_name} data for season {season}...")
        
        try:
            endpoint_class = self.get_endpoint_class(endpoint_name)
        except AttributeError as e:
            print(f"❌ {e}")
            return None
        
        try:
            # Prepare parameters based on endpoint
            if 'PlayerGameLogs' in endpoint_name:
                if date_range:
                    params = {
                        'season_nullable': season,
                        'date_from_nullable': date_range['from'],
                        'date_to_nullable': date_range['to']
                    }
                else:
                    # Default to current month
                    now = datetime.now()
                    start_of_month = now.replace(day=1).strftime('%m/%d/%Y')
                    params = {
                        'season_nullable': season,
                        'date_from_nullable': start_of_month,
                        'date_to_nullable': now.strftime('%m/%d/%Y')
                    }
            else:
                params = {'season': season} if 'season' in endpoint_class.__init__.__code__.co_varnames else {}
            
            print(f"   Using parameters: {params}")
            
            # Make API call
            instance = endpoint_class(**params)
            dataframes = instance.get_data_frames()
            
            all_dataframes = []
            for df in dataframes:
                if not df.empty:
                    df['source_season'] = season
                    df['collection_timestamp'] = datetime.now().isoformat()
                    all_dataframes.append(df)
            
            total_rows = sum(len(df) for df in dataframes if not df.empty)
            print(f"   ✅ {len(dataframes)} tables, {total_rows:,} rows")
            
            if all_dataframes:
                combined_data = pd.concat(all_dataframes, ignore_index=True)
                
                # Save to file
                output_file = f"{self.output_dir}/{endpoint_name.lower()}_league_data.csv"
                combined_data.to_csv(output_file, index=False)
                
                print(f"💾 Saved {len(combined_data):,} rows to {output_file}")
                
                self.successful_collections += 1
                
                return {
                    'endpoint': endpoint_name,
                    'total_rows': len(combined_data),
                    'output_file': output_file,
                    'season': season
                }
            else:
                print(f"❌ No data collected for {endpoint_name}")
                self.failed_collections += 1
                return None
                
        except Exception as e:
            print(f"❌ Error collecting {endpoint_name}: {str(e)}")
            self.failed_collections += 1
            return None
    
    def run_high_priority_collection(self, test_mode=True):
        """Run collection for high priority endpoints with optional test mode"""
        print("🚀 Starting HIGH PRIORITY Data Collection...")
        print("=" * 60)
        
        if test_mode:
            print("📝 RUNNING IN TEST MODE - Limited data collection")
            max_games = 50
            max_players = 20
        else:
            print("🏭 RUNNING IN PRODUCTION MODE - Full data collection")
            max_games = None
            max_players = None
        
        results = []
        
        # 1. Game-based high priority endpoints
        recent_games = self.master_games.sample(n=min(100 if test_mode else len(self.master_games), len(self.master_games)))
        game_ids = recent_games['GAME_ID'].tolist()
        
        high_priority_game_endpoints = [
            'BoxScoreTraditionalV3',
            'BoxScoreAdvancedV3', 
            'BoxScoreScoringV3',
            'PlayByPlayV3'
        ]
        
        for endpoint in high_priority_game_endpoints:
            result = self.collect_game_based_data(game_ids, endpoint, max_games=max_games)
            if result:
                results.append(result)
        
        # 2. Player-based high priority endpoints
        active_players = self.master_players.sample(n=min(50 if test_mode else len(self.master_players), len(self.master_players)))
        player_ids = active_players['PLAYER_ID'].tolist()
        
        high_priority_player_endpoints = [
            'CommonPlayerInfo',
            'PlayerGameLog',
            'PlayerDashboardByShootingSplits'
        ]
        
        for endpoint in high_priority_player_endpoints:
            result = self.collect_player_based_data(player_ids, endpoint, max_players=max_players)
            if result:
                results.append(result)
        
        # 3. League-based high priority endpoints
        league_endpoints = [
            'PlayerGameLogs',
            'LeagueDashPlayerBioStats'
        ]
        
        for endpoint in league_endpoints:
            # For testing, use a small date range
            date_range = {'from': '01/01/2024', 'to': '01/07/2024'} if test_mode else None
            result = self.collect_league_data(endpoint, date_range=date_range)
            if result:
                results.append(result)
        
        # Generate final report
        self.generate_collection_report(results, test_mode)
        
        return results
    
    def generate_collection_report(self, results, test_mode=False):
        """Generate comprehensive collection report"""
        print(f"\n📋 DATA COLLECTION REPORT")
        print("=" * 60)
        
        total_endpoints = len(results)
        total_rows = sum(r.get('total_rows', 0) for r in results)
        
        mode_text = "TEST MODE" if test_mode else "PRODUCTION MODE"
        print(f"🎯 {mode_text} SUMMARY:")
        print(f"   Endpoints processed: {total_endpoints}")
        print(f"   Successful collections: {self.successful_collections}")
        print(f"   Failed collections: {self.failed_collections}")
        print(f"   Total data rows collected: {total_rows:,}")
        
        if results:
            print(f"\n📊 DETAILED RESULTS:")
            for result in results:
                print(f"   ✅ {result['endpoint']}: {result['total_rows']:,} rows → {result['output_file']}")
        
        # Save summary
        summary = {
            'collection_timestamp': datetime.now().isoformat(),
            'mode': mode_text,
            'total_endpoints': total_endpoints,
            'successful_collections': self.successful_collections,
            'failed_collections': self.failed_collections,
            'total_rows': total_rows,
            'results': results
        }
        
        summary_file = f"{self.output_dir}/collection_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n💾 Summary saved to: {summary_file}")
        print(f"📁 All data files saved to: {self.output_dir}/")


def main():
    """Main collection function"""
    collector = ProductionDataCollector()
    
    print("Select collection mode:")
    print("1. TEST MODE - Limited data collection (recommended for first run)")
    print("2. PRODUCTION MODE - Full data collection")
    
    choice = input("Enter choice (1 or 2): ").strip()
    test_mode = (choice != "2")
    
    results = collector.run_high_priority_collection(test_mode=test_mode)
    
    if results:
        print(f"\n🎉 Collection completed successfully!")
        print(f"📈 Next steps:")
        print(f"   1. Review the data files in 'production_data/' folder")
        print(f"   2. Validate data quality and completeness")
        print(f"   3. Set up database integration")
        print(f"   4. Schedule automated runs")
    else:
        print(f"\n⚠️ No data was collected. Check the error messages above.")


if __name__ == "__main__":
    main()
