"""
Improved Production NBA Data Collection System with Enhanced Error Handling

This version includes robust error handling for common API issues:
- Empty dataframes (list index out of range)
- JSON parsing errors (rate limiting, timeouts)
- Game ID format validation
- Exponential backoff retry logic
"""

import pandas as pd
import time
import os
import json
import sys
from datetime import datetime, timedelta
from nba_api.stats import endpoints
import requests
from requests.exceptions import RequestException

# Add parent directory to path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.nba_endpoints_config import ALL_ENDPOINTS


class ImprovedProductionDataCollector:
    """Enhanced production-ready NBA data collection system with robust error handling"""
    
    def __init__(self, data_dir='data', output_dir='production_data_improved'):
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
        self.skipped_collections = 0
        
        # Error categorization
        self.error_stats = {
            'json_errors': 0,
            'empty_data_errors': 0,
            'timeout_errors': 0,
            'invalid_game_errors': 0,
            'other_errors': 0
        }
        
        print(f"🏗️ Improved NBA Data Collection System Initialized")
        print(f"📊 Master Tables: {len(self.master_games):,} games, {len(self.master_players):,} players, {len(self.master_teams)} teams")
    
    def validate_game_id(self, game_id):
        """Validate game ID format and convert to proper string format"""
        try:
            game_id_str = str(game_id)
            
            # Check if it's a reasonable length (NBA game IDs are typically 10 digits)
            if len(game_id_str) < 8 or len(game_id_str) > 15:
                return None, f"Invalid game ID length: {len(game_id_str)}"
            
            # Ensure it starts with expected prefixes for different leagues
            valid_prefixes = ['002', '1002', '1042', '2022', '22', '0', '1']
            if not any(game_id_str.startswith(prefix) for prefix in valid_prefixes):
                return None, f"Invalid game ID prefix: {game_id_str[:4]}"
            
            return game_id_str, None
            
        except Exception as e:
            return None, f"Error validating game ID: {str(e)}"
    
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
    
    def make_api_call_with_retry(self, endpoint_class, params, max_retries=3):
        """Make API call with exponential backoff retry logic"""
        
        for attempt in range(max_retries):
            try:
                print(f"      Attempt {attempt + 1}/{max_retries}", end="")
                
                # Make API call
                instance = endpoint_class(**params)
                dataframes = instance.get_data_frames()
                
                # Validate that we got data
                if not dataframes:
                    raise ValueError("No dataframes returned from API")
                
                # Check if all dataframes are empty
                non_empty_dfs = [df for df in dataframes if not df.empty]
                if not non_empty_dfs and len(dataframes) > 0:
                    # This is valid - some games/players might have no data
                    return dataframes, "success_empty"
                
                return dataframes, "success"
                
            except json.JSONDecodeError as e:
                self.error_stats['json_errors'] += 1
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 1  # Exponential backoff: 1, 2, 4 seconds
                    print(f" → JSON error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise e
                    
            except RequestException as e:
                self.error_stats['timeout_errors'] += 1
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 2  # Longer wait for network issues
                    print(f" → Network error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise e
                    
            except Exception as e:
                if "expecting value" in str(e).lower():
                    self.error_stats['json_errors'] += 1
                elif "list index out of range" in str(e).lower():
                    self.error_stats['empty_data_errors'] += 1
                else:
                    self.error_stats['other_errors'] += 1
                
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 1
                    print(f" → {str(e)[:50]}..., retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise e
        
        raise Exception("Max retries exceeded")
    
    def collect_game_based_data_improved(self, game_ids, endpoint_name, priority='high', max_games=None):
        """Improved game-based data collection with robust error handling"""
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
        skipped_games = []
        
        for i, game_id in enumerate(game_ids):
            # Validate game ID first
            validated_game_id, validation_error = self.validate_game_id(game_id)
            if validation_error:
                print(f"   Game {i+1}/{len(game_ids)}: {game_id} ⚠️  SKIPPED - {validation_error}")
                skipped_games.append({'game_id': game_id, 'reason': validation_error})
                self.skipped_collections += 1
                continue
            
            try:
                print(f"   Processing game {i+1}/{len(game_ids)}: {validated_game_id}", end="")
                
                # Make API call with retry logic
                dataframes, status = self.make_api_call_with_retry(
                    endpoint_class, 
                    {'game_id': validated_game_id}
                )
                
                if status == "success_empty":
                    print(f" ✅ No data (valid for this game)")
                    self.successful_collections += 1
                    continue
                
                # Process non-empty dataframes
                valid_dataframes = []
                for df in dataframes:
                    if not df.empty:
                        df['source_game_id'] = validated_game_id
                        df['collection_timestamp'] = datetime.now().isoformat()
                        df['endpoint'] = endpoint_name
                        valid_dataframes.append(df)
                
                if valid_dataframes:
                    all_dataframes.extend(valid_dataframes)
                    total_rows = sum(len(df) for df in valid_dataframes)
                    print(f" ✅ {len(valid_dataframes)} tables, {total_rows} rows")
                else:
                    print(f" ✅ Empty result (valid)")
                
                self.successful_collections += 1
                
                # Progressive rate limiting - slower after errors
                base_sleep = 0.6
                if self.error_stats['json_errors'] > 5:
                    base_sleep = 1.2
                elif self.error_stats['timeout_errors'] > 3:
                    base_sleep = 1.0
                
                time.sleep(base_sleep)
                
            except Exception as e:
                error_msg = str(e)
                print(f" ❌ {error_msg[:50]}...")
                failed_games.append({'game_id': validated_game_id, 'error': error_msg})
                self.failed_collections += 1
                
                # Longer wait after failures
                time.sleep(2)
        
        # Combine and save data
        if all_dataframes:
            try:
                combined_data = pd.concat(all_dataframes, ignore_index=True)
                
                # Save to file with error info
                output_file = f"{self.output_dir}/{endpoint_name.lower()}_games_data_improved.csv"
                combined_data.to_csv(output_file, index=False)
                
                print(f"💾 Saved {len(combined_data):,} rows to {output_file}")
                
                return {
                    'endpoint': endpoint_name,
                    'total_rows': len(combined_data),
                    'successful_games': len(game_ids) - len(failed_games) - len(skipped_games),
                    'failed_games': len(failed_games),
                    'skipped_games': len(skipped_games),
                    'output_file': output_file,
                    'failed_game_details': failed_games[:10],  # Limit to first 10 for brevity
                    'skipped_game_details': skipped_games[:10]
                }
            except Exception as e:
                print(f"❌ Error combining data: {str(e)}")
                return None
        else:
            print(f"⚠️  No valid data collected for {endpoint_name}")
            return {
                'endpoint': endpoint_name,
                'total_rows': 0,
                'successful_games': 0,
                'failed_games': len(failed_games),
                'skipped_games': len(skipped_games),
                'error': 'No valid data collected',
                'failed_game_details': failed_games[:10],
                'skipped_game_details': skipped_games[:10]
            }
    
    def run_improved_collection(self, test_mode=True, endpoints_to_test=None):
        """Run improved collection with enhanced error handling"""
        print("🚀 Starting IMPROVED High Priority Data Collection...")
        print("=" * 60)
        
        if test_mode:
            print("📝 RUNNING IN TEST MODE - Limited data collection with enhanced error handling")
            max_games = 25  # Smaller test set
        else:
            print("🏭 RUNNING IN PRODUCTION MODE - Full data collection")
            max_games = None
        
        results = []
        
        # Default high priority game endpoints if none specified
        if endpoints_to_test is None:
            endpoints_to_test = [
                'BoxScoreTraditionalV3',
                'BoxScoreAdvancedV3', 
                'BoxScoreScoringV3'
            ]
        
        # Get a mix of game types - some recent, some older, different leagues
        print(f"\n📊 Selecting diverse game sample...")
        
        # Get a representative sample of games  
        available_games = self.master_games.sample(n=min(50 if test_mode else 200, len(self.master_games)))
        game_ids = available_games['GAME_ID'].tolist()
        
        print(f"   Selected {len(game_ids)} games from master table")
        
        # Show some sample game IDs to understand the format
        print(f"   Sample game IDs: {game_ids[:5]}")
        
        # Test each endpoint
        for endpoint in endpoints_to_test:
            result = self.collect_game_based_data_improved(game_ids, endpoint, max_games=max_games)
            if result:
                results.append(result)
        
        # Generate enhanced report
        self.generate_improved_report(results, test_mode)
        
        return results
    
    def generate_improved_report(self, results, test_mode=False):
        """Generate comprehensive report with error analysis"""
        print(f"\n📋 IMPROVED DATA COLLECTION REPORT")
        print("=" * 60)
        
        total_endpoints = len(results)
        total_rows = sum(r.get('total_rows', 0) for r in results)
        
        mode_text = "TEST MODE" if test_mode else "PRODUCTION MODE"
        print(f"🎯 {mode_text} SUMMARY:")
        print(f"   Endpoints processed: {total_endpoints}")
        print(f"   Successful collections: {self.successful_collections}")
        print(f"   Failed collections: {self.failed_collections}")
        print(f"   Skipped collections: {self.skipped_collections}")
        print(f"   Total data rows collected: {total_rows:,}")
        
        # Error analysis
        print(f"\n🔍 ERROR ANALYSIS:")
        print(f"   JSON/Parsing errors: {self.error_stats['json_errors']}")
        print(f"   Empty data errors: {self.error_stats['empty_data_errors']}")
        print(f"   Timeout errors: {self.error_stats['timeout_errors']}")
        print(f"   Invalid game errors: {self.error_stats['invalid_game_errors']}")
        print(f"   Other errors: {self.error_stats['other_errors']}")
        
        if results:
            print(f"\n📊 DETAILED RESULTS:")
            for result in results:
                success_rate = (result.get('successful_games', 0) / 
                               (result.get('successful_games', 0) + result.get('failed_games', 0) + result.get('skipped_games', 0)) * 100) if (result.get('successful_games', 0) + result.get('failed_games', 0) + result.get('skipped_games', 0)) > 0 else 0
                print(f"   ✅ {result['endpoint']}: {result['total_rows']:,} rows ({success_rate:.1f}% success rate)")
                if result.get('output_file'):
                    print(f"      → {result['output_file']}")
        
        # Save enhanced summary
        summary = {
            'collection_timestamp': datetime.now().isoformat(),
            'mode': mode_text,
            'total_endpoints': total_endpoints,
            'successful_collections': self.successful_collections,
            'failed_collections': self.failed_collections,
            'skipped_collections': self.skipped_collections,
            'total_rows': total_rows,
            'error_statistics': self.error_stats,
            'results': results
        }
        
        summary_file = f"{self.output_dir}/improved_collection_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n💾 Enhanced summary saved to: {summary_file}")
        print(f"📁 All data files saved to: {self.output_dir}/")
        
        # Recommendations based on error patterns
        print(f"\n💡 RECOMMENDATIONS:")
        if self.error_stats['json_errors'] > 10:
            print("   • High JSON errors detected - consider longer rate limiting")
        if self.error_stats['timeout_errors'] > 5:
            print("   • Network timeouts detected - consider retry with longer delays")
        if self.error_stats['empty_data_errors'] > 20:
            print("   • Many empty data responses - consider filtering game IDs by date/league")


def main():
    """Main improved collection function"""
    collector = ImprovedProductionDataCollector()
    
    print("Select collection mode:")
    print("1. TEST MODE - Enhanced error handling test (recommended)")
    print("2. PRODUCTION MODE - Full collection with improvements")
    
    choice = input("Enter choice (1 or 2): ").strip()
    test_mode = (choice != "2")
    
    # Let user choose specific endpoints to test
    print("\nSelect endpoints to test:")
    print("1. Game endpoints only (BoxScore Traditional, Advanced, Scoring)")
    print("2. All high priority endpoints")
    
    endpoint_choice = input("Enter choice (1 or 2): ").strip()
    
    if endpoint_choice == "1":
        endpoints_to_test = ['BoxScoreTraditionalV3', 'BoxScoreAdvancedV3', 'BoxScoreScoringV3']
    else:
        endpoints_to_test = None  # Use default set
    
    results = collector.run_improved_collection(test_mode=test_mode, endpoints_to_test=endpoints_to_test)
    
    if results:
        print(f"\n🎉 Improved collection completed!")
        print(f"📈 The enhanced error handling should significantly reduce failures.")


if __name__ == "__main__":
    main()
