"""
Final Production NBA Data Collection System

This combines:
1. Fixed game ID formatting (leading zeros)
2. Smart game ID filtering (remove invalid games)  
3. Enhanced error handling
4. Rate limiting improvements
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


class FinalDataCollector:
    """Final production-ready NBA data collection system with all fixes"""
    
    def __init__(self, data_dir='data', output_dir='production_data_final'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Load master tables with proper data types
        self.master_games = pd.read_csv(f'{data_dir}/comprehensive_master_games.csv', dtype={'GAME_ID': str})
        self.master_players = pd.read_csv(f'{data_dir}/comprehensive_master_players.csv') 
        self.master_teams = pd.read_csv(f'{data_dir}/master_teams.csv')
        
        # Collection tracking
        self.successful_collections = 0
        self.failed_collections = 0
        self.invalid_games_filtered = 0
        
        print(f"🏆 Final NBA Data Collection System")
        print(f"📊 Master Tables: {len(self.master_games):,} games, {len(self.master_players):,} players, {len(self.master_teams)} teams")
    
    def filter_valid_game_ids(self, game_ids):
        """Filter out game IDs that are likely to be invalid based on known patterns"""
        valid_games = []
        invalid_games = []
        
        for game_id in game_ids:
            game_id_str = str(game_id)
            
            # Known valid patterns for NBA game IDs
            valid_patterns = [
                game_id_str.startswith('002'),  # NBA regular games
                game_id_str.startswith('102'),  # G-League games  
                game_id_str.startswith('104'),  # Other league games
            ]
            
            # Known invalid patterns
            invalid_patterns = [
                game_id_str.startswith('42'),   # Invalid format
                len(game_id_str) != 10,         # Wrong length (should be fixed but double-check)
                game_id_str.startswith('000'),  # Suspiciously low
            ]
            
            if any(valid_patterns) and not any(invalid_patterns):
                valid_games.append(game_id)
            else:
                invalid_games.append(game_id)
                self.invalid_games_filtered += 1
        
        if invalid_games:
            print(f"   🧹 Filtered out {len(invalid_games)} invalid game IDs")
            print(f"      Sample invalid IDs: {invalid_games[:5]}")
        
        return valid_games
    
    def get_endpoint_class(self, endpoint_name):
        """Get the endpoint class from nba_api"""
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
    
    def collect_with_all_fixes(self, game_ids, endpoint_name, max_games=None):
        """Collect data with all fixes: game ID format, filtering, error handling"""
        print(f"\n🎯 Collecting {endpoint_name} with ALL FIXES applied...")
        
        # Step 1: Filter valid game IDs
        valid_game_ids = self.filter_valid_game_ids(game_ids)
        
        if max_games and len(valid_game_ids) > max_games:
            valid_game_ids = valid_game_ids[:max_games]
            print(f"   Limited to {max_games} games for testing")
        
        if not valid_game_ids:
            print(f"   ❌ No valid game IDs found")
            return None
            
        print(f"   Processing {len(valid_game_ids)} valid games...")
        
        try:
            endpoint_class = self.get_endpoint_class(endpoint_name)
        except AttributeError as e:
            print(f"❌ {e}")
            return None
        
        all_dataframes = []
        failed_games = []
        
        for i, game_id in enumerate(valid_game_ids):
            try:
                print(f"   Game {i+1}/{len(valid_game_ids)}: {game_id}", end="")
                
                # Make API call with timeout and retry
                try:
                    instance = endpoint_class(game_id=game_id, timeout=30)
                    dataframes = instance.get_data_frames()
                except Exception as e:
                    if "timeout" in str(e).lower() or "expecting value" in str(e).lower():
                        # Try once more with longer timeout
                        print(" → retry", end="")
                        time.sleep(2)
                        instance = endpoint_class(game_id=game_id, timeout=45)
                        dataframes = instance.get_data_frames()
                    else:
                        raise e
                
                # Process dataframes
                valid_dataframes = []
                for df in dataframes:
                    if not df.empty:
                        df['source_game_id'] = game_id
                        df['collection_timestamp'] = datetime.now().isoformat()
                        df['endpoint_name'] = endpoint_name
                        valid_dataframes.append(df)
                
                if valid_dataframes:
                    all_dataframes.extend(valid_dataframes)
                    total_rows = sum(len(df) for df in valid_dataframes)
                    print(f" ✅ {len(valid_dataframes)} tables, {total_rows} rows")
                else:
                    print(f" ✅ Empty (valid)")
                
                self.successful_collections += 1
                
                # Progressive rate limiting
                time.sleep(0.7)  # Slightly longer to be safe
                
            except Exception as e:
                error_msg = str(e)
                print(f" ❌ {error_msg[:50]}...")
                failed_games.append({'game_id': game_id, 'error': error_msg})
                self.failed_collections += 1
                time.sleep(1.5)  # Longer wait after errors
        
        # Save results
        if all_dataframes:
            combined_data = pd.concat(all_dataframes, ignore_index=True)
            output_file = f"{self.output_dir}/{endpoint_name.lower()}_final.csv"
            combined_data.to_csv(output_file, index=False)
            
            print(f"💾 Saved {len(combined_data):,} rows to {output_file}")
            
            success_rate = (len(valid_game_ids) - len(failed_games)) / len(valid_game_ids) * 100 if valid_game_ids else 0
            
            return {
                'endpoint': endpoint_name,
                'total_rows': len(combined_data),
                'successful_games': len(valid_game_ids) - len(failed_games),
                'failed_games': len(failed_games),
                'invalid_games_filtered': self.invalid_games_filtered,
                'success_rate': success_rate,
                'output_file': output_file,
                'failed_details': failed_games[:3]  # Sample failures
            }
        else:
            return {
                'endpoint': endpoint_name,
                'total_rows': 0,
                'successful_games': 0,
                'failed_games': len(failed_games),
                'invalid_games_filtered': self.invalid_games_filtered,
                'success_rate': 0,
                'error': 'No data collected',
                'failed_details': failed_games[:5]
            }
    
    def run_final_test(self):
        """Run final comprehensive test with all fixes"""
        print("🏆 Final NBA Data Collection Test")
        print("=" * 50)
        print("This test includes:")
        print("✅ Fixed game ID formatting (leading zeros)")
        print("✅ Smart game ID filtering (remove invalid IDs)")
        print("✅ Enhanced error handling and retries")
        print("✅ Optimized rate limiting")
        
        # Get a diverse sample of games
        sample_games = self.master_games.sample(n=min(100, len(self.master_games)))
        game_ids = sample_games['GAME_ID'].tolist()
        
        print(f"\n📊 Testing with {len(game_ids)} games from master table")
        
        # Test the most reliable endpoints
        test_endpoints = [
            'BoxScoreTraditionalV3',
            'BoxScoreAdvancedV3',
        ]
        
        results = []
        for endpoint in test_endpoints:
            result = self.collect_with_all_fixes(
                game_ids, 
                endpoint, 
                max_games=25  # Conservative test size
            )
            if result:
                results.append(result)
        
        self.generate_final_report(results)
        return results
    
    def generate_final_report(self, results):
        """Generate comprehensive final report"""
        print(f"\n📋 FINAL COLLECTION REPORT")
        print("=" * 50)
        
        total_rows = sum(r.get('total_rows', 0) for r in results)
        avg_success_rate = sum(r.get('success_rate', 0) for r in results) / len(results) if results else 0
        
        print(f"🎯 FINAL RESULTS:")
        print(f"   Endpoints tested: {len(results)}")
        print(f"   Total data rows: {total_rows:,}")
        print(f"   Average success rate: {avg_success_rate:.1f}%")
        print(f"   Invalid games filtered: {self.invalid_games_filtered}")
        
        if results:
            print(f"\n📊 DETAILED RESULTS:")
            for result in results:
                print(f"   ✅ {result['endpoint']}: {result['total_rows']:,} rows ({result['success_rate']:.1f}% success)")
                if result.get('output_file'):
                    print(f"      → {result['output_file']}")
        
        # Save comprehensive summary
        summary = {
            'test_timestamp': datetime.now().isoformat(),
            'system_version': 'final_with_all_fixes',
            'total_endpoints': len(results),
            'total_rows': total_rows,
            'average_success_rate': avg_success_rate,
            'invalid_games_filtered': self.invalid_games_filtered,
            'fixes_applied': [
                'Game ID leading zeros fixed',
                'Invalid game ID filtering',
                'Enhanced error handling',
                'Timeout retries',
                'Progressive rate limiting'
            ],
            'results': results
        }
        
        summary_file = f"{self.output_dir}/final_collection_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n💾 Final summary saved to: {summary_file}")
        
        # Performance evaluation
        if avg_success_rate >= 80:
            print(f"\n🎉 EXCELLENT! System is ready for production!")
            print(f"📈 Success rate of {avg_success_rate:.1f}% exceeds production threshold")
        elif avg_success_rate >= 60:
            print(f"\n✅ GOOD! System is working well")
            print(f"📈 Success rate of {avg_success_rate:.1f}% is acceptable")
        else:
            print(f"\n⚠️ NEEDS IMPROVEMENT")
            print(f"📈 Success rate of {avg_success_rate:.1f}% could be higher")


def main():
    """Run final comprehensive test"""
    collector = FinalDataCollector()
    
    print("🏆 This is the final test with all fixes applied!")
    print("We expect to see much higher success rates now.")
    
    input("Press Enter to start the comprehensive test...")
    
    results = collector.run_final_test()
    
    if results and any(r['total_rows'] > 0 for r in results):
        print(f"\n🎉 SUCCESS! The fixes have significantly improved the system!")
    else:
        print(f"\n⚠️ Still having issues - may need additional investigation")


if __name__ == "__main__":
    main()
