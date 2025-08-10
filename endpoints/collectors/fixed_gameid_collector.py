"""
Fixed NBA Data Collection System - Game ID Leading Zero Fix

This version properly formats game IDs by adding leading zeros where needed.
NBA game IDs should be 10 digits with leading zeros preserved.
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


class FixedGameIdDataCollector:
    """NBA data collector with proper game ID formatting (leading zeros fix)"""
    
    def __init__(self, data_dir='data', output_dir='production_data_fixed'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Load master tables
        self.master_games = pd.read_csv(f'{data_dir}/comprehensive_master_games.csv')
        self.master_players = pd.read_csv(f'{data_dir}/comprehensive_master_players.csv') 
        self.master_teams = pd.read_csv(f'{data_dir}/master_teams.csv')
        
        # Collection tracking
        self.successful_collections = 0
        self.failed_collections = 0
        self.game_id_fixes = 0
        
        print(f"🔧 Fixed NBA Data Collection System Initialized")
        print(f"📊 Master Tables: {len(self.master_games):,} games, {len(self.master_players):,} players, {len(self.master_teams)} teams")
    
    def fix_game_id_format(self, game_id):
        """Fix game ID by adding leading zeros to make it 10 digits"""
        game_id_str = str(game_id)
        original_length = len(game_id_str)
        
        # NBA game IDs should be 10 digits
        if original_length < 10:
            # Add leading zeros to make it 10 digits
            fixed_game_id = game_id_str.zfill(10)
            self.game_id_fixes += 1
            return fixed_game_id, f"Added {10 - original_length} leading zeros"
        elif original_length == 10:
            return game_id_str, "Already correct length"
        else:
            # Some game IDs might be longer (different leagues)
            return game_id_str, "Longer than standard (kept as-is)"
    
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
    
    def collect_with_fixed_game_ids(self, game_ids, endpoint_name, max_games=None):
        """Collect data with properly formatted game IDs"""
        print(f"\n🎯 Collecting {endpoint_name} data with FIXED game IDs...")
        
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
        fixed_games = []
        
        for i, game_id in enumerate(game_ids):
            # Fix the game ID format
            fixed_game_id, fix_reason = self.fix_game_id_format(game_id)
            
            if "leading zeros" in fix_reason:
                fixed_games.append({'original': game_id, 'fixed': fixed_game_id, 'reason': fix_reason})
            
            try:
                print(f"   Processing game {i+1}/{len(game_ids)}: {game_id} -> {fixed_game_id}", end="")
                
                # Make API call with fixed game ID
                instance = endpoint_class(game_id=fixed_game_id)
                dataframes = instance.get_data_frames()
                
                # Process dataframes
                valid_dataframes = []
                for df in dataframes:
                    if not df.empty:
                        df['source_game_id'] = fixed_game_id
                        df['original_game_id'] = game_id
                        df['collection_timestamp'] = datetime.now().isoformat()
                        valid_dataframes.append(df)
                
                if valid_dataframes:
                    all_dataframes.extend(valid_dataframes)
                    total_rows = sum(len(df) for df in valid_dataframes)
                    print(f" ✅ {len(valid_dataframes)} tables, {total_rows} rows")
                else:
                    print(f" ✅ Empty result (valid)")
                
                self.successful_collections += 1
                time.sleep(0.6)  # Rate limiting
                
            except Exception as e:
                error_msg = str(e)
                print(f" ❌ {error_msg}")
                failed_games.append({
                    'original_game_id': game_id, 
                    'fixed_game_id': fixed_game_id, 
                    'error': error_msg
                })
                self.failed_collections += 1
                time.sleep(1)
        
        # Save results
        if all_dataframes:
            combined_data = pd.concat(all_dataframes, ignore_index=True)
            output_file = f"{self.output_dir}/{endpoint_name.lower()}_fixed_gameids.csv"
            combined_data.to_csv(output_file, index=False)
            
            print(f"💾 Saved {len(combined_data):,} rows to {output_file}")
            
            return {
                'endpoint': endpoint_name,
                'total_rows': len(combined_data),
                'successful_games': len(game_ids) - len(failed_games),
                'failed_games': len(failed_games),
                'game_id_fixes': len(fixed_games),
                'output_file': output_file,
                'sample_fixes': fixed_games[:5],
                'failed_details': failed_games[:5]
            }
        else:
            return {
                'endpoint': endpoint_name,
                'total_rows': 0,
                'successful_games': 0,
                'failed_games': len(failed_games),
                'game_id_fixes': len(fixed_games),
                'error': 'No data collected',
                'sample_fixes': fixed_games[:5],
                'failed_details': failed_games[:5]
            }
    
    def run_game_id_fix_test(self):
        """Test the game ID fix with a focused sample"""
        print("🔧 Testing Game ID Fix...")
        print("=" * 50)
        
        # Get a mix of games that we know had issues
        problematic_games = [22100684, 22300121, 22100306, 22000675, 22000150]  # 8-digit games
        working_games = [2022300449, 2022200075, 1022001035]  # 10-digit games  
        
        test_games = problematic_games + working_games
        
        print(f"Testing with {len(test_games)} games:")
        print("Problematic games (8-digit):", problematic_games)
        print("Working games (10-digit):", working_games)
        
        # Test with BoxScoreTraditionalV3 (known to work)
        result = self.collect_with_fixed_game_ids(
            test_games, 
            'BoxScoreTraditionalV3', 
            max_games=len(test_games)
        )
        
        # Generate report
        self.generate_fix_test_report(result)
        return result
    
    def generate_fix_test_report(self, result):
        """Generate report specifically for the game ID fix test"""
        print(f"\n📋 GAME ID FIX TEST REPORT")
        print("=" * 50)
        
        if result:
            success_rate = (result['successful_games'] / (result['successful_games'] + result['failed_games']) * 100) if (result['successful_games'] + result['failed_games']) > 0 else 0
            
            print(f"🎯 RESULTS:")
            print(f"   Total data rows: {result['total_rows']:,}")
            print(f"   Successful games: {result['successful_games']}")
            print(f"   Failed games: {result['failed_games']}")
            print(f"   Success rate: {success_rate:.1f}%")
            print(f"   Game IDs fixed: {result['game_id_fixes']}")
            
            if result['sample_fixes']:
                print(f"\n🔧 SAMPLE GAME ID FIXES:")
                for fix in result['sample_fixes']:
                    print(f"   {fix['original']} -> {fix['fixed']} ({fix['reason']})")
            
            if result['failed_details']:
                print(f"\n❌ REMAINING FAILURES:")
                for failure in result['failed_details']:
                    print(f"   {failure['original_game_id']} -> {failure['fixed_game_id']}: {failure['error']}")
            
            if result.get('output_file'):
                print(f"\n💾 Data saved to: {result['output_file']}")
        
        # Save summary
        summary = {
            'test_timestamp': datetime.now().isoformat(),
            'total_game_id_fixes': self.game_id_fixes,
            'test_results': result
        }
        
        summary_file = f"{self.output_dir}/game_id_fix_test_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"📄 Test summary saved to: {summary_file}")


def main():
    """Test the game ID fix"""
    collector = FixedGameIdDataCollector()
    
    print("🔧 This will test the game ID leading zero fix")
    print("We'll test with games that were failing before due to missing leading zeros")
    
    input("Press Enter to start the test...")
    
    result = collector.run_game_id_fix_test()
    
    if result and result['total_rows'] > 0:
        print(f"\n🎉 SUCCESS! The game ID fix is working!")
        print(f"📈 We should see a significant improvement in success rates.")
        print(f"🔧 {collector.game_id_fixes} game IDs were fixed with leading zeros.")
    else:
        print(f"\n⚠️ The fix needs more work. Check the error details above.")


if __name__ == "__main__":
    main()
