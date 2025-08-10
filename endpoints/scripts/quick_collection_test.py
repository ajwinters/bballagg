#!/usr/bin/env python3
"""
Quick Collection Test

Test our endpoint collection process with a small sample of games
using the endpoints that passed our systematic testing.
"""

import pandas as pd
import time
import os
import json
from datetime import datetime
from nba_api.stats import endpoints

def test_collection():
    """Run a small collection test with verified working endpoints"""
    
    print("🚀 Starting Quick Endpoint Collection Test...")
    print("=" * 60)
    
    # Load a few test games
    master_games = pd.read_csv('masters/data/comprehensive/games.csv', dtype={'GAME_ID': str})
    test_games = master_games['GAME_ID'].head(3).tolist()  # Test with 3 games
    
    print(f"Testing with game IDs: {test_games}")
    
    # Successful game-based endpoints from our systematic test
    successful_endpoints = [
        'BoxScoreAdvancedV3',
        'BoxScoreFourFactorsV3', 
        'BoxScoreMiscV3',
        'BoxScorePlayerTrackV3',
        'BoxScoreScoringV3',
        'BoxScoreSummaryV2',
        'BoxScoreTraditionalV3',
        'BoxScoreUsageV3'
    ]
    
    # Create output directory
    output_dir = 'endpoints/data/test_collection'
    os.makedirs(output_dir, exist_ok=True)
    
    results = []
    
    for endpoint_name in successful_endpoints:
        print(f"\n📊 Collecting {endpoint_name} data...")
        endpoint_data = []
        
        try:
            # Get endpoint class
            endpoint_class = getattr(endpoints, endpoint_name)
            
            for i, game_id in enumerate(test_games):
                try:
                    print(f"   Game {i+1}/3: {game_id}")
                    
                    # Make API call
                    instance = endpoint_class(game_id=game_id)
                    dataframes = instance.get_data_frames()
                    
                    # Combine dataframes if multiple
                    for df_idx, df in enumerate(dataframes):
                        if not df.empty:
                            # Add metadata
                            df['game_id'] = game_id
                            df['endpoint'] = endpoint_name
                            df['dataframe_index'] = df_idx
                            df['collection_timestamp'] = datetime.now().isoformat()
                            
                            endpoint_data.append(df)
                    
                    # Rate limiting
                    time.sleep(0.6)
                    
                except Exception as e:
                    print(f"     ❌ Failed for game {game_id}: {str(e)}")
                    continue
            
            # Save collected data
            if endpoint_data:
                combined_df = pd.concat(endpoint_data, ignore_index=True)
                output_file = f"{output_dir}/{endpoint_name.lower()}_test_collection.csv"
                combined_df.to_csv(output_file, index=False)
                
                result = {
                    'endpoint': endpoint_name,
                    'status': 'SUCCESS',
                    'games_collected': len(test_games),
                    'total_rows': len(combined_df),
                    'columns': len(combined_df.columns),
                    'output_file': output_file
                }
                
                print(f"   ✅ SUCCESS: {len(combined_df):,} rows collected")
                results.append(result)
            else:
                print(f"   ⚠️  No data collected for {endpoint_name}")
                
        except Exception as e:
            print(f"   ❌ FAILED: {str(e)}")
            results.append({
                'endpoint': endpoint_name,
                'status': 'FAILED',
                'error': str(e)
            })
    
    # Generate summary
    print(f"\n📋 COLLECTION TEST SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r.get('status') == 'SUCCESS']
    failed = [r for r in results if r.get('status') == 'FAILED']
    
    print(f"✅ Successful collections: {len(successful)}/{len(results)}")
    print(f"❌ Failed collections: {len(failed)}")
    
    if successful:
        total_rows = sum(r['total_rows'] for r in successful)
        print(f"📊 Total data rows collected: {total_rows:,}")
        
        print(f"\n📈 Successful Endpoints:")
        for result in successful:
            print(f"   {result['endpoint']}: {result['total_rows']:,} rows")
    
    if failed:
        print(f"\n❌ Failed Endpoints:")
        for result in failed:
            print(f"   {result['endpoint']}: {result.get('error', 'Unknown error')}")
    
    # Save summary
    summary_file = f"{output_dir}/collection_test_summary.json"
    with open(summary_file, 'w') as f:
        json.dump({
            'test_games': test_games,
            'results': results,
            'summary': {
                'total_endpoints': len(results),
                'successful': len(successful),
                'failed': len(failed),
                'timestamp': datetime.now().isoformat()
            }
        }, f, indent=2)
    
    print(f"\n💾 Summary saved: {summary_file}")
    print(f"📁 Data files saved: {output_dir}/")
    print(f"\n🎉 Collection test complete!")


if __name__ == "__main__":
    test_collection()
