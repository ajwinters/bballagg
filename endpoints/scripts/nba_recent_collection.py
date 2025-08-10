#!/usr/bin/env python3
"""
NBA Recent Games Collection Test

Collect data from recent NBA games only to test the full collection process
without overwhelming the API or taking too long.
"""

import pandas as pd
import time
import os
import json
from datetime import datetime
from nba_api.stats import endpoints

def collect_recent_nba_games(max_games=50):
    """Collect data from recent NBA games"""
    
    print("🏀 Starting NBA Recent Games Collection Test...")
    print("=" * 60)
    
    # Load NBA games only from recent period
    master_games = pd.read_csv('masters/data/comprehensive/games.csv', dtype={'GAME_ID': str})
    nba_games = master_games[master_games['league_name'] == 'NBA'].head(max_games)
    
    print(f"Collecting data for {len(nba_games)} recent NBA games")
    print(f"Date range: {nba_games['GAME_DATE'].min()} to {nba_games['GAME_DATE'].max()}")
    
    # Successful game-based endpoints 
    endpoints_config = [
        ('BoxScoreAdvancedV3', 'Advanced box score statistics'),
        ('BoxScoreFourFactorsV3', 'Four factors analytics'),
        ('BoxScoreMiscV3', 'Miscellaneous statistics'),
        ('BoxScorePlayerTrackV3', 'Player tracking data'),
        ('BoxScoreScoringV3', 'Detailed scoring statistics'),
        ('BoxScoreTraditionalV3', 'Traditional box score'),
        ('BoxScoreUsageV3', 'Usage rate statistics')
    ]
    
    # Create output directory
    output_dir = 'endpoints/data/nba_recent_collection'
    os.makedirs(output_dir, exist_ok=True)
    
    collection_results = {}
    overall_start_time = datetime.now()
    
    for endpoint_name, description in endpoints_config:
        print(f"\n📊 Collecting {endpoint_name}: {description}")
        endpoint_start = datetime.now()
        endpoint_data = []
        successful_games = 0
        failed_games = 0
        
        try:
            endpoint_class = getattr(endpoints, endpoint_name)
            
            for i, (_, game_row) in enumerate(nba_games.iterrows()):
                game_id = game_row['GAME_ID']
                
                try:
                    # Progress indicator
                    if i % 10 == 0 or i == len(nba_games) - 1:
                        print(f"   Progress: {i+1}/{len(nba_games)} games ({(i+1)/len(nba_games)*100:.1f}%)")
                    
                    # Make API call
                    instance = endpoint_class(game_id=game_id)
                    dataframes = instance.get_data_frames()
                    
                    # Process each dataframe
                    for df_idx, df in enumerate(dataframes):
                        if not df.empty:
                            # Add metadata
                            df['game_id'] = game_id
                            df['game_date'] = game_row['GAME_DATE']
                            df['endpoint'] = endpoint_name
                            df['dataframe_index'] = df_idx
                            df['collection_timestamp'] = datetime.now().isoformat()
                            
                            endpoint_data.append(df)
                    
                    successful_games += 1
                    
                    # Rate limiting
                    time.sleep(0.6)
                    
                except Exception as e:
                    failed_games += 1
                    if failed_games <= 3:  # Only show first few errors
                        print(f"     ❌ Failed game {game_id}: {str(e)[:100]}...")
                    continue
            
            # Save collected data
            if endpoint_data:
                combined_df = pd.concat(endpoint_data, ignore_index=True)
                output_file = f"{output_dir}/{endpoint_name.lower()}_collection.csv"
                combined_df.to_csv(output_file, index=False)
                
                endpoint_duration = (datetime.now() - endpoint_start).total_seconds()
                
                collection_results[endpoint_name] = {
                    'status': 'SUCCESS',
                    'games_attempted': len(nba_games),
                    'games_successful': successful_games,
                    'games_failed': failed_games,
                    'success_rate': successful_games / len(nba_games) * 100,
                    'total_rows': len(combined_df),
                    'columns': len(combined_df.columns),
                    'duration_seconds': endpoint_duration,
                    'rows_per_second': len(combined_df) / endpoint_duration,
                    'output_file': output_file
                }
                
                print(f"   ✅ SUCCESS: {len(combined_df):,} rows, {successful_games}/{len(nba_games)} games ({successful_games/len(nba_games)*100:.1f}%)")
            else:
                print(f"   ⚠️  No data collected")
                collection_results[endpoint_name] = {
                    'status': 'NO_DATA',
                    'games_attempted': len(nba_games),
                    'games_failed': failed_games
                }
                
        except Exception as e:
            print(f"   ❌ FAILED: {str(e)}")
            collection_results[endpoint_name] = {
                'status': 'FAILED',
                'error': str(e)
            }
    
    # Generate comprehensive summary
    overall_duration = (datetime.now() - overall_start_time).total_seconds()
    
    print(f"\n📋 NBA RECENT GAMES COLLECTION SUMMARY")
    print("=" * 60)
    
    successful_endpoints = [k for k, v in collection_results.items() if v.get('status') == 'SUCCESS']
    failed_endpoints = [k for k, v in collection_results.items() if v.get('status') not in ['SUCCESS', 'NO_DATA']]
    
    print(f"⏱️  Total collection time: {overall_duration/60:.1f} minutes")
    print(f"✅ Successful endpoints: {len(successful_endpoints)}/{len(endpoints_config)}")
    print(f"❌ Failed endpoints: {len(failed_endpoints)}")
    
    if successful_endpoints:
        total_rows = sum(collection_results[ep]['total_rows'] for ep in successful_endpoints)
        avg_success_rate = sum(collection_results[ep]['success_rate'] for ep in successful_endpoints) / len(successful_endpoints)
        
        print(f"📊 Total data rows collected: {total_rows:,}")
        print(f"📈 Average success rate per endpoint: {avg_success_rate:.1f}%")
        print(f"⚡ Collection rate: {total_rows/overall_duration:.1f} rows/second")
        
        print(f"\n📈 Endpoint Performance:")
        for endpoint in successful_endpoints:
            result = collection_results[endpoint]
            print(f"   {endpoint}: {result['total_rows']:,} rows ({result['success_rate']:.1f}% success)")
    
    if failed_endpoints:
        print(f"\n❌ Failed Endpoints:")
        for endpoint in failed_endpoints:
            error = collection_results[endpoint].get('error', 'Unknown error')
            print(f"   {endpoint}: {error}")
    
    # Save comprehensive summary
    summary_file = f"{output_dir}/nba_recent_collection_summary.json"
    with open(summary_file, 'w') as f:
        json.dump({
            'collection_info': {
                'games_collected': len(nba_games),
                'date_range': {
                    'start': nba_games['GAME_DATE'].min(),
                    'end': nba_games['GAME_DATE'].max()
                },
                'total_duration_minutes': overall_duration/60,
                'timestamp': datetime.now().isoformat()
            },
            'results': collection_results
        }, f, indent=2)
    
    print(f"\n💾 Summary saved: {summary_file}")
    print(f"📁 Data files saved: {output_dir}/")
    
    # Provide next steps
    if len(successful_endpoints) >= 5:
        estimated_full_time = (overall_duration / len(nba_games)) * 5213 / 60  # Full NBA games
        print(f"\n🚀 NEXT STEPS:")
        print(f"   Current performance suggests full NBA collection would take ~{estimated_full_time/60:.1f} hours")
        print(f"   Consider running production collection in batches of 500-1000 games")
    
    print(f"\n🎉 NBA Recent Games Collection Complete!")


if __name__ == "__main__":
    collect_recent_nba_games(max_games=50)  # Test with 50 games
