#!/usr/bin/env python3
"""
Comprehensive NBA Data Collection

Collect data from all 33 successfully tested NBA API endpoints using a sample of recent games.
This will demonstrate the full collection capability of our system.
"""

import pandas as pd
import time
import os
import json
from datetime import datetime
from nba_api.stats import endpoints

def run_comprehensive_collection(max_games=25):
    """Run comprehensive data collection using all successful endpoints"""
    
    print("🚀 Starting COMPREHENSIVE NBA Data Collection...")
    print("=" * 70)
    
    # Load recent NBA games
    master_games = pd.read_csv('masters/data/comprehensive/games.csv', dtype={'GAME_ID': str})
    master_players = pd.read_csv('masters/data/comprehensive/players.csv')
    
    # Select recent NBA games for testing
    nba_games = master_games[master_games['league_name'] == 'NBA'].head(max_games)
    recent_players = master_players.head(10)  # Sample players for player endpoints
    
    print(f"📊 Collection Scope:")
    print(f"   Games: {len(nba_games)} recent NBA games")
    print(f"   Players: {len(recent_players)} sample players")
    print(f"   Date range: {nba_games['GAME_DATE'].min()} to {nba_games['GAME_DATE'].max()}")
    
    # All successful endpoints from our comprehensive test
    successful_endpoints = {
        'game_based': [
            'BoxScoreAdvancedV3', 'BoxScoreAdvancedV2', 'BoxScoreDefensiveV2',
            'BoxScoreFourFactorsV3', 'BoxScoreFourFactorsV2', 'BoxScoreHustleV2',
            'BoxScoreMatchupsV3', 'BoxScoreMiscV3', 'BoxScoreMiscV2',
            'BoxScorePlayerTrackV3', 'BoxScorePlayerTrackV2', 'BoxScoreScoringV3',
            'BoxScoreScoringV2', 'BoxScoreSummaryV2', 'BoxScoreTraditionalV3',
            'BoxScoreTraditionalV2', 'BoxScoreUsageV3', 'BoxScoreUsageV2',
            'GameRotation', 'HustleStatsBoxScore', 'PlayByPlayV3'
        ],
        'player_based': [
            'CommonPlayerInfo', 'PlayerGameLog', 'PlayerDashboardByGameSplits',
            'PlayerDashboardByGeneralSplits', 'PlayerDashboardByShootingSplits',
            'PlayerDashboardByTeamPerformance', 'PlayerDashboardByYearOverYear',
            'PlayerDashboardByClutch', 'PlayerDashboardByLastNGames'
        ],
        'league_based': [
            'PlayerGameLogs', 'LeagueDashPlayerBioStats', 'LeagueGameFinder'
        ]
    }
    
    # Create output directory
    output_dir = 'endpoints/data/comprehensive_collection'
    os.makedirs(output_dir, exist_ok=True)
    
    collection_results = {}
    total_start_time = datetime.now()
    total_rows_collected = 0
    
    # Collect Game-based endpoint data
    print(f"\n🎮 COLLECTING GAME-BASED ENDPOINTS ({len(successful_endpoints['game_based'])} endpoints)")
    print("-" * 70)
    
    for endpoint_name in successful_endpoints['game_based']:
        print(f"\n📊 {endpoint_name}")
        endpoint_start = datetime.now()
        endpoint_data = []
        successful_games = 0
        
        try:
            endpoint_class = getattr(endpoints, endpoint_name)
            
            for i, (_, game_row) in enumerate(nba_games.iterrows()):
                game_id = game_row['GAME_ID']
                
                if i % 5 == 0:
                    print(f"   Progress: {i+1}/{len(nba_games)} games")
                
                try:
                    instance = endpoint_class(game_id=game_id)
                    dataframes = instance.get_data_frames()
                    
                    for df_idx, df in enumerate(dataframes):
                        if not df.empty:
                            df['game_id'] = game_id
                            df['game_date'] = game_row['GAME_DATE']
                            df['endpoint'] = endpoint_name
                            df['dataframe_index'] = df_idx
                            df['collection_timestamp'] = datetime.now().isoformat()
                            endpoint_data.append(df)
                    
                    successful_games += 1
                    time.sleep(0.6)  # Rate limiting
                    
                except Exception as e:
                    continue
            
            if endpoint_data:
                combined_df = pd.concat(endpoint_data, ignore_index=True)
                output_file = f"{output_dir}/{endpoint_name.lower()}_data.csv"
                combined_df.to_csv(output_file, index=False)
                
                collection_results[endpoint_name] = {
                    'category': 'game_based',
                    'status': 'SUCCESS',
                    'rows': len(combined_df),
                    'games_successful': successful_games,
                    'duration': (datetime.now() - endpoint_start).total_seconds()
                }
                
                total_rows_collected += len(combined_df)
                print(f"   ✅ {len(combined_df):,} rows ({successful_games}/{len(nba_games)} games)")
            
        except Exception as e:
            collection_results[endpoint_name] = {'status': 'FAILED', 'error': str(e)}
            print(f"   ❌ FAILED: {str(e)[:50]}...")
    
    # Collect Player-based endpoint data  
    print(f"\n👤 COLLECTING PLAYER-BASED ENDPOINTS ({len(successful_endpoints['player_based'])} endpoints)")
    print("-" * 70)
    
    for endpoint_name in successful_endpoints['player_based']:
        print(f"\n📊 {endpoint_name}")
        endpoint_start = datetime.now()
        endpoint_data = []
        successful_players = 0
        
        try:
            endpoint_class = getattr(endpoints, endpoint_name)
            
            for i, (_, player_row) in enumerate(recent_players.iterrows()):
                player_id = str(player_row['PLAYER_ID'])
                
                try:
                    instance = endpoint_class(player_id=player_id)
                    dataframes = instance.get_data_frames()
                    
                    for df_idx, df in enumerate(dataframes):
                        if not df.empty:
                            df['player_id'] = player_id
                            df['endpoint'] = endpoint_name
                            df['dataframe_index'] = df_idx
                            df['collection_timestamp'] = datetime.now().isoformat()
                            endpoint_data.append(df)
                    
                    successful_players += 1
                    time.sleep(0.6)
                    
                except Exception as e:
                    continue
            
            if endpoint_data:
                combined_df = pd.concat(endpoint_data, ignore_index=True)
                output_file = f"{output_dir}/{endpoint_name.lower()}_data.csv"
                combined_df.to_csv(output_file, index=False)
                
                collection_results[endpoint_name] = {
                    'category': 'player_based',
                    'status': 'SUCCESS',
                    'rows': len(combined_df),
                    'players_successful': successful_players,
                    'duration': (datetime.now() - endpoint_start).total_seconds()
                }
                
                total_rows_collected += len(combined_df)
                print(f"   ✅ {len(combined_df):,} rows ({successful_players}/{len(recent_players)} players)")
            
        except Exception as e:
            collection_results[endpoint_name] = {'status': 'FAILED', 'error': str(e)}
            print(f"   ❌ FAILED: {str(e)[:50]}...")
    
    # Collect League-based endpoint data
    print(f"\n🏀 COLLECTING LEAGUE-BASED ENDPOINTS ({len(successful_endpoints['league_based'])} endpoints)")
    print("-" * 70)
    
    for endpoint_name in successful_endpoints['league_based']:
        print(f"\n📊 {endpoint_name}")
        endpoint_start = datetime.now()
        
        try:
            endpoint_class = getattr(endpoints, endpoint_name)
            
            if endpoint_name == 'PlayerGameLogs':
                # Limited date range for testing
                instance = endpoint_class(
                    season_nullable='2023-24',
                    date_from_nullable='04/01/2024',
                    date_to_nullable='04/14/2024'  # Recent 2 weeks
                )
            else:
                instance = endpoint_class()
            
            dataframes = instance.get_data_frames()
            
            if dataframes and not dataframes[0].empty:
                df = dataframes[0]
                df['endpoint'] = endpoint_name
                df['collection_timestamp'] = datetime.now().isoformat()
                
                output_file = f"{output_dir}/{endpoint_name.lower()}_data.csv"
                df.to_csv(output_file, index=False)
                
                collection_results[endpoint_name] = {
                    'category': 'league_based',
                    'status': 'SUCCESS',
                    'rows': len(df),
                    'duration': (datetime.now() - endpoint_start).total_seconds()
                }
                
                total_rows_collected += len(df)
                print(f"   ✅ {len(df):,} rows")
            
        except Exception as e:
            collection_results[endpoint_name] = {'status': 'FAILED', 'error': str(e)}
            print(f"   ❌ FAILED: {str(e)[:50]}...")
        
        time.sleep(0.6)
    
    # Generate comprehensive summary
    total_duration = (datetime.now() - total_start_time).total_seconds()
    
    print(f"\n📋 COMPREHENSIVE COLLECTION SUMMARY")
    print("=" * 70)
    
    successful_endpoints_list = [k for k, v in collection_results.items() if v.get('status') == 'SUCCESS']
    failed_endpoints_list = [k for k, v in collection_results.items() if v.get('status') == 'FAILED']
    
    print(f"⏱️  Total collection time: {total_duration/60:.1f} minutes")
    print(f"📊 Total data rows collected: {total_rows_collected:,}")
    print(f"✅ Successful endpoints: {len(successful_endpoints_list)}/33")
    print(f"❌ Failed endpoints: {len(failed_endpoints_list)}")
    print(f"⚡ Collection rate: {total_rows_collected/total_duration:.1f} rows/second")
    
    # Category breakdown
    for category in ['game_based', 'player_based', 'league_based']:
        category_endpoints = [k for k, v in collection_results.items() if v.get('category') == category and v.get('status') == 'SUCCESS']
        category_rows = sum(collection_results[k]['rows'] for k in category_endpoints)
        
        if category_endpoints:
            print(f"\n📈 {category.upper()} RESULTS:")
            print(f"   Successful endpoints: {len(category_endpoints)}")
            print(f"   Total rows: {category_rows:,}")
            
            # Show top endpoints by data volume
            sorted_endpoints = sorted(category_endpoints, key=lambda x: collection_results[x]['rows'], reverse=True)
            for endpoint in sorted_endpoints[:5]:  # Top 5
                rows = collection_results[endpoint]['rows']
                print(f"   • {endpoint}: {rows:,} rows")
    
    # Save detailed results
    summary_file = f"{output_dir}/comprehensive_collection_summary.json"
    with open(summary_file, 'w') as f:
        json.dump({
            'collection_info': {
                'games_analyzed': len(nba_games),
                'players_analyzed': len(recent_players),
                'total_duration_minutes': total_duration/60,
                'total_rows_collected': total_rows_collected,
                'timestamp': datetime.now().isoformat()
            },
            'results': collection_results,
            'summary': {
                'successful_endpoints': len(successful_endpoints_list),
                'failed_endpoints': len(failed_endpoints_list),
                'collection_rate_rows_per_second': total_rows_collected/total_duration
            }
        }, f, indent=2)
    
    print(f"\n💾 Summary saved: {summary_file}")
    print(f"📁 Data files saved: {output_dir}/")
    print(f"\n🎉 COMPREHENSIVE COLLECTION COMPLETE!")
    
    return collection_results


if __name__ == "__main__":
    run_comprehensive_collection(max_games=25)
