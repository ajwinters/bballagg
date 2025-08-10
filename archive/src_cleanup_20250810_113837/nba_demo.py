"""
NBA Systematic Data Processing Demonstration

This demonstrates the complete workflow of our streamlined NBA data collection system:
1. Master tables creation
2. Systematic endpoint processing 
3. Incremental data updates
"""

import pandas as pd
import time
import os
from datetime import datetime
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.static import teams


class NBADataProcessor:
    """Streamlined NBA data processor"""
    
    def __init__(self, data_dir='../data'):
        self.data_dir = data_dir
        self.endpoint_dir = os.path.join(data_dir, 'endpoints')
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.endpoint_dir, exist_ok=True)
        
    def create_master_tables(self):
        """Create comprehensive master tables"""
        print("=== CREATING MASTER TABLES ===")
        
        # Master Teams
        teams_data = teams.get_teams()
        master_teams = pd.DataFrame(teams_data)
        master_teams.to_csv(os.path.join(self.data_dir, 'master_teams.csv'), index=False)
        print(f"✓ Master Teams: {len(master_teams)} teams")
        
        # Master Seasons
        seasons = []
        for year in range(2020, 2025):  # Last 5 seasons for demo
            seasons.append(f"{year}-{str(year+1)[2:]}")
        
        master_seasons = pd.DataFrame({
            'season': seasons,
            'start_year': [int(s.split('-')[0]) for s in seasons],
            'end_year': [int('20' + s.split('-')[1]) for s in seasons],
            'created_date': pd.Timestamp.now()
        })
        master_seasons.to_csv(os.path.join(self.data_dir, 'master_seasons.csv'), index=False)
        print(f"✓ Master Seasons: {len(master_seasons)} seasons")
        
        # Sample Master Games (recent season only for demo)
        try:
            print("Fetching sample games...")
            gamefinder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable='00', 
                season_type_nullable="Regular Season",
                season_nullable='2023-24'
            ).get_data_frames()[0]
            
            # Take sample for demo
            sample_games = gamefinder.head(50).copy()
            sample_games['GAME_DATE'] = pd.to_datetime(sample_games['GAME_DATE'])
            sample_games.to_csv(os.path.join(self.data_dir, 'master_games.csv'), index=False)
            print(f"✓ Master Games: {len(sample_games)} games ({sample_games['GAME_ID'].nunique()} unique)")
            
            return master_teams, master_seasons, sample_games
            
        except Exception as e:
            print(f"⚠ Error fetching games: {str(e)}")
            return master_teams, master_seasons, None
    
    def get_game_ids(self):
        """Get game IDs from master games"""
        try:
            games_df = pd.read_csv(os.path.join(self.data_dir, 'master_games.csv'))
            return games_df['GAME_ID'].unique()[:5]  # Limit for demo
        except:
            return []
    
    def process_game_endpoint(self, endpoint_name, endpoint_class, game_ids, max_games=3):
        """Process a single game-based endpoint"""
        print(f"\nProcessing {endpoint_name}...")
        success_count = 0
        
        for i, game_id in enumerate(game_ids[:max_games]):
            try:
                print(f"  Game {game_id} ({i+1}/{min(len(game_ids), max_games)})...")
                
                # Make API call
                endpoint_instance = endpoint_class(game_id=game_id)
                dataframes = endpoint_instance.get_data_frames()
                expected_keys = list(endpoint_instance.expected_data.keys()) if hasattr(endpoint_instance, 'expected_data') else []
                
                # Process each dataframe
                for df_index, df in enumerate(dataframes):
                    if df.empty:
                        continue
                        
                    # Generate table name
                    df_name = expected_keys[df_index] if df_index < len(expected_keys) else f'df_{df_index}'
                    table_name = f"{endpoint_name.lower()}_{df_name.lower()}"
                    
                    # Save to CSV (simulating database table)
                    csv_path = os.path.join(self.endpoint_dir, f'{table_name}.csv')
                    
                    if os.path.exists(csv_path):
                        # Append new data
                        existing = pd.read_csv(csv_path)
                        combined = pd.concat([existing, df], ignore_index=True)
                        combined.to_csv(csv_path, index=False)
                    else:
                        # Create new table
                        df.to_csv(csv_path, index=False)
                    
                    print(f"    ✓ {table_name}: +{len(df)} rows")
                
                success_count += 1
                time.sleep(0.6)  # Rate limiting
                
            except Exception as e:
                print(f"    ✗ Error: {str(e)}")
        
        print(f"  Summary: {success_count}/{min(len(game_ids), max_games)} games processed")
        return success_count
    
    def run_systematic_collection(self):
        """Run systematic data collection for key endpoints"""
        print("\n=== SYSTEMATIC ENDPOINT PROCESSING ===")
        
        game_ids = self.get_game_ids()
        if len(game_ids) == 0:
            print("No game IDs available for processing")
            return
        
        print(f"Processing {len(game_ids)} games with key endpoints...")
        
        # High-priority game endpoints
        endpoints = [
            ('BoxScoreTraditionalV2', nbaapi.BoxScoreTraditionalV2),
            ('BoxScoreAdvancedV2', nbaapi.BoxScoreAdvancedV2),
            ('BoxScoreScoringV2', nbaapi.BoxScoreScoringV2)
        ]
        
        results = {}
        for endpoint_name, endpoint_class in endpoints:
            results[endpoint_name] = self.process_game_endpoint(
                endpoint_name, endpoint_class, game_ids, max_games=3
            )
        
        return results
    
    def show_summary(self):
        """Show summary of collected data"""
        print("\n=== DATA COLLECTION SUMMARY ===")
        
        # Master tables
        try:
            teams = pd.read_csv(os.path.join(self.data_dir, 'master_teams.csv'))
            seasons = pd.read_csv(os.path.join(self.data_dir, 'master_seasons.csv'))
            games = pd.read_csv(os.path.join(self.data_dir, 'master_games.csv'))
            
            print("Master Tables:")
            print(f"  ✓ Teams: {len(teams):,}")
            print(f"  ✓ Seasons: {len(seasons):,}")
            print(f"  ✓ Games: {len(games):,} ({games['GAME_ID'].nunique():,} unique)")
        except Exception as e:
            print(f"Error reading master tables: {str(e)}")
        
        # Endpoint tables
        try:
            endpoint_files = [f for f in os.listdir(self.endpoint_dir) if f.endswith('.csv')]
            print(f"\nEndpoint Tables Created: {len(endpoint_files)}")
            
            total_rows = 0
            for file in sorted(endpoint_files):
                df = pd.read_csv(os.path.join(self.endpoint_dir, file))
                total_rows += len(df)
                print(f"  ✓ {file.replace('.csv', '')}: {len(df):,} rows")
            
            print(f"\nTotal data rows collected: {total_rows:,}")
            
        except Exception as e:
            print(f"Error reading endpoint data: {str(e)}")


def main():
    """Main demonstration"""
    print("NBA Systematic Data Processing Demonstration")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize processor
    processor = NBADataProcessor()
    
    # Step 1: Create master tables
    start_time = time.time()
    master_data = processor.create_master_tables()
    master_time = time.time() - start_time
    
    # Step 2: Run systematic collection
    start_time = time.time()
    results = processor.run_systematic_collection()
    processing_time = time.time() - start_time
    
    # Step 3: Show summary
    processor.show_summary()
    
    # Performance summary
    print("\n=== PERFORMANCE SUMMARY ===")
    print(f"Master tables creation: {master_time:.1f}s")
    print(f"Endpoint processing: {processing_time:.1f}s")
    print(f"Total runtime: {master_time + processing_time:.1f}s")
    
    if results:
        success_rate = sum(results.values()) / (len(results) * 3) * 100  # 3 games per endpoint
        print(f"Success rate: {success_rate:.1f}%")
    
    print("\n🎉 NBA systematic data collection completed!")
    print("\nNext steps:")
    print("1. Connect to database when available")
    print("2. Schedule weekly runs")
    print("3. Add more endpoints")
    print("4. Implement data validation")


if __name__ == "__main__":
    main()
