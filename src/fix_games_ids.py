"""
Fix GAME_ID formatting issue in comprehensive_master_games.csv
This script ensures all GAME_IDs are properly formatted as 10-digit strings
"""

import pandas as pd
from datetime import datetime
import os

def fix_games_table_ids():
    """Fix GAME_ID formatting in comprehensive_master_games.csv"""
    
    print("🔧 FIXING GAME_ID FORMATTING")
    print("=" * 40)
    
    games_file = 'data/comprehensive_master_games.csv'
    
    if not os.path.exists(games_file):
        print(f"❌ {games_file} not found")
        return
    
    print(f"📊 Loading {games_file}...")
    
    # Load the games table
    df = pd.read_csv(games_file)
    print(f"   Total games: {len(df):,}")
    
    # Check current GAME_ID format
    print(f"   Current GAME_ID dtype: {df['GAME_ID'].dtype}")
    
    # Convert to string and check lengths
    game_id_str = df['GAME_ID'].astype(str)
    length_counts = game_id_str.str.len().value_counts().sort_index()
    
    print(f"   Current length distribution:")
    for length, count in length_counts.items():
        print(f"      {length} digits: {count:,} games")
    
    # Create backup
    backup_file = f'{games_file}.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    print(f"   💾 Creating backup: {backup_file}")
    df.to_csv(backup_file, index=False)
    
    # Fix GAME_ID formatting - pad with leading zeros to 10 digits
    original_ids = df['GAME_ID'].copy()
    df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
    
    # Check how many were changed
    fixed_count = sum(original_ids.astype(str) != df['GAME_ID'])
    print(f"   🔧 Fixed {fixed_count:,} GAME_IDs with leading zeros")
    
    # Verify new format
    new_length_counts = df['GAME_ID'].str.len().value_counts().sort_index()
    print(f"   New length distribution:")
    for length, count in new_length_counts.items():
        print(f"      {length} digits: {count:,} games")
    
    # Show some examples of fixes
    if fixed_count > 0:
        print(f"   Examples of fixes:")
        mask = original_ids.astype(str) != df['GAME_ID']
        examples = df.loc[mask, ['GAME_ID']].head(5)
        original_examples = original_ids[mask].head(5)
        
        for i, (idx, row) in enumerate(examples.iterrows()):
            print(f"      {original_examples.iloc[i]} → {row['GAME_ID']}")
    
    # Save the fixed version
    df.to_csv(games_file, index=False)
    print(f"   ✅ Fixed GAME_IDs saved to {games_file}")
    
    return fixed_count

def update_league_separated_tables():
    """Update the league-separated tables with fixed GAME_IDs"""
    
    print(f"\n🏀 UPDATING LEAGUE-SEPARATED TABLES")
    print("=" * 40)
    
    # Load the fixed comprehensive table
    df = pd.read_csv('data/comprehensive_master_games.csv', dtype={'GAME_ID': str})
    
    leagues_dir = 'data/leagues'
    
    # Get league distribution
    league_counts = df['league_name'].value_counts()
    
    for league in league_counts.index:
        print(f"\n📋 Updating {league} table...")
        
        # Filter by league
        league_df = df[df['league_name'] == league].copy()
        
        # Save updated table
        filename = f"{leagues_dir}/{league.lower().replace('-', '_')}_master_games.csv"
        league_df.to_csv(filename, index=False)
        
        print(f"   ✅ {league}: {len(league_df):,} games → {filename}")

def main():
    """Main function"""
    print("🔧 Master Tables GAME_ID Fix")
    print("=" * 50)
    
    # Fix the main comprehensive games table
    fixed_count = fix_games_table_ids()
    
    # Update the league-separated tables
    update_league_separated_tables()
    
    print(f"\n✅ GAME_ID formatting complete!")
    print(f"   🔧 {fixed_count:,} GAME_IDs fixed with leading zeros")
    print(f"   🏀 League-separated tables updated")

if __name__ == "__main__":
    main()
