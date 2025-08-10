"""
Fix Master Games Table - Permanent Game ID Leading Zero Fix

This script will update the master games CSV file to have properly formatted
game IDs with leading zeros preserved.
"""

import pandas as pd
import os
from datetime import datetime


def fix_master_games_table(data_dir='data'):
    """Fix all game IDs in the master games table"""
    
    # Load the current master games table
    games_file = f'{data_dir}/comprehensive_master_games.csv'
    print(f"📊 Loading master games from: {games_file}")
    
    df = pd.read_csv(games_file)
    print(f"   Loaded {len(df):,} games")
    
    # Analyze current game ID lengths
    print(f"\n🔍 Current Game ID Analysis:")
    game_id_lengths = df['GAME_ID'].astype(str).str.len()
    print(f"   Game ID length distribution:")
    for length, count in game_id_lengths.value_counts().sort_index().items():
        print(f"     {length} digits: {count:,} games")
    
    # Fix game IDs by adding leading zeros to make them 10 digits
    print(f"\n🔧 Fixing Game IDs...")
    
    original_game_ids = df['GAME_ID'].copy()
    df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
    
    # Count how many were fixed
    fixes_made = (original_game_ids.astype(str) != df['GAME_ID']).sum()
    print(f"   Fixed {fixes_made:,} game IDs by adding leading zeros")
    
    # Show examples of fixes
    if fixes_made > 0:
        print(f"\n✅ Sample fixes made:")
        fixed_examples = df[original_game_ids.astype(str) != df['GAME_ID']].head(5)
        for idx, row in fixed_examples.iterrows():
            original = str(original_game_ids.iloc[idx])
            fixed = row['GAME_ID']
            print(f"     {original} → {fixed}")
    
    # Verify all game IDs are now 10 digits
    new_lengths = df['GAME_ID'].str.len()
    print(f"\n📋 After fix - Game ID length distribution:")
    for length, count in new_lengths.value_counts().sort_index().items():
        print(f"     {length} digits: {count:,} games")
    
    # Create backup of original file
    backup_file = f'{games_file}.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    print(f"\n💾 Creating backup: {backup_file}")
    original_df = pd.read_csv(games_file)
    original_df.to_csv(backup_file, index=False)
    
    # Save the fixed version - ensure GAME_ID stays as string
    print(f"💾 Saving fixed master games to: {games_file}")
    df.to_csv(games_file, index=False)
    
    # Verification - explicitly load GAME_ID as string
    print(f"\n✅ Verification - reloading file...")
    verification_df = pd.read_csv(games_file, dtype={'GAME_ID': str})
    verification_lengths = verification_df['GAME_ID'].str.len()
    
    if (verification_lengths == 10).all():
        print(f"   ✅ SUCCESS: All {len(verification_df):,} game IDs are now exactly 10 digits!")
    else:
        print(f"   ❌ Issue: Some game IDs still have incorrect lengths")
        print(verification_lengths.value_counts())
    
    return {
        'total_games': len(df),
        'fixes_made': fixes_made,
        'backup_file': backup_file,
        'success': (verification_lengths == 10).all()
    }


def main():
    """Fix the master games table"""
    print("🔧 NBA Master Games Table - Game ID Fix")
    print("=" * 50)
    print("This will fix all game IDs in the master table by adding leading zeros.")
    print("A backup will be created before making changes.")
    
    confirm = input("\nProceed with fixing the master games table? (y/n): ").strip().lower()
    
    if confirm == 'y':
        result = fix_master_games_table()
        
        if result['success']:
            print(f"\n🎉 MASTER GAMES TABLE SUCCESSFULLY FIXED!")
            print(f"📊 Summary:")
            print(f"   • Total games: {result['total_games']:,}")
            print(f"   • Game IDs fixed: {result['fixes_made']:,}")
            print(f"   • Backup created: {result['backup_file']}")
            print(f"\n📈 Your data collection should now have much higher success rates!")
        else:
            print(f"\n❌ Fix had issues - check the output above")
    else:
        print("Fix cancelled.")


if __name__ == "__main__":
    main()
