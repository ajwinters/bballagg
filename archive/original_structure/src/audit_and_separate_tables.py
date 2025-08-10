"""
Master Tables Audit and League Separation

This script will:
1. Audit all master tables for data type issues (like the game ID leading zeros problem)
2. Separate master tables by league (NBA, WNBA, G-League)
3. Create organized league-specific master tables
"""

import pandas as pd
import os
import json
from datetime import datetime


def audit_master_tables(data_dir='data'):
    """Audit all master tables for data type and formatting issues"""
    
    print("🔍 MASTER TABLES AUDIT")
    print("=" * 50)
    
    audit_results = {}
    
    # Define all master tables to check
    master_tables = {
        'comprehensive_master_games.csv': ['GAME_ID'],
        'comprehensive_master_players.csv': ['PLAYER_ID', 'TEAM_ID'], 
        'master_players.csv': ['id'],
        'master_teams.csv': ['id'],
        'master_seasons.csv': []  # Will check all columns
    }
    
    for filename, id_columns in master_tables.items():
        filepath = f'{data_dir}/{filename}'
        
        if not os.path.exists(filepath):
            print(f"❌ {filename}: File not found")
            continue
            
        print(f"\n📊 Auditing {filename}...")
        
        try:
            # Load without specifying dtypes first to see what we get
            df = pd.read_csv(filepath)
            print(f"   Rows: {len(df):,}")
            print(f"   Columns: {list(df.columns)}")
            
            table_audit = {
                'filename': filename,
                'rows': len(df),
                'columns': list(df.columns),
                'issues_found': [],
                'id_column_analysis': {}
            }
            
            # Check ID columns for potential formatting issues
            for col in id_columns:
                if col in df.columns:
                    print(f"\n   🔍 Analyzing {col}:")
                    
                    # Data type
                    dtype = df[col].dtype
                    print(f"      Data type: {dtype}")
                    
                    # Convert to string to analyze
                    col_str = df[col].astype(str)
                    
                    # Length analysis
                    lengths = col_str.str.len()
                    length_counts = lengths.value_counts().sort_index()
                    print(f"      Length distribution:")
                    for length, count in length_counts.items():
                        print(f"        {length} chars: {count:,} values")
                    
                    # Check for leading zeros issues
                    if dtype in ['int64', 'int32', 'float64']:
                        # Potential leading zeros issue
                        sample_values = df[col].head(10).tolist()
                        print(f"      Sample values: {sample_values}")
                        
                        # Check if values look like they should have leading zeros
                        if col in ['GAME_ID', 'PLAYER_ID'] and any(len(str(v)) < 10 for v in sample_values if pd.notna(v)):
                            table_audit['issues_found'].append(f"{col}: Potential leading zeros missing")
                            print(f"      ⚠️  ISSUE: {col} may be missing leading zeros")
                    
                    table_audit['id_column_analysis'][col] = {
                        'dtype': str(dtype),
                        'length_distribution': dict(length_counts),
                        'sample_values': df[col].head(5).tolist()
                    }
                else:
                    print(f"   ❌ Column {col} not found in table")
                    table_audit['issues_found'].append(f"Missing expected column: {col}")
            
            # Check for any league identifiers
            league_columns = ['league', 'league_name', 'League', 'LEAGUE_ID']
            found_league_cols = [col for col in league_columns if col in df.columns]
            
            if found_league_cols:
                print(f"\n   🏀 League information found:")
                for col in found_league_cols:
                    league_counts = df[col].value_counts()
                    print(f"      {col}: {dict(league_counts)}")
                    table_audit[f'league_breakdown_{col}'] = dict(league_counts)
            else:
                print(f"   ℹ️  No league columns found")
            
            audit_results[filename] = table_audit
            
        except Exception as e:
            print(f"   ❌ Error reading {filename}: {str(e)}")
            audit_results[filename] = {'error': str(e)}
    
    return audit_results


def create_league_separated_tables(data_dir='data', output_dir='data/leagues'):
    """Create league-separated versions of master tables"""
    
    print(f"\n🏀 CREATING LEAGUE-SEPARATED TABLES")
    print("=" * 50)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Load the comprehensive master tables
    print(f"📊 Loading comprehensive master tables...")
    
    try:
        # Load games with proper dtypes
        games = pd.read_csv(f'{data_dir}/comprehensive_master_games.csv', dtype={'GAME_ID': str})
        players = pd.read_csv(f'{data_dir}/comprehensive_master_players.csv')
        
        print(f"   Games loaded: {len(games):,}")
        print(f"   Players loaded: {len(players):,}")
        
        # Analyze league distribution in games
        if 'league_name' in games.columns:
            league_col = 'league_name'
        elif 'League' in games.columns:
            league_col = 'League'
        else:
            print("   ❌ No league column found in games table")
            return
        
        print(f"\n🏀 League distribution in games:")
        league_counts = games[league_col].value_counts()
        for league, count in league_counts.items():
            print(f"   {league}: {count:,} games")
        
        # Create separate tables for each league
        results = {}
        
        for league in league_counts.index:
            print(f"\n📋 Creating tables for {league}...")
            
            # Filter games by league
            league_games = games[games[league_col] == league].copy()
            
            # Filter players by league (if possible)
            if 'league_name' in players.columns:
                league_players = players[players['league_name'] == league].copy()
            elif 'League' in players.columns:
                league_players = players[players['League'] == league].copy()
            else:
                # If no league info in players, we'll keep all players
                print(f"   ⚠️  No league info in players table - keeping all players")
                league_players = players.copy()
            
            # Save league-specific tables
            games_file = f"{output_dir}/{league.lower().replace('-', '_')}_master_games.csv"
            players_file = f"{output_dir}/{league.lower().replace('-', '_')}_master_players.csv"
            
            league_games.to_csv(games_file, index=False)
            league_players.to_csv(players_file, index=False)
            
            print(f"   ✅ {league} games: {len(league_games):,} → {games_file}")
            print(f"   ✅ {league} players: {len(league_players):,} → {players_file}")
            
            results[league] = {
                'games_count': len(league_games),
                'players_count': len(league_players),
                'games_file': games_file,
                'players_file': players_file
            }
        
        # Create a summary of the league separation
        summary = {
            'separation_timestamp': datetime.now().isoformat(),
            'total_leagues': len(results),
            'leagues': results,
            'output_directory': output_dir
        }
        
        summary_file = f"{output_dir}/league_separation_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n📄 League separation summary saved to: {summary_file}")
        
        return results
        
    except Exception as e:
        print(f"❌ Error creating league-separated tables: {str(e)}")
        return None


def fix_any_id_issues(data_dir='data'):
    """Fix any ID formatting issues found in the audit"""
    
    print(f"\n🔧 FIXING ID FORMATTING ISSUES")
    print("=" * 50)
    
    fixes_made = []
    
    # Check players table for potential PLAYER_ID issues
    try:
        players_file = f'{data_dir}/comprehensive_master_players.csv'
        print(f"📊 Checking {players_file}...")
        
        df = pd.read_csv(players_file)
        
        if 'PLAYER_ID' in df.columns:
            print(f"   Analyzing PLAYER_ID column...")
            
            # Check data type
            if df['PLAYER_ID'].dtype in ['int64', 'int32']:
                print(f"   📋 PLAYER_ID is {df['PLAYER_ID'].dtype} - converting to string")
                
                # Create backup
                backup_file = f'{players_file}.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
                df.to_csv(backup_file, index=False)
                
                # Convert to string (this will preserve the current format)
                df['PLAYER_ID'] = df['PLAYER_ID'].astype(str)
                
                # Save fixed version
                df.to_csv(players_file, index=False)
                
                print(f"   ✅ PLAYER_ID converted to string format")
                print(f"   💾 Backup created: {backup_file}")
                
                fixes_made.append({
                    'file': players_file,
                    'column': 'PLAYER_ID',
                    'fix': 'Converted to string format',
                    'backup': backup_file
                })
            else:
                print(f"   ✅ PLAYER_ID already in correct format: {df['PLAYER_ID'].dtype}")
        
        # Check TEAM_ID if it exists
        if 'TEAM_ID' in df.columns:
            if df['TEAM_ID'].dtype in ['int64', 'int32']:
                print(f"   🔧 Also fixing TEAM_ID format...")
                df['TEAM_ID'] = df['TEAM_ID'].astype(str)
                df.to_csv(players_file, index=False)
                print(f"   ✅ TEAM_ID converted to string format")
                
                fixes_made.append({
                    'file': players_file,
                    'column': 'TEAM_ID',
                    'fix': 'Converted to string format'
                })
        
    except Exception as e:
        print(f"❌ Error fixing players table: {str(e)}")
    
    # Check other master tables
    other_tables = ['master_players.csv', 'master_teams.csv']
    
    for table_file in other_tables:
        try:
            filepath = f'{data_dir}/{table_file}'
            if os.path.exists(filepath):
                print(f"\n📊 Checking {table_file}...")
                
                df = pd.read_csv(filepath)
                
                # Look for ID columns
                id_columns = [col for col in df.columns if 'id' in col.lower()]
                
                for col in id_columns:
                    if df[col].dtype in ['int64', 'int32', 'float64']:
                        print(f"   🔧 Converting {col} to string format...")
                        
                        # Create backup
                        backup_file = f'{filepath}.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
                        df.to_csv(backup_file, index=False)
                        
                        # Convert to string
                        df[col] = df[col].astype(str)
                        df.to_csv(filepath, index=False)
                        
                        print(f"   ✅ {col} converted to string")
                        
                        fixes_made.append({
                            'file': filepath,
                            'column': col,
                            'fix': 'Converted to string format',
                            'backup': backup_file
                        })
        
        except Exception as e:
            print(f"❌ Error checking {table_file}: {str(e)}")
    
    if fixes_made:
        print(f"\n✅ Summary of fixes made:")
        for fix in fixes_made:
            print(f"   • {fix['file']} - {fix['column']}: {fix['fix']}")
    else:
        print(f"\n✅ No ID formatting issues found - all tables are correctly formatted!")
    
    return fixes_made


def main():
    """Main audit and separation function"""
    print("🔍 NBA Master Tables - Audit and League Separation")
    print("=" * 60)
    
    # Step 1: Audit all master tables
    audit_results = audit_master_tables()
    
    # Step 2: Fix any ID formatting issues found
    fixes = fix_any_id_issues()
    
    # Step 3: Create league-separated tables
    league_results = create_league_separated_tables()
    
    # Final summary
    print(f"\n📋 FINAL SUMMARY")
    print("=" * 50)
    
    if audit_results:
        issues_found = sum(len(table.get('issues_found', [])) for table in audit_results.values() if isinstance(table, dict))
        print(f"🔍 Tables audited: {len(audit_results)}")
        print(f"⚠️  Issues found: {issues_found}")
    
    if fixes:
        print(f"🔧 Fixes applied: {len(fixes)}")
    
    if league_results:
        print(f"🏀 League-separated tables created: {len(league_results)} leagues")
        for league, info in league_results.items():
            print(f"   {league}: {info['games_count']:,} games, {info['players_count']:,} players")
    
    print(f"\n✅ Master tables audit and league separation complete!")


if __name__ == "__main__":
    main()
