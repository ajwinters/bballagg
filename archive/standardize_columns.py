#!/usr/bin/env python3
"""
NBA Database Column Standardization Tool

This script:
1. Audits all existing NBA tables to identify column naming inconsistencies
2. Creates standardized versions of master tables with consistent column naming
3. Migrates data from old tables to new standardized tables
4. Updates all systems to use the new standardized column names

Key standardization rules:
- All columns lowercase
- No special characters (underscore, spaces, etc.)
- Consistent field naming across all tables
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import pandas as pd
import psycopg2
from psycopg2 import sql
import re
from datetime import datetime

# Import our database connection
sys.path.append('.')
import allintwo

class DatabaseColumnStandardizer:
    """Standardizes column names across all NBA database tables"""
    
    def __init__(self):
        self.conn = allintwo.connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')
        
        # Standard column mappings for common fields
        self.standard_mappings = {
            # Game ID variations
            'game_id': ['GAME_ID', 'GAMEID', 'GameID', 'game_id'],
            'gameid': ['GAME_ID', 'GAMEID', 'GameID', 'game_id'],
            
            # Player ID variations  
            'player_id': ['PLAYER_ID', 'PLAYERID', 'PlayerId', 'player_id'],
            'playerid': ['PLAYER_ID', 'PLAYERID', 'PlayerId', 'player_id'],
            
            # Team ID variations
            'team_id': ['TEAM_ID', 'TEAMID', 'TeamId', 'team_id'],
            'teamid': ['TEAM_ID', 'TEAMID', 'TeamId', 'team_id'],
            
            # Common field standardizations
            'playername': ['PLAYER_NAME', 'PlayerName', 'player_name'],
            'teamname': ['TEAM_NAME', 'TeamName', 'team_name'],
            'teamabbreviation': ['TEAM_ABBREVIATION', 'TeamAbbreviation', 'team_abbreviation'],
            'gamedate': ['GAME_DATE', 'GameDate', 'game_date'],
            'season': ['SEASON', 'Season', 'season'],
            'seasontype': ['SEASON_TYPE', 'SeasonType', 'season_type'],
        }
        
    def clean_column_name(self, col_name):
        """Apply standard column name cleaning rules"""
        return re.sub(r'[^a-zA-Z0-9]', '', str(col_name)).lower()
    
    def audit_existing_tables(self):
        """Audit all NBA tables to identify column naming patterns"""
        print("🔍 AUDITING NBA TABLES FOR COLUMN NAMING PATTERNS")
        print("=" * 60)
        
        cursor = self.conn.cursor()
        
        # Get all NBA tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'nba_%' 
            AND table_schema = 'public'
            ORDER BY table_name;
        """)
        
        nba_tables = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(nba_tables)} NBA tables to audit")
        
        audit_results = {}
        column_variations = {}
        
        for table_name in nba_tables:
            print(f"\n📊 Auditing: {table_name}")
            
            try:
                # Get column information
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                
                columns = cursor.fetchall()
                
                # Count total records
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                record_count = cursor.fetchone()[0]
                
                # Analyze column patterns
                column_info = []
                for col_name, data_type, nullable in columns:
                    cleaned_name = self.clean_column_name(col_name)
                    column_info.append({
                        'original': col_name,
                        'cleaned': cleaned_name,
                        'type': data_type,
                        'nullable': nullable
                    })
                    
                    # Track variations of key fields
                    if any(key_field in cleaned_name for key_field in ['gameid', 'playerid', 'teamid']):
                        if cleaned_name not in column_variations:
                            column_variations[cleaned_name] = set()
                        column_variations[cleaned_name].add(col_name)
                
                audit_results[table_name] = {
                    'columns': column_info,
                    'record_count': record_count,
                    'total_columns': len(columns)
                }
                
                print(f"   Columns: {len(columns)}, Records: {record_count:,}")
                
                # Show key field variations in this table
                key_fields = [col for col in column_info if any(key in col['cleaned'] for key in ['gameid', 'playerid', 'teamid'])]
                if key_fields:
                    key_field_mappings = [f"{col['original']} → {col['cleaned']}" for col in key_fields]
                    print(f"   Key fields: {key_field_mappings}")
                    
            except Exception as e:
                print(f"   ❌ Error auditing {table_name}: {str(e)}")
                audit_results[table_name] = {'error': str(e)}
        
        print(f"\n🎯 COLUMN VARIATION SUMMARY")
        print("=" * 40)
        for cleaned_name, variations in column_variations.items():
            if len(variations) > 1:
                print(f"   {cleaned_name}: {variations}")
        
        return audit_results
    
    def create_standardized_master_tables(self):
        """Create new master tables with standardized column names"""
        print("\n🔧 CREATING STANDARDIZED MASTER TABLES")
        print("=" * 50)
        
        # Core master tables to standardize (all leagues)
        master_tables = [
            'nba_games', 'nba_players', 'nba_teams',
            'wnba_games', 'wnba_players', 'wnba_teams',
            'gleague_games', 'gleague_players', 'gleague_teams'
        ]
        
        cursor = self.conn.cursor()
        
        for table_name in master_tables:
            print(f"\n📋 Standardizing: {table_name}")
            
            try:
                # Get current table structure
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                
                current_columns = cursor.fetchall()
                
                if not current_columns:
                    print(f"   ⚠️ Table {table_name} does not exist, skipping...")
                    continue
                
                # Create backup table name
                backup_table_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                standardized_table_name = f"{table_name}_standardized"
                
                print(f"   Creating backup: {backup_table_name}")
                
                # 1. Create backup of original table
                cursor.execute(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name};")
                
                # 2. Build standardized column definitions
                standardized_columns = []
                column_mapping = {}  # old_name -> new_name
                
                for col_name, data_type, nullable, default in current_columns:
                    cleaned_name = self.clean_column_name(col_name)
                    column_mapping[col_name] = cleaned_name
                    
                    # Build column definition
                    null_clause = "NULL" if nullable == "YES" else "NOT NULL"
                    default_clause = f"DEFAULT {default}" if default else ""
                    
                    column_def = f"{cleaned_name} {data_type} {null_clause} {default_clause}".strip()
                    standardized_columns.append(column_def)
                
                # 3. Create standardized table
                create_sql = f"""
                    CREATE TABLE {standardized_table_name} (
                        {', '.join(standardized_columns)}
                    );
                """
                
                cursor.execute(create_sql)
                print(f"   ✅ Created standardized table: {standardized_table_name}")
                
                # 4. Migrate data with column name mapping
                old_cols = ', '.join([f'"{col}"' for col in column_mapping.keys()])
                new_cols = ', '.join(column_mapping.values())
                
                migrate_sql = f"""
                    INSERT INTO {standardized_table_name} ({new_cols})
                    SELECT {old_cols}
                    FROM {table_name};
                """
                
                cursor.execute(migrate_sql)
                
                # Get record count
                cursor.execute(f"SELECT COUNT(*) FROM {standardized_table_name};")
                migrated_count = cursor.fetchone()[0]
                
                print(f"   ✅ Migrated {migrated_count:,} records")
                print(f"   📋 Column mapping: {len(column_mapping)} columns standardized")
                
                # Show some column mappings
                key_mappings = {old: new for old, new in column_mapping.items() 
                              if old != new and any(key in new for key in ['gameid', 'playerid', 'teamid', 'name'])}
                if key_mappings:
                    print(f"   Key changes: {key_mappings}")
                
                self.conn.commit()
                
            except Exception as e:
                print(f"   ❌ Error standardizing {table_name}: {str(e)}")
                self.conn.rollback()
    
    def promote_standardized_tables(self, confirm=False):
        """Replace original tables with standardized versions"""
        if not confirm:
            print("\n⚠️ PROMOTION PREVIEW (DRY RUN)")
            print("=" * 40)
            print("This would replace original master tables with standardized versions.")
            print("Run with confirm=True to execute the promotion.")
            return
        
        print("\n🚀 PROMOTING STANDARDIZED TABLES TO PRODUCTION")
        print("=" * 55)
        
        master_tables = [
            'nba_games', 'nba_players', 'nba_teams',
            'wnba_games', 'wnba_players', 'wnba_teams', 
            'gleague_games', 'gleague_players', 'gleague_teams'
        ]
        cursor = self.conn.cursor()
        
        for table_name in master_tables:
            standardized_table_name = f"{table_name}_standardized"
            
            try:
                # Check if standardized table exists
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{standardized_table_name}'
                    );
                """)
                
                if not cursor.fetchone()[0]:
                    print(f"   ⚠️ {standardized_table_name} does not exist, skipping...")
                    continue
                
                print(f"\n📋 Promoting: {table_name}")
                
                # 1. Drop original table (backup already created)
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                
                # 2. Rename standardized table to original name
                cursor.execute(f"ALTER TABLE {standardized_table_name} RENAME TO {table_name};")
                
                print(f"   ✅ {table_name} now uses standardized column names")
                
                self.conn.commit()
                
            except Exception as e:
                print(f"   ❌ Error promoting {table_name}: {str(e)}")
                self.conn.rollback()
    
    def test_endpoint_processor_compatibility(self):
        """Test that endpoint processor works with standardized columns"""
        print("\n🧪 TESTING ENDPOINT PROCESSOR COMPATIBILITY")
        print("=" * 50)
        
        cursor = self.conn.cursor()
        
        # Test all league master tables
        test_tables = [
            ('nba_games', 'gameid'),
            ('wnba_games', 'gameid'), 
            ('gleague_games', 'gameid'),
            ('nba_players', 'playerid'),
            ('wnba_players', 'playerid'),
            ('gleague_players', 'playerid')
        ]
        
        for table_name, id_column in test_tables:
            try:
                cursor.execute(f"SELECT {id_column} FROM {table_name} LIMIT 1;")
                result = cursor.fetchone()
                if result:
                    print(f"   ✅ {table_name}.{id_column} accessible")
                else:
                    print(f"   ⚠️ {table_name} table is empty")
                    
            except Exception as e:
                print(f"   ❌ Error accessing {table_name}.{id_column}: {str(e)}")
        
        # Check endpoint processor table compatibility
        endpoint_test_tables = [
            'nba_boxscoreadvancedv3_playerstats',
            'nba_boxscoresummaryv2_gameinfo'
        ]
        
        for table_name in endpoint_test_tables:
            try:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND column_name IN ('gameid', 'game_id', 'playerid', 'player_id');
                """)
                
                id_columns = [row[0] for row in cursor.fetchall()]
                print(f"   {table_name}: ID columns = {id_columns}")
                
            except Exception as e:
                print(f"   ❌ Error checking {table_name}: {str(e)}")
    
    def standardize_all_endpoint_tables(self):
        """Standardize ALL endpoint tables (not just master tables)"""
        print("\n🔧 STANDARDIZING ALL ENDPOINT TABLES")
        print("=" * 50)
        
        cursor = self.conn.cursor()
        
        # Get all NBA endpoint tables (excluding master tables and backups)
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'nba_%' 
            AND table_name NOT LIKE '%_backup_%'
            AND table_name NOT LIKE '%_standardized'
            AND table_name NOT IN ('nba_games', 'nba_players', 'nba_teams')
            AND table_schema = 'public'
            ORDER BY table_name;
        """)
        
        endpoint_tables = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(endpoint_tables)} endpoint tables to standardize")
        
        standardized_count = 0
        
        for table_name in endpoint_tables:
            print(f"\n📋 Standardizing: {table_name}")
            
            try:
                # Get current table structure
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                
                current_columns = cursor.fetchall()
                
                if not current_columns:
                    print(f"   ⚠️ Table {table_name} does not exist, skipping...")
                    continue
                
                # Check if any columns need standardization
                needs_standardization = False
                column_mapping = {}
                
                for col_name, data_type, nullable, default in current_columns:
                    cleaned_name = self.clean_column_name(col_name)
                    column_mapping[col_name] = cleaned_name
                    if col_name != cleaned_name:
                        needs_standardization = True
                
                if not needs_standardization:
                    print(f"   ✅ Already standardized (all columns lowercase, no special chars)")
                    continue
                
                # Create backup table name
                backup_table_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                temp_table_name = f"{table_name}_temp_standard"
                
                print(f"   Creating backup: {backup_table_name}")
                
                # 1. Create backup of original table
                cursor.execute(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name};")
                
                # 2. Build standardized column definitions
                standardized_columns = []
                
                for col_name, data_type, nullable, default in current_columns:
                    cleaned_name = self.clean_column_name(col_name)
                    
                    # Build column definition
                    null_clause = "NULL" if nullable == "YES" else "NOT NULL"
                    default_clause = f"DEFAULT {default}" if default else ""
                    
                    column_def = f"{cleaned_name} {data_type} {null_clause} {default_clause}".strip()
                    standardized_columns.append(column_def)
                
                # 3. Create temporary standardized table
                create_sql = f"""
                    CREATE TABLE {temp_table_name} (
                        {', '.join(standardized_columns)}
                    );
                """
                
                cursor.execute(create_sql)
                
                # 4. Migrate data with column name mapping
                old_cols = ', '.join([f'"{col}"' for col in column_mapping.keys()])
                new_cols = ', '.join(column_mapping.values())
                
                migrate_sql = f"""
                    INSERT INTO {temp_table_name} ({new_cols})
                    SELECT {old_cols}
                    FROM {table_name};
                """
                
                cursor.execute(migrate_sql)
                
                # 5. Replace original table with standardized version
                cursor.execute(f"DROP TABLE {table_name};")
                cursor.execute(f"ALTER TABLE {temp_table_name} RENAME TO {table_name};")
                
                # Get record count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                migrated_count = cursor.fetchone()[0]
                
                print(f"   ✅ Standardized {migrated_count:,} records")
                
                # Show key column changes
                key_changes = {old: new for old, new in column_mapping.items() 
                              if old != new and any(key in new.lower() for key in ['gameid', 'playerid', 'teamid'])}
                if key_changes:
                    print(f"   Key changes: {list(key_changes.items())[:3]}")
                
                standardized_count += 1
                self.conn.commit()
                
            except Exception as e:
                print(f"   ❌ Error standardizing {table_name}: {str(e)}")
                self.conn.rollback()
        
        print(f"\n✅ ENDPOINT TABLES STANDARDIZATION COMPLETE")
        print(f"   Tables standardized: {standardized_count}")
        print(f"   Tables skipped: {len(endpoint_tables) - standardized_count}")
        
        return standardized_count
    
    def show_standardization_summary(self):
        """Show summary of standardization results"""
        print("\n📊 STANDARDIZATION SUMMARY")
        print("=" * 40)
        
        cursor = self.conn.cursor()
        
        # Count tables by type
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN table_name LIKE '%_backup_%' THEN 'Backup Tables'
                    WHEN table_name LIKE '%_standardized' THEN 'Standardized Tables'
                    WHEN table_name LIKE 'nba_games' OR table_name LIKE 'nba_players' OR table_name LIKE 'nba_teams' THEN 'Master Tables'
                    ELSE 'Endpoint Tables'
                END as table_type,
                COUNT(*) as table_count
            FROM information_schema.tables 
            WHERE table_name LIKE 'nba_%' 
            GROUP BY table_type
            ORDER BY table_count DESC;
        """)
        
        table_counts = cursor.fetchall()
        for table_type, count in table_counts:
            print(f"   {table_type}: {count}")
        
        # Show total records in master tables
        master_tables = [
            'nba_games', 'nba_players', 'nba_teams',
            'wnba_games', 'wnba_players', 'wnba_teams',
            'gleague_games', 'gleague_players', 'gleague_teams'
        ]
        print(f"\n📈 Master Table Records:")
        
        for table_name in master_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                print(f"   {table_name}: {count:,} records")
            except:
                print(f"   {table_name}: Not accessible")


def main():
    """Interactive standardization process"""
    print("🔧 NBA DATABASE COLUMN STANDARDIZATION TOOL")
    print("=" * 60)
    
    standardizer = DatabaseColumnStandardizer()
    
    while True:
        print(f"\nChoose an action:")
        print(f"1. Audit existing tables for column naming patterns")
        print(f"2. Create standardized master tables (backup + standardize)")
        print(f"3. Promote standardized tables to production")
        print(f"4. Standardize ALL endpoint tables")
        print(f"5. Test endpoint processor compatibility") 
        print(f"6. Show standardization summary")
        print(f"7. Run full standardization process (masters + endpoints)")
        print(f"0. Exit")
        
        choice = input("\nEnter your choice (0-7): ").strip()
        
        if choice == "0":
            print("👋 Goodbye!")
            break
        elif choice == "1":
            standardizer.audit_existing_tables()
        elif choice == "2":
            standardizer.create_standardized_master_tables()
        elif choice == "3":
            confirm = input("⚠️ This will replace original tables! Type 'YES' to confirm: ").strip()
            standardizer.promote_standardized_tables(confirm=confirm == 'YES')
        elif choice == "4":
            standardizer.standardize_all_endpoint_tables()
        elif choice == "5":
            standardizer.test_endpoint_processor_compatibility()
        elif choice == "6":
            standardizer.show_standardization_summary()
        elif choice == "7":
            print("🚀 Running full standardization process...")
            standardizer.audit_existing_tables()
            standardizer.create_standardized_master_tables()
            
            confirm = input("\n⚠️ Promote master tables to production? Type 'YES' to confirm: ").strip()
            if confirm == 'YES':
                standardizer.promote_standardized_tables(confirm=True)
                print("✅ Master tables standardized!")
            else:
                print("⏸️ Master table standardization skipped.")
            
            confirm_endpoints = input("\n⚠️ Standardize ALL endpoint tables? Type 'YES' to confirm: ").strip()
            if confirm_endpoints == 'YES':
                standardizer.standardize_all_endpoint_tables()
                print("✅ All endpoint tables standardized!")
            else:
                print("⏸️ Endpoint table standardization skipped.")
                
            standardizer.test_endpoint_processor_compatibility()
            standardizer.show_standardization_summary()
        else:
            print("❌ Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
