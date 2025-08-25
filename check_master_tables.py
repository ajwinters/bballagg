#!/usr/bin/env python3
"""
Database Master Tables Inspector

Inspects the database to find existing master tables and their structures.
This helps identify the correct table names to use instead of fallback values.
"""

import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from endpoints.collectors.rds_connection_manager import RDSConnectionManager
import logging

def load_database_config(config_path):
    """Load database configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Set environment variables for the connection manager
        os.environ['DB_HOST'] = config['host']
        os.environ['DB_NAME'] = config['name'] 
        os.environ['DB_USER'] = config['user']
        os.environ['DB_PASSWORD'] = config['password']
        os.environ['DB_PORT'] = str(config['port'])
        os.environ['DB_SSLMODE'] = config.get('ssl_mode', 'require')
        
        return config
    except Exception as e:
        logging.error(f"Error loading database config: {e}")
        return None

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def inspect_master_tables():
    """Inspect database for master tables and their structures"""
    
    try:
        # Load database configuration
        config_path = os.path.join(os.path.dirname(__file__), 'endpoints', 'config', 'database_config.json')
        db_config = load_database_config(config_path)
        
        if not db_config:
            logger.error("Failed to load database configuration")
            return
            
        # Create database connection
        db_manager = RDSConnectionManager()
        
        # Ensure connection is established
        if not db_manager.create_connection():
            logger.error("Failed to establish database connection")
            return
            
        conn = db_manager.connection
            
        cursor = conn.cursor()
        
        print("🔍 DATABASE MASTER TABLES INSPECTION")
        print("="*50)
        
        # 1. Find all tables that might be master tables
        print("\n1. SEARCHING FOR MASTER TABLES...")
        master_table_patterns = [
            "master_games_%",
            "master_players_%", 
            "master_teams_%",
            "%mastergames%",
            "%masterplayers%",
            "%masterteams%",
            "%games%",
            "%players%", 
            "%teams%"
        ]
        
        found_tables = set()
        
        for pattern in master_table_patterns:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE %s
                ORDER BY table_name
            """, (pattern,))
            
            tables = cursor.fetchall()
            for table in tables:
                found_tables.add(table[0])
        
        if not found_tables:
            print("❌ No potential master tables found!")
            return
            
        print(f"✅ Found {len(found_tables)} potential master tables:")
        for table in sorted(found_tables):
            print(f"   - {table}")
        
        # 2. Analyze each table
        print("\n2. ANALYZING TABLE STRUCTURES AND DATA...")
        
        master_tables = {
            'games': [],
            'players': [],
            'teams': []
        }
        
        for table_name in sorted(found_tables):
            print(f"\n📊 Table: {table_name}")
            print("-" * (len(table_name) + 10))
            
            try:
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                print(f"   Rows: {row_count:,}")
                
                if row_count == 0:
                    print("   ⚠️  Empty table - skipping detailed analysis")
                    continue
                
                # Get column information
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = cursor.fetchall()
                print(f"   Columns ({len(columns)}):")
                
                key_columns = []
                for col_name, data_type, nullable, default in columns[:10]:  # Show first 10 columns
                    nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                    default_str = f" DEFAULT {default}" if default else ""
                    print(f"     • {col_name} ({data_type}) {nullable_str}{default_str}")
                    
                    # Identify key columns
                    col_lower = col_name.lower()
                    if any(key in col_lower for key in ['game_id', 'player_id', 'team_id', 'id']):
                        key_columns.append(col_name)
                
                if len(columns) > 10:
                    print(f"     ... and {len(columns) - 10} more columns")
                
                if key_columns:
                    print(f"   🔑 Key columns: {', '.join(key_columns)}")
                
                # Sample a few records to understand data
                sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                cursor.execute(sample_query)
                sample_rows = cursor.fetchall()
                
                if sample_rows:
                    print(f"   📋 Sample data (first 3 rows):")
                    col_names = [desc[0] for desc in cursor.description]
                    for i, row in enumerate(sample_rows, 1):
                        print(f"      Row {i}: {dict(zip(col_names[:5], row[:5]))}")  # Show first 5 cols
                
                # Categorize table
                table_lower = table_name.lower()
                if 'game' in table_lower:
                    master_tables['games'].append({
                        'name': table_name,
                        'rows': row_count,
                        'key_columns': key_columns
                    })
                elif 'player' in table_lower:
                    master_tables['players'].append({
                        'name': table_name,
                        'rows': row_count,
                        'key_columns': key_columns
                    })
                elif 'team' in table_lower:
                    master_tables['teams'].append({
                        'name': table_name,
                        'rows': row_count,
                        'key_columns': key_columns
                    })
                
            except Exception as e:
                print(f"   ❌ Error analyzing table: {e}")
                continue
        
        # 3. Summary and recommendations
        print("\n3. MASTER TABLES SUMMARY")
        print("="*30)
        
        for category, tables in master_tables.items():
            print(f"\n🎯 {category.upper()} TABLES:")
            if not tables:
                print("   ❌ No tables found")
                continue
                
            # Sort by row count (highest first)
            tables.sort(key=lambda x: x['rows'], reverse=True)
            
            for table in tables:
                print(f"   ✅ {table['name']} ({table['rows']:,} rows)")
                if table['key_columns']:
                    print(f"      Keys: {', '.join(table['key_columns'])}")
        
        # 4. Recommendations for processor
        print("\n4. RECOMMENDATIONS FOR PROCESSOR")
        print("="*35)
        
        for category, tables in master_tables.items():
            if tables:
                best_table = tables[0]  # Highest row count
                print(f"\n{category.upper()}:")
                print(f"   📋 Primary table: {best_table['name']}")
                print(f"   📊 Record count: {best_table['rows']:,}")
                if best_table['key_columns']:
                    print(f"   🔑 Key columns: {', '.join(best_table['key_columns'])}")
                    
                    # Suggest parameter resolution
                    if category == 'games' and any('game_id' in col.lower() for col in best_table['key_columns']):
                        print("   💡 Use for: from_mastergames parameter resolution")
                    elif category == 'players' and any('player_id' in col.lower() for col in best_table['key_columns']):
                        print("   💡 Use for: from_masterplayers parameter resolution")
                    elif category == 'teams' and any('team_id' in col.lower() for col in best_table['key_columns']):
                        print("   💡 Use for: from_masterteams parameter resolution")
        
        cursor.close()
        db_manager.close_connection()
        
        print("\n✅ Database inspection completed!")
        
    except Exception as e:
        logger.error(f"Error during database inspection: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    inspect_master_tables()
