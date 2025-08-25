#!/usr/bin/env python3
"""
Check what master tables exist in the database
"""

import os
import sys
import json

# Add project paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'endpoints'))

from endpoints.collectors.rds_connection_manager import RDSConnectionManager

def load_database_config():
    """Load database configuration"""
    config_path = "config/database_config.json"
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Set environment variables
    os.environ['DB_HOST'] = config['host']
    os.environ['DB_NAME'] = config['name'] 
    os.environ['DB_USER'] = config['user']
    os.environ['DB_PASSWORD'] = config['password']
    os.environ['DB_PORT'] = str(config['port'])
    os.environ['DB_SSLMODE'] = config.get('ssl_mode', 'require')
    
    return config

def check_master_tables():
    """Check what master tables exist in the database"""
    
    print("NBA Master Tables Database Inspection")
    print("=" * 50)
    
    # Load config and connect
    config = load_database_config()
    conn_manager = RDSConnectionManager()
    
    if not conn_manager.ensure_connection():
        print("❌ Failed to connect to database")
        return
    
    print(f"✅ Connected to database: {config['host']}/{config['name']}")
    print()
    
    try:
        # List all tables that might be master tables
        print("Searching for master tables...")
        
        with conn_manager.get_cursor() as cursor:
            # Get all table names that contain 'master' or common patterns
            cursor.execute("""
                SELECT table_name, table_schema 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE' 
                AND (
                    table_name LIKE '%master%' 
                    OR table_name LIKE '%games%'
                    OR table_name LIKE '%players%' 
                    OR table_name LIKE '%teams%'
                )
                ORDER BY table_name;
            """)
            
            tables = cursor.fetchall()
            
            if tables:
                print(f"Found {len(tables)} relevant tables:")
                print("-" * 40)
                for table_name, schema in tables:
                    print(f"  {schema}.{table_name}")
                    
                    # Get row count for each table
                    try:
                        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                        count = cursor.fetchone()[0]
                        print(f"    └─ {count:,} rows")
                        
                        # Show column names for master-like tables
                        if 'master' in table_name.lower():
                            cursor.execute(f"""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_name = '{table_name}' 
                                ORDER BY ordinal_position
                                LIMIT 10
                            """)
                            columns = [row[0] for row in cursor.fetchall()]
                            print(f"    └─ Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                        
                    except Exception as e:
                        print(f"    └─ Error getting info: {e}")
                    
                    print()
            else:
                print("❌ No master tables found!")
                
                # Show all tables to help debug
                print("\nAll tables in database:")
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_type = 'BASE TABLE' 
                    ORDER BY table_name
                    LIMIT 20
                """)
                all_tables = cursor.fetchall()
                for table_name, in all_tables:
                    print(f"  {table_name}")
                    
    except Exception as e:
        print(f"❌ Error inspecting database: {e}")
        
    finally:
        conn_manager.close_connection()

if __name__ == '__main__':
    check_master_tables()
