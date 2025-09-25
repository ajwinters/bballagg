#!/usr/bin/env python3
"""
Database Cleanup Utility
Provides tools to clean up NBA/WNBA/G-League tables from the database
"""

import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.rds_connection_manager import RDSConnectionManager

def load_database_config():
    """Load database configuration from config file"""
    config_path = project_root / 'config' / 'database_config.json'
    with open(config_path, 'r') as f:
        db_config = json.load(f)
    
    return {
        'host': db_config['host'],
        'database': db_config['name'],
        'user': db_config['user'],
        'password': db_config['password'],
        'port': int(db_config['port']),
        'sslmode': db_config.get('ssl_mode', 'require'),
        'connect_timeout': 60
    }

def preview_cleanup():
    """Preview what tables would be cleaned up without actually deleting them"""
    print("DATABASE CLEANUP PREVIEW")
    print("=" * 60)
    
    db_config = load_database_config()
    db_manager = RDSConnectionManager(db_config)
    
    try:
        with db_manager.get_cursor() as cursor:
            # Get all tables
            cursor.execute("""
                SELECT tablename, schemaname,
                       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            
            all_tables = cursor.fetchall()
            
            if not all_tables:
                print("SUCCESS: No tables found in the database!")
                return
            
            nba_tables = []
            other_tables = []
            
            for table_name, schema, size in all_tables:
                if any(table_name.startswith(prefix) for prefix in ['nba_', 'wnba_', 'gleague_']) or table_name == 'failed_api_calls':
                    nba_tables.append((table_name, size))
                else:
                    other_tables.append((table_name, size))
            
            print(f"CURRENT DATABASE STATE:")
            print(f"   • Total tables: {len(all_tables)}")
            print(f"   • NBA/WNBA/G-League tables: {len(nba_tables)}")
            print(f"   • Other tables: {len(other_tables)}")
            print()
            
            if nba_tables:
                print("NBA/WNBA/G-League + API Error tables TO BE DELETED:")
                print("-" * 50)
                for table_name, size in nba_tables:
                    print(f"   • {table_name:<45} {size}")
                print()
            
            if other_tables:
                print("Other tables (WILL BE KEPT):")
                print("-" * 50)
                for table_name, size in other_tables:
                    print(f"   • {table_name:<45} {size}")
                print()
            
            if nba_tables:
                print("WARNING: This will permanently delete all NBA/WNBA/G-League + API error data!")
                print(f"WARNING: {len(nba_tables)} tables will be removed from the database!")
            else:
                print("SUCCESS: No tables to clean up - database is already clean!")
    
    except Exception as e:
        print(f"ERROR: Error previewing cleanup: {e}")

def cleanup_database(confirm_cleanup=False):
    """Clean up all NBA/WNBA/G-League tables from the database"""
    if not confirm_cleanup:
        print("SAFETY CHECK: confirm_cleanup=True required to proceed")
        print("   Use: cleanup_database(confirm_cleanup=True)")
        return
    
    print("STARTING DATABASE CLEANUP...")
    print("=" * 60)
    
    db_config = load_database_config()
    db_manager = RDSConnectionManager(db_config)
    
    try:
        with db_manager.get_cursor() as cursor:
            # Get NBA/WNBA/G-League tables and failed_api_calls
            cursor.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                  AND (tablename LIKE 'nba_%' 
                       OR tablename LIKE 'wnba_%' 
                       OR tablename LIKE 'gleague_%'
                       OR tablename = 'failed_api_calls')
                ORDER BY tablename
            """)
            
            tables_to_delete = [row[0] for row in cursor.fetchall()]
            
            if not tables_to_delete:
                print("SUCCESS: No NBA tables found - database is already clean!")
                return
            
            print(f"FOUND: Found {len(tables_to_delete)} tables to delete")
            print("DELETING: Dropping tables...")
            print("-" * 40)
            
            deleted_count = 0
            for table_name in tables_to_delete:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                    print(f"   SUCCESS: Deleted: {table_name}")
                    deleted_count += 1
                except Exception as e:
                    print(f"   ERROR: Failed to delete {table_name}: {e}")
            
            # Commit the changes
            cursor.connection.commit()
            
            print("-" * 40)
            print(f"SUCCESS: DATABASE CLEANUP COMPLETE!")
            print(f"   • Tables deleted: {deleted_count}")
            print(f"   • Tables failed: {len(tables_to_delete) - deleted_count}")
            print("READY: Database is now ready for fresh data collection!")
    
    except Exception as e:
        print(f"ERROR: Error during cleanup: {e}")

def main():
    """Main function to run cleanup operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Database Cleanup Utility')
    parser.add_argument('--preview', action='store_true', help='Preview cleanup without deleting')
    parser.add_argument('--cleanup', action='store_true', help='Actually perform the cleanup')
    parser.add_argument('--confirm', action='store_true', help='Confirm cleanup (required for actual deletion)')
    
    args = parser.parse_args()
    
    if args.preview:
        preview_cleanup()
    elif args.cleanup:
        cleanup_database(confirm_cleanup=args.confirm)
    else:
        print("Database Cleanup Utility")
        print("========================")
        print("Options:")
        print("  --preview          Preview what would be cleaned up")
        print("  --cleanup --confirm Actually perform the cleanup")
        print()
        print("Examples:")
        print("  python database_cleanup.py --preview")
        print("  python database_cleanup.py --cleanup --confirm")

if __name__ == "__main__":
    main()