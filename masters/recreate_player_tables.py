#!/usr/bin/env python3
"""
Recreate Player Tables Script

This script uses the database_manager functions to properly recreate
all player tables with the correct structure for the collection system.
"""

from database_manager import MasterTablesManager

def main():
    print("🏀 NBA PLAYER TABLES RECREATION")
    print("=" * 50)
    
    print("This will drop and recreate all player tables:")
    print("  • nba_players")
    print("  • wnba_players") 
    print("  • gleague_players")
    print("\n⚠️  WARNING: This will delete all existing player data!")
    
    confirm = input("\nContinue with table recreation? (yes/no): ")
    if confirm.lower() != 'yes':
        print("❌ Table recreation cancelled.")
        return
    
    # Initialize database manager
    db_manager = MasterTablesManager()
    
    # Recreate all player tables
    print("\n🔨 Starting table recreation...")
    success = db_manager.recreate_players_tables()
    
    if success:
        print("\n🎉 ALL PLAYER TABLES SUCCESSFULLY RECREATED!")
        print("\n🎯 NEXT STEPS:")
        print("1. Run the comprehensive players collection:")
        print("   python run_comprehensive_players_collection.py --mode test --league NBA")
        print("2. The tables now have proper PRIMARY KEY constraints")
        print("3. ON CONFLICT clauses will work correctly")
    else:
        print("\n❌ Some tables failed to recreate. Check the error messages above.")

if __name__ == '__main__':
    main()
