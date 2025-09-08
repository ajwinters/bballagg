#!/usr/bin/env python3
"""Fix the nba_players table structure to match the collector expectations"""

from database_manager import MasterTablesManager

def fix_players_tables():
    db = MasterTablesManager()
    conn = db.connect_to_database()
    
    if not conn:
        print("❌ Could not connect to database")
        return
    
    try:
        cursor = conn.cursor()
        
        # Drop and recreate nba_players table with correct structure
        print("🔄 Recreating nba_players table with correct structure...")
        
        # Drop existing table
        cursor.execute("DROP TABLE IF EXISTS nba_players CASCADE;")
        
        # Create new table with correct structure
        create_table_sql = """
            CREATE TABLE nba_players (
                playerid VARCHAR(50) PRIMARY KEY,
                playername VARCHAR(200) NOT NULL,
                firstname VARCHAR(100),
                lastname VARCHAR(100),
                birthdate VARCHAR(50),
                college VARCHAR(200),
                country VARCHAR(100),
                height VARCHAR(20),
                weight VARCHAR(20),
                position VARCHAR(20),
                draftyear INTEGER,
                draftround INTEGER,
                draftnumber INTEGER,
                isactive BOOLEAN DEFAULT TRUE,
                league VARCHAR(20) DEFAULT 'NBA',
                createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        
        cursor.execute(create_table_sql)
        print("✅ nba_players table recreated successfully")
        
        # Also fix other league tables
        for league_prefix in ['wnba', 'gleague']:
            table_name = f"{league_prefix}_players"
            print(f"🔄 Recreating {table_name} table...")
            
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            
            create_league_table = f"""
                CREATE TABLE {table_name} (
                    playerid VARCHAR(50) PRIMARY KEY,
                    playername VARCHAR(200) NOT NULL,
                    firstname VARCHAR(100),
                    lastname VARCHAR(100),
                    birthdate VARCHAR(50),
                    college VARCHAR(200),
                    country VARCHAR(100),
                    height VARCHAR(20),
                    weight VARCHAR(20),
                    position VARCHAR(20),
                    draftyear INTEGER,
                    draftround INTEGER,
                    draftnumber INTEGER,
                    isactive BOOLEAN DEFAULT TRUE,
                    league VARCHAR(20) DEFAULT '{league_prefix.upper().replace('GLEAGUE', 'G-League')}',
                    createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            
            cursor.execute(create_league_table)
            print(f"✅ {table_name} table recreated successfully")
        
        conn.commit()
        print("\n🎉 All player tables fixed and ready for data collection!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    fix_players_tables()
