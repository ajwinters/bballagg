#!/usr/bin/env python3
"""Quick script to check database table structure"""

from database_manager import MasterTablesManager

def check_tables():
    db = MasterTablesManager()
    conn = db.connect_to_database()
    
    if not conn:
        print("❌ Could not connect to database")
        return
    
    try:
        cursor = conn.cursor()
        
        # Check what player tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE '%players%'
        """)
        
        tables = cursor.fetchall()
        print("🏀 Player tables found:", [t[0] for t in tables])
        
        # Check if nba_players table exists and its structure
        if any('nba_players' in str(t) for t in tables):
            print("\n📋 nba_players table structure:")
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'nba_players'
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[0]}: {col[1]} (nullable: {col[2]})")
            
            # Check constraints
            print("\n🔐 nba_players constraints:")
            cursor.execute("""
                SELECT constraint_name, constraint_type 
                FROM information_schema.table_constraints 
                WHERE table_name = 'nba_players'
            """)
            
            constraints = cursor.fetchall()
            for const in constraints:
                print(f"  {const[0]}: {const[1]}")
        else:
            print("❌ nba_players table does not exist")
            
            # Let's create the table
            print("\n🔨 Creating nba_players table...")
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS nba_players (
                    playerid VARCHAR(50) PRIMARY KEY,
                    playername VARCHAR(200),
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
                    isactive BOOLEAN,
                    league VARCHAR(20),
                    createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            
            cursor.execute(create_table_sql)
            conn.commit()
            print("✅ nba_players table created successfully")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        conn.close()

if __name__ == '__main__':
    check_tables()
