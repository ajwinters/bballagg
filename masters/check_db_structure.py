"""
Quick check of existing database structure
"""
import psycopg2

# Database config
db_config = {
    'database': 'thebigone',
    'user': 'ajwin', 
    'password': 'CharlesBark!23',
    'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
    'port': 5432
}

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Check mastergames table structure
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'mastergames' 
        ORDER BY ordinal_position;
    """)
    
    columns = cursor.fetchall()
    print("mastergames table structure:")
    for col_name, col_type in columns:
        print(f"  {col_name}: {col_type}")
    
    # Check sample data
    cursor.execute("SELECT * FROM mastergames LIMIT 3;")
    sample = cursor.fetchall()
    print(f"\nSample data (first 3 rows):")
    for i, row in enumerate(sample):
        print(f"  Row {i+1}: {row}")
    
    # Check total count
    cursor.execute("SELECT COUNT(*) FROM mastergames;")
    count = cursor.fetchone()[0]
    print(f"\nTotal games in mastergames: {count:,}")
    
    conn.close()
    print("\n✅ Database structure check complete")
    
except Exception as e:
    print(f"Error: {str(e)}")
