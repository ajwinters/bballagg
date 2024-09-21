import psycopg2
import pandas as pd
from nba_api.stats.endpoints import CommonPlayerInfo, LeagueGameFinder  # Example endpoints
from nba_api.stats.static import players, teams

# Database Connection
def connect_to_rds(db_name, username, password, host, port=5432):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=host,
            port=port
        )
        print("Connected to RDS PostgreSQL database")
        return conn
    except Exception as e:
        print(f"Error connecting to RDS: {str(e)}")
        return None

# Function to create master tables (e.g., playerids, gameids)
def create_master_table(conn, table_name, dataframe):
    cursor = conn.cursor()
    columns = ', '.join([f"{col} {map_dtype_to_postgresql(dtype)}" for col, dtype in zip(dataframe.columns, dataframe.dtypes)])

    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    cursor.execute(create_query)
    conn.commit()
    print(f"Master table {table_name} created successfully.")

# Insert data into master tables
def insert_master_data(conn, table_name, dataframe):
    cursor = conn.cursor()
    columns = ', '.join(dataframe.columns)
    placeholders = ', '.join(['%s'] * len(dataframe.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
    
    data = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]
    cursor.executemany(insert_query, data)
    conn.commit()
    print(f"Inserted {len(data)} records into {table_name}.")

# Data type mapping from pandas to PostgreSQL
def map_dtype_to_postgresql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'TEXT'  # Default to TEXT for object or string types

# Create a table in PostgreSQL based on the dataframe schema and inferred data types
def create_endpoint_table(conn, endpoint_name, dataframe, index):
    table_name = f"{endpoint_name}_{index}"
    cursor = conn.cursor()

    # Generate the SQL statement for creating a table with inferred datatypes
    columns = ', '.join([f"{col} {map_dtype_to_postgresql(dtype)}" for col, dtype in zip(dataframe.columns, dataframe.dtypes)])
    
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    
    cursor.execute(create_query)
    conn.commit()
    print(f"Table {table_name} created with inferred schema for endpoint {endpoint_name}.")

# Insert data into endpoint tables
def insert_endpoint_data(conn, table_name, dataframe):
    cursor = conn.cursor()
    columns = ', '.join(dataframe.columns)
    placeholders = ', '.join(['%s'] * len(dataframe.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
    
    data = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]
    cursor.executemany(insert_query, data)
    conn.commit()
    print(f"Inserted {len(data)} records into {table_name}.")

# Process endpoints based on parameters from master tables
def process_endpoint(conn, endpoint_name, endpoint_function, params):
    for param in params:
        response = endpoint_function(param).get_data_frames()  # Call the endpoint
        for index, dataframe in enumerate(response):
            table_name = f"{endpoint_name}_{index}"
            create_endpoint_table(conn, endpoint_name, dataframe, index)
            insert_endpoint_data(conn, table_name, dataframe)

# Example: Update Master Table for Player IDs
def update_master_playerids(conn):
    all_players = players.get_players()  # Fetch player metadata
    player_df = pd.DataFrame(all_players)
    create_master_table(conn, 'playerids', player_df)
    insert_master_data(conn, 'playerids', player_df)

# Example: Process CommonPlayerInfo for each player
def process_commonplayerinfo(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM playerids;")
    player_ids = cursor.fetchall()
    process_endpoint(conn, 'commonplayerinfo', CommonPlayerInfo, player_ids)

# Main Function
def main():
    # Database connection
    conn = connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')

    if conn:
        # Update master tables
        update_master_playerids(conn)

        # Process specific endpoint (e.g., CommonPlayerInfo)
        process_commonplayerinfo(conn)

        conn.close()

if __name__ == "__main__":
    main()





