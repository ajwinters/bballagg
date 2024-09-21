import psycopg2
import pandas as pd
from nba_api.stats.endpoints import CommonPlayerInfo, LeagueGameFinder  # Example endpoints
from nba_api.stats.static import players, teams

#pandas to sql type conversion
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


def create_table(conn, table_name, dataframe):
    cursor = conn.cursor()
    columns = ', '.join([f"{col} {map_dtype_to_postgresql(dtype)}" for col, dtype in zip(dataframe.columns, dataframe.dtypes)])

    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    cursor.execute(create_query)
    conn.commit()
    print(f"Master table {table_name} created successfully.")









