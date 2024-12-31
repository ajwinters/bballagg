import pandas as pd
import time
import numpy as np
from nba_api.stats.endpoints import playercareerstats
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.endpoints import leaguegamefinder
import psycopg2
from psycopg2.extras import execute_batch  # Import execute_batch from extras
import re
from psycopg2 import sql
import rdshelp

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

def clean_column_names(df):
    # Function to remove special characters and spaces, and convert to lowercase
    df.columns = [re.sub(r'[^a-zA-Z0-9]', '', col).lower() for col in df.columns]
    return df

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

# Define a function to pull data from a table and return a DataFrame
def fetch_table_data(connection, table_name):
    try:
        # Create a cursor to interact with the database
        cursor = connection.cursor()
        # Query to pull all data from the table
        query = f"SELECT * FROM {table_name};"
        # Execute the query
        cursor.execute(query)
        # Fetch all rows and column names
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        # Close the cursor
        cursor.close()
        # Create a DataFrame from the fetched data
        df = pd.DataFrame(rows, columns=columns)
        return df

    except Exception as error:
        print(f"Error: {error}")
        return None
    
def check_table_exists(connection, table_name):
    try:
        cursor = connection.cursor()

        # Query to check if the table exists in the 'public' schema
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = 'public' AND tablename = %s
            );
        """, (table_name,))
        
        # Fetch the result (True/False)
        exists = cursor.fetchone()[0]
        
        # Close the cursor
        cursor.close()
        
        return exists

    except Exception as error:
        print(f"Error: {error}")
        return False
    
def create_table(conn, table_name, dataframe):
    cursor = conn.cursor()
    columns = ', '.join([f"{col} {map_dtype_to_postgresql(dtype)}" for col, dtype in zip(clean_column_names(dataframe).columns, dataframe.dtypes)])

    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    cursor.execute(create_query)
    conn.commit()
    print(f"Master table {table_name} created successfully or already exists.")

def query_database_to_dataframe(conn, query):
    try:
        dataframe = pd.read_sql(query, conn)
        return dataframe
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return None
    

def insert_dataframe_to_rds(conn, df, table_name):
   
    # Create a cursor object
    cur = conn.cursor()
    
    # Prepare a SQL query for inserting data
    df = clean_column_names(df)
    columns = df.columns
    insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    
    # Convert DataFrame rows into a list of tuples for psycopg2 to handle
    data_tuples = [tuple(row) for row in df.to_numpy()]
    
    try:
        # Execute batch insert to improve performance
        psycopg2.extras.execute_batch(cur, insert_query, data_tuples)
        # Commit the transaction
        conn.commit()
        print(f"Data inserted successfully into {table_name} table.")
    except Exception as e:
        # Roll back the transaction in case of error
        conn.rollback()
        print(f"Error: {e}")

def fetch_table_to_dataframe(conn, table_name):

    # Create a cursor object
    cur = conn.cursor()
    
    try:
        # Execute the SQL query to fetch all data from the table
        query = f"SELECT * FROM {table_name};"
        cur.execute(query)
        
        # Fetch all the data
        data = cur.fetchall()
        
        # Get the column names from the cursor
        colnames = [desc[0] for desc in cur.description]
        
        # Convert the data into a pandas DataFrame
        df = pd.DataFrame(data, columns=colnames)
        
        print(f"Data fetched successfully from {table_name} table.")
        return df
    
    except Exception as e:
        print(f"Error: {e}")
        return None
    
def game_difference(conn,tablename):
    allgames = fetch_table_data(conn,"mastergames")
    allgameslist = allgames['gameid'].to_list()

    tablegames = fetch_table_data(conn,tablename)
    tablegameslist = tablegames['gameid'].to_list()
    difference = set(allgameslist) - set(tablegameslist)
    result = list(difference)
    return result

def player_difference(conn,tablename):
    allplayers = fetch_table_data(conn,"masterplayers")
    allplayerslist = allplayers['playerid'].to_list()

    tableplayers = fetch_table_data(conn,tablename)
    tableplayerslist = tableplayers['playerid'].to_list()
    difference = set(allplayerslist) - set(tableplayerslist)
    result = list(difference)
    return result

