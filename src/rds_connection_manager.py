#!/usr/bin/env python3
"""
Enhanced RDS Connection Manager for NBA Data Collection
Consolidates database operations with connection management, sleep/wake detection, and data utilities
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
import re
import time
import logging
import os
import json
from contextlib import contextmanager
from typing import Optional, Any

# Setup logger with ASCII-only messages
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rds_connection_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RDSConnectionManager:
    """
    Enhanced RDS connection manager with sleep/wake detection and comprehensive data utilities
    """
    
    def __init__(self, db_config=None, max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize the RDS connection manager
        
        Args:
            db_config: Database configuration dict or None to load from config file
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retry attempts in seconds
        """
        self.connection = None
        self.cursor = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.last_activity_time = time.time()
        self.connection_attempts = 0
        
        # Load database configuration
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = self._load_database_config()
    
    def _load_database_config(self):
        """Load database configuration from config file"""
        try:
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'database_config.json')
            with open(config_path, 'r') as f:
                config = json.load(f)
                
            return {
                'host': config['host'],
                'database': config['name'],
                'user': config['user'],
                'password': config['password'],
                'port': int(config['port']),
                'sslmode': config.get('ssl_mode', 'require'),
                'connect_timeout': 60
            }
        except Exception as e:
            logger.error(f"Failed to load database config: {e}")
            # Fallback to environment variables
            return {
                'host': os.getenv('DB_HOST', 'localhost'),
                'database': os.getenv('DB_NAME', 'nba_data'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', ''),
                'port': int(os.getenv('DB_PORT', 5432)),
                'sslmode': os.getenv('DB_SSLMODE', 'require'),
                'connect_timeout': 60
            }

    def detect_sleep_wake_cycle(self, threshold_minutes: int = 5) -> bool:
        """
        Detect if the computer has been asleep/hibernated based on time gaps
        
        Args:
            threshold_minutes: Time gap in minutes that indicates sleep/wake cycle
            
        Returns:
            bool: True if sleep/wake cycle detected
        """
        current_time = time.time()
        time_gap = current_time - self.last_activity_time
        self.last_activity_time = current_time
        
        if time_gap > (threshold_minutes * 60):
            logger.warning(f"[SLEEP/WAKE] Detected {time_gap/60:.1f} minute gap - possible sleep/wake cycle")
            return True
        return False

    def create_connection(self) -> bool:
        """
        Create a new database connection
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
                self.connection = None
                self.cursor = None
            
            logger.info("[CONNECT] Creating new database connection...")
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            self.connection_attempts = 0
            logger.info("[SUCCESS] Database connection established")
            return True
            
        except Exception as e:
            self.connection_attempts += 1
            logger.error(f"[ERROR] Connection failed (attempt {self.connection_attempts}): {e}")
            return False

    def test_connection(self) -> bool:
        """
        Test if the current connection is still valid
        
        Returns:
            bool: True if connection is healthy, False otherwise
        """
        if not self.connection or not self.cursor:
            return False
        
        try:
            # Simple test query
            self.cursor.execute("SELECT 1")
            self.cursor.fetchone()
            return True
        except Exception as e:
            logger.warning(f"[WARNING] Connection test failed: {e}")
            return False

    def ensure_connection(self) -> bool:
        """
        Ensure we have a valid database connection with sleep/wake detection
        
        Returns:
            bool: True if connection is available, False otherwise
        """
        # Check for sleep/wake cycle
        if self.detect_sleep_wake_cycle():
            logger.warning("[SLEEP/WAKE] Sleep/wake cycle detected - forcing reconnection")
            return self.reconnect()
        
        # Test existing connection
        if self.test_connection():
            return True
        
        logger.warning("[RECONNECT] Connection lost - attempting to reconnect...")
        return self.reconnect()

    def reconnect(self) -> bool:
        """
        Reconnect to the database with retry logic
        
        Returns:
            bool: True if reconnection successful, False otherwise
        """
        for attempt in range(self.max_retries):
            logger.info(f"[RECONNECT] Reconnection attempt {attempt + 1}/{self.max_retries}")
            
            if self.create_connection():
                logger.info("[SUCCESS] Reconnection successful")
                return True
            
            if attempt < self.max_retries - 1:
                logger.warning(f"[WAIT] Waiting {self.retry_delay} seconds before next attempt...")
                time.sleep(self.retry_delay)
        
        logger.error("[FAILED] All reconnection attempts failed")
        return False

    @contextmanager
    def get_cursor(self):
        """
        Context manager for database operations with automatic connection management
        """
        if not self.ensure_connection():
            raise Exception("Could not establish database connection")
        
        try:
            yield self.cursor
            self.connection.commit()
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            logger.error(f"[ERROR] Database operation failed: {e}")
            raise

    def execute_query(self, query: str, params: tuple = None) -> Optional[Any]:
        """
        Execute a query with automatic connection management
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query result or None if failed
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                if query.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                return cursor.rowcount
        except Exception as e:
            logger.error(f"[ERROR] Query execution failed: {e}")
            return None

    def close_connection(self):
        """
        Close the database connection
        """
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
            logger.info("[CLOSED] Database connection closed")
        except Exception as e:
            logger.error(f"[ERROR] Error closing connection: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_connection()

    # === DATA UTILITY FUNCTIONS (Consolidated from allintwo files) ===

    def clean_column_names(self, df):
        """
        Clean column names for PostgreSQL compatibility with reserved keyword handling
        Enhanced version from allintwo_1.py
        """
        # PostgreSQL reserved keywords that need special handling
        reserved_keywords = {
            'to': 'turnovers',
            'from': 'from_field', 
            'order': 'order_field',
            'group': 'group_field',
            'select': 'select_field',
            'where': 'where_field',
            'having': 'having_field',
            'union': 'union_field',
            'user': 'user_field'
        }
        
        # Function to remove special characters and spaces, and convert to lowercase
        cleaned_columns = []
        for col in df.columns:
            cleaned = re.sub(r'[^a-zA-Z0-9]', '', col).lower()
            # Handle reserved keywords
            if cleaned in reserved_keywords:
                cleaned = reserved_keywords[cleaned]
            cleaned_columns.append(cleaned)
        
        df.columns = cleaned_columns
        return df

    def map_dtype_to_postgresql(self, dtype):
        """Map pandas dtypes to PostgreSQL types"""
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

    def fetch_table_data(self, table_name):
        """
        Pull data from a table and return a DataFrame
        Uses the enhanced connection management
        """
        try:
            with self.get_cursor() as cursor:
                query = f"SELECT * FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                return df
        except Exception as error:
            logger.error(f"Error fetching table data: {error}")
            return None

    def check_table_exists(self, table_name):
        """Check if a table exists in the public schema"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_tables
                        WHERE schemaname = 'public' AND tablename = %s
                    );
                """, (table_name,))
                exists = cursor.fetchone()[0]
                return exists
        except Exception as error:
            logger.error(f"Error checking table existence: {error}")
            return False
    
    def create_table(self, table_name, dataframe):
        """Create a table based on DataFrame structure"""
        try:
            with self.get_cursor() as cursor:
                # Clean the dataframe for column names
                cleaned_df = self.clean_column_names(dataframe.copy())
                columns = ', '.join([f"{col} {self.map_dtype_to_postgresql(dtype)}" 
                                   for col, dtype in zip(cleaned_df.columns, dataframe.dtypes)])

                create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
                cursor.execute(create_query)
                logger.info(f"Table {table_name} created successfully or already exists.")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    def query_database_to_dataframe(self, query):
        """Execute a query and return results as DataFrame"""
        try:
            if not self.ensure_connection():
                raise Exception("Could not establish database connection")
            
            dataframe = pd.read_sql(query, self.connection)
            return dataframe
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None

    def insert_dataframe_to_rds(self, df, table_name):
        """
        Insert DataFrame to RDS table with batch processing
        Enhanced version with better error handling
        """
        try:
            with self.get_cursor() as cursor:
                # Clean column names
                df = self.clean_column_names(df.copy())
                columns = df.columns
                
                # Prepare SQL query for inserting data
                insert_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                
                # Convert DataFrame rows into a list of tuples
                data_tuples = [tuple(row) for row in df.to_numpy()]
                
                # Execute batch insert for better performance
                execute_batch(cursor, insert_query, data_tuples)
                logger.info(f"Data inserted successfully into {table_name} table ({len(data_tuples)} rows).")
                
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            raise

    def fetch_table_to_dataframe(self, table_name):
        """Fetch all data from a table as DataFrame"""
        try:
            with self.get_cursor() as cursor:
                query = f"SELECT * FROM {table_name};"
                cursor.execute(query)
                data = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(data, columns=colnames)
                logger.info(f"Data fetched successfully from {table_name} table.")
                return df
        except Exception as e:
            logger.error(f"Error fetching data from {table_name}: {e}")
            return None

    def game_difference(self, tablename):
        """
        Find games in master games table that are not in the specified endpoint table
        """
        try:
            query = f"""
                SELECT gameid 
                FROM nba_games 
                WHERE gameid NOT IN (
                    SELECT DISTINCT gameid 
                    FROM {tablename} 
                    WHERE gameid IS NOT NULL
                )
                ORDER BY gameid;
            """
            return self.query_database_to_dataframe(query)
        except Exception as e:
            logger.error(f"Error finding game differences: {e}")
            return None

    def player_difference(self, tablename):
        """
        Find players in master players table that are not in the specified endpoint table
        """
        try:
            query = f"""
                SELECT personid 
                FROM nba_players 
                WHERE personid NOT IN (
                    SELECT DISTINCT player_id 
                    FROM {tablename} 
                    WHERE player_id IS NOT NULL
                )
                ORDER BY personid;
            """
            return self.query_database_to_dataframe(query)
        except Exception as e:
            logger.error(f"Error finding player differences: {e}")
            return None


# === CONVENIENCE FUNCTIONS FOR BACKWARD COMPATIBILITY ===

def connect_to_rds(db_name, username, password, host, port=5432):
    """
    Legacy function for backward compatibility
    """
    db_config = {
        'database': db_name,
        'user': username,
        'password': password,
        'host': host,
        'port': port
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        print("Connected to RDS PostgreSQL database")
        return conn
    except Exception as e:
        print(f"Error connecting to RDS: {str(e)}")
        return None


def clean_column_names(df):
    """Legacy function - use RDSConnectionManager.clean_column_names() instead"""
    manager = RDSConnectionManager()
    return manager.clean_column_names(df)


def map_dtype_to_postgresql(dtype):
    """Legacy function - use RDSConnectionManager.map_dtype_to_postgresql() instead"""
    manager = RDSConnectionManager()
    return manager.map_dtype_to_postgresql(dtype)


if __name__ == "__main__":
    # Test the connection manager
    print("Testing RDS Connection Manager...")
    
    with RDSConnectionManager() as conn_manager:
        if conn_manager.ensure_connection():
            print("SUCCESS: Connection successful!")
            
            # Test a simple query
            result = conn_manager.execute_query("SELECT 1 as test")
            if result:
                print(f"SUCCESS: Query test successful: {result}")
            else:
                print("ERROR: Query test failed")
        else:
            print("ERROR: Connection failed")
