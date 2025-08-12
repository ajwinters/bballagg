#!/usr/bin/env python3

"""
RDS Connection Manager with Sleep/Wake Detection and ASCII Logging
Enhanced database connection management for long-running NBA data collection processes
"""

import psycopg2
import time
import logging
from contextlib import contextmanager
from typing import Optional, Any
import os

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
    Enhanced RDS connection manager with sleep/wake cycle detection and automatic reconnection
    All logging messages use ASCII characters for Windows PowerShell compatibility
    """
    
    def __init__(self, max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize the RDS connection manager
        
        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retry attempts in seconds
        """
        self.connection = None
        self.cursor = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.last_activity_time = time.time()
        self.connection_attempts = 0
        
        # Database configuration from environment
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'nba_data'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': int(os.getenv('DB_PORT', 5432))
        }
        
        logger.info("[INIT] RDS Connection Manager initialized with ASCII logging")
    
    def detect_sleep_wake_cycle(self, threshold_minutes: int = 5) -> bool:
        """
        Detect if the system likely went through a sleep/wake cycle
        
        Args:
            threshold_minutes: Time gap that suggests sleep/wake occurred
            
        Returns:
            bool: True if sleep/wake cycle detected
        """
        current_time = time.time()
        time_gap_minutes = (current_time - self.last_activity_time) / 60
        
        if time_gap_minutes > threshold_minutes:
            logger.warning(f"[SLEEP/WAKE] Time gap detected: {time_gap_minutes:.1f} minutes")
            logger.warning("[INFO] This suggests the PC went through a sleep/wake cycle")
            self.last_activity_time = current_time
            return True
        
        self.last_activity_time = current_time
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
