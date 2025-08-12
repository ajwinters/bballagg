#!/usr/bin/env python3
"""
RDS Connection Manager with automatic reconnection and connection health checks
"""
import psycopg2
from psycopg2.extras import execute_batch
import time
import logging

logger = logging.getLogger(__name__)

class RDSConnectionManager:
    """
    Robust RDS connection manager with automatic reconnection, connection pooling,
    and health checks for long-running processes.
    """
    
    def __init__(self, db_name, username, password, host, port=5432, 
                 max_retries=3, retry_delay=5, connection_timeout=30):
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_timeout = connection_timeout
        self.conn = None
        self.last_activity = time.time()
        
    def connect(self):
        """Establish initial connection to RDS"""
        for attempt in range(self.max_retries + 1):
            try:
                self.conn = psycopg2.connect(
                    dbname=self.db_name,
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    connect_timeout=self.connection_timeout
                )
                self.conn.autocommit = False  # Manual transaction control
                self.last_activity = time.time()
                logger.info("✅ Connected to RDS PostgreSQL database")
                return self.conn
                
            except Exception as e:
                logger.error(f"❌ Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries:
                    logger.info(f"⏳ Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("🚫 All connection attempts failed")
                    raise e
    
    def is_connection_alive(self):
        """Check if connection is still alive"""
        if not self.conn:
            return False
            
        try:
            # Use a lightweight query to test connection
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
                return True
        except (psycopg2.Error, psycopg2.InterfaceError, psycopg2.OperationalError):
            return False
    
    def ensure_connection(self):
        """Ensure connection is alive, reconnect if necessary with idle state handling"""
        try:
            # Check if connection has been idle for too long (30 minutes)
            idle_time = time.time() - self.last_activity
            if idle_time > 1800:  # 30 minutes
                logger.warning(f"🕐 Connection has been idle for {idle_time/60:.1f} minutes - proactively reconnecting...")
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                self.conn = None
            
            if not self.is_connection_alive():
                logger.warning("🔄 Connection lost, attempting to reconnect...")
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                self.connect()
                
            self.last_activity = time.time()
            return self.conn
            
        except Exception as e:
            logger.error(f"❌ Failed to ensure connection: {str(e)}")
            raise e
    
    def execute_query(self, query, params=None, fetch_results=False):
        """Execute query with automatic reconnection"""
        max_query_retries = 2
        
        for attempt in range(max_query_retries):
            try:
                conn = self.ensure_connection()
                with conn.cursor() as cur:
                    if params:
                        cur.execute(query, params)
                    else:
                        cur.execute(query)
                    
                    if fetch_results:
                        results = cur.fetchall()
                        colnames = [desc[0] for desc in cur.description] if cur.description else []
                        return results, colnames
                    
                    conn.commit()
                    return None, None
                    
            except (psycopg2.Error, psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                logger.warning(f"⚠️  Query attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_query_retries - 1:
                    # Force reconnection on next attempt
                    if self.conn:
                        try:
                            self.conn.close()
                        except:
                            pass
                        self.conn = None
                    time.sleep(2)
                else:
                    logger.error(f"❌ Query failed after {max_query_retries} attempts")
                    raise e
    
    def cursor(self):
        """Get cursor with connection health check"""
        conn = self.ensure_connection()
        return conn.cursor()
    
    def commit(self):
        """Commit with connection health check"""
        conn = self.ensure_connection()
        conn.commit()
    
    def close(self):
        """Close connection safely"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("✅ Database connection closed")
            except:
                pass
            finally:
                self.conn = None


def connect_to_rds_robust(db_name, username, password, host, port=5432):
    """
    Create a robust RDS connection manager
    Replacement for the simple connect_to_rds function
    """
    manager = RDSConnectionManager(db_name, username, password, host, port)
    manager.connect()
    return manager
