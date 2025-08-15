#!/usr/bin/env python3
"""
Check RDS connection limits and current connections
"""

import psycopg2
import os
import time

def check_connection_limits():
    """Check current connections vs max connections"""
    
    # First, try with environment variables
    config = {
        'host': os.getenv('DB_HOST', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com'),
        'database': os.getenv('DB_NAME', 'thebigone'),
        'user': os.getenv('DB_USER', 'ajwin'),
        'password': os.getenv('DB_PASSWORD', 'your_password_here'),  # You'll need to set this
        'port': int(os.getenv('DB_PORT', 5432)),
        'sslmode': 'require',
        'connect_timeout': 10
    }
    
    print("=== RDS Connection Limits Check ===")
    print(f"Attempting connection to: {config['host']}:{config['port']}")
    print(f"Database: {config['database']}")
    print(f"User: {config['user']}")
    
    try:
        # Try to establish a connection
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        
        print("SUCCESS: Connected to database!")
        
        # Check current connections
        cursor.execute("""
            SELECT 
                count(*) as current_connections,
                setting as max_connections
            FROM pg_stat_activity 
            CROSS JOIN pg_settings 
            WHERE pg_settings.name = 'max_connections';
        """)
        
        current, max_conn = cursor.fetchone()
        print(f"Current connections: {current}")
        print(f"Max connections: {max_conn}")
        print(f"Usage: {(current/int(max_conn))*100:.1f}%")
        
        if current >= int(max_conn) * 0.9:
            print("WARNING: Connection pool is nearly full!")
        
        # Check for idle connections
        cursor.execute("""
            SELECT 
                state, 
                count(*) as count,
                avg(extract(epoch from now() - state_change)) as avg_duration_seconds
            FROM pg_stat_activity 
            WHERE pid <> pg_backend_pid()
            GROUP BY state
            ORDER BY count DESC;
        """)
        
        print("\n=== Connection States ===")
        for state, count, avg_duration in cursor.fetchall():
            if avg_duration:
                print(f"{state}: {count} connections (avg: {avg_duration:.0f}s)")
            else:
                print(f"{state}: {count} connections")
        
        # Check for long-running queries
        cursor.execute("""
            SELECT 
                pid,
                usename,
                application_name,
                state,
                extract(epoch from now() - query_start) as duration_seconds,
                left(query, 100) as query_snippet
            FROM pg_stat_activity 
            WHERE state = 'active' 
                AND pid <> pg_backend_pid()
                AND query_start < now() - interval '1 minute'
            ORDER BY duration_seconds DESC
            LIMIT 10;
        """)
        
        long_queries = cursor.fetchall()
        if long_queries:
            print("\n=== Long-Running Queries (>1 min) ===")
            for pid, user, app, state, duration, query in long_queries:
                print(f"PID {pid} ({user}): {duration:.0f}s - {query}...")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        error_str = str(e)
        print(f"CONNECTION FAILED: {error_str}")
        
        if "too many connections" in error_str.lower():
            print("\n=== DIAGNOSIS: TOO MANY CONNECTIONS ===")
            print("The RDS instance has reached its connection limit.")
            print("Solutions:")
            print("1. Wait for connections to close naturally")
            print("2. Increase RDS instance size for more connections")
            print("3. Kill idle connections (requires admin access)")
            return False
        elif "server closed the connection unexpectedly" in error_str:
            print("\n=== DIAGNOSIS: SERVER CONNECTION ISSUE ===")
            print("Possible causes:")
            print("1. RDS instance is restarting/maintenance")
            print("2. Network connectivity issues")
            print("3. SSL/authentication problems")
            return False
        elif "timeout" in error_str.lower():
            print("\n=== DIAGNOSIS: CONNECTION TIMEOUT ===")
            print("Possible causes:")
            print("1. RDS instance is stopped or unavailable")
            print("2. Network/firewall blocking connection")
            print("3. Instance is under heavy load")
            return False
        else:
            print(f"\n=== UNKNOWN CONNECTION ERROR ===")
            print("Check RDS instance status in AWS console")
            return False
            
    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}")
        return False

def try_multiple_ssl_modes():
    """Try connecting with different SSL modes"""
    
    base_config = {
        'host': os.getenv('DB_HOST', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com'),
        'database': os.getenv('DB_NAME', 'thebigone'),
        'user': os.getenv('DB_USER', 'ajwin'),
        'password': os.getenv('DB_PASSWORD', 'your_password_here'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'connect_timeout': 10
    }
    
    ssl_modes = ['disable', 'allow', 'prefer', 'require']
    
    print("\n=== Testing Different SSL Modes ===")
    
    for ssl_mode in ssl_modes:
        config = base_config.copy()
        config['sslmode'] = ssl_mode
        
        print(f"Trying SSL mode: {ssl_mode}...")
        
        try:
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            cursor.fetchone()
            cursor.close()
            conn.close()
            print(f"SUCCESS with SSL mode: {ssl_mode}")
            return ssl_mode
        except Exception as e:
            print(f"FAILED with {ssl_mode}: {str(e)[:100]}")
    
    print("All SSL modes failed")
    return None

if __name__ == "__main__":
    # Try the connection limits check first
    success = check_connection_limits()
    
    if not success:
        print("\n" + "="*50)
        # If that fails, try different SSL modes
        working_ssl = try_multiple_ssl_modes()
        
        if working_ssl:
            print(f"\nFound working SSL mode: {working_ssl}")
            print(f"Set this environment variable: export DB_SSLMODE={working_ssl}")
