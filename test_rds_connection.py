#!/usr/bin/env python3
"""
Simple RDS connection test script
Tests basic connectivity without the full endpoint processor
"""

import psycopg2
import os
import sys

def test_basic_connection():
    """Test basic PostgreSQL connection with different SSL modes"""
    
    # Database configuration
    config = {
        'host': os.getenv('DB_HOST', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com'),
        'database': os.getenv('DB_NAME', 'thebigone'),
        'user': os.getenv('DB_USER', 'ajwin'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 5432))
    }
    
    print(f"Testing connection to: {config['host']}:{config['port']}")
    print(f"Database: {config['database']}")
    print(f"User: {config['user']}")
    print(f"Password set: {'Yes' if config['password'] else 'No'}")
    print("-" * 50)
    
    # Test different SSL modes
    ssl_modes = ['disable', 'prefer', 'require', 'verify-ca', 'verify-full']
    
    for ssl_mode in ssl_modes:
        print(f"\nTrying SSL mode: {ssl_mode}")
        try:
            test_config = config.copy()
            test_config['sslmode'] = ssl_mode
            
            # Attempt connection with timeout
            test_config['connect_timeout'] = 10
            
            conn = psycopg2.connect(**test_config)
            cursor = conn.cursor()
            
            # Test query
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"SUCCESS: Connected with SSL mode '{ssl_mode}'")
            print(f"PostgreSQL Version: {version[:50]}...")
            
            # Test a simple query on your database
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables;")
            table_count = cursor.fetchone()[0]
            print(f"Tables in database: {table_count}")
            
            cursor.close()
            conn.close()
            print(f"SUCCESS: Connection test passed with SSL mode '{ssl_mode}'")
            return ssl_mode
            
        except psycopg2.OperationalError as e:
            print(f"FAILED: {str(e)}")
        except Exception as e:
            print(f"ERROR: {str(e)}")
    
    print("\nAll SSL modes failed. Possible issues:")
    print("1. RDS instance is not running")
    print("2. Network connectivity issues")
    print("3. Incorrect credentials")
    print("4. Database doesn't exist")
    return None

def test_network_connectivity():
    """Test basic network connectivity"""
    import socket
    
    host = os.getenv('DB_HOST', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')
    port = int(os.getenv('DB_PORT', 5432))
    
    print(f"\nTesting network connectivity to {host}:{port}")
    
    try:
        # Create socket and test connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)  # 10 second timeout
        
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("SUCCESS: Port is open and accessible")
            return True
        else:
            print(f"FAILED: Cannot connect to port (error code: {result})")
            return False
            
    except socket.gaierror as e:
        print(f"FAILED: DNS resolution error - {e}")
        return False
    except Exception as e:
        print(f"FAILED: Network error - {e}")
        return False

if __name__ == "__main__":
    print("=== RDS Connection Test ===\n")
    
    # Check environment variables
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"WARNING: Missing environment variables: {missing_vars}")
        print("You may need to set these before running the test.")
    
    # Test network connectivity first
    network_ok = test_network_connectivity()
    
    if network_ok:
        # Test database connection
        working_ssl_mode = test_basic_connection()
        
        if working_ssl_mode:
            print(f"\n=== SOLUTION FOUND ===")
            print(f"Use SSL mode: {working_ssl_mode}")
            print("Add this to your connection configuration:")
            print(f"  'sslmode': '{working_ssl_mode}'")
        else:
            print(f"\n=== NO WORKING CONNECTION FOUND ===")
            print("Check RDS instance status and credentials")
    else:
        print(f"\n=== NETWORK CONNECTIVITY FAILED ===")
        print("Check security groups, VPC settings, and RDS instance status")
