#!/usr/bin/env python3
"""
Advanced RDS connection debugging for ROAR
Tests various PostgreSQL connection parameters
"""

import psycopg2
import socket
import time
import sys

def test_verbose_connection():
    """Test with maximum verbosity and different parameters"""
    
    base_config = {
        'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'database': 'thebigone',
        'user': 'ajwin',
        'password': 'your_password_here'  # REPLACE WITH ACTUAL PASSWORD
    }
    
    # Different configuration attempts
    test_configs = [
        # Test 1: Minimal config
        {
            **base_config,
            'sslmode': 'disable',
            'connect_timeout': 10
        },
        # Test 2: With application name
        {
            **base_config,
            'sslmode': 'disable', 
            'connect_timeout': 30,
            'application_name': 'roar_test'
        },
        # Test 3: Different database
        {
            **base_config,
            'database': 'postgres',  # Try default postgres database
            'sslmode': 'disable',
            'connect_timeout': 10
        },
        # Test 4: With keepalives
        {
            **base_config,
            'sslmode': 'disable',
            'connect_timeout': 60,
            'keepalives_idle': '600',
            'keepalives_interval': '30',
            'keepalives_count': '3'
        },
        # Test 5: Force older protocol version
        {
            **base_config,
            'sslmode': 'disable',
            'connect_timeout': 30,
            'options': '-c default_transaction_isolation=read_committed'
        }
    ]
    
    print("=== Advanced PostgreSQL Connection Testing ===")
    
    for i, config in enumerate(test_configs, 1):
        print(f"\nTest {i}: {config.get('database', 'thebigone')} with {config.get('sslmode', 'default')} SSL")
        print(f"Timeout: {config.get('connect_timeout', 'default')}")
        
        try:
            # More detailed error catching
            print("  Attempting connection...")
            conn = psycopg2.connect(**config)
            
            print("  ✅ Connection established!")
            
            cursor = conn.cursor()
            
            # Test basic query
            print("  Testing basic query...")
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"  PostgreSQL Version: {version[:50]}...")
            
            # Test permissions
            print("  Testing database access...")
            if config.get('database') == 'thebigone':
                cursor.execute("SELECT COUNT(*) FROM information_schema.tables;")
                table_count = cursor.fetchone()[0]
                print(f"  Tables visible: {table_count}")
            
            cursor.close()
            conn.close()
            
            print(f"  🎉 SUCCESS with configuration {i}!")
            print(f"  Working config: {config}")
            return config
            
        except psycopg2.OperationalError as e:
            error_str = str(e)
            print(f"  ❌ OperationalError: {error_str[:100]}...")
            
            # Analyze specific errors
            if "closed unexpectedly" in error_str:
                print("    → Server terminated connection immediately")
                print("    → Possible causes: auth method, database doesn't exist, IP restriction")
            elif "timeout" in error_str:
                print("    → Connection timeout")
            elif "authentication" in error_str:
                print("    → Authentication failed")
            elif "database" in error_str and "does not exist" in error_str:
                print("    → Database doesn't exist")
                
        except psycopg2.Error as e:
            print(f"  ❌ PostgreSQL Error: {e}")
            
        except Exception as e:
            print(f"  ❌ Unexpected Error: {e}")
    
    print("\n❌ All configurations failed!")
    return None

def test_raw_socket_postgresql():
    """Test raw socket communication with PostgreSQL protocol"""
    
    print("\n=== Raw Socket PostgreSQL Protocol Test ===")
    
    try:
        host = 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com'
        port = 5432
        
        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(30)
        
        print(f"Connecting to {host}:{port}...")
        sock.connect((host, port))
        print("✅ TCP connection established")
        
        # Send PostgreSQL startup message
        # This is a minimal PostgreSQL protocol startup
        import struct
        
        # PostgreSQL startup message format
        protocol_version = 196608  # PostgreSQL 3.0 protocol
        startup_msg = struct.pack('!I', protocol_version)
        startup_msg += b'user\x00ajwin\x00'
        startup_msg += b'database\x00thebigone\x00'
        startup_msg += b'\x00'
        
        # Add length prefix
        msg_length = len(startup_msg) + 4
        full_msg = struct.pack('!I', msg_length) + startup_msg
        
        print("Sending PostgreSQL startup message...")
        sock.send(full_msg)
        
        # Try to read response
        print("Waiting for server response...")
        response = sock.recv(1024)
        
        if len(response) > 0:
            print(f"✅ Server responded! Response length: {len(response)} bytes")
            print(f"First few bytes: {response[:20]}")
            
            # Check response type
            if response[0:1] == b'R':
                print("→ Authentication request received")
            elif response[0:1] == b'E':
                print("→ Error message received")
                # Try to decode error
                try:
                    error_msg = response[5:].decode('utf-8', errors='ignore')
                    print(f"→ Error details: {error_msg[:100]}")
                except:
                    pass
            else:
                print(f"→ Unknown response type: {response[0]}")
        else:
            print("❌ No response from server")
            
        sock.close()
        
    except Exception as e:
        print(f"❌ Raw socket test failed: {e}")
        try:
            sock.close()
        except:
            pass

def main():
    print("ROAR PostgreSQL Advanced Diagnostic")
    print("=" * 40)
    
    # Test 1: Various connection parameters
    working_config = test_verbose_connection()
    
    # Test 2: Raw protocol communication
    test_raw_socket_postgresql()
    
    if working_config:
        print(f"\n🎉 SOLUTION FOUND!")
        print(f"Working configuration:")
        for key, value in working_config.items():
            if key != 'password':
                print(f"  {key}: {value}")
    else:
        print(f"\n❌ NO SOLUTION FOUND")
        print("Recommendations:")
        print("1. Check RDS instance logs in AWS Console")
        print("2. Verify 'thebigone' database exists")
        print("3. Check user 'ajwin' has proper permissions")
        print("4. Try connecting to 'postgres' database instead")

if __name__ == "__main__":
    main()
