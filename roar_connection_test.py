#!/usr/bin/env python3
"""
ROAR-specific RDS connection test
Tests common HPC cluster connection issues
"""

import psycopg2
import os
import socket
import ssl
import sys

def test_network_connectivity():
    """Test basic network connectivity from ROAR"""
    host = "nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com"
    port = 5432
    
    print(f"=== Testing Network Connectivity from ROAR ===")
    print(f"Target: {host}:{port}")
    
    # Test DNS resolution
    try:
        ip = socket.gethostbyname(host)
        print(f"DNS Resolution: SUCCESS - {host} -> {ip}")
    except socket.gaierror as e:
        print(f"DNS Resolution: FAILED - {e}")
        return False
    
    # Test TCP connection
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(30)  # Longer timeout for HPC
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"TCP Connection: SUCCESS - Port {port} is accessible")
            return True
        else:
            print(f"TCP Connection: FAILED - Cannot connect (error: {result})")
            print("This suggests ROAR blocks outbound connections on port 5432")
            return False
            
    except Exception as e:
        print(f"TCP Connection: ERROR - {e}")
        return False

def test_ssl_connectivity():
    """Test SSL/TLS connectivity"""
    host = "nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com"
    port = 5432
    
    print(f"\n=== Testing SSL Connectivity ===")
    
    try:
        # Create SSL context
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        # Test SSL connection
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(30)
            with context.wrap_socket(sock, server_hostname=host) as ssock:
                ssock.connect((host, port))
                print("SSL Connection: SUCCESS - Can establish encrypted connection")
                return True
                
    except Exception as e:
        print(f"SSL Connection: FAILED - {e}")
        return False

def test_postgresql_connection_roar():
    """Test PostgreSQL connection with ROAR-specific configurations"""
    
    configs = [
        # Configuration 1: Disable SSL
        {
            'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            'database': 'thebigone',
            'user': 'ajwin',
            'password': os.getenv('DB_PASSWORD', 'your_password'),
            'port': 5432,
            'sslmode': 'disable',
            'connect_timeout': 60,
            'name': 'No SSL'
        },
        # Configuration 2: Require SSL but don't verify
        {
            'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            'database': 'thebigone', 
            'user': 'ajwin',
            'password': os.getenv('DB_PASSWORD', 'your_password'),
            'port': 5432,
            'sslmode': 'require',
            'sslcert': '/dev/null',  # Skip client cert
            'sslkey': '/dev/null',   # Skip client key
            'sslrootcert': '/dev/null',  # Skip CA verification
            'connect_timeout': 60,
            'name': 'Require SSL (no verification)'
        },
        # Configuration 3: Allow SSL
        {
            'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            'database': 'thebigone',
            'user': 'ajwin', 
            'password': os.getenv('DB_PASSWORD', 'your_password'),
            'port': 5432,
            'sslmode': 'allow',
            'connect_timeout': 60,
            'name': 'Allow SSL'
        }
    ]
    
    print(f"\n=== Testing PostgreSQL Connections ===")
    
    for config in configs:
        config_name = config.pop('name')
        print(f"\nTrying configuration: {config_name}")
        
        try:
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            
            # Test query
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"SUCCESS: Connected with {config_name}")
            print(f"PostgreSQL: {version[:80]}...")
            
            # Test your database
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cursor.fetchone()[0]
            print(f"Tables in database: {table_count}")
            
            cursor.close()
            conn.close()
            
            print(f"SOLUTION FOUND: Use configuration '{config_name}'")
            return config_name, config
            
        except psycopg2.OperationalError as e:
            error_str = str(e)
            print(f"FAILED: {error_str[:100]}...")
            
            if "timeout" in error_str.lower():
                print("  -> Network/timeout issue")
            elif "ssl" in error_str.lower():
                print("  -> SSL configuration issue")
            elif "authentication" in error_str.lower():
                print("  -> Authentication/password issue")
                
        except Exception as e:
            print(f"ERROR: {str(e)[:100]}...")
    
    return None, None

def print_roar_specific_solutions():
    """Print ROAR-specific solutions"""
    print(f"\n" + "="*60)
    print("ROAR-SPECIFIC SOLUTIONS")
    print("="*60)
    print()
    print("1. REQUEST FIREWALL EXCEPTION:")
    print("   Contact ROAR support to open outbound port 5432")
    print("   Email: roar-support@psu.edu")
    print()
    print("2. USE SSH TUNNEL:")
    print("   ssh -L 5432:nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com:5432 your_local_machine")
    print("   Then connect to localhost:5432")
    print()
    print("3. ALTERNATIVE PORTS:")
    print("   Some firewalls allow 443 (HTTPS) or 80 (HTTP)")
    print("   You might need to create an RDS proxy on allowed ports")
    print()
    print("4. RDS PROXY:")
    print("   Set up AWS RDS Proxy to handle connections")
    print("   Can sometimes bypass HPC firewall restrictions")

def main():
    print("ROAR HPC Cluster - RDS Connection Diagnostic")
    print("=" * 50)
    
    # Check environment
    if not os.getenv('DB_PASSWORD'):
        print("WARNING: DB_PASSWORD environment variable not set")
        print("Set it with: export DB_PASSWORD='your_password'")
        print()
    
    # Test network connectivity first
    network_ok = test_network_connectivity()
    
    if network_ok:
        # Test SSL
        ssl_ok = test_ssl_connectivity()
        
        # Test PostgreSQL connections
        working_config, config = test_postgresql_connection_roar()
        
        if working_config:
            print(f"\n🎉 SUCCESS: Found working configuration!")
            print(f"Use SSL mode: {config.get('sslmode', 'default')}")
        else:
            print(f"\n❌ NO WORKING CONFIGURATION FOUND")
            print_roar_specific_solutions()
    else:
        print(f"\n❌ NETWORK CONNECTIVITY FAILED")
        print("ROAR is blocking outbound connections to port 5432")
        print_roar_specific_solutions()

if __name__ == "__main__":
    main()
