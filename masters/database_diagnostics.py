"""
Database Connection Troubleshooting Tool

Diagnoses RDS connectivity issues and provides solutions.
"""

import psycopg2
import time
import sys
import os
from datetime import datetime

# Add parent directory to path to access allintwo
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'archive'))

try:
    import allintwo
    print("✅ Found allintwo module with database functions")
    HAS_ALLINTWO = True
except ImportError:
    print("❌ Could not import allintwo module")
    HAS_ALLINTWO = False


def test_basic_connection():
    """Test basic psycopg2 connection"""
    print("\n🔍 Testing Basic Database Connection")
    print("=" * 50)
    
    connection_params = {
        'dbname': 'thebigone',
        'user': 'ajwin',
        'password': 'CharlesBark!23',
        'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
        'port': 5432
    }
    
    print(f"Host: {connection_params['host']}")
    print(f"Port: {connection_params['port']}")
    print(f"Database: {connection_params['dbname']}")
    print(f"User: {connection_params['user']}")
    
    try:
        print("\n⏳ Attempting connection...")
        start_time = time.time()
        
        conn = psycopg2.connect(**connection_params)
        
        elapsed = time.time() - start_time
        print(f"✅ Connection successful! ({elapsed:.2f} seconds)")
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"📊 PostgreSQL Version: {version}")
        
        # Test database access
        cursor.execute("SELECT current_database(), current_user;")
        db_info = cursor.fetchone()
        print(f"📊 Connected to database: {db_info[0]} as user: {db_info[1]}")
        
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        elapsed = time.time() - start_time
        print(f"❌ Connection failed after {elapsed:.2f} seconds")
        print(f"Error: {str(e)}")
        return False
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ Unexpected error after {elapsed:.2f} seconds")
        print(f"Error: {str(e)}")
        return False


def test_allintwo_connection():
    """Test connection using allintwo module"""
    if not HAS_ALLINTWO:
        print("\n❌ Cannot test allintwo - module not available")
        return False
        
    print("\n🔍 Testing allintwo.connect_to_rds Function")
    print("=" * 50)
    
    try:
        print("⏳ Using allintwo.connect_to_rds...")
        start_time = time.time()
        
        conn = allintwo.connect_to_rds(
            'thebigone', 
            'ajwin', 
            'CharlesBark!23', 
            'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com'
        )
        
        elapsed = time.time() - start_time
        
        if conn:
            print(f"✅ allintwo connection successful! ({elapsed:.2f} seconds)")
            
            # Test query
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cursor.fetchone()[0]
            print(f"📊 Found {table_count} tables in public schema")
            
            conn.close()
            return True
        else:
            print(f"❌ allintwo connection failed after {elapsed:.2f} seconds")
            return False
            
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ allintwo connection error after {elapsed:.2f} seconds")
        print(f"Error: {str(e)}")
        return False


def diagnose_connection_issues():
    """Provide diagnosis and solutions for connection problems"""
    print("\n🩺 CONNECTION ISSUE DIAGNOSIS")
    print("=" * 50)
    
    issues_and_solutions = [
        {
            "issue": "Connection timeout (10060 error)",
            "causes": [
                "RDS instance is stopped or unavailable",
                "Security group doesn't allow connections from your IP",
                "Network connectivity issues",
                "Wrong hostname or port"
            ],
            "solutions": [
                "1. Check AWS Console - ensure RDS instance is 'Available'",
                "2. Verify security group allows inbound PostgreSQL (5432) from your IP",
                "3. Test network: ping nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com",
                "4. Try different network (mobile hotspot) to test connectivity",
                "5. Check if RDS endpoint has changed in AWS Console"
            ]
        },
        {
            "issue": "Authentication failed (28000 error)", 
            "causes": [
                "Wrong username or password",
                "Database doesn't exist",
                "User doesn't have access to database"
            ],
            "solutions": [
                "1. Verify credentials in AWS RDS Console",
                "2. Reset password if needed", 
                "3. Check database name is correct",
                "4. Ensure user has proper permissions"
            ]
        },
        {
            "issue": "SSL/TLS connection errors",
            "causes": [
                "RDS requires SSL connections",
                "Certificate verification issues"
            ],
            "solutions": [
                "1. Add sslmode='require' to connection parameters",
                "2. Try sslmode='disable' for testing (not recommended for production)",
                "3. Download and use proper RDS certificates"
            ]
        }
    ]
    
    for item in issues_and_solutions:
        print(f"\n🔴 {item['issue']}")
        print("   Possible causes:")
        for cause in item['causes']:
            print(f"     • {cause}")
        print("   Solutions:")
        for solution in item['solutions']:
            print(f"     {solution}")


def check_network_connectivity():
    """Test basic network connectivity to RDS endpoint"""
    print("\n🌐 Testing Network Connectivity")
    print("=" * 40)
    
    hostname = "nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com"
    
    try:
        import socket
        
        print(f"⏳ Testing DNS resolution for {hostname}...")
        ip = socket.gethostbyname(hostname)
        print(f"✅ DNS Resolution: {hostname} -> {ip}")
        
        print(f"⏳ Testing port 5432 connectivity...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        
        start_time = time.time()
        result = sock.connect_ex((ip, 5432))
        elapsed = time.time() - start_time
        
        if result == 0:
            print(f"✅ Port 5432 is reachable ({elapsed:.2f} seconds)")
            sock.close()
            return True
        else:
            print(f"❌ Port 5432 is not reachable (error {result}, {elapsed:.2f} seconds)")
            print("   This suggests a firewall, security group, or network issue")
            sock.close()
            return False
            
    except socket.gaierror as e:
        print(f"❌ DNS resolution failed: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Network test failed: {str(e)}")
        return False


def test_alternative_connection_params():
    """Test connection with different parameters"""
    print("\n🔧 Testing Alternative Connection Parameters")
    print("=" * 50)
    
    base_params = {
        'dbname': 'thebigone',
        'user': 'ajwin', 
        'password': 'CharlesBark!23',
        'host': 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
        'port': 5432
    }
    
    # Test different SSL modes
    ssl_modes = ['require', 'prefer', 'disable']
    
    for ssl_mode in ssl_modes:
        print(f"\n⏳ Testing with sslmode='{ssl_mode}'...")
        test_params = base_params.copy()
        test_params['sslmode'] = ssl_mode
        
        try:
            start_time = time.time()
            conn = psycopg2.connect(**test_params)
            elapsed = time.time() - start_time
            
            print(f"   ✅ Success with sslmode='{ssl_mode}' ({elapsed:.2f} seconds)")
            conn.close()
            return ssl_mode
            
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"   ❌ Failed with sslmode='{ssl_mode}' ({elapsed:.2f} seconds)")
            print(f"   Error: {str(e)}")
    
    # Test with increased timeout
    print(f"\n⏳ Testing with increased connect_timeout...")
    test_params = base_params.copy()
    test_params['connect_timeout'] = 30
    
    try:
        start_time = time.time()
        conn = psycopg2.connect(**test_params)
        elapsed = time.time() - start_time
        
        print(f"   ✅ Success with longer timeout ({elapsed:.2f} seconds)")
        conn.close()
        return 'timeout'
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"   ❌ Failed even with longer timeout ({elapsed:.2f} seconds)")
        print(f"   Error: {str(e)}")
    
    return None


def run_comprehensive_diagnosis():
    """Run all diagnostic tests"""
    print("🏥 NBA RDS DATABASE - COMPREHENSIVE DIAGNOSIS")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = {}
    
    # Test 1: Network connectivity
    print("\n" + "="*60)
    results['network'] = check_network_connectivity()
    
    # Test 2: Basic connection
    print("\n" + "="*60) 
    results['basic_connection'] = test_basic_connection()
    
    # Test 3: allintwo connection
    print("\n" + "="*60)
    results['allintwo_connection'] = test_allintwo_connection()
    
    # Test 4: Alternative parameters (only if basic failed)
    if not results['basic_connection']:
        print("\n" + "="*60)
        results['alternative_params'] = test_alternative_connection_params()
    
    # Show diagnosis
    print("\n" + "="*60)
    diagnose_connection_issues()
    
    # Summary
    print("\n" + "="*60)
    print("🏁 DIAGNOSIS SUMMARY")
    print("=" * 60)
    
    for test, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test.replace('_', ' ').title()}: {status}")
    
    # Recommendations
    print("\n🎯 RECOMMENDATIONS:")
    
    if not results.get('network', False):
        print("   🔴 CRITICAL: Network connectivity failed")
        print("      → Check AWS Console - RDS instance status")
        print("      → Verify security group settings")
        print("      → Contact AWS support if needed")
    elif not results.get('basic_connection', False):
        print("   🟡 WARNING: Network OK but database connection failed")
        print("      → Verify credentials and database name") 
        print("      → Try SSL connection parameters")
        print("      → Check RDS instance logs in AWS Console")
    else:
        print("   🟢 SUCCESS: Database connection working!")
        print("      → Ready to proceed with data collection")
    
    return results


if __name__ == "__main__":
    run_comprehensive_diagnosis()
