#!/usr/bin/env python3
"""
Demo: How Failed API Calls Are Handled in the Comprehensive Processor
"""

import json
import psycopg2

def show_failed_api_handling():
    """Demonstrate the failed API call handling system"""
    print("🛠️  FAILED API CALL HANDLING SYSTEM")
    print("=" * 60)
    
    print("\n📋 OVERVIEW:")
    print("The comprehensive processor has a robust 3-layer failed ID tracking system:")
    print("1. ❌ DETECTION: Catches all API call failures")
    print("2. 📝 RECORDING: Stores failed IDs in 'failed_api_calls' table")
    print("3. 🚫 EXCLUSION: Skips previously failed IDs in future runs")
    
    print("\n🔍 FAILURE DETECTION POINTS:")
    print("━" * 40)
    print("✓ API call returns None/empty data")
    print("✓ Network/connection errors")
    print("✓ NBA API rate limiting errors")
    print("✓ Invalid parameters (404 errors)")
    print("✓ Database insertion failures")
    print("✓ Data processing/cleaning errors")
    
    print("\n📊 FAILED_API_CALLS TABLE STRUCTURE:")
    print("━" * 40)
    print("CREATE TABLE failed_api_calls (")
    print("    id SERIAL PRIMARY KEY,")
    print("    endpoint_prefix VARCHAR(255),    -- e.g., 'nba_boxscoreadvancedv3'")
    print("    id_column VARCHAR(50),           -- e.g., 'gameid' or 'playerid'")
    print("    id_value VARCHAR(255),           -- e.g., '0012300001'")
    print("    error_message TEXT,              -- Full error details")
    print("    failed_at TIMESTAMP,             -- When failure occurred")
    print("    UNIQUE(endpoint_prefix, id_column, id_value)")
    print(")")
    
    print("\n⚡ FAILURE HANDLING PROCESS:")
    print("━" * 40)
    print("1. 🎯 ATTEMPT: Try API call for ID (e.g., game_id='0012300001')")
    print("2. ❌ FAILURE: API call fails (network error, 404, etc.)")
    print("3. 📝 RECORD: Store in failed_api_calls table:")
    print("   - endpoint_prefix: 'nba_boxscoreadvancedv3'")
    print("   - id_column: 'gameid'") 
    print("   - id_value: '0012300001'")
    print("   - error_message: 'Connection timeout after 30s'")
    print("4. ⏭️  CONTINUE: Move to next ID without retrying")
    
    print("\n🚫 EXCLUSION LOGIC:")
    print("━" * 40)
    print("When finding missing IDs:")
    print("  missing_ids = all_master_ids - existing_endpoint_ids - failed_ids")
    print("  Example:")
    print("    Master table has: [0012300001, 0012300002, 0012300003]")
    print("    Endpoint table has: [0012300002] (already processed)")
    print("    Failed table has: [0012300003] (failed before)")
    print("    → Only process: [0012300001] (never attempted)")
    
    print("\n📈 EXAMPLE SCENARIOS:")
    print("━" * 40)
    
    print("\n  SCENARIO 1 - Network Error:")
    print("  ❌ requests.exceptions.ConnectTimeout: Connection timeout")
    print("  📝 Records: error_message = 'Connection timeout after 30s'")
    print("  🚫 Future runs skip this ID permanently")
    
    print("\n  SCENARIO 2 - Invalid Game ID:")
    print("  ❌ NBA API returns 404: Game not found")
    print("  📝 Records: error_message = 'Game not found (404)'")
    print("  🚫 Future runs skip this ID (game doesn't exist)")
    
    print("\n  SCENARIO 3 - Rate Limiting:")
    print("  ❌ NBA API returns 429: Too many requests")
    print("  📝 Records: error_message = 'Rate limit exceeded (429)'")
    print("  🚫 Future runs skip this ID (persistent rate limit)")
    
    print("\n  SCENARIO 4 - Data Processing Error:")
    print("  ✅ API call succeeds, gets data")
    print("  ❌ Database insertion fails (constraint violation)")
    print("  📝 Records: error_message = 'Database constraint error'")
    print("  🚫 Future runs skip this ID")
    
    print("\n🔄 RETRY BEHAVIOR:")
    print("━" * 40)
    print("❌ NO RETRIES: Once an ID fails, it's permanently excluded")
    print("✅ WHY: Prevents infinite loops on persistently broken IDs")
    print("🛠️  MANUAL OVERRIDE: Admin can delete from failed_api_calls to retry")
    
    print("\n📊 MONITORING:")
    print("━" * 40)
    print("View failed IDs:")
    print("  SELECT * FROM failed_api_calls ORDER BY failed_at DESC;")
    print("\nCount failures by endpoint:")
    print("  SELECT endpoint_prefix, COUNT(*) FROM failed_api_calls GROUP BY endpoint_prefix;")
    print("\nClear failed IDs for retry:")
    print("  DELETE FROM failed_api_calls WHERE id_value = '0012300001';")
    
    print("\n✅ BENEFITS:")
    print("━" * 40)
    print("🚀 Efficiency: No wasted API calls on broken IDs")
    print("📈 Progress: Process continues despite individual failures")
    print("🛡️  Stability: No infinite retry loops")
    print("📋 Visibility: Full audit trail of all failures")
    print("🎯 Targeted: Can retry specific IDs after fixes")

def check_current_failed_ids():
    """Check if there are any failed IDs in the current database"""
    print("\n\n🔍 CHECKING CURRENT FAILED API CALLS:")
    print("=" * 50)
    
    try:
        # Load database config
        with open('endpoints/config/database_config.json', 'r') as f:
            config = json.load(f)
        
        # Connect to database
        connection = psycopg2.connect(
            host=config['host'],
            database=config['name'],
            user=config['user'],
            password=config['password'],
            port=int(config['port'])
        )
        cursor = connection.cursor()
        
        # Check if failed_api_calls table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'failed_api_calls'
            )
        """)
        
        if cursor.fetchone()[0]:
            # Table exists, check for failed IDs
            cursor.execute("SELECT COUNT(*) FROM failed_api_calls")
            total_failed = cursor.fetchone()[0]
            
            if total_failed > 0:
                print(f"📊 Total failed API calls: {total_failed}")
                
                # Show breakdown by endpoint
                cursor.execute("""
                    SELECT endpoint_prefix, COUNT(*) as failures
                    FROM failed_api_calls 
                    GROUP BY endpoint_prefix 
                    ORDER BY failures DESC
                """)
                
                print("\n📈 Failures by endpoint:")
                for row in cursor.fetchall():
                    print(f"   {row[0]}: {row[1]} failures")
                
                # Show recent failures
                cursor.execute("""
                    SELECT endpoint_prefix, id_column, id_value, error_message, failed_at
                    FROM failed_api_calls 
                    ORDER BY failed_at DESC 
                    LIMIT 5
                """)
                
                print("\n🕒 Recent failures:")
                for row in cursor.fetchall():
                    error_msg = row[3][:50] + "..." if len(row[3]) > 50 else row[3]
                    print(f"   {row[0]} | {row[1]}={row[2]} | {error_msg} | {row[4]}")
                    
            else:
                print("✅ No failed API calls recorded yet!")
                print("   The comprehensive processor ran successfully with 0 failures")
        else:
            print("📋 No failed_api_calls table found")
            print("   This means no failures have been recorded yet")
            
        connection.close()
        
    except Exception as e:
        print(f"❌ Error checking failed IDs: {e}")

if __name__ == "__main__":
    show_failed_api_handling()
    check_current_failed_ids()
