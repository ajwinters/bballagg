#!/usr/bin/env python3
"""
Show current error tracking status and how it works
"""
import sys
sys.path.append('src')
import rdshelp

print("🔍 NBA ENDPOINT ERROR TRACKING SYSTEM")
print("=" * 60)

try:
    conn = rdshelp.connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')
    
    # Check the failed calls table structure
    print("📊 FAILED CALLS TABLE STRUCTURE:")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'nba_endpoint_failed_calls'
            ORDER BY ordinal_position;
        """)
        columns = cur.fetchall()
        for col_name, data_type in columns:
            print(f"   {col_name}: {data_type}")
    
    # Show error summary by type
    print(f"\n📈 ERROR SUMMARY BY TYPE:")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                LEFT(error_message, 50) as error_type,
                COUNT(*) as count,
                endpoint_name
            FROM nba_endpoint_failed_calls 
            GROUP BY LEFT(error_message, 50), endpoint_name
            ORDER BY COUNT(*) DESC
            LIMIT 5;
        """)
        
        for error_type, count, endpoint in cur.fetchall():
            print(f"   {endpoint}: {count:,} calls - {error_type}...")
    
    # Show recent failures
    print(f"\n⏰ MOST RECENT FAILURES (Last 5):")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                failed_at,
                parameter_value as game_id,
                LEFT(error_message, 40) as error,
                retry_count
            FROM nba_endpoint_failed_calls 
            ORDER BY failed_at DESC
            LIMIT 5;
        """)
        
        for failed_at, game_id, error, retry_count in cur.fetchall():
            print(f"   {failed_at}: Game {game_id} - {error}... (retry #{retry_count})")
    
    # Show total counts
    print(f"\n📊 OVERALL STATISTICS:")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM nba_endpoint_failed_calls;")
        total_failed = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT parameter_value) FROM nba_endpoint_failed_calls;")
        unique_games = cur.fetchone()[0]
        
        cur.execute("SELECT AVG(retry_count) FROM nba_endpoint_failed_calls;")
        avg_retries = cur.fetchone()[0]
        
        print(f"   Total Failed Calls: {total_failed:,}")
        print(f"   Unique Failed Games: {unique_games:,}")
        print(f"   Average Retry Count: {avg_retries:.1f}")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {str(e)}")

print(f"\n🎯 HOW ERROR TRACKING WORKS:")
print(f"   1. 🔄 In-Memory Tracking: Failed calls stored in processor.failed_calls dict")
print(f"   2. 💾 Database Persistence: Failures saved to nba_endpoint_failed_calls table")
print(f"   3. 🚫 Skip on Retry: System skips parameters that have already failed")
print(f"   4. 📝 Detailed Logging: All errors logged with game ID, endpoint, and error message")
print(f"   5. 🔄 Resumable: When processor restarts, it loads existing failures and skips them")
print(f"   6. 📊 Statistics: Track retry counts and failure patterns")

print(f"\n✅ BENEFITS:")
print(f"   • No time wasted re-trying known failed game IDs")
print(f"   • System can resume from any interruption point")  
print(f"   • Clear visibility into which games have issues")
print(f"   • Prevents infinite retry loops on permanently broken game IDs")
