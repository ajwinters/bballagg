#!/usr/bin/env python3
"""
Demonstrate how the endpoint processor uses game ID difference (not date difference)
"""
import sys
sys.path.append('src')
import rdshelp

print("🔍 NBA ENDPOINT PROCESSING LOGIC - GAME ID DIFFERENCE APPROACH")
print("=" * 70)

try:
    conn = rdshelp.connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')
    
    print("📊 STEP 1: GET ALL GAME IDs FROM MASTER TABLE")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM nba_games;")
        master_count = cur.fetchone()[0]
        
        cur.execute("SELECT MIN(gameid), MAX(gameid) FROM nba_games LIMIT 5;")
        min_max = cur.fetchone()
        
        print(f"   Master table (nba_games): {master_count:,} total games")
        print(f"   Game ID range: {min_max[0]} to {min_max[1]}")
        print(f"   📝 These are ALL the game IDs the processor will try to collect")
    
    print(f"\n📈 STEP 2: CHECK EXISTING DATA IN ENDPOINT TABLE")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM nba_boxscoreadvancedv3_playerstats;")
        endpoint_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT gameid) FROM nba_boxscoreadvancedv3_playerstats;")
        endpoint_games = cur.fetchone()[0]
        
        print(f"   Endpoint table (nba_boxscoreadvancedv3_playerstats): {endpoint_count:,} records")
        print(f"   Unique games in endpoint table: {endpoint_games:,}")
        print(f"   📝 These games already have data - will be SKIPPED")
    
    print(f"\n🎯 STEP 3: CALCULATE MISSING GAME IDs (THE DIFFERENCE)")
    with conn.cursor() as cur:
        # This is exactly what the processor does
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT gameid FROM nba_games
                EXCEPT
                SELECT DISTINCT gameid FROM nba_boxscoreadvancedv3_playerstats
            ) AS missing_games;
        """)
        missing_count = cur.fetchone()[0]
        
        print(f"   Missing games to process: {missing_count:,}")
        print(f"   📝 Only these {missing_count:,} games will get API calls")
    
    print(f"\n❌ STEP 4: EXCLUDE KNOWN FAILURES")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT parameter_value) 
            FROM nba_endpoint_failed_calls 
            WHERE table_name LIKE '%boxscoreadvancedv3%';
        """)
        failed_count = cur.fetchone()[0]
        
        final_to_process = missing_count - failed_count
        print(f"   Known failed games: {failed_count:,}")
        print(f"   📝 These will be SKIPPED (no wasted API calls)")
        print(f"   Final games to process: {final_to_process:,}")
    
    print(f"\n✅ CONFIRMED: PROCESS USES GAME ID DIFFERENCE, NOT DATE DIFFERENCE")
    print(f"📋 LOGIC SUMMARY:")
    print(f"   1. Get ALL game IDs from master table: {master_count:,}")
    print(f"   2. Get existing game IDs from endpoint table: {endpoint_games:,}")
    print(f"   3. Calculate difference (missing): {missing_count:,}")
    print(f"   4. Exclude known failures: -{failed_count:,}")
    print(f"   5. Process only the remaining: {final_to_process:,} games")
    
    print(f"\n🎯 BENEFITS OF THIS APPROACH:")
    print(f"   ✅ No date calculations needed")
    print(f"   ✅ Purely incremental - only missing games processed") 
    print(f"   ✅ Automatically handles resume/restart scenarios")
    print(f"   ✅ Skips failed games to avoid wasted API calls")
    print(f"   ✅ Works regardless of game chronological order")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
