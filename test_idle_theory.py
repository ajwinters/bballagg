#!/usr/bin/env python3
"""
Enhanced NBA data collection with idle state monitoring
This version will run with improved monitoring to detect state-dependent timeout issues
"""
import sys
import os

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'endpoints', 'collectors'))

def run_with_idle_monitoring():
    """Run NBA data collection with enhanced idle state monitoring"""
    try:
        from rds_connection_manager import RDSConnectionManager
        from endpoint_processor import NBAEndpointProcessor
        import time
        
        print("🔧 Starting NBA data collection with idle state monitoring...")
        print("🎯 This version is designed to handle timeout issues that occur after idle periods")
        
        # Create connection manager with enhanced idle handling
        conn_manager = RDSConnectionManager(
            'thebigone', 
            'ajwin', 
            'CharlesBark!23', 
            'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            connection_timeout=30,
            max_retries=3,
            retry_delay=5
        )
        
        print("📡 Connecting to database...")
        conn_manager.connect()
        print("✅ Connected with idle state monitoring")
        
        # Create processor with enhanced timeout handling
        processor = NBAEndpointProcessor(conn_manager, league='NBA', rate_limit=0.6)
        print("🏀 NBA Processor ready with timeout resilience")
        
        start_time = time.time()
        
        print("\n" + "="*60)
        print("🚀 STARTING ENHANCED NBA DATA COLLECTION")
        print("💡 Key improvements for idle state issues:")
        print("   • Proactive connection refresh after 30min idle")
        print("   • Enhanced timeout detection (SSL, network reset)")
        print("   • Periodic connection health checks every 100 calls")
        print("   • Database connection refresh on network errors")
        print("   • Extended retry logic with exponential backoff")
        print("="*60)
        
        # Start processing (this will run the main collection logic)
        return processor.process_endpoints_by_category('game_based', 'high')
        
    except Exception as e:
        print(f"❌ Error in enhanced collection: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 Testing theory: Timeouts occur after idle periods when stepping away from PC")
    print("⏳ Starting enhanced data collection - step away and see if timeouts still occur...\n")
    
    success = run_with_idle_monitoring()
    
    if success:
        print("\n✅ Enhanced collection completed successfully!")
    else:
        print("\n⚠️  Collection ended - check logs for timeout patterns")
        
    print("\n📊 Monitor the logs to see if:")
    print("   • Timeouts correlate with idle periods")
    print("   • Connection health checks help prevent issues")
    print("   • Enhanced retry logic recovers from idle-state problems")
