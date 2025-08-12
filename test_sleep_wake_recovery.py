#!/usr/bin/env python3
"""
Sleep/Wake Cycle Detection and Recovery Script
This script specifically addresses PC sleep/wake network disruption issues
"""
import sys
import os
import time
import psutil

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'endpoints', 'collectors'))

def kill_existing_processes():
    """Kill any existing NBA processing to start fresh"""
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if 'python' in proc.info['name'].lower():
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'endpoint_processor.py' in cmdline:
                    print(f"🛑 Killing existing NBA process: PID {proc.info['pid']}")
                    proc.kill()
                    time.sleep(1)
    except Exception as e:
        print(f"Note: {e}")

def run_nba_with_sleep_detection():
    """Run NBA processing with enhanced sleep/wake detection"""
    try:
        from rds_connection_manager import RDSConnectionManager
        from endpoint_processor import NBAEndpointProcessor
        
        print("="*70)
        print("🛌 NBA DATA COLLECTION WITH SLEEP/WAKE DETECTION")
        print("="*70)
        print("🎯 Problem identified: PC sleep/wake cycles disrupt network connections")
        print("✅ Solution implemented: Sleep/wake detection with automatic recovery")
        print("💡 Test: Put PC to sleep, wake it up, and watch recovery happen!")
        print("="*70)
        
        # Create connection manager with sleep/wake detection
        conn_manager = RDSConnectionManager(
            'thebigone', 
            'ajwin', 
            'CharlesBark!23', 
            'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            connection_timeout=30,
            max_retries=3,
            retry_delay=5
        )
        
        print("\n📡 Connecting to database with sleep/wake detection...")
        conn_manager.connect()
        print("✅ Connected! Sleep/wake monitoring active")
        
        # Create processor with enhanced error handling
        processor = NBAEndpointProcessor(conn_manager, league='NBA', rate_limit=0.6)
        print("🏀 NBA Processor ready with network disruption recovery")
        
        print("\n🚀 Starting NBA data collection...")
        print("💤 Go ahead and put your PC to sleep to test the recovery!")
        
        # Process high priority game-based endpoints
        success = processor.process_endpoints_by_category('game_based', 'high')
        
        return success
        
    except KeyboardInterrupt:
        print("\n⏸️  Processing interrupted by user")
        return True
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 TESTING: PC Sleep/Wake Cycle Network Disruption Recovery")
    
    # Kill any existing processes to start fresh
    kill_existing_processes()
    time.sleep(2)
    
    # Run with enhanced sleep/wake detection
    success = run_nba_with_sleep_detection()
    
    if success:
        print("\n✅ NBA processing completed or stopped successfully!")
    else:
        print("\n⚠️  NBA processing encountered issues")
        
    print("\n📊 Key features implemented:")
    print("   • Time gap detection (>10s indicates sleep/wake)")
    print("   • Automatic connection refresh after wake")
    print("   • Enhanced network error detection")
    print("   • Forced reconnection on sleep/wake events")
    print("   • Extended retry logic with connection refresh")
