"""
NBA Master Tables Scheduler

Automated scheduler for master table updates:
- Daily: Games tables (all leagues)
- Weekly: Players tables (all leagues) 
- Yearly: Teams tables (all leagues)

Designed for Windows Task Scheduler or cron job execution.
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
import logging
from pathlib import Path

# Add the masters directory to Python path
masters_dir = Path(__file__).parent.absolute()
project_root = masters_dir.parent
sys.path.insert(0, str(masters_dir))
sys.path.insert(0, str(project_root))

from database_manager import MasterTablesManager


def setup_logging():
    """Setup logging for scheduler operations"""
    logs_dir = masters_dir / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    log_filename = logs_dir / f"scheduler_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)


def load_scheduler_config():
    """Load scheduler configuration"""
    config_file = masters_dir / "scheduler_config.json"
    
    default_config = {
        "schedules": {
            "games": {
                "frequency": "daily",
                "time": "06:00",
                "enabled": True,
                "last_run": None
            },
            "players": {
                "frequency": "weekly", 
                "day": "sunday",
                "time": "02:00",
                "enabled": True,
                "last_run": None
            },
            "teams": {
                "frequency": "yearly",
                "month": "october",
                "day": 1,
                "time": "01:00", 
                "enabled": True,
                "last_run": None
            }
        },
        "notifications": {
            "email_enabled": False,
            "email_recipients": [],
            "slack_webhook": None
        },
        "database": {
            "retry_attempts": 3,
            "retry_delay_minutes": 15,
            "connection_timeout": 30
        }
    }
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        # Merge with defaults
        for key, value in default_config.items():
            if key not in config:
                config[key] = value
    else:
        config = default_config
        # Save default config
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
    
    return config


def save_scheduler_config(config):
    """Save scheduler configuration"""
    config_file = masters_dir / "scheduler_config.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)


def should_run_process(process_name, config, logger):
    """Check if a process should run based on schedule"""
    schedule = config['schedules'][process_name]
    
    if not schedule['enabled']:
        logger.info(f"{process_name} is disabled in config")
        return False
    
    now = datetime.now()
    last_run_str = schedule.get('last_run')
    
    if not last_run_str:
        logger.info(f"{process_name} has never run - should run now")
        return True
    
    last_run = datetime.fromisoformat(last_run_str)
    frequency = schedule['frequency']
    
    if frequency == 'daily':
        return (now - last_run).days >= 1
    elif frequency == 'weekly':
        return (now - last_run).days >= 7
    elif frequency == 'yearly':
        return (now - last_run).days >= 365
    
    return False


def update_last_run(process_name, config, success=True):
    """Update last run timestamp for a process"""
    config['schedules'][process_name]['last_run'] = datetime.now().isoformat()
    config['schedules'][process_name]['last_success'] = success
    save_scheduler_config(config)


def run_daily_games_update(manager, config, logger):
    """Run daily games update for all leagues"""
    logger.info("🎯 Starting daily games update")
    
    try:
        success = manager.update_master_games(test_mode=False)
        
        if success:
            logger.info("✅ Daily games update completed successfully")
        else:
            logger.error("❌ Daily games update failed")
        
        update_last_run('games', config, success)
        return success
        
    except Exception as e:
        logger.error(f"❌ Daily games update crashed: {str(e)}")
        update_last_run('games', config, False)
        return False


def run_weekly_players_update(manager, config, logger):
    """Run weekly players update for all leagues"""
    logger.info("👥 Starting weekly players update")
    
    try:
        success = manager.update_master_players(test_mode=False)
        
        if success:
            logger.info("✅ Weekly players update completed successfully")
        else:
            logger.error("❌ Weekly players update failed")
        
        update_last_run('players', config, success)
        return success
        
    except Exception as e:
        logger.error(f"❌ Weekly players update crashed: {str(e)}")
        update_last_run('players', config, False)
        return False


def run_yearly_teams_update(manager, config, logger):
    """Run yearly teams update for all leagues"""
    logger.info("🏟️ Starting yearly teams update")
    
    try:
        success = manager.update_master_teams(test_mode=False)
        
        if success:
            logger.info("✅ Yearly teams update completed successfully")
        else:
            logger.error("❌ Yearly teams update failed")
        
        update_last_run('teams', config, success)
        return success
        
    except Exception as e:
        logger.error(f"❌ Yearly teams update crashed: {str(e)}")
        update_last_run('teams', config, False)
        return False


def run_scheduled_updates():
    """Main scheduler function - checks and runs due updates"""
    logger = setup_logging()
    logger.info("🚀 NBA Master Tables Scheduler Starting")
    
    # Load configuration
    config = load_scheduler_config()
    logger.info("📋 Configuration loaded")
    
    # Initialize manager
    manager = MasterTablesManager()
    
    # Check and run each process
    processes_run = []
    results = {}
    
    # Games (daily)
    if should_run_process('games', config, logger):
        results['games'] = run_daily_games_update(manager, config, logger)
        processes_run.append('games')
    else:
        logger.info("🎯 Games update not due")
    
    # Players (weekly)
    if should_run_process('players', config, logger):
        results['players'] = run_weekly_players_update(manager, config, logger)
        processes_run.append('players')
    else:
        logger.info("👥 Players update not due")
    
    # Teams (yearly)
    if should_run_process('teams', config, logger):
        results['teams'] = run_yearly_teams_update(manager, config, logger)
        processes_run.append('teams')
    else:
        logger.info("🏟️ Teams update not due")
    
    # Summary
    if processes_run:
        logger.info(f"📊 Processes run: {', '.join(processes_run)}")
        successful = [p for p, success in results.items() if success]
        failed = [p for p, success in results.items() if not success]
        
        if successful:
            logger.info(f"✅ Successful: {', '.join(successful)}")
        if failed:
            logger.error(f"❌ Failed: {', '.join(failed)}")
    else:
        logger.info("😴 No processes were due to run")
    
    logger.info("🏁 Scheduler run complete")
    return results


def create_windows_task_commands():
    """Generate Windows Task Scheduler commands"""
    python_exe = sys.executable
    script_path = Path(__file__).absolute()
    
    commands = {
        "daily_check": {
            "description": "Daily check for NBA master table updates",
            "command": f'schtasks /create /tn "NBA_Masters_Daily" /tr "{python_exe} {script_path} --run" /sc daily /st 06:00 /f',
            "delete": 'schtasks /delete /tn "NBA_Masters_Daily" /f'
        },
        "manual_games": {
            "description": "Manual games update",
            "command": f'schtasks /create /tn "NBA_Masters_Games" /tr "{python_exe} {script_path} --games" /sc once /st 00:00 /f',
            "delete": 'schtasks /delete /tn "NBA_Masters_Games" /f'
        },
        "manual_players": {
            "description": "Manual players update", 
            "command": f'schtasks /create /tn "NBA_Masters_Players" /tr "{python_exe} {script_path} --players" /sc once /st 00:00 /f',
            "delete": 'schtasks /delete /tn "NBA_Masters_Players" /f'
        }
    }
    
    return commands


def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description='NBA Master Tables Scheduler')
    parser.add_argument('--run', action='store_true', help='Run scheduled updates check')
    parser.add_argument('--games', action='store_true', help='Force games update')
    parser.add_argument('--players', action='store_true', help='Force players update')
    parser.add_argument('--teams', action='store_true', help='Force teams update')
    parser.add_argument('--status', action='store_true', help='Show scheduler status')
    parser.add_argument('--setup-windows', action='store_true', help='Show Windows Task Scheduler commands')
    
    args = parser.parse_args()
    
    if args.run:
        # Run scheduled updates
        run_scheduled_updates()
        
    elif args.games:
        # Force games update
        logger = setup_logging()
        config = load_scheduler_config()
        manager = MasterTablesManager()
        run_daily_games_update(manager, config, logger)
        
    elif args.players:
        # Force players update
        logger = setup_logging()
        config = load_scheduler_config()
        manager = MasterTablesManager()
        run_weekly_players_update(manager, config, logger)
        
    elif args.teams:
        # Force teams update
        logger = setup_logging()
        config = load_scheduler_config()
        manager = MasterTablesManager()
        run_yearly_teams_update(manager, config, logger)
        
    elif args.status:
        # Show status
        config = load_scheduler_config()
        
        print("🏀 NBA Master Tables Scheduler Status")
        print("=" * 50)
        
        for process_name, schedule in config['schedules'].items():
            enabled = "✅" if schedule['enabled'] else "❌"
            last_run = schedule.get('last_run', 'Never')
            if last_run and last_run != 'Never':
                try:
                    last_run = datetime.fromisoformat(last_run).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    last_run = 'Invalid timestamp'
            
            print(f"\n{process_name.upper()}:")
            print(f"  Status: {enabled}")
            print(f"  Frequency: {schedule['frequency']}")
            print(f"  Last run: {last_run}")
            
    elif args.setup_windows:
        # Show Windows Task Scheduler setup
        commands = create_windows_task_commands()
        
        print("🏀 Windows Task Scheduler Setup Commands")
        print("=" * 60)
        print("\nRun these commands in an Administrator Command Prompt:")
        
        for task_name, task_info in commands.items():
            print(f"\n{task_info['description']}:")
            print(f"CREATE: {task_info['command']}")
            print(f"DELETE: {task_info['delete']}")
        
        print("\n📋 Additional Notes:")
        print("- Run Command Prompt as Administrator")
        print("- The daily task will check all schedules and run what's due")
        print("- Manual tasks are for one-time runs")
        print("- Check logs in: masters/logs/")
        
    else:
        # Interactive mode
        print("🏀 NBA Master Tables Scheduler")
        print("=" * 40)
        print("\nOptions:")
        print("1. Run scheduled check")
        print("2. Force games update")
        print("3. Force players update") 
        print("4. Force teams update")
        print("5. Show status")
        print("6. Show Windows Task Scheduler setup")
        print("7. Exit")
        
        choice = input("\nEnter choice (1-7): ").strip()
        
        if choice == '1':
            run_scheduled_updates()
        elif choice == '2':
            logger = setup_logging()
            config = load_scheduler_config()
            manager = MasterTablesManager()
            run_daily_games_update(manager, config, logger)
        elif choice == '3':
            logger = setup_logging()
            config = load_scheduler_config()
            manager = MasterTablesManager()
            run_weekly_players_update(manager, config, logger)
        elif choice == '4':
            logger = setup_logging()
            config = load_scheduler_config()
            manager = MasterTablesManager()
            run_yearly_teams_update(manager, config, logger)
        elif choice == '5':
            main_with_args(['--status'])
        elif choice == '6':
            main_with_args(['--setup-windows'])
        elif choice == '7':
            print("👋 Goodbye!")
        else:
            print("Invalid choice")


def main_with_args(argv):
    """Helper to call main with specific arguments"""
    import sys
    old_argv = sys.argv
    sys.argv = ['scheduler.py'] + argv
    main()
    sys.argv = old_argv


if __name__ == "__main__":
    main()
