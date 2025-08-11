#!/usr/bin/env python3
"""
Daily NBA Data Collection Script (Windows-friendly version)
Simple standalone script for scheduled execution via Windows Task Scheduler.
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime, date

# Add src directory to path
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.append(str(project_root / 'src'))

# Import our collectors
from games_collector import GamesCollector
from players_collector import PlayersCollector
from teams_collector import TeamsCollector

def setup_logging():
    """Configure logging for daily collection"""
    log_dir = current_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f'daily_collection_{date.today().strftime("%Y%m%d")}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def main():
    """Run daily data collection"""
    logger = setup_logging()
    
    logger.info("STARTING DAILY NBA DATA COLLECTION")
    logger.info("=" * 50)
    logger.info(f"Collection Date: {date.today()}")
    
    total_games = 0
    total_players = 0
    total_teams = 0
    
    # 1. Collect recent games for active leagues
    logger.info("\nCOLLECTING RECENT GAMES...")
    try:
        games_collector = GamesCollector()
        games_results = games_collector.collect_active_leagues_games(days_back=3)
        total_games = sum(games_results.values())
        logger.info(f"Games collection completed: {total_games} games")
        
        for league, count in games_results.items():
            if count > 0:
                logger.info(f"   {league.upper()}: {count} games")
        
    except Exception as e:
        logger.error(f"Games collection failed: {str(e)}")
    
    # 2. Update players (weekly on Sundays)
    logger.info("\nCHECKING PLAYERS DATA...")
    try:
        current_day = date.today().weekday()  # 0 = Monday, 6 = Sunday
        
        if current_day == 6:  # Sunday
            logger.info("Sunday detected - updating players data")
            players_collector = PlayersCollector()
            players_results = players_collector.collect_all_leagues_players()
            total_players = sum(players_results.values())
            
            logger.info(f"Players collection completed: {total_players} players")
            
            for league, count in players_results.items():
                if count > 0:
                    logger.info(f"   {league.upper()}: {count} players")
        else:
            logger.info("Not Sunday - skipping players update")
    
    except Exception as e:
        logger.error(f"Players collection failed: {str(e)}")
    
    # 3. Update teams (monthly on 1st)
    logger.info("\nCHECKING TEAMS DATA...")
    try:
        if date.today().day == 1:
            logger.info("First of month detected - updating teams data")
            teams_collector = TeamsCollector()
            teams_results = teams_collector.collect_all_leagues_teams()
            total_teams = sum(teams_results.values())
            
            logger.info(f"Teams collection completed: {total_teams} teams updated")
            
            for league, count in teams_results.items():
                if count > 0:
                    logger.info(f"   {league.upper()}: {count} teams updated")
                else:
                    logger.info(f"   {league.upper()}: No update needed")
        else:
            logger.info("Not first of month - skipping teams update")
    
    except Exception as e:
        logger.error(f"Teams collection failed: {str(e)}")
    
    # Final summary
    logger.info("\n" + "=" * 50)
    logger.info("DAILY COLLECTION COMPLETE!")
    logger.info(f"Summary: {total_games} games, {total_players} players, {total_teams} teams")
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
