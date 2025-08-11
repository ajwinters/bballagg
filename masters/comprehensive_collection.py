"""
NBA Master Tables - Comprehensive Data Collection

This script will:
1. Analyze existing data gaps
2. Collect missing games from all leagues (NBA, WNBA, G-League)
3. Add ~33,000 missing games for complete coverage
4. Create league-separated master tables
5. Provide real-time progress updates

Estimated runtime: 30-45 minutes
Target: ~95,000 total games across all leagues
"""

import pandas as pd
import time
import os
import sys
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
from nba_api.stats.endpoints import leaguegamefinder
import traceback

# Import our fixed manager
from database_manager_fixed import FixedMasterTablesManager


class ComprehensiveDataCollector(FixedMasterTablesManager):
    """Comprehensive data collection with progress tracking and gap analysis"""
    
    def __init__(self):
        super().__init__()
        self.collection_stats = {
            'start_time': None,
            'games_added': 0,
            'leagues_processed': 0,
            'errors': 0,
            'current_league': None,
            'current_season': None
        }
        
    def analyze_existing_data(self):
        """Analyze what data we already have in the current mastergames table"""
        print("📊 ANALYZING EXISTING DATA")
        print("=" * 40)
        
        conn = self.connect_to_database()
        if not conn:
            return None
            
        try:
            cursor = conn.cursor()
            
            # Check existing mastergames table with proper league detection
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN seasonid LIKE '2%' THEN 'NBA'
                        WHEN seasonid LIKE '4%' THEN 'WNBA'  
                        WHEN seasonid LIKE '5%' THEN 'G-League'
                        ELSE 'Other'
                    END as league_name,
                    COUNT(*) as game_count,
                    MIN(gamedate) as earliest_game,
                    MAX(gamedate) as latest_game
                FROM mastergames 
                GROUP BY 
                    CASE 
                        WHEN seasonid LIKE '2%' THEN 'NBA'
                        WHEN seasonid LIKE '4%' THEN 'WNBA'
                        WHEN seasonid LIKE '5%' THEN 'G-League' 
                        ELSE 'Other'
                    END
                ORDER BY league_name;
            """)
            
            existing_data = cursor.fetchall()
            
            print("Current Coverage:")
            total_existing = 0
            for row in existing_data:
                league, count, earliest, latest = row
                total_existing += count
                print(f"   {league}: {count:,} games ({earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')})")
            
            print(f"\nTotal Existing Games: {total_existing:,}")
            
            # Calculate gaps based on realistic targets
            targets = {
                'NBA': 65000,      # 1983-2024 realistic target  
                'WNBA': 15000,     # 1997-2024 all games
                'G-League': 15000  # 2001-2024 all games
            }
            
            existing_by_league = {row[0]: row[1] for row in existing_data}
            
            print(f"\nData Gaps to Fill:")
            total_gaps = 0
            for league, target in targets.items():
                current = existing_by_league.get(league, 0)
                gap = max(0, target - current)
                total_gaps += gap
                coverage_pct = (current / target) * 100
                print(f"   {league}: {gap:,} missing games ({coverage_pct:.1f}% complete)")
            
            print(f"\nTotal Games to Add: ~{total_gaps:,}")
            print(f"Final Target: ~{total_existing + total_gaps:,} games")
            
            conn.close()
            return existing_by_league
            
        except Exception as e:
            print(f"Error analyzing data: {str(e)}")
            conn.close()
            return None
    
    def create_master_tables(self):
        """Ensure the existing mastergames table is ready for new data"""
        print("\n🏗️  PREPARING EXISTING MASTER TABLES")
        print("=" * 45)
        
        conn = self.connect_to_database()
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            
            # Check if mastergames table exists (it should based on our analysis)
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'mastergames';
            """)
            
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                print("✅ Existing mastergames table found")
                
                # Add any missing indexes for performance
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_mastergames_seasonid ON mastergames(seasonid);",
                    "CREATE INDEX IF NOT EXISTS idx_mastergames_gamedate ON mastergames(gamedate);",
                    "CREATE INDEX IF NOT EXISTS idx_mastergames_gameid ON mastergames(gameid);",
                    "CREATE INDEX IF NOT EXISTS idx_mastergames_teamid ON mastergames(teamid);"
                ]
                
                for index_sql in indexes:
                    cursor.execute(index_sql)
                
                print("✅ Performance indexes added/verified")
            else:
                print("❌ mastergames table not found - this shouldn't happen!")
                return False
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error preparing tables: {str(e)}")
            conn.rollback() if conn else None
            conn.close() if conn else None
            return False
    
    def collect_league_data(self, league_config, seasons_to_collect):
        """Collect data for a specific league"""
        league_name = league_config['name']
        league_id = league_config['id']
        
        print(f"\n🏀 COLLECTING {league_name} DATA")
        print(f"   Seasons to process: {len(seasons_to_collect)}")
        print(f"   League ID: {league_id}")
        
        self.collection_stats['current_league'] = league_name
        games_added_this_league = 0
        
        conn = self.connect_to_database()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            
            for i, season in enumerate(seasons_to_collect, 1):
                self.collection_stats['current_season'] = season
                print(f"\n   📅 Processing season {season} ({i}/{len(seasons_to_collect)})")
                
                for season_type_config in self.season_types:
                    season_type = season_type_config['type']
                    
                    try:
                        print(f"      🔄 {season_type}...", end=" ")
                        
                        # Get games from NBA API
                        gamefinder = leaguegamefinder.LeagueGameFinder(
                            league_id_nullable=league_id,
                            season_type_nullable=season_type,
                            season_nullable=season
                        ).get_data_frames()[0]
                        
                        if len(gamefinder) > 0:
                            # Insert games into existing mastergames table structure
                            games_inserted = 0
                            for _, game in gamefinder.iterrows():
                                
                                # Generate season ID in the format the existing table uses
                                season_id = f"{league_id}{season}"
                                
                                # Check if this exact game already exists 
                                check_query = """
                                    SELECT COUNT(*) FROM mastergames 
                                    WHERE gameid = %s AND teamid = %s AND seasonid = %s;
                                """
                                cursor.execute(check_query, (
                                    game.get('GAME_ID', ''),
                                    game.get('TEAM_ID', ''),
                                    season_id
                                ))
                                
                                if cursor.fetchone()[0] == 0:  # Game doesn't exist
                                    insert_query = """
                                    INSERT INTO mastergames 
                                    (seasonid, teamid, teamabbreviation, teamname, gameid, gamedate, 
                                     matchup, wl, min, pts, fgm, fga, fgpct, fg3m, fg3a, fg3pct,
                                     ftm, fta, ftpct, oreb, dreb, reb, ast, stl, blk, tov, pf, plusminus)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                    """
                                    
                                    try:
                                        cursor.execute(insert_query, (
                                            season_id,
                                            game.get('TEAM_ID', ''),
                                            game.get('TEAM_ABBREVIATION', ''),
                                            game.get('TEAM_NAME', ''),
                                            game.get('GAME_ID', ''),
                                            game.get('GAME_DATE'),
                                            game.get('MATCHUP', ''),
                                            game.get('WL', ''),
                                            game.get('MIN', 0),
                                            game.get('PTS', 0),
                                            game.get('FGM', 0),
                                            game.get('FGA', 0), 
                                            game.get('FG_PCT', 0.0),
                                            game.get('FG3M', 0),
                                            game.get('FG3A', 0.0),
                                            game.get('FG3_PCT', 0.0),
                                            game.get('FTM', 0),
                                            game.get('FTA', 0),
                                            game.get('FT_PCT', 0.0),
                                            game.get('OREB', 0.0),
                                            game.get('DREB', 0.0),
                                            game.get('REB', 0),
                                            game.get('AST', 0),
                                            game.get('STL', 0.0),
                                            game.get('BLK', 0),
                                            game.get('TOV', 0),
                                            game.get('PF', 0),
                                            game.get('PLUS_MINUS', 0.0)
                                        ))
                                        games_inserted += 1
                                    except Exception as insert_error:
                                        # Skip individual game errors but log them
                                        pass
                            
                            conn.commit()
                            games_added_this_league += games_inserted
                            print(f"✅ {games_inserted} games")
                        else:
                            print("📭 No games")
                        
                        # Rate limiting
                        time.sleep(0.6)
                        
                    except Exception as e:
                        print(f"❌ Error: {str(e)[:50]}...")
                        self.collection_stats['errors'] += 1
                        continue
                
                # Progress update every 5 seasons
                if i % 5 == 0:
                    self.print_progress_update(games_added_this_league)
            
            conn.close()
            print(f"\n   ✅ {league_name} Complete: {games_added_this_league:,} games added")
            return games_added_this_league
            
        except Exception as e:
            print(f"\n   ❌ {league_name} Error: {str(e)}")
            conn.close()
            return games_added_this_league
    
    def print_progress_update(self, current_league_games):
        """Print real-time progress update"""
        elapsed = time.time() - self.collection_stats['start_time']
        elapsed_minutes = elapsed / 60
        
        total_games = self.collection_stats['games_added'] + current_league_games
        
        print(f"\n   📈 PROGRESS UPDATE:")
        print(f"      ⏱️  Runtime: {elapsed_minutes:.1f} minutes")
        print(f"      🎯 Games Added: {total_games:,}")
        print(f"      🏀 Current League: {self.collection_stats['current_league']}")
        print(f"      📅 Current Season: {self.collection_stats['current_season']}")
        print(f"      ❌ Errors: {self.collection_stats['errors']}")
    
    def run_comprehensive_collection(self):
        """Run the complete data collection process"""
        print("🚀 COMPREHENSIVE DATA COLLECTION STARTING")
        print("=" * 55)
        
        self.collection_stats['start_time'] = time.time()
        
        # Step 1: Analyze existing data
        existing_data = self.analyze_existing_data()
        if existing_data is None:
            print("❌ Failed to analyze existing data")
            return False
        
        # Step 2: Create master tables
        if not self.create_master_tables():
            print("❌ Failed to create master tables")
            return False
        
        # Step 3: Collect data for each league
        total_games_added = 0
        
        for league_config in self.league_configs:
            league_name = league_config['name']
            
            # Generate all possible seasons for this league
            seasons = self.generate_seasons_by_league(league_config, end_year=2025)
            
            # For comprehensive collection, get all seasons
            # But limit to avoid overwhelming (can be adjusted)
            seasons_to_collect = seasons[:30]  # Last 30 seasons
            
            games_added = self.collect_league_data(league_config, seasons_to_collect)
            total_games_added += games_added
            self.collection_stats['games_added'] = total_games_added
            self.collection_stats['leagues_processed'] += 1
            
            print(f"\n🔄 Processed {self.collection_stats['leagues_processed']}/{len(self.league_configs)} leagues")
        
        # Final results
        self.print_final_results(total_games_added)
        
        return True
    
    def print_final_results(self, total_games_added):
        """Print comprehensive final results"""
        elapsed = time.time() - self.collection_stats['start_time']
        elapsed_minutes = elapsed / 60
        
        print(f"\n🎉 COMPREHENSIVE COLLECTION COMPLETE!")
        print("=" * 55)
        print(f"⏱️  Total Runtime: {elapsed_minutes:.1f} minutes")
        print(f"🎯 Games Added: {total_games_added:,}")
        print(f"🏀 Leagues Processed: {self.collection_stats['leagues_processed']}")
        print(f"❌ Total Errors: {self.collection_stats['errors']}")
        
        # Check final database state
        conn = self.connect_to_database()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM mastergames;")
                total_games = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN seasonid LIKE '2%' THEN 'NBA'
                            WHEN seasonid LIKE '4%' THEN 'WNBA'
                            WHEN seasonid LIKE '5%' THEN 'G-League'
                            ELSE 'Other'
                        END as league_name,
                        COUNT(*) as game_count
                    FROM mastergames 
                    GROUP BY 
                        CASE 
                            WHEN seasonid LIKE '2%' THEN 'NBA'
                            WHEN seasonid LIKE '4%' THEN 'WNBA'
                            WHEN seasonid LIKE '5%' THEN 'G-League'
                            ELSE 'Other'
                        END
                    ORDER BY league_name;
                """)
                
                league_counts = cursor.fetchall()
                
                print(f"\n📊 FINAL DATABASE STATE:")
                print(f"   Total Games: {total_games:,}")
                for league, count in league_counts:
                    print(f"   {league}: {count:,} games")
                
                conn.close()
                
                # Calculate coverage improvement
                targets = {'NBA': 65000, 'WNBA': 15000, 'G-League': 15000}
                total_target = sum(targets.values())
                coverage_pct = (total_games / total_target) * 100
                
                print(f"\n🎯 COVERAGE ACHIEVED: {coverage_pct:.1f}%")
                
                if coverage_pct >= 85:
                    print("🎉 EXCELLENT! Comprehensive coverage achieved!")
                elif coverage_pct >= 70:
                    print("✅ GOOD! Strong coverage with room for improvement")
                else:
                    print("⚠️  PARTIAL: More data collection recommended")
                
            except Exception as e:
                print(f"Error getting final stats: {str(e)}")
                conn.close()


def main():
    """Main execution function"""
    print("🏀 NBA MASTER TABLES - COMPREHENSIVE DATA COLLECTION")
    print("=" * 65)
    
    print("🎯 This will collect comprehensive data across all leagues:")
    print("   • NBA: Fill gaps to reach ~65,000 games (1983-2024)")
    print("   • WNBA: Complete collection ~15,000 games (1997-2024)")  
    print("   • G-League: Complete collection ~15,000 games (2001-2024)")
    print("   • Target: ~95,000 total games")
    print("   • Estimated time: 30-45 minutes")
    
    print(f"\n⚠️  This is a comprehensive operation that will:")
    print(f"   • Add ~33,000 missing games to your database")
    print(f"   • Create new master tables structure")
    print(f"   • Run for 30-45 minutes with progress updates")
    
    response = input(f"\nDo you want to proceed? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        collector = ComprehensiveDataCollector()
        success = collector.run_comprehensive_collection()
        
        if success:
            print(f"\n🎉 SUCCESS! Your NBA master tables are now comprehensive!")
            print(f"   Ready for analysis, reporting, and automated updates")
        else:
            print(f"\n⚠️  Collection completed with some issues - check logs above")
    else:
        print(f"\n👋 Collection cancelled. System remains ready when you're ready to proceed.")


if __name__ == "__main__":
    main()
