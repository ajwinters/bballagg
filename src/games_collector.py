"""
NBA Games Data Collector

Handles daily collection of game data during active seasons:
- Scheduled games and results
- Real-time updates during seasons
- League-separated tables (nba_games, wnba_games, gleague_games)
- Incremental collection for recent games
"""

import pandas as pd
import time
from datetime import datetime, date, timedelta
from psycopg2.extras import execute_batch
from nba_api.stats.endpoints import (
    leaguegamefinder, scoreboardv2, teamgamelogs, 
    leaguedashteamstats
)
from database_manager import MasterTablesManager


class GamesCollector:
    """Collects and manages game data for active seasons"""
    
    def __init__(self):
        self.db_manager = MasterTablesManager()
        
        # League configurations with season schedules
        self.leagues = [
            {
                'name': 'NBA',
                'id': '00', 
                'table_prefix': 'nba',
                'season_months': (10, 6),  # October to June
                'preseason_start': (9, 15),  # Mid-September preseason
                'playoffs_end': (6, 30),  # End of June
            },
            {
                'name': 'WNBA',
                'id': '10',
                'table_prefix': 'wnba', 
                'season_months': (5, 10),  # May to October
                'preseason_start': (4, 15),  # Mid-April preseason
                'playoffs_end': (10, 31),  # End of October
            },
            {
                'name': 'G-League',
                'id': '20',
                'table_prefix': 'gleague',
                'season_months': (11, 3),  # November to March
                'preseason_start': (10, 15),  # Mid-October preseason
                'playoffs_end': (4, 15),  # Mid-April
            }
        ]
    
    def is_season_active(self, league_name):
        """Check if a league's season is currently active"""
        league_config = next((l for l in self.leagues if l['name'] == league_name), None)
        if not league_config:
            return False
        
        today = date.today()
        current_month = today.month
        
        season_start, season_end = league_config['season_months']
        
        # Handle seasons that cross calendar year
        if season_start > season_end:
            # Season crosses year boundary (e.g., October to June)
            return current_month >= season_start or current_month <= season_end
        else:
            # Season within same year (e.g., May to October)
            return season_start <= current_month <= season_end
    
    def get_current_season(self, league_name):
        """Get current season string for the league"""
        current_year = datetime.now().year
        
        if league_name in ['NBA', 'G-League']:
            # Two-year format: 2024-25
            if datetime.now().month >= 10:  # Season starts in fall
                return f"{current_year}-{str(current_year + 1)[-2:]}"
            else:
                return f"{current_year - 1}-{str(current_year)[-2:]}"
        else:  # WNBA
            # Single year format: 2024
            if datetime.now().month >= 5:  # WNBA season starts in May
                return str(current_year)
            else:
                return str(current_year - 1)
    
    def get_recent_games_date_range(self, days_back=7):
        """Get date range for recent games collection"""
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        return start_date, end_date
    
    def get_last_game_date_from_table(self, conn, table_name):
        """Get the latest game date from the master table for incremental collection"""
        try:
            cursor = conn.cursor()
            query = f"SELECT MAX(game_date) FROM {table_name};"
            cursor.execute(query)
            result = cursor.fetchone()[0]
            
            if result:
                # Convert to date if it's a datetime
                if hasattr(result, 'date'):
                    return result.date()
                return result
            else:
                # If no data, return a date far in the past to collect all available data
                return date(2020, 1, 1)  # Start from 2020 for modern data
                
        except Exception as e:
            print(f"   Warning: Could not get last game date from {table_name}: {str(e)}")
            # Return a reasonable default date
            return date(2023, 1, 1)

    def collect_league_recent_games(self, league_name, days_back=None):
        """Collect games for a specific league using incremental approach"""
        league_config = next((l for l in self.leagues if l['name'] == league_name), None)
        if not league_config:
            print(f"❌ Unknown league: {league_name}")
            return 0
        
        league_id = league_config['id']
        table_name = f"{league_config['table_prefix']}_games"
        current_season = self.get_current_season(league_name)
        
        conn = self.db_manager.connect_to_database()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            
            # Get the last game date from the table
            last_game_date = self.get_last_game_date_from_table(conn, table_name)
            today = date.today()
            
            # If days_back is provided (for testing/manual runs), use that instead
            if days_back is not None:
                start_date = today - timedelta(days=days_back)
            else:
                # Use incremental approach: from last game date to today
                start_date = last_game_date + timedelta(days=1)  # Start from day after last game
            
            end_date = today
            
            print(f"🏀 COLLECTING {league_name} GAMES (INCREMENTAL)")
            print(f"   Season: {current_season}")
            print(f"   Last game in table: {last_game_date}")
            print(f"   Collection range: {start_date} to {end_date}")
            print(f"   Target table: {table_name}")
            
            # Skip if no new dates to collect
            if start_date > end_date:
                print(f"   ✅ No new games to collect - table is up to date")
                return 0
            
            # Use LeagueGameFinder as primary method
            games_data = self.get_games_from_finder(league_id, current_season, start_date, end_date)
            
            if games_data is None or len(games_data) == 0:
                print(f"   ⚠️  No games found for {league_name} in date range")
                return 0
            
            # Process and deduplicate games
            unique_games = self.extract_unique_games_from_finder(games_data, league_name)
            
            if len(unique_games) > 0:
                games_inserted = self.bulk_insert_games(cursor, conn, unique_games, table_name)
                print(f"   ✅ {games_inserted} games processed for {league_name}")
                return games_inserted
            else:
                print(f"   ⚠️  No unique games to insert for {league_name}")
                return 0
                
        except Exception as e:
            print(f"   ❌ Error collecting {league_name} games: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()
    
    def get_games_from_finder(self, league_id, season, start_date, end_date):
        """Get games using LeagueGameFinder (primary method)"""
        try:
            print(f"      🔄 Using LeagueGameFinder for date range...", end=" ")
            
            game_finder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable=league_id,
                season_nullable=season,
                season_type_nullable='Regular Season',  # Can be extended to include playoffs
                date_from_nullable=start_date.strftime('%m/%d/%Y'),
                date_to_nullable=end_date.strftime('%m/%d/%Y')
            )
            
            games_df = game_finder.get_data_frames()[0]
            print(f"✅ {len(games_df)} games found")
            
            # Add rate limiting
            time.sleep(0.6)
            
            return games_df
            
        except Exception as e:
            print(f"❌ LeagueGameFinder failed: {str(e)[:100]}...")
            return None

    def extract_unique_games_from_finder(self, games_df, league_name):
        """Extract unique games from LeagueGameFinder data (1 record per game)"""
        unique_games = []
        
        try:
            # LeagueGameFinder returns 2 rows per game (one for each team)
            # We need to deduplicate to get 1 row per game
            if 'GAME_ID' in games_df.columns:
                game_groups = games_df.groupby('GAME_ID')
                
                for game_id, game_group in game_groups:
                    try:
                        # Sort by team to ensure consistent home/away assignment
                        game_group = game_group.sort_values('TEAM_ID')
                        
                        # Get game info (should be same for all records of same game)
                        first_record = game_group.iloc[0]
                        
                        # Extract home and away teams from the two records
                        if len(game_group) >= 2:
                            # LeagueGameFinder typically has away team first, home team second
                            # But we'll determine based on MATCHUP field
                            home_team_id, away_team_id = self.parse_matchup_from_finder(game_group)
                        else:
                            # Single record case - shouldn't happen but handle it
                            continue
                        
                        if not home_team_id or not away_team_id:
                            continue
                        
                        # Extract all relevant fields for the game
                        game_date = self.safe_get_date(first_record.get('GAME_DATE'))
                        season_id = str(first_record.get('SEASON_ID', ''))
                        
                        unique_game = (
                            str(game_id),
                            game_date,
                            season_id,
                            str(away_team_id),
                            str(home_team_id),
                            str(first_record.get('MATCHUP', ''))
                        )
                        
                        unique_games.append(unique_game)
                        
                    except Exception as e:
                        print(f"      Warning: Error processing game {game_id}: {str(e)}")
                        continue
                        
        except Exception as e:
            print(f"   Error extracting unique games: {str(e)}")
        
        print(f"      Processed {len(unique_games)} unique games from {len(games_df)} records")
        return unique_games

    def parse_matchup_from_finder(self, game_group):
        """Parse home/away teams from LeagueGameFinder data"""
        try:
            # LeagueGameFinder has MATCHUP field like "LAL @ NYK" or "LAL vs. NYK"
            # @ indicates away team, vs indicates home team
            
            away_team_id = None
            home_team_id = None
            
            for _, row in game_group.iterrows():
                matchup = str(row.get('MATCHUP', ''))
                team_id = str(row['TEAM_ID'])
                
                if ' @ ' in matchup:
                    # This team is away (playing @ opponent)
                    away_team_id = team_id
                elif ' vs. ' in matchup:
                    # This team is home (playing vs opponent)
                    home_team_id = team_id
            
            return home_team_id, away_team_id
            
        except Exception as e:
            print(f"      Error parsing matchup: {str(e)}")
            return None, None
    
    def get_games_from_team_logs(self, league_id, season, start_date, end_date):
        """Get games using team game logs (last resort fallback)"""
        try:
            # This is more complex as it requires getting all teams first
            # For now, return empty - in production you'd implement this fully
            print(f"      🔄 Team logs method not fully implemented")
            return None
            
        except Exception as e:
            print(f"❌ Team logs failed: {str(e)[:50]}...")
            return None
    
    def safe_get_date(self, date_value):
        """Safely convert date value to date object"""
        if pd.isna(date_value) or date_value is None:
            return None
        
        try:
            if isinstance(date_value, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        return datetime.strptime(date_value, fmt).date()
                    except:
                        continue
            return date_value
        except:
            return None
    
    def safe_get_int(self, value):
        """Safely convert value to int"""
        if pd.isna(value) or value is None:
            return None
        try:
            return int(value)
        except:
            return None
    
    def bulk_insert_games(self, cursor, conn, games_data, table_name):
        """Bulk insert games with conflict handling"""
        if not games_data:
            return 0
        
        try:
            insert_query = f"""
                INSERT INTO {table_name} 
                (game_id, game_date, season_id, away_team_id, home_team_id, 
                 matchup)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, season_id) 
                DO UPDATE SET 
                    game_date = EXCLUDED.game_date,
                    away_team_id = EXCLUDED.away_team_id,
                    home_team_id = EXCLUDED.home_team_id,
                    matchup = EXCLUDED.matchup,
                    updated_at = CURRENT_TIMESTAMP;
            """
            
            # Remove extra fields that don't match schema
            games_for_insert = []
            for game in games_data:
                # Only keep the first 6 fields that match the schema
                games_for_insert.append(game[:6])
            
            execute_batch(cursor, insert_query, games_for_insert, page_size=100)
            conn.commit()
            
            return len(games_for_insert)
            
        except Exception as e:
            print(f"Error bulk inserting games: {str(e)}")
            conn.rollback()
            return 0
    
    def collect_active_leagues_games(self, days_back=None):
        """Collect games for all active leagues using incremental approach"""
        print("🏀 COLLECTING GAMES FOR ACTIVE LEAGUES (INCREMENTAL)")
        print("=" * 60)
        
        total_games = 0
        results = {}
        
        for league_config in self.leagues:
            league_name = league_config['name']
            
            try:
                if self.is_season_active(league_name):
                    print(f"\n{league_name} season is active, collecting games...")
                    games_added = self.collect_league_recent_games(league_name, days_back)
                    total_games += games_added
                    results[league_name] = games_added
                else:
                    print(f"\n{league_name} season is not active, skipping...")
                    results[league_name] = 0
                
                # Rate limiting between leagues
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error with {league_name}: {str(e)}")
                results[league_name] = 0
        
        print(f"\n✅ INCREMENTAL GAMES COLLECTION COMPLETE")
        print(f"   Total games updated: {total_games}")
        
        for league, count in results.items():
            if count > 0:
                print(f"   {league}: {count} games updated")
            else:
                print(f"   {league}: No games or season inactive")
        
        return results


def main():
    """Test the games collector with new incremental approach"""
    collector = GamesCollector()
    
    print("🧪 TESTING INCREMENTAL GAMES COLLECTOR")
    print("=" * 50)
    
    # Test season checking
    print("\n1. Checking which leagues are active...")
    for league_config in collector.leagues:
        league_name = league_config['name']
        is_active = collector.is_season_active(league_name)
        season = collector.get_current_season(league_name)
        print(f"   {league_name}: {'Active' if is_active else 'Inactive'} (Season: {season})")
    
    # Test incremental collection for one league
    print("\n2. Testing incremental NBA games collection...")
    nba_result = collector.collect_league_recent_games('NBA')  # No days_back = incremental
    
    print(f"\n3. Testing incremental collection for all leagues...")
    all_results = collector.collect_active_leagues_games()  # Incremental approach
    
    print(f"\n4. Testing with manual date range (3 days back)...")
    manual_results = collector.collect_active_leagues_games(days_back=3)  # Override for testing
    
    print(f"\n✅ Testing complete!")
    print(f"📊 Incremental results: {all_results}")
    print(f"📊 Manual results: {manual_results}")


if __name__ == "__main__":
    main()
