"""
NBA Teams Data Collector

Handles annual collection of team data for all leagues:
- Team information and metadata
- Conference and division assignments
- Franchise history tracking
- League-separated tables (nba_teams, wnba_teams, gleague_teams)
"""

import pandas as pd
import time
from datetime import datetime, date, timedelta
from psycopg2.extras import execute_batch
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguedashteamstats
from database_manager import MasterTablesManager


class TeamsCollector:
    """Collects and manages team data for all leagues"""
    
    def __init__(self):
        self.db_manager = MasterTablesManager()
        
        # League configurations
        self.leagues = [
            {'name': 'NBA', 'id': '00', 'table_prefix': 'nba'},
            {'name': 'WNBA', 'id': '10', 'table_prefix': 'wnba'},
            {'name': 'G-League', 'id': '20', 'table_prefix': 'gleague'}
        ]
    
    def needs_annual_update(self, league_name):
        """Check if team data needs annual update (usually at season start)"""
        league_config = next((l for l in self.leagues if l['name'] == league_name), None)
        if not league_config:
            return False
        
        table_name = f"{league_config['table_prefix']}_teams"
        
        conn = self.db_manager.connect_to_database()
        if not conn:
            return True  # If can't connect, assume update needed
        
        try:
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                return True  # Table doesn't exist, need to create and populate
            
            # Check last update date
            cursor.execute(f"""
                SELECT MAX(updated_at) FROM {table_name};
            """)
            
            last_update = cursor.fetchone()[0]
            
            if not last_update:
                return True  # No data, need update
            
            # Update if last update was more than 6 months ago
            six_months_ago = datetime.now() - timedelta(days=180)
            
            return last_update < six_months_ago
            
        except Exception as e:
            print(f"Error checking update needs for {league_name}: {str(e)}")
            return True  # If error, assume update needed
        finally:
            if conn:
                conn.close()
    
    def get_current_season(self, league_name):
        """Get current season string based on league"""
        current_year = datetime.now().year
        
        if league_name in ['NBA', 'G-League']:
            # Two-year format: 2024-25
            if datetime.now().month >= 10:  # Season starts in October
                return f"{current_year}-{str(current_year + 1)[-2:]}"
            else:
                return f"{current_year - 1}-{str(current_year)[-2:]}"
        else:  # WNBA
            # Single year format: 2024
            if datetime.now().month >= 5:  # WNBA season starts in May
                return str(current_year)
            else:
                return str(current_year - 1)
    
    def collect_league_teams(self, league_name):
        """Collect teams for a specific league"""
        league_config = next((l for l in self.leagues if l['name'] == league_name), None)
        if not league_config:
            print(f"❌ Unknown league: {league_name}")
            return 0
        
        league_id = league_config['id']
        table_name = f"{league_config['table_prefix']}_teams"
        current_season = self.get_current_season(league_name)
        
        print(f"🏟️ COLLECTING {league_name} TEAMS")
        print(f"   Season: {current_season}")
        print(f"   Target table: {table_name}")
        
        conn = self.db_manager.connect_to_database()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            
            # Method 1: Try league-specific team stats (more complete data)
            teams_data = self.get_teams_from_stats(league_id, current_season)
            
            # Method 2: Fallback to static teams data
            if teams_data is None or len(teams_data) == 0:
                print("   Trying static team data source...")
                teams_data = self.get_teams_from_static(league_name)
            
            if teams_data is None or len(teams_data) == 0:
                print(f"   ⚠️  No team data found for {league_name}")
                return 0
            
            # Process and insert teams
            teams_processed = self.process_teams_data(teams_data, league_name)
            
            if len(teams_processed) > 0:
                teams_inserted = self.bulk_insert_teams(cursor, conn, teams_processed, table_name)
                print(f"   ✅ {teams_inserted} teams processed for {league_name}")
                return teams_inserted
            else:
                print(f"   ⚠️  No teams to insert for {league_name}")
                return 0
                
        except Exception as e:
            print(f"   ❌ Error collecting {league_name} teams: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()
    
    def get_teams_from_stats(self, league_id, season):
        """Get teams using LeagueDashTeamStats endpoint (more complete data)"""
        try:
            print(f"      🔄 Fetching team stats...", end=" ")
            
            team_stats = leaguedashteamstats.LeagueDashTeamStats(
                league_id_nullable=league_id,
                season=season,
                season_type_all_star='Regular Season'
            )
            
            teams_df = team_stats.get_data_frames()[0]
            print(f"✅ {len(teams_df)} teams")
            return teams_df
            
        except Exception as e:
            print(f"❌ Team stats failed: {str(e)[:50]}...")
            return None
    
    def get_teams_from_static(self, league_name):
        """Get teams using static data (fallback)"""
        try:
            print(f"      🔄 Fetching static teams...", end=" ")
            
            # Get all teams from static data
            all_teams = teams.get_teams()
            
            # Filter by league if possible (this is limited for NBA only)
            if league_name == 'NBA':
                teams_df = pd.DataFrame(all_teams)
            else:
                # For WNBA/G-League, we'll need to make API calls or use known team lists
                teams_df = self.get_non_nba_teams(league_name)
            
            print(f"✅ {len(teams_df)} teams")
            return teams_df
            
        except Exception as e:
            print(f"❌ Static teams failed: {str(e)[:50]}...")
            return None
    
    def get_non_nba_teams(self, league_name):
        """Get teams for non-NBA leagues using known team information"""
        # This is a simplified approach - in production, you'd want more comprehensive data
        
        if league_name == 'WNBA':
            # Known WNBA teams (as of 2024)
            wnba_teams = [
                {'id': 1611661312, 'full_name': 'Atlanta Dream', 'abbreviation': 'ATL', 'nickname': 'Dream', 'city': 'Atlanta', 'state': 'Georgia'},
                {'id': 1611661313, 'full_name': 'Chicago Sky', 'abbreviation': 'CHI', 'nickname': 'Sky', 'city': 'Chicago', 'state': 'Illinois'},
                {'id': 1611661314, 'full_name': 'Connecticut Sun', 'abbreviation': 'CONN', 'nickname': 'Sun', 'city': 'Uncasville', 'state': 'Connecticut'},
                {'id': 1611661315, 'full_name': 'Dallas Wings', 'abbreviation': 'DAL', 'nickname': 'Wings', 'city': 'Dallas', 'state': 'Texas'},
                {'id': 1611661316, 'full_name': 'Indiana Fever', 'abbreviation': 'IND', 'nickname': 'Fever', 'city': 'Indianapolis', 'state': 'Indiana'},
                {'id': 1611661317, 'full_name': 'Las Vegas Aces', 'abbreviation': 'LV', 'nickname': 'Aces', 'city': 'Las Vegas', 'state': 'Nevada'},
                {'id': 1611661318, 'full_name': 'Minnesota Lynx', 'abbreviation': 'MIN', 'nickname': 'Lynx', 'city': 'Minneapolis', 'state': 'Minnesota'},
                {'id': 1611661319, 'full_name': 'New York Liberty', 'abbreviation': 'NY', 'nickname': 'Liberty', 'city': 'New York', 'state': 'New York'},
                {'id': 1611661320, 'full_name': 'Phoenix Mercury', 'abbreviation': 'PHX', 'nickname': 'Mercury', 'city': 'Phoenix', 'state': 'Arizona'},
                {'id': 1611661321, 'full_name': 'Seattle Storm', 'abbreviation': 'SEA', 'nickname': 'Storm', 'city': 'Seattle', 'state': 'Washington'},
                {'id': 1611661322, 'full_name': 'Washington Mystics', 'abbreviation': 'WAS', 'nickname': 'Mystics', 'city': 'Washington', 'state': 'D.C.'}
            ]
            return pd.DataFrame(wnba_teams)
            
        elif league_name == 'G-League':
            # For G-League, return empty DataFrame - teams change frequently
            # In production, you'd want to query the API or maintain a current list
            print("G-League teams require API query...")
            return pd.DataFrame()
        
        return pd.DataFrame()
    
    def process_teams_data(self, teams_df, league_name):
        """Process and standardize team data"""
        processed_teams = []
        
        try:
            for _, team in teams_df.iterrows():
                # Handle different column names from different sources
                team_id = str(team.get('TEAM_ID', team.get('id', '')))
                team_name = str(team.get('TEAM_NAME', team.get('full_name', '')))
                team_abbreviation = str(team.get('TEAM_ABBREVIATION', team.get('abbreviation', '')))
                
                # Skip if essential data is missing
                if not team_id or not team_name:
                    continue
                
                processed_team = (
                    team_id,
                    team_name,
                    team_abbreviation,
                    str(team.get('TEAM_CITY', team.get('city', ''))),
                    str(team.get('TEAM_STATE', team.get('state', ''))),
                    int(team.get('FOUNDED', 0)) if team.get('FOUNDED') else None,
                    str(team.get('CONFERENCE', '')),
                    str(team.get('DIVISION', '')),
                    True,  # is_active - assume true for current data
                )
                
                processed_teams.append(processed_team)
                
        except Exception as e:
            print(f"Error processing teams data: {str(e)}")
        
        return processed_teams
    
    def bulk_insert_teams(self, cursor, conn, teams_data, table_name):
        """Bulk insert teams with conflict handling"""
        if not teams_data:
            return 0
        
        try:
            insert_query = f"""
                INSERT INTO {table_name} 
                (team_id, team_name, team_abbreviation, team_city, team_state,
                 year_founded, conference, division, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id) 
                DO UPDATE SET 
                    team_name = EXCLUDED.team_name,
                    team_abbreviation = EXCLUDED.team_abbreviation,
                    team_city = EXCLUDED.team_city,
                    team_state = EXCLUDED.team_state,
                    conference = EXCLUDED.conference,
                    division = EXCLUDED.division,
                    is_active = EXCLUDED.is_active,
                    updated_at = CURRENT_TIMESTAMP;
            """
            
            # Remove league_name from the data (9th element)
            teams_without_league = []
            for team in teams_data:
                # team should have 10 elements, we want only the first 9 (without league_name)
                teams_without_league.append(team[:9])
            
            execute_batch(cursor, insert_query, teams_without_league, page_size=100)
            conn.commit()
            
            return len(teams_without_league)
            
        except Exception as e:
            print(f"Error bulk inserting teams: {str(e)}")
            conn.rollback()
            return 0
    
    def collect_all_leagues_teams(self):
        """Collect teams for all leagues that need updates"""
        print("🏟️ COLLECTING TEAMS FOR ALL LEAGUES")
        print("=" * 45)
        
        total_teams = 0
        results = {}
        
        for league_config in self.leagues:
            league_name = league_config['name']
            
            try:
                if self.needs_annual_update(league_name):
                    print(f"\n{league_name} needs team data update...")
                    teams_added = self.collect_league_teams(league_name)
                    total_teams += teams_added
                    results[league_name] = teams_added
                else:
                    print(f"\n{league_name} team data is current, skipping...")
                    results[league_name] = 0
                
                # Rate limiting between leagues
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error with {league_name}: {str(e)}")
                results[league_name] = 0
        
        print(f"\n✅ TEAMS COLLECTION COMPLETE")
        print(f"   Total teams updated: {total_teams}")
        
        for league, count in results.items():
            if count > 0:
                print(f"   {league}: {count} teams updated")
            else:
                print(f"   {league}: No update needed")
        
        return results


def main():
    """Test the teams collector"""
    collector = TeamsCollector()
    
    print("🧪 TESTING TEAMS COLLECTOR")
    print("=" * 40)
    
    # Test update checking
    print("\n1. Checking which leagues need updates...")
    for league in ['NBA', 'WNBA', 'G-League']:
        needs_update = collector.needs_annual_update(league)
        print(f"   {league}: {'Needs update' if needs_update else 'Current'}")
    
    # Test single league
    print("\n2. Testing NBA teams collection...")
    nba_result = collector.collect_league_teams('NBA')
    
    print(f"\n3. Testing all leagues...")
    all_results = collector.collect_all_leagues_teams()
    
    print(f"\n✅ Testing complete!")


if __name__ == "__main__":
    main()
