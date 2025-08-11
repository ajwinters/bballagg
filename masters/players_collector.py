"""
NBA Players Data Collector

Handles bi-weekly collection of player data for all leagues:
- Player roster information
- Player statistics
- Bulk insert with conflict handling
- League-separated tables (nba_players, wnba_players, gleague_players)
"""

import pandas as pd
import time
from datetime import datetime
from psycopg2.extras import execute_batch
from nba_api.stats.endpoints import leaguedashplayerbiostats, commonallplayers
from database_manager import MasterTablesManager


class PlayersCollector:
    """Collects and manages player data for all leagues"""
    
    def __init__(self):
        self.db_manager = MasterTablesManager()
        
        # League configurations
        self.leagues = [
            {'name': 'NBA', 'id': '00', 'table_prefix': 'nba'},
            {'name': 'WNBA', 'id': '10', 'table_prefix': 'wnba'},
            {'name': 'G-League', 'id': '20', 'table_prefix': 'gleague'}
        ]
    
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
    
    def collect_league_players(self, league_name):
        """Collect players for a specific league"""
        league_config = next((l for l in self.leagues if l['name'] == league_name), None)
        if not league_config:
            print(f"❌ Unknown league: {league_name}")
            return 0
        
        league_id = league_config['id']
        table_name = f"{league_config['table_prefix']}_players"
        current_season = self.get_current_season(league_name)
        
        print(f"👥 COLLECTING {league_name} PLAYERS")
        print(f"   Season: {current_season}")
        print(f"   Target table: {table_name}")
        
        conn = self.db_manager.connect_to_database()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            
            # Method 1: Try league-specific player bio stats
            players_data = self.get_players_from_biostats(league_id, current_season)
            
            # Method 2: Fallback to common all players
            if players_data is None or len(players_data) == 0:
                print("   Trying alternative player data source...")
                players_data = self.get_players_from_common(league_id, current_season)
            
            if players_data is None or len(players_data) == 0:
                print(f"   ⚠️  No player data found for {league_name}")
                return 0
            
            # Process and insert players
            players_processed = self.process_players_data(players_data, league_name, current_season)
            
            if len(players_processed) > 0:
                players_inserted = self.bulk_insert_players(cursor, conn, players_processed, table_name)
                print(f"   ✅ {players_inserted} players processed for {league_name}")
                return players_inserted
            else:
                print(f"   ⚠️  No players to insert for {league_name}")
                return 0
                
        except Exception as e:
            print(f"   ❌ Error collecting {league_name} players: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()
    
    def get_players_from_biostats(self, league_id, season):
        """Get players using LeagueDashPlayerBioStats endpoint"""
        try:
            print(f"      🔄 Fetching bio stats...", end=" ")
            
            biostats = leaguedashplayerbiostats.LeagueDashPlayerBioStats(
                league_id_nullable=league_id,
                season=season,
                season_type_all_star='Regular Season'
            )
            
            players_df = biostats.get_data_frames()[0]
            print(f"✅ {len(players_df)} players")
            return players_df
            
        except Exception as e:
            print(f"❌ Bio stats failed: {str(e)[:50]}...")
            return None
    
    def get_players_from_common(self, league_id, season):
        """Get players using CommonAllPlayers endpoint"""
        try:
            print(f"      🔄 Fetching common players...", end=" ")
            
            # For current season, use is_only_current_season=1
            common_players = commonallplayers.CommonAllPlayers(
                league_id=league_id,
                season=season,
                is_only_current_season='1'
            )
            
            players_df = common_players.get_data_frames()[0]
            print(f"✅ {len(players_df)} players")
            return players_df
            
        except Exception as e:
            print(f"❌ Common players failed: {str(e)[:50]}...")
            return None
    
    def process_players_data(self, players_df, league_name, season):
        """Process and standardize player data"""
        processed_players = []
        
        try:
            for _, player in players_df.iterrows():
                # Handle different column names from different endpoints
                player_id = str(player.get('PLAYER_ID', player.get('PERSON_ID', '')))
                player_name = str(player.get('PLAYER_NAME', player.get('DISPLAY_FIRST_LAST', '')))
                
                # Skip if essential data is missing
                if not player_id or not player_name:
                    continue
                
                processed_player = (
                    player_id,
                    player_name,
                    str(player.get('TEAM_ID', '')),
                    str(player.get('TEAM_ABBREVIATION', '')),
                    season,
                    str(player.get('POSITION', '')),
                    str(player.get('HEIGHT', '')),
                    str(player.get('WEIGHT', '')),
                    player.get('BIRTH_DATE'),  # May be None
                    int(player.get('AGE', 0)) if player.get('AGE') else None,
                    int(player.get('EXPERIENCE', 0)) if player.get('EXPERIENCE') else None,
                    str(player.get('SCHOOL', '')),  # College
                    str(player.get('COUNTRY', '')),
                    int(player.get('DRAFT_YEAR', 0)) if player.get('DRAFT_YEAR') else None,
                    int(player.get('DRAFT_ROUND', 0)) if player.get('DRAFT_ROUND') else None,
                    int(player.get('DRAFT_NUMBER', 0)) if player.get('DRAFT_NUMBER') else None,
                    True,  # is_active - assume true for current season
                )
                
                processed_players.append(processed_player)
                
        except Exception as e:
            print(f"Error processing players data: {str(e)}")
        
        return processed_players
    
    def bulk_insert_players(self, cursor, conn, players_data, table_name):
        """Bulk insert players with conflict handling"""
        if not players_data:
            return 0
        
        try:
            insert_query = f"""
                INSERT INTO {table_name} 
                (player_id, player_name, team_id, team_abbreviation, season, 
                 position, height, weight, birth_date, age, years_experience,
                 college, country, draft_year, draft_round, draft_number, 
                 is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, season) 
                DO UPDATE SET 
                    player_name = EXCLUDED.player_name,
                    team_id = EXCLUDED.team_id,
                    team_abbreviation = EXCLUDED.team_abbreviation,
                    position = EXCLUDED.position,
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    age = EXCLUDED.age,
                    years_experience = EXCLUDED.years_experience,
                    is_active = EXCLUDED.is_active,
                    updated_at = CURRENT_TIMESTAMP;
            """
            
            # Remove league_name from processed data if present (keep first 17 elements)
            players_without_league = []
            for player in players_data:
                # player should have 18 elements, we want only the first 17 (without league_name)
                players_without_league.append(player[:17])
            
            execute_batch(cursor, insert_query, players_without_league, page_size=100)
            conn.commit()
            
            return len(players_without_league)
            for player in players_data:
                if len(player) == 17:  # Already has league_name
                    players_with_league.append(player)
                else:  # Add league_name
                    players_with_league.append(player + (table_name.split('_')[0].upper(),))
            
            execute_batch(cursor, insert_query, players_with_league, page_size=1000)
            conn.commit()
            
            return len(players_with_league)
            
        except Exception as e:
            print(f"Error bulk inserting players: {str(e)}")
            conn.rollback()
            return 0
    
    def collect_all_leagues_players(self):
        """Collect players for all leagues"""
        print("👥 COLLECTING PLAYERS FOR ALL LEAGUES")
        print("=" * 45)
        
        total_players = 0
        results = {}
        
        for league_config in self.leagues:
            league_name = league_config['name']
            
            try:
                players_added = self.collect_league_players(league_name)
                total_players += players_added
                results[league_name] = players_added
                
                # Rate limiting between leagues
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error with {league_name}: {str(e)}")
                results[league_name] = 0
        
        print(f"\n✅ PLAYERS COLLECTION COMPLETE")
        print(f"   Total players: {total_players}")
        
        for league, count in results.items():
            print(f"   {league}: {count} players")
        
        return results


def main():
    """Test the players collector"""
    collector = PlayersCollector()
    
    print("🧪 TESTING PLAYERS COLLECTOR")
    print("=" * 40)
    
    # Test single league
    print("\n1. Testing NBA players collection...")
    nba_result = collector.collect_league_players('NBA')
    
    print(f"\n2. Testing all leagues...")
    all_results = collector.collect_all_leagues_players()
    
    print(f"\n✅ Testing complete!")


if __name__ == "__main__":
    main()
