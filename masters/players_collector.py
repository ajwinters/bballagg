"""
NBA Players Data Collector - Enhanced Version

Comprehensive master players table builder:
1. Pull ALL players from CommonAllPlayers (complete historical dataset)
2. Enhance each player with detailed info from CommonPlayerInfo
3. Merge datasets with duplicate removal
4. Support both backfill and incremental updates
5. League-separated tables (nba_players, wnba_players, gleague_players)
"""

import pandas as pd
import time
from datetime import datetime
from psycopg2.extras import execute_batch
from nba_api.stats.endpoints import commonallplayers, commonplayerinfo, leaguedashplayerbiostats
from database_manager import MasterTablesManager


class PlayersCollector:
    """Collects and manages player data for all leagues"""
    
    def __init__(self):
        self.db_manager = MasterTablesManager()
        
        # League configurations with historical data settings
        self.leagues = [
            {
                'name': 'NBA', 
                'id': '00', 
                'table_prefix': 'nba',
                'start_year': 1984,  # NBA founded in 1946
                'season_format': 'two_year'  # 2024-25 format
            },
            {
                'name': 'WNBA', 
                'id': '10', 
                'table_prefix': 'wnba',
                'start_year': 1997,  # WNBA founded in 1997
                'season_format': 'single_year'  # 2024 format
            },
            {
                'name': 'G-League', 
                'id': '20', 
                'table_prefix': 'gleague',
                'start_year': 2001,  # G-League founded as NBDL in 2001
                'season_format': 'two_year'  # 2024-25 format
            }
        ]
    
    def collect_comprehensive_players(self, league_name, backfill_mode=True):
        """
        Comprehensive players collection using CommonAllPlayers + CommonPlayerInfo
        
        Process:
        1. Get ALL players from CommonAllPlayers (historical complete dataset)
        2. For each player, get detailed info from CommonPlayerInfo
        3. Merge datasets with duplicate removal
        4. Insert with conflict handling
        
        Args:
            league_name: 'NBA', 'WNBA', or 'G-League'
            backfill_mode: If True, process all players. If False, only new players.
        """
        league_config = next((l for l in self.leagues if l['name'] == league_name), None)
        if not league_config:
            print(f"❌ Unknown league: {league_name}")
            return 0

        league_id = league_config['id']
        table_name = f"{league_config['table_prefix']}_players"
        
        print(f"🏀 COMPREHENSIVE {league_name} PLAYERS COLLECTION")
        print(f"   Mode: {'BACKFILL (All Players)' if backfill_mode else 'INCREMENTAL (New Players Only)'}")
        print(f"   Target table: {table_name}")
        
        conn = self.db_manager.connect_to_database()
        if not conn:
            return 0

        try:
            cursor = conn.cursor()
            
            # Step 1: Get ALL players from CommonAllPlayers
            print(f"   📋 Step 1: Fetching all players from CommonAllPlayers...")
            all_players_df = self.get_all_players_comprehensive(league_id)
            
            if all_players_df is None or len(all_players_df) == 0:
                print(f"   ❌ No players found in CommonAllPlayers for {league_name}")
                return 0
            
            print(f"   ✅ Found {len(all_players_df)} total players in {league_name}")
            
            # Step 2: Filter players if incremental mode
            if not backfill_mode:
                all_players_df = self.filter_new_players_only(cursor, all_players_df, table_name)
                print(f"   📊 After filtering: {len(all_players_df)} new players to process")
            
            if len(all_players_df) == 0:
                print(f"   ✅ No new players to process")
                return 0
            
            # Step 3: Enhance each player with CommonPlayerInfo
            print(f"   🔍 Step 2: Enhancing players with detailed info...")
            enhanced_players_df = self.enhance_players_with_detailed_info(all_players_df)
            
            # Step 4: Process and insert enhanced data
            print(f"   💾 Step 3: Processing and inserting enhanced player data...")
            players_processed = self.process_enhanced_players_data(enhanced_players_df, league_name)
            
            if len(players_processed) > 0:
                players_inserted = self.bulk_insert_enhanced_players(cursor, conn, players_processed, table_name)
                print(f"   ✅ {players_inserted} players processed successfully")
                return players_inserted
            else:
                print(f"   ⚠️  No players to insert")
                return 0
                
        except Exception as e:
            print(f"   ❌ Error in comprehensive collection: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()

    def get_all_players_comprehensive(self, league_id):
        """Get ALL players using CommonAllPlayers - complete historical dataset"""
        try:
            print(f"      🔄 Fetching comprehensive player list...", end=" ")
            
            # Get ALL players who ever played in this league
            all_players = commonallplayers.CommonAllPlayers(
                league_id=league_id,
                season='2024-25',  # Use current season as reference
                is_only_current_season='0'  # KEY: Get ALL historical players
            )
            
            players_df = all_players.get_data_frames()[0]
            print(f"✅ {len(players_df)} players")
            
            # Clean and standardize the base dataset
            return self.clean_base_players_data(players_df)
            
        except Exception as e:
            print(f"❌ Failed: {str(e)[:50]}...")
            return None

    def filter_new_players_only(self, cursor, all_players_df, table_name):
        """Filter to only players not already in the database"""
        try:
            # Get existing player IDs from database
            cursor.execute(f"SELECT DISTINCT playerid FROM {table_name}")
            existing_ids = set([str(row[0]) for row in cursor.fetchall()])
            
            # Filter to only new players
            all_players_df['PLAYER_ID'] = all_players_df['PERSON_ID'].astype(str)
            new_players_df = all_players_df[~all_players_df['PLAYER_ID'].isin(existing_ids)]
            
            print(f"      Existing players in DB: {len(existing_ids)}")
            print(f"      New players found: {len(new_players_df)}")
            
            return new_players_df
            
        except Exception as e:
            print(f"      ⚠️ Error filtering players, processing all: {str(e)}")
            return all_players_df

    def enhance_players_with_detailed_info(self, base_players_df):
        """Enhance each player with detailed info from CommonPlayerInfo"""
        enhanced_data = []
        total_players = len(base_players_df)
        
        print(f"      Processing {total_players} players with detailed info...")
        
        for i, (_, player) in enumerate(base_players_df.iterrows()):
            player_id = str(player.get('PERSON_ID', ''))
            
            if i % 50 == 0:  # Progress update every 50 players
                print(f"      Progress: {i}/{total_players} ({(i/total_players)*100:.1f}%)")
            
            try:
                # Get detailed player info
                detailed_info = self.get_player_detailed_info(player_id)
                
                if detailed_info is not None:
                    # Merge base data with detailed info
                    merged_player = self.merge_player_data(player, detailed_info)
                    enhanced_data.append(merged_player)
                else:
                    # Use base data only if detailed info fails
                    enhanced_data.append(player.to_dict())
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"      ⚠️ Error enhancing player {player_id}: {str(e)[:30]}...")
                # Add base data as fallback
                enhanced_data.append(player.to_dict())
                continue
        
        print(f"      ✅ Enhanced {len(enhanced_data)} players")
        return pd.DataFrame(enhanced_data)

    def get_player_detailed_info(self, player_id):
        """Get detailed info for a specific player using CommonPlayerInfo"""
        try:
            player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            info_df = player_info.get_data_frames()[0]
            
            if len(info_df) > 0:
                return info_df.iloc[0]  # Return first (and only) row as Series
            return None
            
        except Exception as e:
            # Fail silently - many historical players may not have detailed info
            return None

    def merge_player_data(self, base_player, detailed_info):
        """Merge base player data with detailed info, removing duplicates"""
        merged = base_player.to_dict().copy()
        
        if detailed_info is not None:
            detailed_dict = detailed_info.to_dict()
            
            # Mapping of duplicate fields (detailed_info_key: base_key)
            field_mappings = {
                'PERSON_ID': 'PERSON_ID',
                'FIRST_NAME': 'FIRST_NAME', 
                'LAST_NAME': 'LAST_NAME',
                'DISPLAY_FIRST_LAST': 'DISPLAY_FIRST_LAST'
            }
            
            # Add detailed info, avoiding duplicates
            for detailed_key, detailed_value in detailed_dict.items():
                # Skip if this field is already covered by base data
                if detailed_key not in field_mappings:
                    merged[detailed_key] = detailed_value
                # For mapped fields, prefer detailed info if base is empty/null
                elif detailed_key in field_mappings:
                    base_key = field_mappings[detailed_key]
                    if pd.isna(merged.get(base_key)) or merged.get(base_key) == '':
                        merged[base_key] = detailed_value
        
        return merged

    def clean_base_players_data(self, players_df):
        """Clean and standardize the base players dataset"""
        # Standardize column names for consistency
        column_mapping = {
            'PERSON_ID': 'PERSON_ID',
            'DISPLAY_FIRST_LAST': 'DISPLAY_FIRST_LAST', 
            'FIRST_NAME': 'FIRST_NAME',
            'LAST_NAME': 'LAST_NAME',
            'IS_ACTIVE': 'IS_ACTIVE'
        }
        
        # Keep only columns we have
        available_columns = [col for col in column_mapping.keys() if col in players_df.columns]
        cleaned_df = players_df[available_columns].copy()
        
        return cleaned_df

    def process_enhanced_players_data(self, enhanced_df, league_name):
        """Process the enhanced players data for database insertion"""
        processed_players = []
        
        try:
            for _, player in enhanced_df.iterrows():
                # Extract key fields with fallbacks
                player_id = str(player.get('PERSON_ID', ''))
                player_name = str(player.get('DISPLAY_FIRST_LAST', ''))
                
                # Skip if essential data is missing
                if not player_id or not player_name:
                    continue
                
                # Build comprehensive player record with proper data cleaning
                def safe_int_convert(value, default=None):
                    """Safely convert value to int, handling 'Undrafted' and other non-numeric values"""
                    if not value or value == '' or str(value).lower() in ['undrafted', 'none', 'null', 'nan']:
                        return default
                    try:
                        return int(float(str(value)))  # Handle both int and float strings
                    except (ValueError, TypeError):
                        return default
                
                processed_player = (
                    player_id,
                    player_name,
                    str(player.get('FIRST_NAME', '')),
                    str(player.get('LAST_NAME', '')),
                    str(player.get('BIRTHDATE', '')),
                    str(player.get('SCHOOL', '')),  # College
                    str(player.get('COUNTRY', '')),
                    str(player.get('HEIGHT', '')),
                    str(player.get('WEIGHT', '')),
                    str(player.get('POSITION', '')),
                    safe_int_convert(player.get('DRAFT_YEAR')),
                    safe_int_convert(player.get('DRAFT_ROUND')),
                    safe_int_convert(player.get('DRAFT_NUMBER')),
                    bool(player.get('IS_ACTIVE', True)),
                    league_name
                )
                
                processed_players.append(processed_player)
                
        except Exception as e:
            print(f"Error processing enhanced players data: {str(e)}")
        
        return processed_players

    def bulk_insert_enhanced_players(self, cursor, conn, players_data, table_name):
        """Bulk insert enhanced players with conflict handling and batch processing"""
        if not players_data:
            return 0
        
        try:
            insert_query = f"""
                INSERT INTO {table_name} 
                (playerid, playername, firstname, lastname, birthdate, 
                 college, country, height, weight, position, 
                 draftyear, draftround, draftnumber, isactive, league)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (playerid) 
                DO UPDATE SET 
                    playername = EXCLUDED.playername,
                    firstname = EXCLUDED.firstname,
                    lastname = EXCLUDED.lastname,
                    birthdate = EXCLUDED.birthdate,
                    college = EXCLUDED.college,
                    country = EXCLUDED.country,
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    position = EXCLUDED.position,
                    draftyear = EXCLUDED.draftyear,
                    draftround = EXCLUDED.draftround,
                    draftnumber = EXCLUDED.draftnumber,
                    isactive = EXCLUDED.isactive,
                    updatedat = CURRENT_TIMESTAMP;
            """
            
            # Process in smaller batches to avoid connection timeouts
            batch_size = 50
            total_inserted = 0
            
            for i in range(0, len(players_data), batch_size):
                batch = players_data[i:i + batch_size]
                
                try:
                    execute_batch(cursor, insert_query, batch, page_size=batch_size)
                    conn.commit()
                    total_inserted += len(batch)
                    
                    # Progress update for large datasets
                    if len(players_data) > 100 and i % (batch_size * 10) == 0:
                        progress = (i / len(players_data)) * 100
                        print(f"      Insert progress: {i}/{len(players_data)} ({progress:.1f}%)")
                
                except Exception as batch_error:
                    print(f"      ⚠️ Error inserting batch {i//batch_size + 1}: {str(batch_error)[:50]}...")
                    conn.rollback()
                    continue
            
            return total_inserted
            
        except Exception as e:
            print(f"Error bulk inserting enhanced players: {str(e)}")
            conn.rollback()
            return 0

    def generate_historical_seasons(self, league_config, end_year=None):
        """Generate all historical seasons for a league"""
        if end_year is None:
            end_year = datetime.now().year + 1
            
        seasons = []
        start_year = league_config['start_year']
        
        if league_config['season_format'] == 'two_year':
            # NBA/G-League format: 2023-24
            for year in range(start_year, end_year):
                season_str = f"{year}-{str(year+1)[2:].zfill(2)}"
                seasons.append(season_str)
        else:
            # WNBA format: 2024
            for year in range(start_year, end_year):
                seasons.append(str(year))
        
        return seasons[::-1]  # Most recent first for API efficiency

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
    
    def collect_league_players_comprehensive(self, league_name, test_mode=False):
        """Collect players for a specific league across ALL historical seasons"""
        league_config = next((l for l in self.leagues if l['name'] == league_name), None)
        if not league_config:
            print(f"❌ Unknown league: {league_name}")
            return 0

        league_id = league_config['id']
        table_name = f"{league_config['table_prefix']}_players"
        
        # Generate ALL historical seasons for this league
        historical_seasons = self.generate_historical_seasons(league_config)
        if test_mode:
            historical_seasons = historical_seasons[:5]  # Only recent seasons for testing
        
        print(f"👥 COLLECTING {league_name} PLAYERS - COMPREHENSIVE HISTORICAL MODE")
        print(f"   Total seasons to process: {len(historical_seasons)}")
        print(f"   Season range: {historical_seasons[-1]} → {historical_seasons[0]}")
        print(f"   Target table: {table_name}")
        
        conn = self.db_manager.connect_to_database()
        if not conn:
            return 0

        total_players_processed = 0
        successful_seasons = []
        failed_seasons = []

        try:
            cursor = conn.cursor()
            
            # Process each historical season
            for i, season in enumerate(historical_seasons):
                print(f"   📅 Season {i+1}/{len(historical_seasons)}: {season}")
                
                try:
                    # Primary method: Use CommonAllPlayers endpoint
                    players_data = self.get_players_from_common_historical(league_id, season)
                    
                    # Fallback: Try bio stats if CommonAllPlayers fails
                    if players_data is None or len(players_data) == 0:
                        print("      Trying bio stats as fallback...")
                        players_data = self.get_players_from_biostats(league_id, season)
                    
                    if players_data is None or len(players_data) == 0:
                        print(f"      ⚠️  No player data found for {league_name} {season}")
                        failed_seasons.append(season)
                        continue
                    
                    # Process and insert players for this season
                    players_processed = self.process_players_data(players_data, league_name, season)
                    
                    if len(players_processed) > 0:
                        players_inserted = self.bulk_insert_players(cursor, conn, players_processed, table_name)
                        print(f"      ✅ {players_inserted} players processed")
                        total_players_processed += players_inserted
                        successful_seasons.append(season)
                    else:
                        print(f"      ⚠️  No players to insert for {season}")
                        failed_seasons.append(season)
                        
                except Exception as e:
                    print(f"      ❌ Error processing {season}: {str(e)}")
                    failed_seasons.append(season)
                    continue
                
                # Rate limiting between seasons
                time.sleep(0.6)
                
        except Exception as e:
            print(f"   ❌ Critical error collecting {league_name} players: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()
        
        # Summary
        print(f"   ✅ COMPREHENSIVE COLLECTION COMPLETE")
        print(f"   Total players processed: {total_players_processed}")
        print(f"   Successful seasons: {len(successful_seasons)}")
        print(f"   Failed seasons: {len(failed_seasons)}")
        if failed_seasons:
            print(f"   Failed season list: {failed_seasons[:10]}...")  # Show first 10 failures
        
        return total_players_processed

    def get_players_from_common_historical(self, league_id, season):
        """Get players using CommonAllPlayers endpoint for historical seasons"""
        try:
            print(f"      🔄 Fetching historical common players...", end=" ")
            
            # For historical seasons, do NOT use is_only_current_season=1
            common_players = commonallplayers.CommonAllPlayers(
                league_id=league_id,
                season=season,
                is_only_current_season='0'  # Get ALL players who played in that season
            )
            
            players_df = common_players.get_data_frames()[0]
            print(f"✅ {len(players_df)} players")
            return players_df
            
        except Exception as e:
            print(f"❌ Historical common players failed: {str(e)[:50]}...")
            return None

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
            
            # Primary method: Use CommonAllPlayers endpoint
            players_data = self.get_players_from_common(league_id, current_season)
            
            # Fallback: Try bio stats if CommonAllPlayers fails
            if players_data is None or len(players_data) == 0:
                print("   Trying bio stats as fallback...")
                players_data = self.get_players_from_biostats(league_id, current_season)
            
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
                (playerid, playername, teamid, teamabbreviation, season, 
                 position, height, weight, birthdate, age, yearsexperience,
                 college, country, draftyear, draftround, draftnumber, 
                 isactive)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (playerid, season) 
                DO UPDATE SET 
                    playername = EXCLUDED.playername,
                    teamid = EXCLUDED.teamid,
                    teamabbreviation = EXCLUDED.teamabbreviation,
                    position = EXCLUDED.position,
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    age = EXCLUDED.age,
                    yearsexperience = EXCLUDED.yearsexperience,
                    isactive = EXCLUDED.isactive,
                    updatedat = CURRENT_TIMESTAMP;
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
    
    def collect_all_leagues_players_comprehensive(self, backfill_mode=True):
        """Collect comprehensive players for all leagues using enhanced method"""
        print("🏀 COMPREHENSIVE PLAYERS COLLECTION - ALL LEAGUES")
        print("=" * 70)
        
        mode_text = "BACKFILL (All Players)" if backfill_mode else "INCREMENTAL (New Players Only)"
        print(f"Mode: {mode_text}")
        
        total_players = 0
        results = {}
        
        for league_config in self.leagues:
            league_name = league_config['name']
            
            try:
                print(f"\n📊 Starting {league_name} comprehensive collection...")
                players_added = self.collect_comprehensive_players(league_name, backfill_mode)
                total_players += players_added
                results[league_name] = players_added
                
                # Rate limiting between leagues
                time.sleep(3)
                
            except Exception as e:
                print(f"   ❌ Error with {league_name}: {str(e)}")
                results[league_name] = 0
        
        print(f"\n✅ COMPREHENSIVE PLAYERS COLLECTION COMPLETE")
        print(f"   Total players across all leagues: {total_players}")
        
        for league, count in results.items():
            print(f"   {league}: {count} players")
        
        print(f"\n🎯 RESULT: Master players tables now contain:")
        print(f"   - ALL historical players from CommonAllPlayers")
        print(f"   - Enhanced with detailed info from CommonPlayerInfo")  
        print(f"   - Comprehensive dataset ready for dashboard endpoints!")
        
        return results

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
    """Test the enhanced comprehensive players collector"""
    collector = PlayersCollector()
    
    print("🏀 TESTING ENHANCED COMPREHENSIVE PLAYERS COLLECTOR")
    print("=" * 60)
    
    # Test single league comprehensive collection
    print("\n1. Testing NBA comprehensive collection (BACKFILL MODE)...")
    print("   This will:")
    print("   - Get ALL NBA players from CommonAllPlayers")
    print("   - Enhance each with CommonPlayerInfo details")
    print("   - Merge and deduplicate data")
    print("   - Insert comprehensive player records")
    
    nba_result = collector.collect_comprehensive_players('NBA', backfill_mode=True)
    
    print(f"\n✅ COMPREHENSIVE COLLECTION TEST COMPLETE!")
    print(f"   Players processed: {nba_result}")
    
    print(f"\n💡 USAGE:")
    print(f"   Backfill Mode: collector.collect_all_leagues_players_comprehensive(backfill_mode=True)")
    print(f"   Incremental Mode: collector.collect_all_leagues_players_comprehensive(backfill_mode=False)")
    print(f"\n🎯 This creates a complete master players dataset:")
    print(f"   - All historical NBA players (CommonAllPlayers)")
    print(f"   - Enhanced with detailed biographical info (CommonPlayerInfo)")  
    print(f"   - Perfect foundation for comprehensive dashboard data collection!")
    
    return {"comprehensive": nba_result}


if __name__ == "__main__":
    main()
