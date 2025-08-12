#!/usr/bin/env python3

"""
NBA Game ID Season Filter
Helps identify which seasons actually have BoxScoreAdvancedV3 data
"""

def analyze_game_id_seasons():
    """Analyze the season distribution of game IDs"""
    
    # NBA Game ID format: 00{season}{game_type}{game_number}
    # season: 22 = 2022-23 season, 23 = 2023-24 season, etc.
    # game_type: 2 = regular season, 4 = playoffs, 9 = play-in
    
    # BoxScoreAdvancedV3 was introduced around 2013-14 season (season code 13)
    min_season_for_advanced_stats = 13  # 2013-14 season
    
    print("[INFO] NBA Season Code Reference:")
    print("Season Code | NBA Season")
    print("-----------+-----------")
    for season in range(13, 25):  # 2013-14 to 2024-25
        year1 = 2000 + season if season >= 13 else 1900 + season + 100
        year2 = year1 + 1
        print(f"     {season:2d}     | {year1}-{year2}")
    
    print(f"\n[RECOMMENDATION] Filter games to season >= {min_season_for_advanced_stats} (2013-14 season)")
    print("This would eliminate ~40,000+ games that will never have BoxScoreAdvancedV3 data")
    
    return min_season_for_advanced_stats

def create_season_filter_sql():
    """Create SQL to filter games by season"""
    min_season = analyze_game_id_seasons()
    
    # Create WHERE clause to filter games
    # Game ID format: 00SSTTGGGG where SS is season, TT is type, GGGG is game number
    sql_filter = f"""
-- Filter for games with potential BoxScoreAdvancedV3 data (2013-14 season and later)
SELECT * FROM nba_games 
WHERE CAST(SUBSTR(gameid, 3, 2) AS INTEGER) >= {min_season}
ORDER BY gameid DESC;

-- Count of games by season
SELECT 
    SUBSTR(gameid, 3, 2) as season_code,
    '20' || SUBSTR(gameid, 3, 2) || '-' || ('20' || CAST(CAST(SUBSTR(gameid, 3, 2) AS INTEGER) + 1 AS TEXT)) as season,
    COUNT(*) as game_count
FROM nba_games 
GROUP BY SUBSTR(gameid, 3, 2)
ORDER BY SUBSTR(gameid, 3, 2) DESC;
"""
    
    print(f"\n[SQL] Suggested filter for endpoint processor:")
    print(sql_filter)
    
    return sql_filter

if __name__ == "__main__":
    create_season_filter_sql()
