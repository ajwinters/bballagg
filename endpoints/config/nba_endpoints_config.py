"""
NBA API Endpoints Configuration

This file defines all NBA API endpoints that should be systematically processed,
along with their required parameters and data extraction logic.
"""

import pandas as pd
from datetime import datetime

# Endpoint categories
ENDPOINT_CATEGORIES = {
    'GAME_BASED': 'endpoints that require gameid parameter',
    'PLAYER_BASED': 'endpoints that require playerid parameter', 
    'TEAM_BASED': 'endpoints that require teamid parameter',
    'SEASON_BASED': 'endpoints that require season parameter',
    'LEAGUE_BASED': 'endpoints that work at league level'
}

# Game-based endpoints (use mastergames.gameid)
# Note: V3 versions are prioritized over V2 when available
GAME_ENDPOINTS = [
    {
        'endpoint': 'BoxScoreAdvancedV3', 
        'description': 'Advanced box score stats V3 (PREFERRED)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreAdvancedV2',
        'description': 'Advanced box score stats for a game (backup)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreDefensiveV2',
        'description': 'Defensive stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion', 
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreFourFactorsV3',
        'description': 'Four factors stats V3 (PREFERRED)',
        'parameters': {'game_id': 'from_mastergames'}, 
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreFourFactorsV2',
        'description': 'Four factors stats for a game (backup)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreHustleV2',
        'description': 'Hustle stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'low'
    },
    {
        'endpoint': 'BoxScoreMatchupsV3',
        'description': 'Matchup data for a game (PREFERRED)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreMiscV3',
        'description': 'Miscellaneous stats V3 (PREFERRED)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreMiscV2',
        'description': 'Miscellaneous stats for a game (backup)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScorePlayerTrackV3',
        'description': 'Player tracking stats V3 (PREFERRED)', 
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScorePlayerTrackV2',
        'description': 'Player tracking stats for a game (backup)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreScoringV3',
        'description': 'Scoring stats V3 (PREFERRED)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion', 
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreScoringV2',
        'description': 'Scoring stats for a game (backup)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreSummaryV2',
        'description': 'Game summary and basic info',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreTraditionalV3',
        'description': 'Traditional box score stats V3 (PREFERRED)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreTraditionalV2',
        'description': 'Traditional box score stats (backup)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreUsageV3',
        'description': 'Usage stats V3 (PREFERRED)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreUsageV2',
        'description': 'Usage stats for a game (backup)',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'GameRotation',
        'description': 'Player rotation data for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'HustleStatsBoxScore',
        'description': 'Hustle stats box score',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'low'
    },
    {
        'endpoint': 'PlayByPlayV3',
        'description': 'Play-by-play data for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    }
]

# Player-based endpoints (use masterplayers.playerid)
PLAYER_ENDPOINTS = [
    {
        'endpoint': 'CommonPlayerInfo',
        'description': 'Basic player information',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'high'
    },
    {
        'endpoint': 'PlayerGameLog', 
        'description': 'Player game log for current season',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'high'
    },
    {
        'endpoint': 'PlayerDashboardByGameSplits',
        'description': 'Player dashboard by game splits (all seasons, all players)',
        'parameters': {
            'player_id': 'from_masterplayers_all_seasons',
            'season': 'from_masterplayers_all_seasons'
        },
        'frequency': 'weekly',
        'priority': 'medium',
        'data_type': 'player_season_cumulative'
    },
    {
        'endpoint': 'PlayerDashboardByGeneralSplits',
        'description': 'Player dashboard by general splits (all seasons, all players)',
        'parameters': {
            'player_id': 'from_masterplayers_all_seasons',
            'season': 'from_masterplayers_all_seasons'
        },
        'frequency': 'weekly',
        'priority': 'medium',
        'data_type': 'player_season_cumulative'
    },
    {
        'endpoint': 'PlayerDashboardByShootingSplits',
        'description': 'Player dashboard by shooting splits (all seasons, all players)',
        'parameters': {
            'player_id': 'from_masterplayers_all_seasons',
            'season': 'from_masterplayers_all_seasons'
        },
        'frequency': 'weekly', 
        'priority': 'high',
        'data_type': 'player_season_cumulative'
    },
    {
        'endpoint': 'PlayerDashboardByTeamPerformance',
        'description': 'Player dashboard by team performance (all seasons, all players)',
        'parameters': {
            'player_id': 'from_masterplayers_all_seasons',
            'season': 'from_masterplayers_all_seasons'
        },
        'frequency': 'weekly',
        'priority': 'medium',
        'data_type': 'player_season_cumulative'
    },
    {
        'endpoint': 'PlayerDashboardByYearOverYear',
        'description': 'Player dashboard year over year comparison (all seasons, all players)',
        'parameters': {
            'player_id': 'from_masterplayers_all_seasons',
            'season': 'from_masterplayers_all_seasons'
        },
        'frequency': 'weekly',
        'priority': 'medium',
        'data_type': 'player_season_cumulative'
    },
    {
        'endpoint': 'PlayerDashboardByClutch',
        'description': 'Player dashboard by clutch situations (all seasons, all players)',
        'parameters': {
            'player_id': 'from_masterplayers_all_seasons',
            'season': 'from_masterplayers_all_seasons'
        },
        'frequency': 'weekly',
        'priority': 'high',
        'data_type': 'player_season_cumulative'
    },
    {
        'endpoint': 'PlayerDashboardByLastNGames',
        'description': 'Player dashboard by last N games (all seasons, all players)',
        'parameters': {
            'player_id': 'from_masterplayers_all_seasons',
            'season': 'from_masterplayers_all_seasons'
        },
        'frequency': 'weekly',
        'priority': 'medium',
        'data_type': 'player_season_cumulative'
    },
    {
        'endpoint': 'PlayerNextNGames',
        'description': 'Player next N games',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'low'
    },
    {
        'endpoint': 'PlayerVsPlayer',
        'description': 'Player vs Player comparison (requires second player)',
        'parameters': {'player_id': 'from_masterplayers', 'vs_player_id': 'dynamic'},
        'frequency': 'on_demand',
        'priority': 'low'
    },
    {
        'endpoint': 'PlayerDashPtPass',
        'description': 'Player dash passing stats',
        'parameters': {'player_id': 'from_masterplayers', 'team_id': 'dynamic'},
        'frequency': 'weekly',
        'priority': 'medium'
    },
    {
        'endpoint': 'PlayerDashPtReb', 
        'description': 'Player dash rebounding stats',
        'parameters': {'player_id': 'from_masterplayers', 'team_id': 'dynamic'},
        'frequency': 'weekly',
        'priority': 'medium'
    },
    {
        'endpoint': 'PlayerDashPtShotDefend',
        'description': 'Player dash shot defend stats',
        'parameters': {'player_id': 'from_masterplayers', 'team_id': 'dynamic'},
        'frequency': 'weekly',
        'priority': 'low'
    },
    {
        'endpoint': 'PlayerDashPtShots',
        'description': 'Player dash shot stats',
        'parameters': {'player_id': 'from_masterplayers', 'team_id': 'dynamic'},
        'frequency': 'weekly',
        'priority': 'medium'
    }
]

# Team-based endpoints (use masterteams.id) 
TEAM_ENDPOINTS = [
    {
        'endpoint': 'CommonTeamRoster',
        'description': 'Team roster information',
        'parameters': {'team_id': 'from_masterteams'},
        'frequency': 'weekly',
        'priority': 'high'
    }
]

# Season/League-based endpoints
LEAGUE_ENDPOINTS = [
    {
        'endpoint': 'PlayerGameLogs',
        'description': 'All player game logs for date range',
        'parameters': {
            'date_from_nullable': 'dynamic_date_range',
            'date_to_nullable': 'dynamic_date_range',
            'season_nullable': 'current_season'
        },
        'frequency': 'weekly',
        'priority': 'high'
    },
    {
        'endpoint': 'LeagueDashPlayerBioStats',
        'description': 'League-wide player bio and stats',
        'parameters': {'season': 'current_season'},
        'frequency': 'weekly',
        'priority': 'high'
    },
    {
        'endpoint': 'LeagueGameFinder',
        'description': 'Find games by league and season',
        'parameters': {
            'league_id_nullable': '00',
            'season_nullable': 'current_season',
            'season_type_nullable': 'Regular Season'
        },
        'frequency': 'weekly',
        'priority': 'medium'
    }
]

# Consolidated endpoint registry
ALL_ENDPOINTS = {
    'game_based': GAME_ENDPOINTS,
    'player_based': PLAYER_ENDPOINTS,
    'team_based': TEAM_ENDPOINTS,
    'league_based': LEAGUE_ENDPOINTS
}

def get_endpoints_by_priority(priority='high'):
    """Get all endpoints filtered by priority level"""
    filtered_endpoints = []
    for category, endpoints in ALL_ENDPOINTS.items():
        filtered_endpoints.extend([ep for ep in endpoints if ep['priority'] == priority])
    return filtered_endpoints

def get_endpoints_by_frequency(frequency='weekly'):
    """Get all endpoints filtered by update frequency"""
    filtered_endpoints = []
    for category, endpoints in ALL_ENDPOINTS.items():
        filtered_endpoints.extend([ep for ep in endpoints if ep['frequency'] == frequency])
    return filtered_endpoints

def get_endpoints_by_category(category):
    """Get all endpoints in a specific category"""
    return ALL_ENDPOINTS.get(category, [])


def get_endpoint_by_name(endpoint_name):
    """Get a specific endpoint configuration by name"""
    for category, endpoints in ALL_ENDPOINTS.items():
        for endpoint in endpoints:
            if endpoint['endpoint'] == endpoint_name:
                return endpoint
    return None


def list_all_endpoint_names():
    """Get a list of all available endpoint names"""
    endpoint_names = []
    for category, endpoints in ALL_ENDPOINTS.items():
        for endpoint in endpoints:
            endpoint_names.append(endpoint['endpoint'])
    return sorted(endpoint_names)
