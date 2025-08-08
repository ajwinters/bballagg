"""
NBA API Endpoints Configuration

This file defines all NBA API endpoints that should be systematically processed,
along with their required parameters and data extraction logic.
"""

import pandas as pd
from datetime import datetime

# Endpoint categories
ENDPOINT_CATEGORIES = {
    'GAME_BASED': 'endpoints that require game_id parameter',
    'PLAYER_BASED': 'endpoints that require player_id parameter', 
    'TEAM_BASED': 'endpoints that require team_id parameter',
    'SEASON_BASED': 'endpoints that require season parameter',
    'LEAGUE_BASED': 'endpoints that work at league level'
}

# Game-based endpoints (use mastergames.gameid)
GAME_ENDPOINTS = [
    {
        'endpoint': 'BoxScoreAdvancedV2',
        'description': 'Advanced box score stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreAdvancedV3', 
        'description': 'Advanced box score stats V3',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreDefensiveV2',
        'description': 'Defensive stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion', 
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreFourFactorsV2',
        'description': 'Four factors stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreFourFactorsV3',
        'description': 'Four factors stats V3',
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
        'description': 'Matchup data for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'low'
    },
    {
        'endpoint': 'BoxScoreMiscV2',
        'description': 'Miscellaneous stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreMiscV3',
        'description': 'Miscellaneous stats V3',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScorePlayerTrackV2',
        'description': 'Player tracking stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScorePlayerTrackV3',
        'description': 'Player tracking stats V3', 
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreScoringV2',
        'description': 'Scoring stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreScoringV3',
        'description': 'Scoring stats V3',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion', 
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreSimilarityScore',
        'description': 'Similarity score for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'low'
    },
    {
        'endpoint': 'BoxScoreSummaryV2',
        'description': 'Game summary and basic info',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreTraditionalV2',
        'description': 'Traditional box score stats',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreTraditionalV3',
        'description': 'Traditional box score stats V3',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'high'
    },
    {
        'endpoint': 'BoxScoreUsageV2',
        'description': 'Usage stats for a game',
        'parameters': {'game_id': 'from_mastergames'},
        'frequency': 'after_game_completion',
        'priority': 'medium'
    },
    {
        'endpoint': 'BoxScoreUsageV3',
        'description': 'Usage stats V3',
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
        'description': 'Player dashboard by game splits',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'medium'
    },
    {
        'endpoint': 'PlayerDashboardByGeneralSplits',
        'description': 'Player dashboard by general splits',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'medium'
    },
    {
        'endpoint': 'PlayerDashboardByShootingSplits',
        'description': 'Player dashboard by shooting splits',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly', 
        'priority': 'medium'
    },
    {
        'endpoint': 'PlayerDashboardByTeamPerformance',
        'description': 'Player dashboard by team performance',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'low'
    },
    {
        'endpoint': 'PlayerDashboardByYearOverYear',
        'description': 'Player dashboard year over year',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'low'
    },
    {
        'endpoint': 'PlayerNextNGames',
        'description': 'Player next N games',
        'parameters': {'player_id': 'from_masterplayers'},
        'frequency': 'weekly',
        'priority': 'low'
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
