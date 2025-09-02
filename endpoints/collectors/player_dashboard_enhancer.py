#!/usr/bin/env python3
"""
Player Dashboard Data Enhancer

This module handles special processing for player dashboard endpoints,
including adding player_id and season columns to track data ownership.
"""

import pandas as pd
import sys
import os

# Add project paths
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
import allintwo


def is_player_dashboard_endpoint(endpoint_name):
    """Check if endpoint is a player dashboard type that needs special handling"""
    player_dashboard_indicators = [
        'playerdashboard',
        'PlayerDashboard'
    ]
    
    return any(indicator.lower() in endpoint_name.lower() for indicator in player_dashboard_indicators)


def enhance_player_dashboard_dataframes(dataframes, player_id, season, endpoint_name, logger=None):
    """
    Add player_id and season columns to player dashboard DataFrames.
    
    This is critical because player dashboard data doesn't inherently contain
    the player_id or season it represents, making it impossible to track
    whose data is whose when storing in the database.
    
    Args:
        dataframes: List of DataFrames from NBA API
        player_id: The player ID these stats belong to
        season: The season these stats are for
        endpoint_name: Name of the endpoint for logging
        logger: Optional logger
    
    Returns:
        List of enhanced DataFrames with player_id and season columns
    """
    
    def log(message):
        if logger:
            logger.info(message)
        else:
            print(message)
    
    if not dataframes:
        return dataframes
    
    enhanced_dataframes = []
    
    for i, df in enumerate(dataframes):
        if df is None or df.empty:
            enhanced_dataframes.append(df)
            continue
        
        try:
            # Create a copy to avoid modifying original
            enhanced_df = df.copy()
            
            # Add player identification columns at the beginning
            enhanced_df.insert(0, 'player_id', player_id)
            enhanced_df.insert(1, 'season', season)
            
            # Add data collection metadata
            enhanced_df['collected_at'] = pd.Timestamp.now()
            enhanced_df['endpoint_source'] = endpoint_name.lower()
            
            log(f"Enhanced DataFrame {i} for player {player_id}, season {season}")
            log(f"  Original shape: {df.shape}")
            log(f"  Enhanced shape: {enhanced_df.shape}")
            log(f"  New columns: player_id, season, collected_at, endpoint_source")
            
            enhanced_dataframes.append(enhanced_df)
            
        except Exception as e:
            error_msg = f"Failed to enhance DataFrame {i} for player {player_id}: {str(e)}"
            if logger:
                logger.error(error_msg)
            else:
                print(f"ERROR: {error_msg}")
            # Return original DataFrame if enhancement fails
            enhanced_dataframes.append(df)
    
    log(f"Successfully enhanced {len([df for df in enhanced_dataframes if df is not None])} DataFrames")
    return enhanced_dataframes


def get_table_name_with_context(endpoint_name, dataframe_name, player_context=True):
    """
    Generate table name for player dashboard endpoints with context awareness.
    
    Player dashboard tables should be designed for efficient querying by player and season.
    
    Args:
        endpoint_name: NBA API endpoint name
        dataframe_name: Matched dataframe name
        player_context: Whether this is a player-context endpoint
    
    Returns:
        Formatted table name
    """
    base_name = f"nba_{endpoint_name.lower()}_{dataframe_name.lower()}"
    
    if player_context and is_player_dashboard_endpoint(endpoint_name):
        # Player dashboard tables can use the base name since they now have player_id columns
        # and are designed for multi-player storage
        return base_name
    
    return base_name


def create_player_season_key(player_id, season):
    """Create a unique key for player-season combinations"""
    return f"{player_id}_{season}"


def validate_player_dashboard_data(df, player_id, season, logger=None):
    """
    Validate that player dashboard data has required identification columns.
    
    Args:
        df: DataFrame to validate
        player_id: Expected player ID
        season: Expected season
        logger: Optional logger
    
    Returns:
        bool: True if valid, False otherwise
    """
    def log(message, level="info"):
        if logger:
            getattr(logger, level)(message)
        else:
            print(message)
    
    if df is None or df.empty:
        log("DataFrame is empty", "warning")
        return False
    
    required_columns = ['player_id', 'season']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        log(f"Missing required columns: {missing_columns}", "error")
        return False
    
    # Check that player_id and season values are consistent
    unique_players = df['player_id'].nunique()
    unique_seasons = df['season'].nunique()
    
    if unique_players != 1:
        log(f"DataFrame contains data for {unique_players} players, expected 1", "error")
        return False
    
    if unique_seasons != 1:
        log(f"DataFrame contains data for {unique_seasons} seasons, expected 1", "error")
        return False
    
    # Verify the player_id and season match expectations
    actual_player = df['player_id'].iloc[0]
    actual_season = df['season'].iloc[0]
    
    if str(actual_player) != str(player_id):
        log(f"Player ID mismatch: expected {player_id}, got {actual_player}", "error")
        return False
    
    if str(actual_season) != str(season):
        log(f"Season mismatch: expected {season}, got {actual_season}", "error")
        return False
    
    log(f"Validation passed: {len(df)} rows for player {player_id}, season {season}")
    return True


def get_player_dashboard_upsert_strategy():
    """
    Get the database upsert strategy for player dashboard data.
    
    Player dashboard data should be upserted (insert or update) based on
    player_id + season combination since the same player's season stats
    can be updated throughout the season.
    
    Returns:
        dict: Upsert strategy configuration
    """
    return {
        'conflict_columns': ['player_id', 'season', 'endpoint_source'],
        'update_columns': 'all_except_keys',
        'strategy': 'upsert_on_conflict'
    }


def test_enhancement_function():
    """Test the enhancement function with sample data"""
    try:
        # Create sample DataFrame similar to player dashboard structure
        sample_data = {
            'GROUP_SET': ['Overall'],
            'GROUP_VALUE': ['Overall'],
            'GP': [25],
            'PTS': [28.5],
            'REB': [8.2],
            'AST': [6.8]
        }
        
        sample_df = pd.DataFrame(sample_data)
        dataframes = [sample_df]
        
        print("Testing player dashboard enhancement...")
        print(f"Original DataFrame shape: {sample_df.shape}")
        print(f"Original columns: {list(sample_df.columns)}")
        
        enhanced_dfs = enhance_player_dashboard_dataframes(
            dataframes=dataframes,
            player_id=2544,  # LeBron James
            season="2024-25",
            endpoint_name="PlayerDashboardByGeneralSplits"
        )
        
        enhanced_df = enhanced_dfs[0]
        print(f"\nEnhanced DataFrame shape: {enhanced_df.shape}")
        print(f"Enhanced columns: {list(enhanced_df.columns)}")
        print(f"Sample data:\n{enhanced_df.head()}")
        
        # Test validation
        is_valid = validate_player_dashboard_data(enhanced_df, 2544, "2024-25")
        print(f"\nValidation result: {'✅ PASSED' if is_valid else '❌ FAILED'}")
        
        return True
        
    except Exception as e:
        print(f"Test failed: {e}")
        return False


if __name__ == "__main__":
    test_enhancement_function()
