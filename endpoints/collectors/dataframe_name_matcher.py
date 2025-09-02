#!/usr/bin/env python3
"""
NBA API DataFrame Name Matcher

This module provides robust matching between NBA API DataFrames and their correct names
by analyzing content patterns, row counts, and column structures instead of relying on
dictionary ordering which can be unreliable.

Author: NBA Data Pipeline
Date: September 2025
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
import allintwo


def match_dataframes_to_names(dataframes, endpoint_instance, logger=None):
    """
    Match DataFrames to their correct names using multiple strategies.
    
    This function solves the issue where expected_data dictionary order doesn't
    match the actual DataFrame order returned by get_data_frames().
    
    Strategies used:
    1. Column count matching (when counts are unique)
    2. Content-based matching (GROUP_VALUE patterns for dashboard endpoints)
    3. Row count heuristics (1 row = Overall, 2 rows = Halves, etc.)
    4. Remaining name assignment
    
    Args:
        dataframes: List of pandas DataFrames from get_data_frames()
        endpoint_instance: NBA API endpoint instance
        logger: Optional logger for debugging
        
    Returns:
        List of matched DataFrame names in the same order as dataframes
    """
    
    def log(message, level="info"):
        if logger:
            getattr(logger, level)(message)
        else:
            print(message)
    
    if not hasattr(endpoint_instance, 'expected_data'):
        log("No expected_data available, using index-based naming")
        return [f"dataframe_{i}" for i in range(len(dataframes))]
    
    expected_data = endpoint_instance.expected_data
    matched_names = [None] * len(dataframes)
    used_names = set()
    
    log("Starting advanced DataFrame matching...")
    
    # Strategy 1: Exact column count matching (when counts are unique)
    log("Strategy 1: Column count matching")
    expected_counts = {name: len(cols) for name, cols in expected_data.items()}
    actual_counts = [(i, len(df.columns)) for i, df in enumerate(dataframes) 
                    if df is not None and not df.empty]
    
    # Build count frequency map
    count_frequency = {}
    for name, count in expected_counts.items():
        count_frequency[count] = count_frequency.get(count, []) + [name]
    
    # Match unique column counts
    for i, actual_count in actual_counts:
        if actual_count in count_frequency and len(count_frequency[actual_count]) == 1:
            name = count_frequency[actual_count][0]
            if name not in used_names:
                matched_names[i] = name
                used_names.add(name)
                log(f"  DataFrame {i} → {name} (unique column count: {actual_count})")
    
    # Strategy 2: Content-based matching for dashboard endpoints
    log("Strategy 2: Content-based matching")
    for i, df in enumerate(dataframes):
        if matched_names[i] is not None or df is None or df.empty:
            continue
            
        # Check GROUP_VALUE patterns for dashboard endpoints
        if 'GROUP_VALUE' in df.columns:
            group_values = set(df['GROUP_VALUE'].astype(str))
            
            content_patterns = {
                'OverallPlayerDashboard': ['Overall'],
                'ByHalfPlayerDashboard': ['1st Half', '2nd Half'],
                'ByPeriodPlayerDashboard': ['1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter'],
                'ByScoreMarginPlayerDashboard': ['Behind', 'Ahead', 'Tied', 'Points'],
                'ByActualMarginPlayerDashboard': ['Lost', 'Won'],
                # Add more patterns as needed for other endpoints
                'LastNGamesPlayerDashboard': ['Last', 'Games'],
                'LocationPlayerDashboard': ['Home', 'Road'],
                'MonthPlayerDashboard': ['January', 'February', 'March', 'April'],
                'PrePostPlayerDashboard': ['Pre', 'Post'],
                'StartingPositionPlayerDashboard': ['Guard', 'Forward', 'Center']
            }
            
            for name, patterns in content_patterns.items():
                if name in used_names:
                    continue
                    
                # Check if any patterns match the GROUP_VALUE content
                group_values_text = ' '.join(group_values).upper()
                pattern_matches = sum(1 for pattern in patterns 
                                    if pattern.upper() in group_values_text)
                
                # Match if at least one pattern is found
                if pattern_matches > 0:
                    matched_names[i] = name
                    used_names.add(name)
                    log(f"  DataFrame {i} → {name} (content match: {pattern_matches} patterns)")
                    break
    
    # Strategy 3: Row count heuristics
    log("Strategy 3: Row count heuristics")
    for i, df in enumerate(dataframes):
        if matched_names[i] is not None or df is None or df.empty:
            continue
            
        row_count = len(df)
        
        # Common row count patterns for dashboard endpoints
        heuristics = [
            (1, 'OverallPlayerDashboard', "1 row = overall stats"),
            (2, 'ByHalfPlayerDashboard', "2 rows = halves"),  
            (3, 'ByHalfPlayerDashboard', "3 rows = could be halves with totals"),
            (4, 'ByPeriodPlayerDashboard', "4 rows = quarters"),
            (5, 'ByPeriodPlayerDashboard', "5 rows = quarters + OT"),
            (12, 'MonthPlayerDashboard', "12 rows = months"),
            (7, 'DayPlayerDashboard', "7 rows = days of week")
        ]
        
        for expected_rows, dashboard_name, reason in heuristics:
            if row_count == expected_rows and dashboard_name not in used_names:
                matched_names[i] = dashboard_name
                used_names.add(dashboard_name)
                log(f"  DataFrame {i} → {dashboard_name} ({reason})")
                break
    
    # Strategy 4: Assign remaining available names
    log("Strategy 4: Remaining name assignment")
    available_names = [name for name in expected_data.keys() if name not in used_names]
    
    for i, df in enumerate(dataframes):
        if matched_names[i] is None:
            if available_names:
                name = available_names.pop(0)
                matched_names[i] = name
                log(f"  DataFrame {i} → {name} (remaining assignment)")
            else:
                matched_names[i] = f"dataframe_{i}"
                log(f"  DataFrame {i} → dataframe_{i} (fallback)")
    
    # Convert to lowercase for table naming consistency
    final_names = [name.lower() if name else f"dataframe_{i}" 
                   for i, name in enumerate(matched_names)]
    
    log(f"Final matching complete: {final_names}")
    return final_names


def generate_table_name(endpoint_name, dataframe_name):
    """
    Generate a consistent table name from endpoint and dataframe names.
    
    Args:
        endpoint_name: Name of the NBA API endpoint
        dataframe_name: Matched dataframe name
        
    Returns:
        Formatted table name following convention: nba_{endpoint}_{dataframe}
    """
    return f"nba_{endpoint_name.lower()}_{dataframe_name.lower()}"


def test_matching_function():
    """Test the matching function with a sample endpoint"""
    try:
        from nba_api.stats.endpoints import playerdashboardbygamesplits
        
        print("Testing DataFrame matching function...")
        endpoint = playerdashboardbygamesplits.PlayerDashboardByGameSplits(player_id=2544)
        dataframes = endpoint.get_data_frames()
        
        matched_names = match_dataframes_to_names(dataframes, endpoint)
        
        print(f"\nTest Results:")
        print(f"DataFrames: {len(dataframes)}")
        print(f"Matched names: {matched_names}")
        
        print(f"\nGenerated table names:")
        for i, (df, name) in enumerate(zip(dataframes, matched_names)):
            if df is not None and not df.empty:
                table_name = generate_table_name("playerdashboardbygamesplits", name)
                print(f"  {table_name} ({df.shape[0]} rows, {df.shape[1]} cols)")
        
        return True
        
    except Exception as e:
        print(f"Test failed: {e}")
        return False


if __name__ == "__main__":
    test_matching_function()
