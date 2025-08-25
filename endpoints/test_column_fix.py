#!/usr/bin/env python3
"""
Test script for the database column name fix
"""

import os
import sys
import pandas as pd

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.allintwo import clean_column_names

def test_column_name_cleaning():
    """Test the column name cleaning function"""
    
    # Create test dataframe with problematic column names
    test_data = {
        'TO': [1, 2, 3],  # This should become 'turnovers'
        'FROM': [4, 5, 6],  # This should become 'from_field'
        'AST': [7, 8, 9],  # This should stay 'ast'
        'PTS': [10, 11, 12],  # This should stay 'pts'
        'ORDER': [13, 14, 15],  # This should become 'order_field'
        'SELECT': [16, 17, 18]  # This should become 'select_field'
    }
    
    df = pd.DataFrame(test_data)
    print("Original columns:", list(df.columns))
    
    # Clean column names
    cleaned_df = clean_column_names(df)
    print("Cleaned columns:", list(cleaned_df.columns))
    
    # Check specific transformations
    expected_mappings = {
        'TO': 'turnovers',
        'FROM': 'from_field',
        'AST': 'ast',
        'PTS': 'pts',
        'ORDER': 'order_field',
        'SELECT': 'select_field'
    }
    
    print("\nColumn transformations:")
    for original, expected in expected_mappings.items():
        actual = None
        for col in cleaned_df.columns:
            if col == expected:
                actual = col
                break
        
        if actual == expected:
            print(f"✅ {original} -> {expected}")
        else:
            print(f"❌ {original} -> expected '{expected}' but got columns: {list(cleaned_df.columns)}")

if __name__ == '__main__':
    test_column_name_cleaning()
