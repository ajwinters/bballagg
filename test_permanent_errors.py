#!/usr/bin/env python3
"""
Simple test for permanent error detection
"""

import sys
import os
sys.path.append('.')
sys.path.append('endpoints')
sys.path.append('endpoints/config')

import logging
import nba_api.stats.endpoints as nbaapi
from endpoints.collectors.single_endpoint_processor_simple import make_api_call

logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

print('Testing Permanent Error Detection')
print('='*40)

# Test 1: Invalid game ID should be permanent error
print('\nTest 1: Invalid game ID')
result = make_api_call(nbaapi.BoxScoreAdvancedV3, {'game_id': 'invalid'}, 0.1, logger)
print(f'Result: {result}')
is_permanent = result == 'PERMANENT_ERROR'
print(f'Expected: PERMANENT_ERROR, Got: {"PERMANENT_ERROR" if is_permanent else "Other"}')

print('\nTest Complete')
