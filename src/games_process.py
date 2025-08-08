import pandas as pd
import time
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.endpoints import *
import allintwo as allintwo
import requests
import socket

### 
testplayer='203076'
testgame = '0022201200'
testteam = '1610612755'

import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# class NBAStatsClient:
#     def __init__(self):
#         self.session = self._create_session()
        
#     def _create_session(self):
#         session = requests.Session()
        
#         # Configure retries
#         retries = Retry(
#             total=5,
#             backoff_factor=2,
#             status_forcelist=[408, 429, 500, 502, 503, 504],
#             allowed_methods=["GET", "POST"]
#         )
        
#         session.mount('https://', HTTPAdapter(max_retries=retries))
        
#         # Headers that mimic a modern browser
#         session.headers = {
#             'Host': 'stats.nba.com',
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
#             'Accept': 'application/json, text/plain, */*',
#             'Accept-Language': 'en-US,en;q=0.9',
#             'Accept-Encoding': 'gzip, deflate, br',
#             'x-nba-stats-origin': 'stats',
#             'x-nba-stats-token': 'true',
#             'Origin': 'https://www.nba.com',
#             'Connection': 'keep-alive',
#             'Referer': 'https://www.nba.com/',
#             'Sec-Fetch-Dest': 'empty',
#             'Sec-Fetch-Mode': 'cors',
#             'Sec-Fetch-Site': 'same-site',
#             'Pragma': 'no-cache',
#             'Cache-Control': 'no-cache'
#         }
        
#         return session
    
#     def get_stats(self, endpoint, params=None):
#         url = f'https://stats.nba.com/stats/{endpoint}'
#         max_attempts = 3
#         current_attempt = 0
        
#         while current_attempt < max_attempts:
#             try:
#                 print(f"Attempt {current_attempt + 1}: Requesting {url}")
#                 response = self.session.get(
#                     url,
#                     params=params,
#                     timeout=60  # Increased timeout
#                 )
#                 response.raise_for_status()
#                 return response.json()
                
#             except requests.exceptions.RequestException as e:
#                 current_attempt += 1
#                 print(f"Attempt {current_attempt} failed: {str(e)}")
                
#                 if current_attempt == max_attempts:
#                     raise Exception(f"Failed after {max_attempts} attempts: {str(e)}")
                
#                 # Exponential backoff
#                 wait_time = 2 ** current_attempt + 1
#                 print(f"Waiting {wait_time} seconds before retry...")
#                 time.sleep(wait_time)

# # Test function
# def test_nba_api():
#     client = NBAStatsClient()
#     test_endpoints = [
#         {
#             'name': 'Common All Players',
#             'endpoint': 'commonallplayers',
#             'params': {
#                 'LeagueID': '00',
#                 'Season': '2023-24',
#                 'IsOnlyCurrentSeason': '1'
#             }
#         },
#         {
#             'name': 'League Standings',
#             'endpoint': 'leaguestandings',
#             'params': {
#                 'LeagueID': '00',
#                 'Season': '2023-24',
#                 'SeasonType': 'Regular Season'
#             }
#         }
#     ]
    
#     for test in test_endpoints:
#         print(f"\nTesting endpoint: {test['name']}")
#         try:
#             result = client.get_stats(test['endpoint'], test['params'])
#             print(f"Successfully retrieved data from {test['name']}")
#             # Print first few items of response to verify data
#             print(f"Response preview: {str(result)[:200]}...")
#         except Exception as e:
#             print(f"Failed to retrieve {test['name']}: {str(e)}")

# if __name__ == "__main__":
#     print("Starting NBA API tests...")
#     test_nba_api()

# adds the connection
conn = allintwo.connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')

## Naming stuff

# aa = getattr(nbaapi,"BoxScoreAdvancedV3")
# aa(testgame).get_data_frames()

def games_createandinsert(func,index):
    print(f"Processing data for: {func,index}")
    func = getattr(nbaapi,func)
    index = int(index)
    param = testgame
    dfname = list(func.expected_data.keys())[index]

    # need to replace with the endpoint class
    namepre = func.__name__.lower()
    name = namepre+"_"+dfname.lower()

    df = func(param).get_data_frames()[index]

    ### Creates Table if none exists
    allintwo.create_table(conn,name,df)

    gameslist = allintwo.game_difference(conn,name)

    for i in gameslist:
        try:
            tdf = func(i).get_data_frames()[index]
            allintwo.insert_dataframe_to_rds(conn,tdf,name)
            #print(i)
            time.sleep(1)
        except Exception as e:
            fdf = pd.DataFrame({"gameid":[i]})
            allintwo.insert_dataframe_to_rds(conn,fdf,name)
            print("An error occurred:", e)

if __name__ == "__main__":
    import sys
    # Read parameters passed to the script
    func = sys.argv[1]
    index = sys.argv[2]
    games_createandinsert(func,index)