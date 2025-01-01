import pandas as pd
import time
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.endpoints import *
import allintwo as allintwo

### 
testplayer='203076'
testgame = '0022201200'
testteam = '1610612755'

## adds the connection
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