import pandas as pd

# Fix the comprehensive players table
df = pd.read_csv('data/comprehensive_master_players.csv')
print('Before fix:')
print(f'PLAYER_ID dtype: {df["PLAYER_ID"].dtype}')
print(f'TEAM_ID dtype: {df["TEAM_ID"].dtype}')

df['PLAYER_ID'] = df['PLAYER_ID'].astype(str)
df['TEAM_ID'] = df['TEAM_ID'].astype(str)
df.to_csv('data/comprehensive_master_players.csv', index=False)

print('After fix:')
df2 = pd.read_csv('data/comprehensive_master_players.csv')
print(f'PLAYER_ID dtype: {df2["PLAYER_ID"].dtype}')
print(f'TEAM_ID dtype: {df2["TEAM_ID"].dtype}')
print('✅ Fixed comprehensive players table ID formats')
