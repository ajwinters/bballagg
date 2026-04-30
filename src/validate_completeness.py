#!/usr/bin/env python3
"""Validate completeness of high-priority endpoint tables against master_nba_games.

Three endpoint categories, three different checks:

1. MASTER-TABLE builders — CommonAllPlayers -> master_nba_players,
   CommonTeamYears -> master_nba_teams, LeagueGameFinder -> master_nba_games.
   These feed the reference universe. They have NO nba_{endpoint}_* table of
   their own. Check = row count of the master table.

2. PER-GAME data endpoints — BoxScore*, HustleStatsBoxScore, PlayByPlayV3.
   Missing = gameids in master_nba_games that don't appear in the primary data
   shard. This is the same set the pipeline retries on its next run, so it
   doubles as the work-remaining estimate.

3. OTHER per-key endpoints — LeagueSeasonMatchups, CommonTeamRoster, TeamGameLogs,
   PlayerDashPtReb, PlayerDashPtShots. Report rows + distinct key. PlayerDash*
   additionally requires nba_playergamelogs_a (not currently in DB).
"""

import json
import psycopg2

# endpoint -> primary data table (the "_a" shard is the completeness indicator)
PER_GAME = {
    'BoxScoreAdvancedV3':     'nba_boxscoreadvancedv3_a',
    'BoxScoreDefensiveV2':    'nba_boxscoredefensivev2_a',
    'BoxScoreFourFactorsV3':  'nba_boxscorefourfactorsv3_a',
    'BoxScoreHustleV2':       'nba_boxscorehustlev2_a',
    'BoxScoreMatchupsV3':     'nba_boxscorematchupsv3_a',
    'BoxScoreMiscV3':         'nba_boxscoremiscv3_a',
    'BoxScorePlayerTrackV3':  'nba_boxscoreplayertrackv3_a',
    'BoxScoreScoringV3':      'nba_boxscorescoringv3_a',
    'BoxScoreSummaryV2':      'nba_boxscoresummaryv2_a',
    'BoxScoreTraditionalV3':  'nba_boxscoretraditionalv3_a',
    'BoxScoreUsageV3':        'nba_boxscoreusagev3_a',
    'HustleStatsBoxScore':    'nba_hustlestatsboxscore_a',
    'PlayByPlayV3':           'nba_playbyplayv3_a',
}

MASTER_BUILDERS = {
    'CommonAllPlayers':   'master_nba_players',
    'CommonTeamYears':    'master_nba_teams',
    'LeagueGameFinder':   'master_nba_games',
}

OTHER = {
    'LeagueSeasonMatchups': {'table': 'nba_leagueseasonmatchups_a', 'key': 'season'},
    'CommonTeamRoster':     {'table': 'nba_commonteamroster_a',     'key': 'teamid, season'},
    'TeamGameLogs':         {'table': 'nba_teamgamelogs_a',         'key': 'teamid, seasonyear'},
}

PLAYER_DASH_PREREQ_TABLE = 'nba_playergamelogs_a'
PLAYER_DASH_ENDPOINTS = ['PlayerDashPtReb', 'PlayerDashPtShots']


def connect():
    cfg = json.load(open('config/database_config.json'))
    return psycopg2.connect(
        host=cfg['host'], dbname=cfg['name'], user=cfg['user'],
        password=cfg['password'], port=cfg['port'], sslmode=cfg['ssl_mode'],
        connect_timeout=15,
    )


def check_per_game(cur, data_table):
    """Returns (universe, present, missing). Missing = master gameids not in data."""
    cur.execute(f"""
        WITH universe AS (SELECT DISTINCT gameid FROM master_nba_games),
             present AS (SELECT DISTINCT gameid FROM {data_table})
        SELECT
          (SELECT COUNT(*) FROM universe),
          (SELECT COUNT(*) FROM present),
          (SELECT COUNT(*) FROM universe u
             WHERE NOT EXISTS (SELECT 1 FROM present p WHERE p.gameid = u.gameid))
    """)
    return cur.fetchone()


def check_other(cur, table, key):
    cur.execute(f'SELECT COUNT(*), COUNT(DISTINCT ({key})) FROM {table}')
    return cur.fetchone()


def table_exists(cur, name):
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s)", (name,))
    return cur.fetchone()[0]


def verdict(universe, present, missing):
    if universe == 0:
        return 'EMPTY'
    if missing == 0:
        return 'complete'
    pct = present * 100 // universe
    return f'gaps ({pct}%)'


def main():
    conn = connect()
    cur = conn.cursor()

    print(f"\n{'='*100}")
    print(f"{'MASTER-TABLE BUILDERS (feed the reference universe)':^100}")
    print(f"{'='*100}")
    print(f"{'Endpoint':<22}{'Master table':<28}{'Rows':>12}")
    print('-' * 100)
    for ep, mt in MASTER_BUILDERS.items():
        cur.execute(f'SELECT COUNT(*) FROM {mt}')
        print(f'{ep:<22}{mt:<28}{cur.fetchone()[0]:>12}')

    print(f"\n{'='*100}")
    print(f"{'PER-GAME ENDPOINTS':^100}")
    print(f"{'='*100}")
    print(f"{'Endpoint':<28}{'Universe':>10}{'Present':>10}{'Missing':>10}  Verdict")
    print('-' * 100)
    for ep, dt in PER_GAME.items():
        try:
            u, pres, miss = check_per_game(cur, dt)
            print(f'{ep:<28}{u:>10}{pres:>10}{miss:>10}  {verdict(u, pres, miss)}')
        except Exception as e:
            conn.rollback()
            print(f'{ep:<28}  ERROR: {e}')

    print(f"\n{'='*100}")
    print(f"{'OTHER PER-KEY ENDPOINTS':^100}")
    print(f"{'='*100}")
    print(f"{'Endpoint':<28}{'Table':<36}{'Rows':>10}{'Distinct key':>15}")
    print('-' * 100)
    for ep, info in OTHER.items():
        try:
            rows, distinct = check_other(cur, info['table'], info['key'])
            print(f'{ep:<28}{info["table"]:<36}{rows:>10}{distinct:>15}  ({info["key"]})')
        except Exception as e:
            conn.rollback()
            print(f'{ep:<28}{info["table"]:<36}  ERROR: {e}')

    print(f"\n{'='*100}")
    print(f"{'PREREQUISITE-GATED ENDPOINTS':^100}")
    print(f"{'='*100}")
    prereq_ok = table_exists(cur, PLAYER_DASH_PREREQ_TABLE)
    for ep in PLAYER_DASH_ENDPOINTS:
        status = 'READY' if prereq_ok else f'BLOCKED — needs {PLAYER_DASH_PREREQ_TABLE}'
        print(f'  {ep}: {status}')

    conn.close()


if __name__ == '__main__':
    main()
