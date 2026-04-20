# NBA Data Aggregation Pipeline

## Architecture

- **EC2 instance** (us-east-1): Orchestration hub — deploys code, launches jobs, monitors progress. Cannot call NBA API directly (AWS IPs blocked).
- **8 Vultr VPS machines** (~$5/mo each): API callers using non-cloud IPs. Each runs endpoints sequentially with 1.8s sleep between API calls to respect rate limits.
- **RDS PostgreSQL** (thebigone): Central data store. Connection config in `config/database_config.json`.

## Key Files

- `src/nba_data_processor.py` — Main processing logic. Handles API calls, retries, DB inserts, failed record tracking.
- `config/endpoint_config.json` — Defines all 136 endpoints with priority levels (high/medium/low/null) and required params.
- `config/database_config.json` — RDS connection details.
- `deploy/deploy.sh` — Syncs `src/`, `config/`, `requirements.txt` to all 8 VPS into `/opt/bballagg/` (with `src/` and `config/` subdirectories).
- `deploy/run_jobs.sh` — Distributes endpoints round-robin across VPS, launches via nohup.
- `deploy/monitor.sh` — Checks process status and runner logs across fleet.
- `deploy/vps_config.json` — VPS IPs, user (root), python path (/opt/nba/bin/python3), remote dir (/opt/bballagg).

## VPS Fleet

| VPS | IP |
|-----|----|
| 1 | 149.28.58.190 |
| 2 | 149.28.52.224 |
| 3 | 45.63.12.123 |
| 4 | 149.28.48.95 |
| 5 | 108.61.78.21 |
| 6 | 149.28.59.153 |
| 7 | 45.77.219.9 |
| 8 | 64.176.223.92 |

## Deployment Pattern

1. Run `deploy/deploy.sh` to sync code to all VPS
2. Create runner.sh scripts for each VPS with assigned endpoints
3. `scp` scripts to VPS (heredoc delivery is unreliable)
4. Launch with `nohup bash runner.sh > logs/runner.log 2>&1 &`

## Database

- **56 tables**: 3 master tables + 53 high-priority endpoint tables (data + failed_data)
- Non-priority tables from old test pulls were cleaned up (2026-04-09)
- Table naming: `nba_{endpoint_lowercase}_{a,b,c,...}` for data, `nba_{endpoint_lowercase}_failed_data` for failures
- Master tables: `master_nba_games`, `master_nba_players`, `master_nba_teams`
- Only high-priority endpoints (21 total) are actively collected

## Error Handling

- **Permanent errors** (skip immediately, no retry): `list index out of range`, `NoneType object has no attribute`, `expecting value`, invalid IDs, 400/401/403
- These typically mean the stat type doesn't exist for older games (e.g., MatchupsV3 pre-2015, DefensiveV2 pre-tracking era)
- **Timeout errors**: Retryable, ~5-8% background failure rate is normal API flakiness
- After successful insert, `cleanup_failed_records()` deletes matching failed rows to prevent duplicates

## Priorities

- **High (21 endpoints)**: Box scores (AdvancedV3, DefensiveV2, FourFactorsV3, HustleV2, MatchupsV3, MiscV3, PlayerTrackV3, ScoringV3, SummaryV2, TraditionalV3, UsageV3), HustleStatsBoxScore, PlayByPlayV3, LeagueSeasonMatchups, CommonAllPlayers, CommonTeamRoster, CommonTeamYears, LeagueGameFinder, PlayerDashPtReb, PlayerDashPtShots, TeamGameLogs
- **Medium/Low/Null**: Not actively collected yet
