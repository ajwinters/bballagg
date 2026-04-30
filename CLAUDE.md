# NBA Data Aggregation Pipeline

This file is the cross-session handoff doc. Read it first. When design or state changes, update it before ending the session.

## Purpose

Scrape historical + current NBA stats from `stats.nba.com` into a central Postgres DB (`thebigone` on RDS). Two-phase pipeline: build a **master reference universe** first, then populate per-endpoint data tables against that universe.

## Two-phase flow

**Phase 1 — Master tables (built by 3 "master-builder" endpoints):**

| Endpoint           | Writes to            | What it holds                                |
|--------------------|----------------------|----------------------------------------------|
| `LeagueGameFinder` | `master_nba_games`   | Every game across seasons/types (the universe for per-game endpoints) |
| `CommonAllPlayers` | `master_nba_players` | Every player, used for per-player endpoints  |
| `CommonTeamYears`  | `master_nba_teams`   | Every team across seasons                    |

These **do not** create a `nba_<endpoint>_*` table. They feed the master tables directly. If you see one marked "completed" in the job queue with no endpoint table — that's correct, not a bug.

**Phase 2 — Endpoint data tables**, populated by iterating over the master tables. Split into categories below.

## Endpoint categories (22 high-priority)

**Per-game (13)** — one call per `gameid` from `master_nba_games`. These are the bulk of the work.
- `BoxScoreAdvancedV3`, `BoxScoreDefensiveV2`, `BoxScoreFourFactorsV3`, `BoxScoreHustleV2`, `BoxScoreMatchupsV3`, `BoxScoreMiscV3`, `BoxScorePlayerTrackV3`, `BoxScoreScoringV3`, `BoxScoreSummaryV2`, `BoxScoreTraditionalV3`, `BoxScoreUsageV3`, `HustleStatsBoxScore`, `PlayByPlayV3`
- Tables: `nba_<endpoint>_a`, `_b`, `_c`, ... — each shard corresponds to one NBA API resultset (e.g., PlayerStats vs TeamStats). Use `_a` as the completeness indicator.

**Other per-key (3)** — populated but not per-game.
- `LeagueSeasonMatchups` (per season), `CommonTeamRoster` (per team × season), `TeamGameLogs` (per team × season).

**Prerequisite-gated (2)** — `PlayerDashPtReb`, `PlayerDashPtShots`. Require `nba_playergamelogs_a` (not yet in DB). `PlayerGameLogs` was promoted to high priority on 2026-04-28 to unblock these on the next run.

**Master-builders (3)** — see Phase 1 above.

## Infrastructure

- **Local dev machine** (Windows, `c:\Users\ajwin\Projects\Personal\NBA\thebigone`). Primary dev environment and **the orchestrator** for fleet deploys/dispatch as of 2026-04-28 (winters-dev EC2 retired). Local SSHes directly to each VPS using `~/.ssh/nba_vps`; an `~/.ssh/config` block matches the 8 IPs to that key + `User root`.
- **8 Vultr VPS machines** (see [deploy/vps_config.json](deploy/vps_config.json)). Non-cloud IPs that the NBA API doesn't block. Each runs endpoints sequentially with ~1.8s sleep between API calls.
- **RDS Postgres** (`nba-rds-instance...amazonaws.com`, db `thebigone`, user `ajwin`). Credentials in [config/database_config.json](config/database_config.json). Both local machine and VPS fleet hit RDS directly; only the VPS fleet can reach `stats.nba.com`.

## Key files

- [src/nba_data_processor.py](src/nba_data_processor.py) — main pipeline: API calls, retries, DB inserts, missing-id derivation.
- [src/endpoint_processor.py](src/endpoint_processor.py) — per-endpoint orchestration, error classification (permanent vs retryable).
- [src/database_manager.py](src/database_manager.py) — DB cursor, inserts, cleanup, reconnection.
- [src/rds_connection_manager.py](src/rds_connection_manager.py) — connection pooling, sleep/wake detection.
- [src/parameter_resolver.py](src/parameter_resolver.py) — builds param lists by reading master tables.
- [src/job_queue.py](src/job_queue.py) — job_queue table CLI (`status`, `init`).
- [src/validate_completeness.py](src/validate_completeness.py) — completeness report. **Authoritative state check — don't trust the queue alone.**
- [config/endpoint_config.json](config/endpoint_config.json) — 136 endpoints, priority, required params, shard hints.
- [config/database_config.json](config/database_config.json) — RDS creds.
- [deploy/vps_config.json](deploy/vps_config.json) — VPS IPs, user, python path, remote dir.
- [deploy/deploy.sh](deploy/deploy.sh) — rsync src/+config/ to all VPSes.
- [deploy/run_jobs.sh](deploy/run_jobs.sh) / [deploy/run_queue.sh](deploy/run_queue.sh) — dispatch work.
- [deploy/monitor.sh](deploy/monitor.sh) — fleet status. See monitor quirk below.

## Schema

- Data tables: `nba_<endpoint_lowercase>_<a|b|c|...>`. Shards correspond to NBA API resultsets. Each row carries a `data_collected_date` timestamp.
- Master tables: `master_nba_games` (gameid + gamedate + 80k rows — home+away so 2× distinct gameids), `master_nba_players`, `master_nba_teams`.
- Job queue: `job_queue` table tracks endpoint dispatch status (`completed`, `failed`, error messages).
- **Sort views** `vw_nba_<table>` — join each data table to master_nba_games gamedate and order DESC. For DBeaver browsing convenience. Read-only; edits still go via the base tables.
- **Failure tracking is intentionally absent** (cleaned up 2026-04-28). There are no `_failed_data` tables and no `failedreason` column. Re-runs derive work from `master_nba_games` minus the data table; that's the only mechanism. See "Retry behavior" below.

## Retry behavior

When a per-game API call fails, the processor logs and moves on. Nothing is recorded about the failure. On the next run, `_get_missing_game_ids` recomputes the set difference (master gameids − data gameids) and retries everything still missing.

Consequence: gameids that always permanently fail (e.g., MatchupsV3 / DefensiveV2 pre-tracking-era games) are re-attempted on every run. They fail fast (permanent error → in-call classifier returns immediately) but the API call still happens. If this becomes wasteful, the right fix is a per-(endpoint, gameid) skip list, not a resurrection of `_failed_data`.

## Known issues / quirks

1. **`monitor.sh` RUNNING status is a false positive** when no process is actually running. `pgrep -f 'nba_data_processor'` matches the bash command line that invokes it (because `'nba_data_processor'` is in the script text). Check `ps -eo cmd | grep python` on a VPS to see the truth.
2. **`PlayerGameLogs` is an unscraped prerequisite.** Blocks `PlayerDashPtReb` and `PlayerDashPtShots`. To unblock, add `PlayerGameLogs` to the run and let it populate `nba_playergamelogs_a`.
3. **Empty-body API responses** on `HustleStatsBoxScore` and `BoxScoreUsageV3` produce `Expecting value: line 1 column 1 (char 0)` failures. Distributed across all eras, not era-specific. Root cause not yet confirmed (API change vs. param issue vs. IP throttling) — live curl tests from VPSes currently time out, investigation open.
4. **`python3` shims on Windows** point at a Microsoft Store stub that errors. Deploy scripts use `command -v python || command -v python3`; if you write new shell scripts on Windows, use `python` not `python3`.
5. **Windows Python emits CRLF.** When a deploy script does `IPS=$($PY -c '...')`, every line except the last ends with `\r`, which becomes part of the variable. All deploy scripts pipe through `tr -d '\r'` to strip it. Don't drop that pipe — symptom is `ssh: hostname contains invalid characters` for all but the last IP.
6. **No rsync on Windows.** [deploy/deploy.sh](deploy/deploy.sh) uses `tar | ssh` instead. The remote `src/` and `config/` are wiped and repopulated each deploy; equivalent to rsync's `--delete` semantics.
7. **Worker launch needs `ssh -f` + parallel + `</dev/null`.** Sequential `ssh "...nohup ... &"` from local hits "Connection reset by peer" because the detached process keeps fds open; combined with `set -e` the loop dies on the first VPS. [deploy/run_queue.sh](deploy/run_queue.sh) now backgrounds each ssh, uses `ssh -f`, and redirects stdin from `/dev/null`.
8. **`PYTHONIOENCODING=utf-8` is mandatory in deploy scripts on Windows.** Sharded job names contain `→` which crashes Windows Python 3.12's default cp1252 stdout encoding. Set in [deploy/run_queue.sh](deploy/run_queue.sh) at the top.
9. **PlayerGameLogs writes to `nba_playergamelogs_a` (not `_playergamelogs`).** The PlayerDash prereq lookup in [src/nba_data_processor.py](src/nba_data_processor.py) was hardcoded to the old name; updated to `_a` on 2026-04-30.

## How to check state

**Completeness (authoritative):**
```bash
python src/validate_completeness.py
```
Prints master-builder row counts, per-game missing/retry counts, other-endpoint row counts, and prerequisite gates.

**Fleet activity:**
```bash
bash deploy/monitor.sh
```
Ignore the RUNNING flag (false positive — see quirk 1). Look at the queue summary and log mtimes.

**Job queue:**
```bash
python src/job_queue.py status
```

## Current state (2026-04-30)

- **Phase 2 bulk dispatch in progress**, started 2026-04-30 ~12:15 UTC. 63 jobs total (per-game endpoints sharded 4 ways via `--auto-shard`, plus 9 non-sharded). 8 workers running on the fleet (one per VPS). ETA roughly 6-12 hours of fleet time.
- Phases 1A (master refresh) and 1B (PlayerGameLogs, wrote 837k rows to `nba_playergamelogs_a`) completed earlier today. PlayerDashPtReb / PlayerDashPtShots are now READY (prereq satisfied).
- Master tables refreshed: gamedates current through 2026-04-27. Master tables are *stacking* — `master_nba_games` now has 160k rows for 40,060 distinct gameids; expected, all our queries DISTINCT-project.
- 35 `vw_nba_*` views recreated over the data tables only (after `_failed_data` cleanup).
- Code path simplified earlier 2026-04-28: `create_failed_records`, `cleanup_failed_records`, `get_failed_parameter_combinations` removed; `add_metadata_columns` no longer injects `failed_reason`.

## Session handoff guidance

- When any of: design changes, a new quirk is discovered, a workflow is added, an infrastructure piece moves — **update this file** before ending the session.
- Before trusting a "completed" status anywhere, run `validate_completeness.py`. The queue lies.
- Local has direct SSH to all 8 VPSes via `~/.ssh/nba_vps`. No jump host needed.
- Write new memory records to the [../memory/](../memory/) directory, not to this file.
