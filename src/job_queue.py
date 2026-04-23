#!/usr/bin/env python3
"""
DB-based job queue for distributing NBA endpoint processing across VPS fleet.

Workers pull the next available job from the queue, process it, and mark it done.
No static assignment — idle workers automatically pick up remaining work.
"""

import psycopg2
import psycopg2.extras
import json
import os
import sys
import time
import subprocess
import socket
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def get_db_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'database_config.json')
    with open(config_path) as f:
        cfg = json.load(f)
    return {
        'host': cfg['host'],
        'dbname': cfg['name'],
        'user': cfg['user'],
        'password': cfg['password'],
        'port': int(cfg['port']),
        'sslmode': cfg.get('ssl_mode', 'require'),
        'connect_timeout': 15
    }


def get_connection():
    return psycopg2.connect(**get_db_config())


def init_queue_table():
    """Create the job_queue table if it doesn't exist."""
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_queue (
            id SERIAL PRIMARY KEY,
            endpoint_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            worker_ip TEXT,
            extra_args TEXT DEFAULT '',
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT now()
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_queue_status ON job_queue (status)
    """)
    conn.close()
    logger.info("Job queue table ready.")


def add_jobs(endpoints, extra_args=''):
    """Add endpoints to the queue. Skips duplicates that are pending/running with the same extra_args."""
    conn = get_connection()
    cur = conn.cursor()

    added = 0
    skipped = 0
    for ep in endpoints:
        # Dedup on (endpoint_name, extra_args) so shards of the same endpoint can coexist
        cur.execute(
            "SELECT id FROM job_queue WHERE endpoint_name = %s AND extra_args = %s AND status IN ('pending', 'running')",
            (ep, extra_args)
        )
        if cur.fetchone():
            skipped += 1
            continue
        cur.execute(
            "INSERT INTO job_queue (endpoint_name, extra_args) VALUES (%s, %s)",
            (ep, extra_args)
        )
        added += 1

    conn.commit()
    conn.close()
    logger.info(f"Added {added} jobs, skipped {skipped} (already queued/running).")


def build_season_shards(shard_count, start_year=1996, end_year=None):
    """
    Split NBA seasons from start_year..end_year into shard_count contiguous ranges.
    Returns list of (since_season, until_season) tuples in 'YYYY-YY' format.

    Example: shard_count=4, 1996-97..2025-26 (30 seasons) →
        [('1996-97','2003-04'), ('2004-05','2011-12'),
         ('2012-13','2019-20'), ('2020-21','2025-26')]
    """
    import math
    from datetime import datetime
    if end_year is None:
        now = datetime.now()
        # NBA season YYYY-YY starting in October of year Y
        end_year = now.year if now.month >= 10 else now.year - 1
    years = list(range(start_year, end_year + 1))
    if shard_count <= 1 or len(years) <= 1:
        season_of = lambda y: f"{y}-{str(y+1)[-2:]}"
        return [(season_of(years[0]), season_of(years[-1]))]
    chunk_size = math.ceil(len(years) / shard_count)
    shards = []
    for i in range(0, len(years), chunk_size):
        chunk = years[i:i + chunk_size]
        first, last = chunk[0], chunk[-1]
        shards.append((f"{first}-{str(first+1)[-2:]}", f"{last}-{str(last+1)[-2:]}"))
    return shards


def add_sharded_jobs(endpoint, shard_count, extra_args=''):
    """
    Create shard_count queue entries for a single endpoint, each covering a
    season range. extra_args is prepended to the shard's --since-season/--until-season.
    """
    shards = build_season_shards(shard_count)
    conn = get_connection()
    cur = conn.cursor()

    added = 0
    skipped = 0
    for since_s, until_s in shards:
        shard_args = f"--since-season {since_s} --until-season {until_s}"
        full_args = f"{extra_args} {shard_args}".strip()
        cur.execute(
            "SELECT id FROM job_queue WHERE endpoint_name = %s AND extra_args = %s AND status IN ('pending', 'running')",
            (endpoint, full_args)
        )
        if cur.fetchone():
            skipped += 1
            continue
        cur.execute(
            "INSERT INTO job_queue (endpoint_name, extra_args) VALUES (%s, %s)",
            (endpoint, full_args)
        )
        added += 1

    conn.commit()
    conn.close()
    logger.info(f"Sharded {endpoint} into {len(shards)} ranges: added {added}, skipped {skipped}.")


def claim_job(worker_ip):
    """
    Atomically claim the next pending job.
    Uses FOR UPDATE SKIP LOCKED so multiple workers don't grab the same job.
    Returns (job_id, endpoint_name, extra_args) or None.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE job_queue
        SET status = 'running', worker_ip = %s, started_at = now()
        WHERE id = (
            SELECT id FROM job_queue
            WHERE status = 'pending'
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, endpoint_name, extra_args
    """, (worker_ip,))

    row = cur.fetchone()
    conn.commit()
    conn.close()

    if row:
        return {'id': row[0], 'endpoint': row[1], 'extra_args': row[2] or ''}
    return None


def complete_job(job_id, success=True, error_message=None):
    """Mark a job as completed or failed."""
    conn = get_connection()
    cur = conn.cursor()
    status = 'completed' if success else 'failed'
    cur.execute(
        "UPDATE job_queue SET status = %s, completed_at = now(), error_message = %s WHERE id = %s",
        (status, error_message, job_id)
    )
    conn.commit()
    conn.close()


def reset_stale_jobs(timeout_hours=24):
    """Reset jobs that have been 'running' for too long (worker probably died)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE job_queue
        SET status = 'pending', worker_ip = NULL, started_at = NULL
        WHERE status = 'running'
        AND started_at < now() - interval '%s hours'
        RETURNING id, endpoint_name, worker_ip
    """, (timeout_hours,))
    reset = cur.fetchall()
    conn.commit()
    conn.close()
    if reset:
        for r in reset:
            logger.info(f"Reset stale job #{r[0]} ({r[1]}) from worker {r[2]}")
    return len(reset)


def get_queue_status():
    """Get current queue status summary."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT status, count(*) as cnt
        FROM job_queue
        GROUP BY status
        ORDER BY status
    """)
    summary = {row['status']: row['cnt'] for row in cur.fetchall()}

    cur.execute("""
        SELECT id, endpoint_name, status, worker_ip, extra_args,
               started_at, completed_at, error_message
        FROM job_queue
        ORDER BY id
    """)
    jobs = cur.fetchall()

    conn.close()
    return summary, jobs


def worker_loop(python_path, worker_ip, extra_process_args=None):
    """
    Main worker loop. Claims jobs from queue and processes them.
    Runs until no more pending jobs remain.
    """
    logger.info(f"Worker started on {worker_ip}")

    while True:
        job = claim_job(worker_ip)
        if not job:
            logger.info("No more pending jobs. Worker exiting.")
            break

        job_id = job['id']
        endpoint = job['endpoint']
        extra_args = job['extra_args']

        logger.info(f"Claimed job #{job_id}: {endpoint}")

        # Build command
        cmd = [
            python_path, 'src/nba_data_processor.py',
            '--single-endpoint', endpoint
        ]
        if extra_args:
            cmd.extend(extra_args.split())
        if extra_process_args:
            cmd.extend(extra_process_args)

        # Run the processor
        log_file = f"logs/{endpoint}.log"
        try:
            with open(log_file, 'a') as lf:
                result = subprocess.run(
                    cmd,
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    timeout=86400  # 24 hour max per endpoint
                )

            if result.returncode == 0:
                complete_job(job_id, success=True)
                logger.info(f"Job #{job_id} ({endpoint}) completed successfully.")
            else:
                complete_job(job_id, success=False,
                           error_message=f"Exit code {result.returncode}")
                logger.error(f"Job #{job_id} ({endpoint}) failed with exit code {result.returncode}")

        except subprocess.TimeoutExpired:
            complete_job(job_id, success=False, error_message="Timed out after 24 hours")
            logger.error(f"Job #{job_id} ({endpoint}) timed out")
        except Exception as e:
            complete_job(job_id, success=False, error_message=str(e)[:500])
            logger.error(f"Job #{job_id} ({endpoint}) error: {e}")

    logger.info("Worker done.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NBA Job Queue Manager')
    sub = parser.add_subparsers(dest='command', required=True)

    # init
    sub.add_parser('init', help='Create the queue table')

    # add
    add_p = sub.add_parser('add', help='Add endpoints to the queue')
    add_p.add_argument('endpoints', nargs='+', help='Endpoint names or "high_priority"')
    add_p.add_argument('--extra-args', default='', help='Extra args to pass to processor')
    add_p.add_argument('--auto-shard', action='store_true',
                       help='Split endpoints with shard_count>1 (from config) into multiple season-range jobs')

    # add-sharded (explicit single-endpoint sharding)
    shard_p = sub.add_parser('add-sharded', help='Add N season-range shards for a single endpoint')
    shard_p.add_argument('endpoint', help='Endpoint name to shard')
    shard_p.add_argument('--shards', type=int, help='Override shard count (defaults to config shard_count)')
    shard_p.add_argument('--extra-args', default='', help='Extra args to pass to processor (in addition to shard range)')

    # status
    sub.add_parser('status', help='Show queue status')

    # worker
    worker_p = sub.add_parser('worker', help='Run as a worker (pulls jobs from queue)')
    worker_p.add_argument('--python-path', default=sys.executable)
    worker_p.add_argument('--worker-ip', default=socket.gethostname())

    # reset
    reset_p = sub.add_parser('reset', help='Reset stale running jobs')
    reset_p.add_argument('--timeout-hours', type=int, default=24)

    args = parser.parse_args()

    if args.command == 'init':
        init_queue_table()

    elif args.command == 'add':
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'endpoint_config.json')
        with open(config_path) as f:
            ep_data = json.load(f)
        endpoints = args.endpoints
        if endpoints == ['high_priority']:
            endpoints = [
                name for name, cfg in ep_data['endpoints'].items()
                if cfg.get('priority') == 'high' and cfg.get('latest_version')
            ]
            logger.info(f"Resolved high_priority to {len(endpoints)} endpoints")

        if args.auto_shard:
            # Split endpoints with shard_count>1 via add_sharded_jobs, others go through add_jobs
            non_sharded = []
            for ep in endpoints:
                sc = ep_data['endpoints'].get(ep, {}).get('shard_count', 1)
                if sc and sc > 1:
                    add_sharded_jobs(ep, sc, args.extra_args)
                else:
                    non_sharded.append(ep)
            if non_sharded:
                add_jobs(non_sharded, args.extra_args)
        else:
            add_jobs(endpoints, args.extra_args)

    elif args.command == 'add-sharded':
        shard_count = args.shards
        if shard_count is None:
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'endpoint_config.json')
            with open(config_path) as f:
                ep_data = json.load(f)
            shard_count = ep_data['endpoints'].get(args.endpoint, {}).get('shard_count', 1)
        if shard_count <= 1:
            logger.error(f"Endpoint '{args.endpoint}' has no shard_count > 1 configured. Pass --shards N to override.")
            sys.exit(1)
        add_sharded_jobs(args.endpoint, shard_count, args.extra_args)

    elif args.command == 'status':
        import re
        summary, jobs = get_queue_status()
        print(f"\n{'='*60}")
        print(f"Queue Summary: {summary}")
        print(f"{'='*60}")
        for j in jobs:
            status_icon = {'pending': ' ', 'running': '>', 'completed': '+', 'failed': 'X'}
            icon = status_icon.get(j['status'], '?')
            worker = f" @ {j['worker_ip']}" if j['worker_ip'] else ""
            duration = ""
            if j['started_at'] and j['completed_at']:
                delta = j['completed_at'] - j['started_at']
                hours = delta.total_seconds() / 3600
                duration = f" ({hours:.1f}h)"
            elif j['started_at']:
                duration = f" (running since {j['started_at'].strftime('%m-%d %H:%M')})"
            error = f" ERR: {j['error_message'][:60]}" if j['error_message'] else ""
            # Render season shard range compactly if present
            name = j['endpoint_name']
            extra = j.get('extra_args') or ''
            since = re.search(r'--since-season\s+(\S+)', extra)
            until = re.search(r'--until-season\s+(\S+)', extra)
            if since or until:
                name = f"{name} [{since.group(1) if since else '...'}→{until.group(1) if until else '...'}]"
            print(f"  [{icon}] #{j['id']:2d} {name:<46s} {j['status']:<10s}{worker}{duration}{error}")
        print()

    elif args.command == 'reset':
        count = reset_stale_jobs(args.timeout_hours)
        print(f"Reset {count} stale jobs.")

    elif args.command == 'worker':
        worker_loop(args.python_path, args.worker_ip)
