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
    """Add endpoints to the queue. Skips duplicates that are pending/running."""
    conn = get_connection()
    cur = conn.cursor()

    added = 0
    skipped = 0
    for ep in endpoints:
        # Don't add if already pending or running
        cur.execute(
            "SELECT id FROM job_queue WHERE endpoint_name = %s AND status IN ('pending', 'running')",
            (ep,)
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
        endpoints = args.endpoints
        if endpoints == ['high_priority']:
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'endpoint_config.json')
            with open(config_path) as f:
                data = json.load(f)
            endpoints = [
                name for name, cfg in data['endpoints'].items()
                if cfg.get('priority') == 'high' and cfg.get('latest_version')
            ]
            logger.info(f"Resolved high_priority to {len(endpoints)} endpoints")
        add_jobs(endpoints, args.extra_args)

    elif args.command == 'status':
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
            print(f"  [{icon}] #{j['id']:2d} {j['endpoint_name']:<30s} {j['status']:<10s}{worker}{duration}{error}")
        print()

    elif args.command == 'reset':
        count = reset_stale_jobs(args.timeout_hours)
        print(f"Reset {count} stale jobs.")

    elif args.command == 'worker':
        worker_loop(args.python_path, args.worker_ip)
