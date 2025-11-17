#!/usr/bin/env python3
"""
immich_redis_jobs.py

Usage examples:
  # Dry-run to see what would be touched
  python immich_redis_jobs.py --dry-run --purge-failed

  # Backup then actually purge failed jobs
  python immich_redis_jobs.py --backup --purge-failed

  # Force-clean active jobs by moving them to failed (safer)
  python immich_redis_jobs.py --backup --force-clean-active --move-to-failed

  # Force-clean active jobs by dropping them
  python immich_redis_jobs.py --backup --force-clean-active --drop-active

Environment:
  REDIS_URL or REDIS_HOST/REDIS_PORT/REDIS_DB
"""

import os
import argparse
import time
import json
from pathlib import Path
import redis

# ---------- Config / ENV ----------
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# ---------- Helpers ----------
if not REDIS_URL:
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

r = redis.from_url(REDIS_URL, decode_responses=False)

QUEUE_PATTERNS = [
    "*:failed",
    "*:active",
    "*:waiting",
    "*:wait",
    "*:paused",
    "*:delayed",
]

def scan_keys(pattern):
    cur = 0
    while True:
        cur, keys = r.scan(cur, match=pattern, count=5000)
        for k in keys:
            yield k
        if cur == 0:
            break

def get_job_ids(k):
    t = r.type(k).decode()
    if t == "list":
        return [x.decode() for x in r.lrange(k, 0, -1)]
    if t == "set":
        return [x.decode() for x in r.smembers(k)]
    if t == "zset":
        return [x.decode() for x in r.zrange(k, 0, -1)]
    return []

def delete_job(job_id, dry):
    # BullMQ job keys always contain the jobId
    for k in scan_keys(f"*{job_id}*"):
        if not dry:
            r.delete(k)

def clean_container(k, dry):
    ids = get_job_ids(k)
    if not ids:
        return

    print(f"[{k.decode()}] {len(ids)} jobs")

    if not dry:
        # Remove references
        t = r.type(k).decode()
        if t == "list":
            for jid in ids:
                r.lrem(k, 0, jid)
        elif t == "set":
            for jid in ids:
                r.srem(k, jid)
        elif t == "zset":
            for jid in ids:
                r.zrem(k, jid)

    # Delete job data
    for jid in ids:
        delete_job(jid, dry)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-failed", action="store_true")
    parser.add_argument("--clean-active", action="store_true")
    parser.add_argument("--clean-queued", action="store_true",
                        help="Clean jobs in waiting/wait/paused/delayed")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not (args.clean_failed or args.clean_active or args.clean_queued):
        raise SystemExit("Select at least one: --clean-failed --clean-active --clean-queued")

    print("[i] Connected to Redis:", REDIS_URL)

    for pattern in QUEUE_PATTERNS:
        for k in scan_keys(pattern):
            name = k.decode()

            is_failed = name.endswith(":failed")
            is_active = name.endswith(":active")
            is_queued = (
                name.endswith(":waiting") or
                name.endswith(":wait") or
                name.endswith(":paused") or
                name.endswith(":delayed")
            )

            if is_failed and args.clean_failed:
                clean_container(k, args.dry_run)

            if is_active and args.clean_active:
                clean_container(k, args.dry_run)

            if is_queued and args.clean_queued:
                clean_container(k, args.dry_run)

    print("\n[✓] Done. (dry-run)" if args.dry_run else "\n[✓] Completed.")


if __name__ == "__main__":
    main()
