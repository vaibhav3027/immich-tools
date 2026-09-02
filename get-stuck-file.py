#!/usr/bin/env python3

import json
import redis
import psycopg2

# ---------------- CONFIG ----------------
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "immich"
POSTGRES_USER = "vaibhav"
POSTGRES_PASSWORD = ""
# ---------------------------------------

def main():
    # Redis connection
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )

    # Get active job IDs
    job_ids = r.lrange("immich_bull:thumbnailGeneration:active", 0, -1)

    if not job_ids:
        print("No active jobs found")
        return

    # Postgres connection
    pg = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = pg.cursor()

    for job_id in job_ids:
        job_key = f"immich_bull:thumbnailGeneration:{job_id}"
        job_data = r.hgetall(job_key)

        if not job_data or "data" not in job_data:
            continue

        # Extract asset UUID
        try:
            data_json = json.loads(job_data["data"])
            asset_id = data_json.get("id")
        except json.JSONDecodeError:
            continue

        if not asset_id:
            continue

        # Query asset path
        cursor.execute(
            """
            SELECT id, "originalPath"
            FROM asset
            WHERE id = %s;
            """,
            (asset_id,)
        )

        row = cursor.fetchone()
        if row:
            print(f"Job {job_id} -> Asset {row[0]} -> Path: {row[1]}")
        else:
            print(f"Job {job_id} -> Asset {asset_id} not found")

    cursor.close()
    pg.close()


if __name__ == "__main__":
    main()

