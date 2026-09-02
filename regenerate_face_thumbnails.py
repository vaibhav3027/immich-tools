#!/usr/bin/env python3
"""
regenerate_face_thumbnails.py

Fetches all people/faces in Immich and forcefully invalidates and regenerates
their face thumbnails.

Workflow:
  1. Connects to PostgreSQL (or Immich API) to fetch all persons.
  2. Clears "thumbnailPath" in the person table for all (or missing) face thumbnails.
  3. Triggers the Immich thumbnail generation job via the REST API (POST /api/jobs).

Usage:
  # Dry-run scan to inspect how many faces exist
  python regenerate_face_thumbnails.py --dry-run

  # Forcefully invalidate ALL face thumbnails and trigger regeneration
  python regenerate_face_thumbnails.py --force-all --trigger-job -y

  # Only invalidate face thumbnails whose files are missing on disk
  python regenerate_face_thumbnails.py --missing-only --upload-dir /path/to/immich-data --trigger-job

  # API-only mode (if DB is not directly accessible)
  python regenerate_face_thumbnails.py --api-only --trigger-job
"""

import os
import sys
import argparse
import json
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Set

try:
    import requests
except ImportError:
    requests = None

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# PostgreSQL driver support (supports psycopg2 or psycopg3)
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG_VERSION = 2
except ImportError:
    try:
        import psycopg
        PSYCOPG_VERSION = 3
    except ImportError:
        PSYCOPG_VERSION = None


def get_db_connection(args: argparse.Namespace):
    """Establishes connection to the PostgreSQL database."""
    if PSYCOPG_VERSION is None:
        return None

    db_url = args.db_url or os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if db_url:
        try:
            if PSYCOPG_VERSION == 2:
                return psycopg2.connect(db_url)
            return psycopg.connect(db_url)
        except Exception as e:
            if not args.api_only:
                print(f"[!] Warning: Failed to connect using DB_URL: {e}")
            return None

    host = args.db_host or os.getenv("DB_HOSTNAME") or os.getenv("DB_HOST", "localhost")
    port = int(args.db_port or os.getenv("DB_PORT", 5432))
    user = args.db_user or os.getenv("DB_USERNAME") or os.getenv("DB_USER", "postgres")
    password = args.db_password or os.getenv("DB_PASSWORD", "postgres")
    dbname = args.db_name or os.getenv("DB_DATABASE_NAME") or os.getenv("DB_NAME", "immich")

    try:
        if PSYCOPG_VERSION == 2:
            return psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname
            )
        else:
            return psycopg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname
            )
    except Exception as e:
        if not args.api_only:
            print(f"[!] Warning: Could not connect to PostgreSQL ({host}:{port}): {e}")
        return None


def resolve_disk_path(db_path: str, upload_dir: Path, container_prefix: str = "/usr/src/app/upload") -> Path:
    """Translates a database path to an absolute path on host."""
    p_str = str(db_path).replace("\\", "/")
    norm_prefix = container_prefix.rstrip("/")

    if p_str.startswith(norm_prefix + "/"):
        rel_path = p_str[len(norm_prefix) + 1:]
    elif p_str.startswith("/upload/"):
        rel_path = p_str[8:]
    elif p_str.startswith("upload/"):
        rel_path = p_str[7:]
    else:
        rel_path = p_str.lstrip("/")

    return upload_dir / rel_path


def trigger_immich_job(immich_url: str, api_key: str, force: bool = False) -> bool:
    """Triggers the thumbnail generation job via Immich API."""
    if not requests:
        print("[!] Error: 'requests' package not installed. Run: pip install requests")
        return False

    url = immich_url.rstrip("/")
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # Try modern endpoint: POST /api/jobs
    job_endpoints = [
        (f"{url}/api/jobs", "POST", {"name": "generate-thumbnails", "command": "start", "force": force}),
        (f"{url}/api/jobs/generate-thumbnails", "PUT", {"command": "start", "force": force}),
        (f"{url}/api/jobs/thumbnailGeneration", "PUT", {"command": "start", "force": force})
    ]

    for ep_url, method, payload in job_endpoints:
        try:
            if method == "POST":
                resp = requests.post(ep_url, headers=headers, json=payload, timeout=10)
            else:
                resp = requests.put(ep_url, headers=headers, json=payload, timeout=10)

            if resp.status_code in (200, 201, 204):
                print(f"[✓] Successfully triggered thumbnail job via {ep_url}")
                return True
        except Exception:
            continue

    print("[!] Failed to trigger thumbnail generation job automatically via API.")
    print("    You can trigger it manually in Immich Web UI: Administration -> Jobs -> Generate Thumbnails")
    return False


def fetch_people_api(immich_url: str, api_key: str) -> List[Dict[str, Any]]:
    """Fetches all people via Immich REST API."""
    if not requests:
        raise SystemExit("[!] 'requests' module required for API access: pip install requests")

    url = f"{immich_url.rstrip('/')}/api/people?withHidden=true"
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "people" in data:
            return data["people"]
        elif isinstance(data, list):
            return data
        return []
    except Exception as e:
        print(f"[!] Error fetching people from Immich API: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Forcefully regenerate Immich face/person thumbnails.")
    parser.add_argument("--force-all", action="store_true", default=True,
                        help="Forcefully invalidate thumbnails for all persons with faces (default).")
    parser.add_argument("--missing-only", action="store_true",
                        help="Only invalidate person thumbnails that are missing or 0 bytes on disk.")
    parser.add_argument("--upload-dir", type=str, default=None,
                        help="Path to Immich upload directory on host (e.g., UPLOAD_LOCATION).")
    parser.add_argument("--container-prefix", type=str, default="/usr/src/app/upload",
                        help="Container upload mount prefix (default: /usr/src/app/upload).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate and print affected persons without updating DB.")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Batch size for database updates (default: 500).")
    parser.add_argument("--trigger-job", action="store_true", default=True,
                        help="Automatically trigger 'generate-thumbnails' job via Immich API after invalidation.")
    parser.add_argument("--no-trigger-job", dest="trigger_job", action="store_false",
                        help="Do not trigger Immich job via API.")
    parser.add_argument("--api-only", action="store_true",
                        help="Run purely via Immich API without touching PostgreSQL directly.")
    parser.add_argument("-y", "--yes", action="store_true",
                        help="Skip confirmation prompts.")

    # Immich API Options
    parser.add_argument("--immich-url", type=str, default=None, help="Immich base URL (e.g. http://localhost:2283)")
    parser.add_argument("--api-key", type=str, default=None, help="Immich API Key")

    # DB options
    parser.add_argument("--db-url", type=str, default=None, help="Full PostgreSQL connection URL")
    parser.add_argument("--db-host", type=str, default=None, help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=None, help="PostgreSQL port")
    parser.add_argument("--db-user", type=str, default=None, help="PostgreSQL user")
    parser.add_argument("--db-password", type=str, default=None, help="PostgreSQL password")
    parser.add_argument("--db-name", type=str, default=None, help="PostgreSQL database name")

    args = parser.parse_args()

    immich_url = args.immich_url or os.getenv("IMMICH_URL") or "http://localhost:2283"
    api_key = args.api_key or os.getenv("IMMICH_KEY") or os.getenv("IMMICH_API_KEY")

    upload_dir_raw = args.upload_dir or os.getenv("UPLOAD_LOCATION") or os.getenv("IMMICH_UPLOAD_LOCATION")
    upload_dir = Path(upload_dir_raw).resolve() if upload_dir_raw else None

    if args.missing_only and not upload_dir:
        print("[!] Error: --missing-only requires --upload-dir or UPLOAD_LOCATION environment variable.")
        sys.exit(1)

    print("=" * 70)
    print(" Immich Face Thumbnail Invalidation & Force Regeneration")
    print("=" * 70)
    if args.dry_run:
        print(" [!] DRY-RUN MODE ENABLED: No database updates will be applied.")

    conn = None if args.api_only else get_db_connection(args)

    if conn is None:
        if not api_key:
            print("[!] Neither PostgreSQL connection nor IMMICH_KEY could be established.")
            print("    Please provide DB credentials or IMMICH_KEY in .env / arguments.")
            sys.exit(1)

        print("[*] Running in API Mode (using Immich REST API)...")
        people = fetch_people_api(immich_url, api_key)
        print(f"[*] Found {len(people)} total person records via API.")

        if args.missing_only:
            target_people = []
            for p in people:
                th_path = p.get("thumbnailPath")
                if not th_path:
                    target_people.append(p)
                elif upload_dir:
                    disk_p = resolve_disk_path(th_path, upload_dir, args.container_prefix)
                    if not disk_p.exists() or disk_p.stat().st_size == 0:
                        target_people.append(p)
            print(f"[*] {len(target_people)} persons have missing/empty thumbnails on disk.")
        else:
            target_people = people
            print(f"[*] Targeting all {len(target_people)} persons for face thumbnail regeneration.")

        if args.dry_run:
            print("\n[✓] Dry run complete. Exiting.")
            return

        if args.trigger_job:
            print("\n[*] Triggering thumbnail generation job via Immich API...")
            trigger_immich_job(immich_url, api_key, force=True)
            print("\n[✓] Regeneration triggered via API.")
        return

    # Direct Database Mode
    print("[*] Successfully connected to PostgreSQL database.")

    # Find table names (case insensitive)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        existing_tables = {row[0].lower(): row[0] for row in cur.fetchall()}

    person_table = existing_tables.get("person") or existing_tables.get("people") or "person"

    # Query all persons
    query = f"""
        SELECT "id", "name", "thumbnailPath"
        FROM "{person_table}";
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    total_persons = len(rows)
    print(f"[*] Found {total_persons} person records in '{person_table}' table.")

    if total_persons == 0:
        print("[✓] No person records found in database.")
        conn.close()
        return

    target_ids: List[str] = []
    missing_samples: List[Tuple[str, str, str]] = []

    for row in rows:
        pid, name, th_path = str(row[0]), row[1] or "(unnamed)", row[2]

        if args.missing_only and upload_dir:
            if not th_path or str(th_path).strip() == "":
                target_ids.append(pid)
                if len(missing_samples) < 5:
                    missing_samples.append((pid, name, "None"))
            else:
                disk_p = resolve_disk_path(th_path, upload_dir, args.container_prefix)
                if not disk_p.exists() or disk_p.stat().st_size == 0:
                    target_ids.append(pid)
                    if len(missing_samples) < 5:
                        missing_samples.append((pid, name, str(disk_p)))
        else:
            # Force all persons that currently have a thumbnail or need one
            target_ids.append(pid)

    print("\n" + "-" * 70)
    print(" Scan Summary")
    print("-" * 70)
    print(f"  • Total person records       : {total_persons}")
    print(f"  • Face thumbnails to reset   : {len(target_ids)}")
    print("-" * 70)

    if missing_samples:
        print("\n[i] Sample targets:")
        for pid, name, path_info in missing_samples:
            print(f"  - Person: {name} (ID: {pid}) -> {path_info}")

    if not target_ids:
        print("\n[✓] No face thumbnails need invalidation.")
        conn.close()
        return

    if args.dry_run:
        print("\n[✓] Dry-run finished. No changes made to the database.")
        conn.close()
        return

    if not args.yes:
        print(f"\n[!] About to reset 'thumbnailPath' to empty for {len(target_ids)} persons in PostgreSQL.")
        confirm = input("    Proceed? [y/N]: ").strip().lower()
        if confirm != "y":
            print("[*] Aborted by user.")
            conn.close()
            return

    # Invalidate in PostgreSQL
    print(f"\n[*] Resetting thumbnailPath in table '{person_table}'...")
    update_sql = f'UPDATE "{person_table}" SET "thumbnailPath" = \'\' WHERE "id" = ANY(%s::uuid[]);'
    batch_size = args.batch_size
    total_batches = (len(target_ids) + batch_size - 1) // batch_size

    with conn.cursor() as cur:
        for i in range(0, len(target_ids), batch_size):
            batch = [str(x) for x in target_ids[i:i + batch_size]]
            cur.execute(update_sql, (batch,))
            conn.commit()
            batch_num = (i // batch_size) + 1
            print(f"    [Batch {batch_num}/{total_batches}] Reset {len(batch)} person thumbnails.")

    conn.close()
    print("\n[✓] Database invalidation complete!")

    # Trigger Immich Job
    if args.trigger_job:
        if api_key:
            print("\n[*] Triggering Immich Generate Thumbnails job via API...")
            trigger_immich_job(immich_url, api_key, force=False)
        else:
            print("\n[i] IMMICH_KEY not found in environment. Next steps:")
            print(" 1. Open your Immich Web UI.")
            print(" 2. Go to Administration -> Jobs.")
            print(" 3. Under 'Generate Thumbnails', click 'Missing'.")
            print("    Immich will regenerate all face thumbnails immediately.")

    print("\n" + "=" * 70)
    print(" [✓] Done!")
    print("=" * 70)


if __name__ == "__main__":
    main()
