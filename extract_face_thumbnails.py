#!/usr/bin/env python3
"""
extract_face_thumbnails.py

Queries all face thumbnail paths from PostgreSQL:
    SELECT "thumbnailPath" FROM public.person

Extracts the relative path after 'upload/thumbs/' and copies the files into
a local directory (defaults to './facesThumbnails'), preserving the folder structure.
"""

import os
import sys
import shutil
import argparse
from pathlib import Path
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg2
    PSYCOPG_VERSION = 2
except ImportError:
    try:
        import psycopg
        PSYCOPG_VERSION = 3
    except ImportError:
        PSYCOPG_VERSION = None


def get_db_connection(args: argparse.Namespace):
    """Establishes connection to PostgreSQL using CLI args or .env variables."""
    if PSYCOPG_VERSION is None:
        print("[!] Error: No PostgreSQL driver found. Run: pip install psycopg2-binary")
        sys.exit(1)

    db_url = args.db_url or os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if db_url:
        if PSYCOPG_VERSION == 2:
            return psycopg2.connect(db_url)
        return psycopg.connect(db_url)

    host = args.db_host or os.getenv("DB_HOSTNAME") or os.getenv("DB_HOST", "localhost")
    port = int(args.db_port or os.getenv("DB_PORT", 5432))
    user = args.db_user or os.getenv("DB_USERNAME") or os.getenv("DB_USER", "postgres")
    password = args.db_password or os.getenv("DB_PASSWORD", "postgres")
    dbname = args.db_name or os.getenv("DB_DATABASE_NAME") or os.getenv("DB_NAME", "immich")

    conn_kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname
    }

    if PSYCOPG_VERSION == 2:
        return psycopg2.connect(**conn_kwargs)
    return psycopg.connect(**conn_kwargs)


def fetch_thumbnail_paths(conn) -> List[str]:
    """Fetches all non-empty thumbnailPath entries from public.person."""
    with conn.cursor() as cur:
        # Try case-sensitive quoted "thumbnailPath" first, fall back to lowercase
        try:
            cur.execute("""
                SELECT "thumbnailPath"
                FROM public.person
                WHERE "thumbnailPath" IS NOT NULL AND "thumbnailPath" != '';
            """)
            rows = cur.fetchall()
            return [r[0] for r in rows if r[0]]
        except Exception:
            conn.rollback()
            cur.execute("""
                SELECT thumbnailpath
                FROM public.person
                WHERE thumbnailpath IS NOT NULL AND thumbnailpath != '';
            """)
            rows = cur.fetchall()
            return [r[0] for r in rows if r[0]]


def extract_relative_path(db_path: str) -> str:
    """
    Extracts the path after 'upload/thumbs/'.
    Falls back to 'thumbs/' or strips leading slashes if marker not found.
    """
    normalized = db_path.replace("\\", "/")
    marker = "upload/thumbs/"
    if marker in normalized:
        return normalized.split(marker, 1)[1]

    alt_marker = "thumbs/"
    if alt_marker in normalized:
        return normalized.split(alt_marker, 1)[1]

    return normalized.lstrip("/")


def locate_source_file(db_path: str, rel_path: str, upload_dir: Optional[Path]) -> Optional[Path]:
    """Finds the source file on disk."""
    direct_path = Path(db_path)
    if direct_path.is_file():
        return direct_path

    if upload_dir:
        candidate1 = upload_dir / "thumbs" / rel_path
        if candidate1.is_file():
            return candidate1

        candidate2 = upload_dir / rel_path
        if candidate2.is_file():
            return candidate2

        norm = db_path.replace("\\", "/").lstrip("/")
        candidate3 = upload_dir / norm
        if candidate3.is_file():
            return candidate3

    return None


def copy_single_file(src_path: Optional[Path], dest_path: Path, dry_run: bool, overwrite: bool) -> str:
    """Copies a single file and returns the status string."""
    if not src_path or not src_path.is_file():
        return "missing"

    if dest_path.is_file() and not overwrite:
        return "skipped"

    if dry_run:
        return "copied"

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dest_path)
    return "copied"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract and copy Immich face thumbnails from person.thumbnailPath preserving directory structure."
    )
    parser.add_argument(
        "--output-dir",
        default="./facesThumbnails",
        help="Target folder to copy thumbnails into (default: ./facesThumbnails)"
    )
    parser.add_argument(
        "--upload-dir",
        default=os.getenv("UPLOAD_LOCATION"),
        help="Path to Immich upload directory if DB paths differ from current mount"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files in destination directory"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the extraction and copy without writing files"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Number of parallel worker threads for copying (default: 8)"
    )

    # Database connection arguments
    parser.add_argument("--db-url", help="Full PostgreSQL connection URI")
    parser.add_argument("--db-host", help="PostgreSQL host (default: localhost or DB_HOSTNAME)")
    parser.add_argument("--db-port", type=int, help="PostgreSQL port (default: 5432 or DB_PORT)")
    parser.add_argument("--db-user", help="PostgreSQL username (default: postgres or DB_USERNAME)")
    parser.add_argument("--db-password", help="PostgreSQL password (default: postgres or DB_PASSWORD)")
    parser.add_argument("--db-name", help="PostgreSQL database name (default: immich or DB_DATABASE_NAME)")

    return parser.parse_args()


def main():
    args = parse_args()
    dest_base = Path(args.output_dir).resolve()
    upload_base = Path(args.upload_dir).resolve() if args.upload_dir else None

    print(f"Connecting to database...")
    try:
        conn = get_db_connection(args)
    except Exception as e:
        print(f"[!] Database connection failed: {e}")
        sys.exit(1)

    try:
        print("Querying public.person for thumbnailPath...")
        paths = fetch_thumbnail_paths(conn)
    finally:
        conn.close()

    total = len(paths)
    print(f"[✓] Found {total} person thumbnail path(s) in database.")
    if total == 0:
        return

    print(f"Destination: {dest_base}")
    if upload_base:
        print(f"Upload dir fallback: {upload_base}")
    if args.dry_run:
        print("[*] Running in DRY-RUN mode. No files will be copied.")

    copied_count = 0
    skipped_count = 0
    missing_count = 0
    error_count = 0

    items_to_copy = []
    for db_path in paths:
        rel_path = extract_relative_path(db_path)
        dest_path = dest_base / rel_path
        src_path = locate_source_file(db_path, rel_path, upload_base)
        items_to_copy.append((db_path, src_path, dest_path))

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(
                copy_single_file,
                src_path,
                dest_path,
                args.dry_run,
                args.overwrite
            ): (db_path, src_path, dest_path)
            for db_path, src_path, dest_path in items_to_copy
        }

        for future in as_completed(futures):
            db_path, src_path, dest_path = futures[future]
            try:
                status = future.result()
                if status == "copied":
                    copied_count += 1
                elif status == "skipped":
                    skipped_count += 1
                elif status == "missing":
                    missing_count += 1
                    if missing_count <= 5:
                        print(f"[-] Missing file on disk: {db_path}")
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"[!] Error copying {db_path} -> {dest_path}: {e}")

    print("\n--- Summary ---")
    print(f"Total entries:   {total}")
    print(f"Copied:          {copied_count}{' (dry-run)' if args.dry_run else ''}")
    print(f"Skipped (exist): {skipped_count}")
    print(f"Missing on disk: {missing_count}")
    if error_count:
        print(f"Errors:          {error_count}")
    print(f"Output directory: {dest_base}")


if __name__ == "__main__":
    main()
