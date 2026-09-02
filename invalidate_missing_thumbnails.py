#!/usr/bin/env python3
"""
invalidate_missing_thumbnails.py

Finds Immich assets in PostgreSQL where the database marks thumbnail(s) as existing
(e.g., resizePath is set), but the actual thumbnail file is missing or 0 bytes on disk.

It sets "resizePath" = NULL (and "thumbhash" = NULL) directly in the database.
After running this script, triggering the "Generate Thumbnails -> Missing" job in Immich
will automatically regenerate thumbnails for all missing assets without needing to
reprocess the entire library.

Usage:
  # Dry-run scan to check how many thumbnails are missing on disk
  python invalidate_missing_thumbnails.py --dry-run --upload-dir /path/to/immich-data

  # Invalidate missing thumbnails directly in Postgres
  python invalidate_missing_thumbnails.py --upload-dir /path/to/immich-data

  # Also check and invalidate missing encoded videos
  python invalidate_missing_thumbnails.py --upload-dir /path/to/immich-data --check-videos
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

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
        print("[!] Error: No PostgreSQL driver found. Please install psycopg2-binary or psycopg:")
        print("    pip install psycopg2-binary")
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


def resolve_disk_path(db_path: Optional[str], upload_dir: Path, container_prefix: str) -> Optional[Path]:
    """
    Resolves a database path (which might be relative, absolute container path, or absolute host path)
    to a local filesystem Path.
    """
    if not db_path:
        return None

    cleaned_path = db_path.strip()
    if not cleaned_path:
        return None

    # Check direct absolute path first
    direct_path = Path(cleaned_path)
    if direct_path.is_absolute() and direct_path.exists():
        return direct_path

    # Strip container prefix if present (e.g. /usr/src/app/upload/...)
    norm_container_prefix = container_prefix.rstrip("/") + "/"
    if cleaned_path.startswith(norm_container_prefix):
        relative_part = cleaned_path[len(norm_container_prefix):]
        return upload_dir / relative_part

    if cleaned_path.startswith("/"):
        # If absolute but doesn't exist directly, try stripping leading slash and joining to upload_dir
        rel_candidate = upload_dir / cleaned_path.lstrip("/")
        if rel_candidate.exists():
            return rel_candidate
        # Fallback to upload_dir / basename or relative subpaths
        # Often Immich stores e.g. /usr/src/app/upload/thumbs/<userId>/<assetId>.webp
        if "thumbs/" in cleaned_path:
            rel_thumbs = cleaned_path[cleaned_path.index("thumbs/"):]
            return upload_dir / rel_thumbs
        if "encoded-video/" in cleaned_path:
            rel_enc = cleaned_path[cleaned_path.index("encoded-video/"):]
            return upload_dir / rel_enc
        return direct_path

    # Relative path (e.g. thumbs/uuid/xxx.webp or upload/thumbs/xxx.webp)
    if cleaned_path.startswith("upload/"):
        return upload_dir / cleaned_path[7:]

    return upload_dir / cleaned_path


def is_file_missing_or_empty(file_path: Optional[Path]) -> bool:
    """Returns True if the file does not exist or is 0 bytes."""
    if not file_path:
        return True
    try:
        if not file_path.exists() or not file_path.is_file():
            return True
        if file_path.stat().st_size == 0:
            return True
        return False
    except OSError:
        return True


def get_existing_columns(conn, table_name: str) -> List[str]:
    """Inspects table columns in PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s
        """, (table_name,))
        return [row[0] for row in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(
        description="Invalidate thumbnails directly in PostgreSQL for Immich assets whose thumbnail files are missing on disk."
    )
    parser.add_argument(
        "-u", "--upload-dir",
        default=os.getenv("UPLOAD_LOCATION", "./upload"),
        help="Path to the Immich upload/library directory on host (default: UPLOAD_LOCATION env or ./upload)"
    )
    parser.add_argument(
        "--container-prefix",
        default="/usr/src/app/upload",
        help="Container upload prefix stored in database to map to --upload-dir (default: /usr/src/app/upload)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and report missing files without modifying the database."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for database update operations (default: 1000)"
    )
    parser.add_argument(
        "--check-videos",
        action="store_true",
        help="Also check and invalidate encoded video files if missing on disk."
    )
    parser.add_argument(
        "--table",
        default="assets",
        help="Asset table name in postgres (default: assets, auto-falls back to asset if not found)"
    )

    # Database connection parameters
    parser.add_argument("--db-url", help="PostgreSQL connection string (or DB_URL env)")
    parser.add_argument("--db-host", help="PostgreSQL host (default: localhost or DB_HOSTNAME env)")
    parser.add_argument("--db-port", type=int, help="PostgreSQL port (default: 5432 or DB_PORT env)")
    parser.add_argument("--db-user", help="PostgreSQL username (default: postgres or DB_USERNAME env)")
    parser.add_argument("--db-password", help="PostgreSQL password (or DB_PASSWORD env)")
    parser.add_argument("--db-name", help="PostgreSQL database name (default: immich or DB_DATABASE_NAME env)")

    args = parser.parse_args()

    upload_dir = Path(args.upload_dir).resolve()
    print("=" * 70)
    print(" Immich Missing Thumbnail Invalidator")
    print("=" * 70)
    print(f"[*] Upload Directory : {upload_dir}")
    print(f"[*] Dry-Run Mode     : {'YES (No DB changes will be made)' if args.dry_run else 'NO (DB will be updated)'}")
    print(f"[*] Check Encoded Vid: {'YES' if args.check_videos else 'NO'}")

    if not upload_dir.exists():
        print(f"\n[!] WARNING: Upload directory does not exist on disk: {upload_dir}")
        print("    Please ensure --upload-dir points to the correct Immich data/upload folder.")
        confirm = input("    Do you want to continue anyway? [y/N]: ").strip().lower()
        if confirm != "y":
            sys.exit(1)

    print("\n[*] Connecting to PostgreSQL database...")
    try:
        conn = get_db_connection(args)
    except Exception as e:
        print(f"[!] Database connection failed: {e}")
        sys.exit(1)

    table_name = args.table
    columns = get_existing_columns(conn, table_name)
    if not columns and table_name == "assets":
        # Check legacy table name
        legacy_columns = get_existing_columns(conn, "asset")
        if legacy_columns:
            table_name = "asset"
            columns = legacy_columns

    if not columns:
        print(f"[!] Table '{args.table}' (or 'asset') not found in database!")
        conn.close()
        sys.exit(1)

    print(f"[✓] Connected. Using table '{table_name}'.")

    has_resize_path = "resizePath" in columns
    has_thumbhash = "thumbhash" in columns
    has_webp_path = "webpPath" in columns
    has_encoded_video = "encodedVideoPath" in columns

    if not has_resize_path and not has_webp_path:
        print("[!] Neither 'resizePath' nor 'webpPath' column found in asset table.")
        conn.close()
        sys.exit(1)

    # Select columns to query
    select_fields = ["id"]
    if has_resize_path:
        select_fields.append('"resizePath"')
    if has_webp_path:
        select_fields.append('"webpPath"')
    if has_encoded_video:
        select_fields.append('"encodedVideoPath"')
    if "originalPath" in columns:
        select_fields.append('"originalPath"')

    query_cols = ", ".join(select_fields)
    
    # Query assets where thumbnail is marked as existing
    where_clauses = []
    if has_resize_path:
        where_clauses.append('"resizePath" IS NOT NULL')
    if has_webp_path:
        where_clauses.append('"webpPath" IS NOT NULL')
    if args.check_videos and has_encoded_video:
        where_clauses.append('"encodedVideoPath" IS NOT NULL')

    where_sql = " OR ".join(where_clauses)
    query = f'SELECT {query_cols} FROM "{table_name}" WHERE {where_sql};'

    print("[*] Fetching assets with registered thumbnails/videos from DB...")
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    total_assets = len(rows)
    print(f"[*] Found {total_assets} candidate assets in database to verify on disk.")

    missing_thumb_ids: List[str] = []
    missing_video_ids: List[str] = []
    valid_thumbs_count = 0
    missing_samples: List[Tuple[str, str, str]] = []

    # Map column positions
    col_idx = {name.strip('"'): idx for idx, name in enumerate(select_fields)}

    for row in rows:
        asset_id = str(row[col_idx["id"]])
        
        # Check thumbnail
        resize_path_val = row[col_idx["resizePath"]] if has_resize_path else None
        webp_path_val = row[col_idx["webpPath"]] if has_webp_path else None
        thumb_val = resize_path_val or webp_path_val

        if thumb_val:
            resolved_thumb = resolve_disk_path(thumb_val, upload_dir, args.container_prefix)
            if is_file_missing_or_empty(resolved_thumb):
                missing_thumb_ids.append(asset_id)
                if len(missing_samples) < 5:
                    missing_samples.append((asset_id, thumb_val, str(resolved_thumb)))
            else:
                valid_thumbs_count += 1

        # Check encoded video if requested
        if args.check_videos and has_encoded_video:
            video_val = row[col_idx["encodedVideoPath"]]
            if video_val:
                resolved_video = resolve_disk_path(video_val, upload_dir, args.container_prefix)
                if is_file_missing_or_empty(resolved_video):
                    missing_video_ids.append(asset_id)

    print("\n" + "-" * 70)
    print(" Scan Summary")
    print("-" * 70)
    print(f"  • Total DB records checked       : {total_assets}")
    print(f"  • Thumbnails valid on disk       : {valid_thumbs_count}")
    print(f"  • Thumbnails MISSING on disk     : {len(missing_thumb_ids)}")
    if args.check_videos and has_encoded_video:
        print(f"  • Encoded videos MISSING on disk : {len(missing_video_ids)}")
    print("-" * 70)

    if missing_samples:
        print("\n[i] Sample missing thumbnails detected:")
        for aid, db_p, resolved_p in missing_samples:
            print(f"  - Asset ID : {aid}")
            print(f"    DB Path  : {db_p}")
            print(f"    Disk Path: {resolved_p}\n")

    if not missing_thumb_ids and not missing_video_ids:
        print("[✓] All registered thumbnails exist on disk. Nothing to invalidate!")
        conn.close()
        return

    if args.dry_run:
        print("\n[✓] Dry-run finished. No changes made to the database.")
        print("    Run without --dry-run to apply invalidations.")
        conn.close()
        return

    # Apply database updates
    print("\n[*] Invalidating missing thumbnails in PostgreSQL...")
    batch_size = args.batch_size

    # 1. Invalidate thumbnails
    if missing_thumb_ids:
        set_statements = []
        if has_resize_path:
            set_statements.append('"resizePath" = NULL')
        if has_thumbhash:
            set_statements.append('"thumbhash" = NULL')
        if has_webp_path:
            set_statements.append('"webpPath" = NULL')

        set_clause = ", ".join(set_statements)
        update_thumb_sql = f'UPDATE "{table_name}" SET {set_clause} WHERE id = ANY(%s);'

        total_batches = (len(missing_thumb_ids) + batch_size - 1) // batch_size
        with conn.cursor() as cur:
            for i in range(0, len(missing_thumb_ids), batch_size):
                batch = missing_thumb_ids[i:i + batch_size]
                cur.execute(update_thumb_sql, (batch,))
                conn.commit()
                batch_num = (i // batch_size) + 1
                print(f"    [Batch {batch_num}/{total_batches}] Invalidated {len(batch)} asset thumbnail records.")

    # 2. Invalidate videos if requested
    if missing_video_ids:
        update_vid_sql = f'UPDATE "{table_name}" SET "encodedVideoPath" = NULL WHERE id = ANY(%s);'
        total_vid_batches = (len(missing_video_ids) + batch_size - 1) // batch_size
        with conn.cursor() as cur:
            for i in range(0, len(missing_video_ids), batch_size):
                batch = missing_video_ids[i:i + batch_size]
                cur.execute(update_vid_sql, (batch,))
                conn.commit()
                batch_num = (i // batch_size) + 1
                print(f"    [Video Batch {batch_num}/{total_vid_batches}] Invalidated {len(batch)} video records.")

    conn.close()
    print("\n" + "=" * 70)
    print(" [✓] Invalidation Completed Successfully!")
    print("=" * 70)
    print("Next steps:")
    print(" 1. Open your Immich Web UI.")
    print(" 2. Navigate to Administration -> Jobs.")
    print(" 3. Under 'Generate Thumbnails', click the 'Missing' button.")
    print("    Immich will now regenerate thumbnails ONLY for the assets that were missing!")
    print("=" * 70)


if __name__ == "__main__":
    main()
