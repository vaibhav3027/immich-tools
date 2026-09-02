#!/usr/bin/env python3
"""
invalidate_missing_thumbnails.py

Finds Immich assets in PostgreSQL where the database marks thumbnail(s) as existing,
but the actual thumbnail file is missing or 0 bytes on disk.

It sets thumbnail/preview path columns and "thumbhash" to NULL directly in the database.
After running this script, triggering the "Generate Thumbnails -> Missing" job in Immich
will automatically regenerate thumbnails for all missing assets without needing to
reprocess the entire library.

Usage:
  # Dry-run scan to check how many thumbnails are missing on disk
  python invalidate_missing_thumbnails.py --dry-run --upload-dir /path/to/immich-data

  # Invalidate missing thumbnails directly in Postgres
  python invalidate_missing_thumbnails.py --upload-dir /path/to/immich-data

  # Inspect database schema if you want to see all detected tables & columns
  python invalidate_missing_thumbnails.py --show-schema
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Set

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


def get_all_tables_and_columns(conn) -> Dict[str, List[str]]:
    """Inspects all tables and columns in the public schema."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            ORDER BY table_name, ordinal_position;
        """)
        tables: Dict[str, List[str]] = {}
        for table, col in cur.fetchall():
            tables.setdefault(table, []).append(col)
        return tables


def discover_asset_schema(tables: Dict[str, List[str]], requested_table: Optional[str] = None):
    """
    Discovers the asset table and relevant columns for thumbnails, previews, and thumbhash.
    Supports camelCase, snake_case, lowercase, and various Immich schema versions.
    """
    # 1. Determine target table
    candidate_tables = [requested_table] if requested_table else []
    candidate_tables += ["assets", "asset", "asset_files", "asset_file"]

    matched_table = None
    for cand in candidate_tables:
        if cand and cand in tables:
            matched_table = cand
            break

    if not matched_table:
        # Search for any table starting with asset
        for t in tables:
            if t.lower().startswith("asset"):
                matched_table = t
                break

    if not matched_table:
        return None, {}

    cols = tables[matched_table]
    col_map_lower = {c.lower(): c for c in cols}

    # Helper to find column by multiple possible names
    def find_col(*aliases) -> Optional[str]:
        for a in aliases:
            if a.lower() in col_map_lower:
                return col_map_lower[a.lower()]
        return None

    discovered = {
        "id": find_col("id", "assetId", "asset_id"),
        "resizePath": find_col("resizePath", "resize_path", "resizepath"),
        "thumbnailPath": find_col("thumbnailPath", "thumbnail_path", "thumbnailpath"),
        "previewPath": find_col("previewPath", "preview_path", "previewpath"),
        "webpPath": find_col("webpPath", "webp_path", "webppath"),
        "thumbhash": find_col("thumbhash", "thumb_hash"),
        "encodedVideoPath": find_col("encodedVideoPath", "encoded_video_path", "encodedvideopath"),
        "originalPath": find_col("originalPath", "original_path", "originalpath"),
    }

    return matched_table, discovered


def resolve_disk_path(db_path: Optional[str], upload_dir: Path, container_prefix: str) -> Optional[Path]:
    """
    Resolves a database path (which might be relative, absolute container path, or absolute host path)
    to a local filesystem Path.
    """
    if not db_path:
        return None

    cleaned_path = str(db_path).strip()
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
        
        # Check standard Immich folders
        for folder in ["thumbs/", "encoded-video/", "upload/", "library/"]:
            if folder in cleaned_path:
                rel_sub = cleaned_path[cleaned_path.index(folder):]
                cand = upload_dir / rel_sub
                if cand.exists():
                    return cand
                return cand
        return direct_path

    # Relative path
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
        help="Asset table name in postgres (default: auto-detect)"
    )
    parser.add_argument(
        "--show-schema",
        action="store_true",
        help="Display all tables and columns found in the connected database and exit."
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

    print("\n[*] Connecting to PostgreSQL database...")
    try:
        conn = get_db_connection(args)
    except Exception as e:
        print(f"[!] Database connection failed: {e}")
        sys.exit(1)

    tables = get_all_tables_and_columns(conn)

    if args.show_schema:
        print("\n--- Detected Tables and Columns in Database ---")
        for tbl, cols in sorted(tables.items()):
            print(f"Table: {tbl}")
            print(f"  Columns: {', '.join(cols)}")
        conn.close()
        return

    table_name, discovered_cols = discover_asset_schema(tables, args.table)

    if not table_name:
        print("[!] Could not find an asset table in the database.")
        print("\nAvailable tables in database:")
        for t in sorted(tables.keys()):
            print(f"  - {t}")
        print("\nPlease specify the correct table using --table <name>.")
        conn.close()
        sys.exit(1)

    print(f"[✓] Connected. Found asset table: '{table_name}'.")
    print(f"[*] Upload Directory : {upload_dir}")
    print(f"[*] Dry-Run Mode     : {'YES (No DB changes will be made)' if args.dry_run else 'NO (DB will be updated)'}")
    print(f"[*] Check Encoded Vid: {'YES' if args.check_videos else 'NO'}")

    id_col = discovered_cols.get("id")
    if not id_col:
        print(f"[!] Could not find an ID column in table '{table_name}'.")
        print(f"    Available columns: {', '.join(tables[table_name])}")
        conn.close()
        sys.exit(1)

    # Collect all thumbnail/preview path columns present
    thumb_path_cols = []
    for key in ["resizePath", "thumbnailPath", "previewPath", "webpPath"]:
        col = discovered_cols.get(key)
        if col and col not in thumb_path_cols:
            thumb_path_cols.append(col)

    thumbhash_col = discovered_cols.get("thumbhash")
    video_col = discovered_cols.get("encodedVideoPath") if args.check_videos else None
    orig_path_col = discovered_cols.get("originalPath")

    print("\n[*] Detected Schema Columns:")
    print(f"  • ID Column           : {id_col}")
    print(f"  • Thumbnail Path(s)   : {', '.join(thumb_path_cols) if thumb_path_cols else 'None found'}")
    print(f"  • Thumbhash Column    : {thumbhash_col or 'None'}")
    if args.check_videos:
        print(f"  • Encoded Video Column: {video_col or 'None'}")

    if not thumb_path_cols:
        print(f"\n[!] No thumbnail or preview path columns found in table '{table_name}'.")
        print(f"    Available columns in '{table_name}':")
        for c in tables[table_name]:
            print(f"      - {c}")
        print("\nIf thumbnails are stored in another table, check with --show-schema and use --table.")
        conn.close()
        sys.exit(1)

    if not upload_dir.exists():
        print(f"\n[!] WARNING: Upload directory does not exist on disk: {upload_dir}")
        print("    Please ensure --upload-dir points to the correct Immich data/upload folder.")
        confirm = input("    Do you want to continue anyway? [y/N]: ").strip().lower()
        if confirm != "y":
            conn.close()
            sys.exit(1)

    # Build query to fetch assets with any thumbnail path set
    query_cols = [f'"{id_col}"']
    for col in thumb_path_cols:
        query_cols.append(f'"{col}"')
    if thumbhash_col:
        query_cols.append(f'"{thumbhash_col}"')
    if video_col:
        query_cols.append(f'"{video_col}"')
    if orig_path_col:
        query_cols.append(f'"{orig_path_col}"')

    where_clauses = [f'"{c}" IS NOT NULL' for c in thumb_path_cols]
    if thumbhash_col:
        where_clauses.append(f'"{thumbhash_col}" IS NOT NULL')
    if video_col:
        where_clauses.append(f'"{video_col}" IS NOT NULL')

    where_sql = " OR ".join(where_clauses)
    query = f'SELECT {", ".join(query_cols)} FROM "{table_name}" WHERE {where_sql};'

    print("\n[*] Fetching assets with registered thumbnails from database...")
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    total_assets = len(rows)
    print(f"[*] Found {total_assets} candidate assets in database to verify on disk.")

    missing_thumb_ids: List[Any] = []
    missing_video_ids: List[Any] = []
    valid_thumbs_count = 0
    missing_samples: List[Tuple[str, str, str]] = []

    # Map column index
    raw_col_names = [c.strip('"') for c in query_cols]
    col_idx = {name: idx for idx, name in enumerate(raw_col_names)}

    for row in rows:
        asset_id = row[col_idx[id_col]]

        # Check all thumbnail path columns for this asset
        is_missing = False
        sample_db_p = ""
        sample_resolved_p = ""

        # Check if asset has thumbnail paths set
        has_any_thumb_set = False
        for c in thumb_path_cols:
            p_val = row[col_idx[c]]
            if p_val:
                has_any_thumb_set = True
                resolved = resolve_disk_path(p_val, upload_dir, args.container_prefix)
                if is_file_missing_or_empty(resolved):
                    is_missing = True
                    sample_db_p = str(p_val)
                    sample_resolved_p = str(resolved)
                    break

        if has_any_thumb_set:
            if is_missing:
                missing_thumb_ids.append(asset_id)
                if len(missing_samples) < 5:
                    missing_samples.append((str(asset_id), sample_db_p, sample_resolved_p))
            else:
                valid_thumbs_count += 1
        elif thumbhash_col and row[col_idx[thumbhash_col]] is not None:
            # Asset has thumbhash but no thumbnail file path
            missing_thumb_ids.append(asset_id)

        # Check video if requested
        if video_col:
            v_val = row[col_idx[video_col]]
            if v_val:
                resolved_v = resolve_disk_path(v_val, upload_dir, args.container_prefix)
                if is_file_missing_or_empty(resolved_v):
                    missing_video_ids.append(asset_id)

    print("\n" + "-" * 70)
    print(" Scan Summary")
    print("-" * 70)
    print(f"  • Total DB records checked       : {total_assets}")
    print(f"  • Thumbnails valid on disk       : {valid_thumbs_count}")
    print(f"  • Thumbnails MISSING on disk     : {len(missing_thumb_ids)}")
    if video_col:
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
        set_statements = [f'"{c}" = NULL' for c in thumb_path_cols]
        if thumbhash_col:
            set_statements.append(f'"{thumbhash_col}" = NULL')

        set_clause = ", ".join(set_statements)
        update_thumb_sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{id_col}" = ANY(%s);'

        total_batches = (len(missing_thumb_ids) + batch_size - 1) // batch_size
        with conn.cursor() as cur:
            for i in range(0, len(missing_thumb_ids), batch_size):
                batch = missing_thumb_ids[i:i + batch_size]
                cur.execute(update_thumb_sql, (batch,))
                conn.commit()
                batch_num = (i // batch_size) + 1
                print(f"    [Batch {batch_num}/{total_batches}] Invalidated {len(batch)} asset thumbnail records.")

    # 2. Invalidate videos if requested
    if missing_video_ids and video_col:
        update_vid_sql = f'UPDATE "{table_name}" SET "{video_col}" = NULL WHERE "{id_col}" = ANY(%s);'
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
