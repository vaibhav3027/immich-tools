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


def discover_schema(tables: Dict[str, List[str]], requested_table: Optional[str] = None):
    """
    Discovers the asset table, asset_file table (if present in modern Immich),
    and all relevant columns for thumbnails, previews, and thumbhash.
    Supports camelCase, snake_case, lowercase, and all Immich schema versions.
    """
    # Helper to find column by multiple possible names
    def find_col(cols: List[str], *aliases) -> Optional[str]:
        col_map_lower = {c.lower(): c for c in cols}
        for a in aliases:
            if a.lower() in col_map_lower:
                return col_map_lower[a.lower()]
        return None

    # 1. Detect main asset table
    candidate_asset_tables = [requested_table] if requested_table else []
    candidate_asset_tables += ["asset", "assets"]
    matched_asset_table = None
    for cand in candidate_asset_tables:
        if cand and cand in tables:
            matched_asset_table = cand
            break

    if not matched_asset_table:
        for t in tables:
            if t.lower() in ["asset", "assets"]:
                matched_asset_table = t
                break

    if not matched_asset_table:
        return None, None, {}

    asset_cols = tables[matched_asset_table]
    asset_col_map = {
        "id": find_col(asset_cols, "id", "assetId", "asset_id"),
        "thumbhash": find_col(asset_cols, "thumbhash", "thumb_hash"),
        "originalPath": find_col(asset_cols, "originalPath", "original_path", "originalpath"),
        # Legacy path columns on asset table
        "resizePath": find_col(asset_cols, "resizePath", "resize_path", "resizepath"),
        "webpPath": find_col(asset_cols, "webpPath", "webp_path", "webppath"),
        "thumbnailPath": find_col(asset_cols, "thumbnailPath", "thumbnail_path", "thumbnailpath"),
        "previewPath": find_col(asset_cols, "previewPath", "preview_path", "previewpath"),
        "encodedVideoPath": find_col(asset_cols, "encodedVideoPath", "encoded_video_path", "encodedvideopath"),
    }

    # 2. Detect asset_file table (Modern Immich schema)
    matched_file_table = None
    candidate_file_tables = ["asset_file", "asset_files", "assetfile", "assetfiles"]
    for cand in candidate_file_tables:
        if cand in tables:
            matched_file_table = cand
            break

    file_col_map = {}
    if matched_file_table:
        file_cols = tables[matched_file_table]
        file_col_map = {
            "id": find_col(file_cols, "id", "fileId", "file_id"),
            "assetId": find_col(file_cols, "assetId", "asset_id", "assetid"),
            "type": find_col(file_cols, "type", "fileType", "file_type"),
            "path": find_col(file_cols, "path", "filePath", "file_path"),
            "isEdited": find_col(file_cols, "isEdited", "is_edited", "isedited"),
        }

    return matched_asset_table, matched_file_table, {
        "asset": asset_col_map,
        "asset_file": file_col_map,
    }


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

    asset_tbl, file_tbl, discovered_cols = discover_schema(tables, args.table)

    if not asset_tbl:
        print("[!] Could not find an asset table in the database.")
        print("\nAvailable tables in database:")
        for t in sorted(tables.keys()):
            print(f"  - {t}")
        print("\nPlease specify the correct table using --table <name>.")
        conn.close()
        sys.exit(1)

    asset_col_map = discovered_cols["asset"]
    file_col_map = discovered_cols.get("asset_file", {})
    id_col = asset_col_map.get("id")
    thumbhash_col = asset_col_map.get("thumbhash")

    if not id_col:
        print(f"[!] Could not find an ID column in table '{asset_tbl}'.")
        print(f"    Available columns: {', '.join(tables[asset_tbl])}")
        conn.close()
        sys.exit(1)

    is_modern_schema = bool(
        file_tbl
        and file_col_map.get("id")
        and file_col_map.get("assetId")
        and file_col_map.get("type")
        and file_col_map.get("path")
    )

    print(f"[✓] Connected to PostgreSQL.")
    print(f"[*] Main Asset Table : '{asset_tbl}'")
    if is_modern_schema:
        print(f"[*] Asset File Table : '{file_tbl}' (Modern Immich schema detected)")
    else:
        print(f"[*] Asset File Table : None (Legacy Immich schema detected)")
    print(f"[*] Upload Directory : {upload_dir}")
    print(f"[*] Dry-Run Mode     : {'YES (No DB changes will be made)' if args.dry_run else 'NO (DB will be updated)'}")
    print(f"[*] Check Encoded Vid: {'YES' if args.check_videos else 'NO'}")

    print("\n[*] Detected Schema Columns:")
    print(f"  • Asset Table ID      : {id_col}")
    print(f"  • Thumbhash Column    : {thumbhash_col or 'None'}")
    if is_modern_schema:
        print(f"  • Asset File Table ID : {file_col_map['id']}")
        print(f"  • Asset File Asset ID : {file_col_map['assetId']}")
        print(f"  • Asset File Type Col : {file_col_map['type']}")
        print(f"  • Asset File Path Col : {file_col_map['path']}")
    else:
        legacy_path_cols = [
            c for k, c in asset_col_map.items()
            if k in ["resizePath", "webpPath", "thumbnailPath", "previewPath"] and c
        ]
        print(f"  • Thumbnail Path(s)   : {', '.join(legacy_path_cols) if legacy_path_cols else 'None found'}")
        if args.check_videos:
            print(f"  • Encoded Video Column: {asset_col_map.get('encodedVideoPath') or 'None'}")

    if not upload_dir.exists():
        print(f"\n[!] WARNING: Upload directory does not exist on disk: {upload_dir}")
        print("    Please ensure --upload-dir points to the correct Immich data/upload folder.")
        confirm = input("    Do you want to continue anyway? [y/N]: ").strip().lower()
        if confirm != "y":
            conn.close()
            sys.exit(1)

    missing_file_ids: List[Any] = []
    missing_thumb_asset_ids: Set[Any] = set()
    missing_video_asset_ids: Set[Any] = set()
    missing_video_file_ids: List[Any] = []
    valid_thumbs_count = 0
    missing_samples: List[Tuple[str, str, str, str]] = []

    if is_modern_schema:
        # Modern Immich: Query asset_file table
        f_id = file_col_map["id"]
        f_asset_id = file_col_map["assetId"]
        f_type = file_col_map["type"]
        f_path = file_col_map["path"]

        target_types = ["thumbnail", "preview", "fullsize"]
        if args.check_videos:
            target_types.append("encoded_video")

        query = f"""
            SELECT "{f_id}", "{f_asset_id}", "{f_type}", "{f_path}"
            FROM "{file_tbl}"
            WHERE "{f_type}" = ANY(%s) AND "{f_path}" IS NOT NULL;
        """
        print("\n[*] Fetching thumbnail & preview file records from asset_file table...")
        with conn.cursor() as cur:
            cur.execute(query, (target_types,))
            rows = cur.fetchall()

        total_records = len(rows)
        print(f"[*] Found {total_records} file records in database to verify on disk.")

        for row in rows:
            file_id, asset_id, file_type, db_path = row[0], row[1], str(row[2]).lower(), row[3]
            resolved = resolve_disk_path(db_path, upload_dir, args.container_prefix)
            if is_file_missing_or_empty(resolved):
                if file_type == "encoded_video":
                    missing_video_file_ids.append(file_id)
                    missing_video_asset_ids.add(asset_id)
                else:
                    missing_file_ids.append(file_id)
                    missing_thumb_asset_ids.add(asset_id)

                if len(missing_samples) < 5:
                    missing_samples.append((str(asset_id), file_type, str(db_path), str(resolved)))
            else:
                if file_type != "encoded_video":
                    valid_thumbs_count += 1

    else:
        # Legacy Immich: Query legacy columns on assets table
        legacy_path_cols = [
            c for k, c in asset_col_map.items()
            if k in ["resizePath", "webpPath", "thumbnailPath", "previewPath"] and c
        ]
        video_col = asset_col_map.get("encodedVideoPath") if args.check_videos else None

        if not legacy_path_cols and not thumbhash_col:
            print(f"\n[!] No thumbnail, preview, or thumbhash columns found in table '{asset_tbl}'.")
            print(f"    Available columns in '{asset_tbl}': {', '.join(tables[asset_tbl])}")
            conn.close()
            sys.exit(1)

        query_cols = [f'"{id_col}"']
        for col in legacy_path_cols:
            query_cols.append(f'"{col}"')
        if thumbhash_col:
            query_cols.append(f'"{thumbhash_col}"')
        if video_col:
            query_cols.append(f'"{video_col}"')

        where_clauses = [f'"{c}" IS NOT NULL' for c in legacy_path_cols]
        if thumbhash_col:
            where_clauses.append(f'"{thumbhash_col}" IS NOT NULL')
        if video_col:
            where_clauses.append(f'"{video_col}" IS NOT NULL')

        where_sql = " OR ".join(where_clauses) if where_clauses else "1=1"
        query = f'SELECT {", ".join(query_cols)} FROM "{asset_tbl}" WHERE {where_sql};'

        print("\n[*] Fetching legacy thumbnail records from asset table...")
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        total_records = len(rows)
        print(f"[*] Found {total_records} candidate assets in database to verify on disk.")

        raw_col_names = [c.strip('"') for c in query_cols]
        col_idx = {name: idx for idx, name in enumerate(raw_col_names)}

        for row in rows:
            asset_id = row[col_idx[id_col]]
            is_missing = False
            sample_type = "thumbnail"
            sample_db_p = ""
            sample_resolved_p = ""

            has_any_thumb_set = False
            for c in legacy_path_cols:
                p_val = row[col_idx[c]]
                if p_val:
                    has_any_thumb_set = True
                    resolved = resolve_disk_path(p_val, upload_dir, args.container_prefix)
                    if is_file_missing_or_empty(resolved):
                        is_missing = True
                        sample_type = c
                        sample_db_p = str(p_val)
                        sample_resolved_p = str(resolved)
                        break

            if has_any_thumb_set:
                if is_missing:
                    missing_thumb_asset_ids.add(asset_id)
                    if len(missing_samples) < 5:
                        missing_samples.append((str(asset_id), sample_type, sample_db_p, sample_resolved_p))
                else:
                    valid_thumbs_count += 1
            elif thumbhash_col and row[col_idx[thumbhash_col]] is not None:
                missing_thumb_asset_ids.add(asset_id)

            if video_col:
                v_val = row[col_idx[video_col]]
                if v_val:
                    resolved_v = resolve_disk_path(v_val, upload_dir, args.container_prefix)
                    if is_file_missing_or_empty(resolved_v):
                        missing_video_asset_ids.add(asset_id)

    print("\n" + "-" * 70)
    print(" Scan Summary")
    print("-" * 70)
    print(f"  • Total DB records checked       : {total_records}")
    print(f"  • Thumbnails valid on disk       : {valid_thumbs_count}")
    if is_modern_schema:
        print(f"  • Missing asset_file records     : {len(missing_file_ids)}")
        print(f"  • Unique assets affected         : {len(missing_thumb_asset_ids)}")
        if args.check_videos:
            print(f"  • Missing encoded video files    : {len(missing_video_file_ids)}")
    else:
        print(f"  • Thumbnails MISSING on disk     : {len(missing_thumb_asset_ids)}")
        if args.check_videos:
            print(f"  • Encoded videos MISSING on disk : {len(missing_video_asset_ids)}")
    print("-" * 70)

    if missing_samples:
        print("\n[i] Sample missing thumbnails detected:")
        for aid, ftype, db_p, resolved_p in missing_samples:
            print(f"  - Asset ID : {aid}")
            print(f"    Type     : {ftype}")
            print(f"    DB Path  : {db_p}")
            print(f"    Disk Path: {resolved_p}\n")

    if not missing_file_ids and not missing_thumb_asset_ids and not missing_video_asset_ids:
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

    if is_modern_schema:
        # 1. Delete missing thumbnail rows from asset_file table
        if missing_file_ids:
            f_id = file_col_map["id"]
            delete_file_sql = f'DELETE FROM "{file_tbl}" WHERE "{f_id}" = ANY(%s::uuid[]);'
            total_batches = (len(missing_file_ids) + batch_size - 1) // batch_size
            with conn.cursor() as cur:
                for i in range(0, len(missing_file_ids), batch_size):
                    batch = [str(x) for x in missing_file_ids[i:i + batch_size]]
                    cur.execute(delete_file_sql, (batch,))
                    conn.commit()
                    batch_num = (i // batch_size) + 1
                    print(f"    [Batch {batch_num}/{total_batches}] Deleted {len(batch)} missing asset_file records.")

        # 2. Reset thumbhash to NULL in asset table so Immich Missing job detects them
        if missing_thumb_asset_ids and thumbhash_col:
            thumb_asset_list = list(missing_thumb_asset_ids)
            update_thumbhash_sql = f'UPDATE "{asset_tbl}" SET "{thumbhash_col}" = NULL WHERE "{id_col}" = ANY(%s::uuid[]);'
            total_batches = (len(thumb_asset_list) + batch_size - 1) // batch_size
            with conn.cursor() as cur:
                for i in range(0, len(thumb_asset_list), batch_size):
                    batch = [str(x) for x in thumb_asset_list[i:i + batch_size]]
                    cur.execute(update_thumbhash_sql, (batch,))
                    conn.commit()
                    batch_num = (i // batch_size) + 1
                    print(f"    [Thumbhash Batch {batch_num}/{total_batches}] Reset thumbhash to NULL for {len(batch)} assets.")

        # 3. Handle video files if requested
        if missing_video_file_ids:
            f_id = file_col_map["id"]
            delete_vid_sql = f'DELETE FROM "{file_tbl}" WHERE "{f_id}" = ANY(%s::uuid[]);'
            total_vid_batches = (len(missing_video_file_ids) + batch_size - 1) // batch_size
            with conn.cursor() as cur:
                for i in range(0, len(missing_video_file_ids), batch_size):
                    batch = [str(x) for x in missing_video_file_ids[i:i + batch_size]]
                    cur.execute(delete_vid_sql, (batch,))
                    conn.commit()
                    batch_num = (i // batch_size) + 1
                    print(f"    [Video Batch {batch_num}/{total_vid_batches}] Deleted {len(batch)} video asset_file records.")

    else:
        # Legacy Immich: Set legacy columns to NULL on asset table
        legacy_thumb_cols = [
            c for k, c in asset_col_map.items()
            if k in ["resizePath", "webpPath", "thumbnailPath", "previewPath"] and c
        ]
        if thumbhash_col:
            legacy_thumb_cols.append(thumbhash_col)

        thumb_asset_list = list(missing_thumb_asset_ids)
        if thumb_asset_list and legacy_thumb_cols:
            set_statements = [f'"{c}" = NULL' for c in legacy_thumb_cols]
            set_clause = ", ".join(set_statements)
            update_thumb_sql = f'UPDATE "{asset_tbl}" SET {set_clause} WHERE "{id_col}" = ANY(%s::uuid[]);'

            total_batches = (len(thumb_asset_list) + batch_size - 1) // batch_size
            with conn.cursor() as cur:
                for i in range(0, len(thumb_asset_list), batch_size):
                    batch = [str(x) for x in thumb_asset_list[i:i + batch_size]]
                    cur.execute(update_thumb_sql, (batch,))
                    conn.commit()
                    batch_num = (i // batch_size) + 1
                    print(f"    [Batch {batch_num}/{total_batches}] Invalidated {len(batch)} asset thumbnail records.")

        video_col = asset_col_map.get("encodedVideoPath")
        video_asset_list = list(missing_video_asset_ids)
        if video_asset_list and video_col:
            update_vid_sql = f'UPDATE "{asset_tbl}" SET "{video_col}" = NULL WHERE "{id_col}" = ANY(%s::uuid[]);'
            total_vid_batches = (len(video_asset_list) + batch_size - 1) // batch_size
            with conn.cursor() as cur:
                for i in range(0, len(video_asset_list), batch_size):
                    batch = [str(x) for x in video_asset_list[i:i + batch_size]]
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

