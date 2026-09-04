#!/usr/bin/env python3
"""
refetch_missing_video_metadata.py

Searches all videos in Immich, checks for missing metadata (duration, EXIF record,
FPS, resolution, capture date), and checks if encoded video and thumbnail are available.
If any one of these is not available, triggers metadata refetch/update for those
video assets via the Immich API.

Usage:
  # Dry-run scan to check which videos are missing metadata, encoded video, or thumbnail
  python refetch_missing_video_metadata.py --dry-run

  # Scan and trigger metadata update via API
  python refetch_missing_video_metadata.py -y

  # Scan purely via Immich REST API without connecting to PostgreSQL
  python refetch_missing_video_metadata.py --api-only -y

  # Only check for missing encoded video or thumbnail
  python refetch_missing_video_metadata.py --no-check-fps --no-check-resolution --dry-run
"""

import os
import sys
import argparse
from typing import Optional, List, Dict, Any, Tuple

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import requests
except ImportError:
    requests = None

# PostgreSQL driver support (psycopg2 or psycopg3)
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


def is_duration_empty(duration: Any) -> bool:
    """Checks if a duration value is null, zero, or unpopulated."""
    if duration is None:
        return True
    dur_str = str(duration).strip()
    if not dur_str:
        return True
    # Common zero duration representations: 0, 0:00:00, 0:00:00.000000, 00:00:00
    zero_patterns = {"0", "0.0", "0:00:00", "00:00:00", "0:00:00.000000", "00:00:00.000000"}
    if dur_str in zero_patterns:
        return True
    try:
        if float(dur_str) == 0.0:
            return True
    except ValueError:
        pass
    return False


def get_db_connection(args: argparse.Namespace):
    """Establishes connection to the PostgreSQL database if available."""
    if PSYCOPG_VERSION is None:
        return None

    db_url = args.db_url or os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    host = args.db_host or os.getenv("DB_HOSTNAME") or os.getenv("DB_HOST", "localhost")
    port = int(args.db_port or os.getenv("DB_PORT", 5432))
    user = args.db_user or os.getenv("DB_USERNAME") or os.getenv("DB_USER", "postgres")
    password = args.db_password or os.getenv("DB_PASSWORD", "postgres")
    dbname = args.db_name or os.getenv("DB_DATABASE_NAME") or os.getenv("DB_NAME", "immich")

    try:
        if db_url:
            if PSYCOPG_VERSION == 2:
                return psycopg2.connect(db_url, connect_timeout=5)
            return psycopg.connect(db_url, timeout=5)
        if PSYCOPG_VERSION == 2:
            return psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname,
                connect_timeout=5
            )
        return psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            timeout=5
        )
    except Exception as e:
        if not args.api_only and not args.quiet:
            print(f"[!] PostgreSQL connection unavailable ({e}). Falling back to Immich API.")
        return None


def get_all_tables_and_columns(conn) -> Dict[str, List[str]]:
    """Inspects all tables and columns in public schema."""
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


def discover_video_schema(tables: Dict[str, List[str]]):
    """Discovers asset, asset_file, and exif table and column names."""
    def find_col(cols: List[str], *aliases) -> Optional[str]:
        col_map_lower = {c.lower(): c for c in cols}
        for a in aliases:
            if a.lower() in col_map_lower:
                return col_map_lower[a.lower()]
        return None

    # 1. Main asset table
    asset_table = None
    for cand in ["asset", "assets"]:
        if cand in tables:
            asset_table = cand
            break

    # 2. Exif table
    exif_table = None
    for cand in ["asset_exif", "exif", "asset_exifs"]:
        if cand in tables:
            exif_table = cand
            break

    # 3. Asset file table (modern Immich schema)
    file_table = None
    for cand in ["asset_file", "asset_files", "assetfile", "assetfiles"]:
        if cand in tables:
            file_table = cand
            break

    if not asset_table:
        return None, None, None, {}, {}, {}

    asset_cols = tables[asset_table]
    asset_map = {
        "id": find_col(asset_cols, "id", "assetId", "asset_id"),
        "type": find_col(asset_cols, "type", "assetType", "asset_type"),
        "duration": find_col(asset_cols, "duration"),
        "originalPath": find_col(asset_cols, "originalPath", "original_path", "originalpath"),
        "thumbhash": find_col(asset_cols, "thumbhash", "thumb_hash"),
        "thumbnailPath": find_col(asset_cols, "thumbnailPath", "thumbnail_path", "thumbnailpath", "resizePath", "resize_path", "previewPath", "preview_path"),
        "encodedVideoPath": find_col(asset_cols, "encodedVideoPath", "encoded_video_path", "encodedvideopath"),
        "isArchived": find_col(asset_cols, "isArchived", "is_archived"),
        "isTrashed": find_col(asset_cols, "isTrashed", "is_trashed"),
        "deletedAt": find_col(asset_cols, "deletedAt", "deleted_at")
    }

    exif_map = {}
    if exif_table:
        exif_cols = tables[exif_table]
        exif_map = {
            "assetId": find_col(exif_cols, "assetId", "asset_id", "assetid"),
            "fps": find_col(exif_cols, "fps"),
            "width": find_col(exif_cols, "exifImageWidth", "exif_image_width", "width"),
            "height": find_col(exif_cols, "exifImageHeight", "exif_image_height", "height"),
            "dateTimeOriginal": find_col(exif_cols, "dateTimeOriginal", "date_time_original", "dateTime"),
            "videoCodec": find_col(exif_cols, "videoCodec", "video_codec")
        }

    file_map = {}
    if file_table:
        f_cols = tables[file_table]
        file_map = {
            "id": find_col(f_cols, "id", "fileId", "file_id"),
            "assetId": find_col(f_cols, "assetId", "asset_id", "assetid"),
            "type": find_col(f_cols, "type", "fileType", "file_type"),
            "path": find_col(f_cols, "path", "filePath", "file_path")
        }

    return asset_table, exif_table, file_table, asset_map, exif_map, file_map


def scan_videos_db(conn, args: argparse.Namespace) -> List[Dict[str, Any]]:
    """Scans videos directly from PostgreSQL database."""
    tables = get_all_tables_and_columns(conn)
    asset_table, exif_table, file_table, asset_map, exif_map, file_map = discover_video_schema(tables)

    if not asset_table or not asset_map.get("id"):
        print("[!] Could not resolve asset table schema. Falling back to API mode.")
        return []

    id_col_name = asset_map["id"]
    id_col = f'a."{id_col_name}"'
    type_col = f'a."{asset_map["type"]}"' if asset_map.get("type") else None
    dur_col = f'a."{asset_map["duration"]}"' if asset_map.get("duration") else "NULL"
    path_col = f'a."{asset_map["originalPath"]}"' if asset_map.get("originalPath") else "NULL"
    encoded_col = asset_map.get("encodedVideoPath")
    thumbhash_col = asset_map.get("thumbhash")
    thumb_path_col = asset_map.get("thumbnailPath")

    conditions = []
    if type_col:
        conditions.append(f"{type_col} = 'VIDEO'")

    if asset_map.get("isTrashed"):
        conditions.append(f'a."{asset_map["isTrashed"]}" = false')
    if asset_map.get("deletedAt"):
        conditions.append(f'a."{asset_map["deletedAt"]}" IS NULL')

    exif_join = ""
    exif_selects = ["NULL AS fps", "NULL AS width", "NULL AS height", "NULL AS date_time_orig", "NULL AS has_exif"]

    if exif_table and exif_map.get("assetId"):
        e_asset_id = f'e."{exif_map["assetId"]}"'
        fps_c = f'e."{exif_map["fps"]}"' if exif_map.get("fps") else "NULL"
        w_c = f'e."{exif_map["width"]}"' if exif_map.get("width") else "NULL"
        h_c = f'e."{exif_map["height"]}"' if exif_map.get("height") else "NULL"
        dt_c = f'e."{exif_map["dateTimeOriginal"]}"' if exif_map.get("dateTimeOriginal") else "NULL"

        exif_join = f'LEFT JOIN "{exif_table}" e ON {id_col} = {e_asset_id}'
        exif_selects = [
            f"{fps_c} AS fps",
            f"{w_c} AS width",
            f"{h_c} AS height",
            f"{dt_c} AS date_time_orig",
            f"CASE WHEN {e_asset_id} IS NOT NULL THEN 1 ELSE 0 END AS has_exif"
        ]

    # Check for encoded video availability in DB
    encoded_conditions = []
    if file_table and file_map.get("assetId") and file_map.get("type") and file_map.get("path"):
        f_asset_id = file_map["assetId"]
        f_type = file_map["type"]
        f_path = file_map["path"]
        encoded_conditions.append(f"""EXISTS (
            SELECT 1 FROM "{file_table}" f
            WHERE f."{f_asset_id}" = {id_col}
              AND LOWER(f."{f_type}") IN ('encoded_video', 'encoded-video')
              AND f."{f_path}" IS NOT NULL
              AND TRIM(f."{f_path}") != ''
        )""")
    if encoded_col:
        encoded_conditions.append(f'(a."{encoded_col}" IS NOT NULL AND TRIM(a."{encoded_col}") != \'\')')

    if encoded_conditions:
        encoded_expr = f"CASE WHEN ({' OR '.join(encoded_conditions)}) THEN 1 ELSE 0 END"
    else:
        encoded_expr = "1"  # If schema has no encoded video tracking, treat as present

    # Check for thumbnail availability in DB
    thumb_conditions = []
    if file_table and file_map.get("assetId") and file_map.get("type") and file_map.get("path"):
        f_asset_id = file_map["assetId"]
        f_type = file_map["type"]
        f_path = file_map["path"]
        thumb_conditions.append(f"""EXISTS (
            SELECT 1 FROM "{file_table}" f
            WHERE f."{f_asset_id}" = {id_col}
              AND LOWER(f."{f_type}") IN ('thumbnail', 'preview', 'fullsize')
              AND f."{f_path}" IS NOT NULL
              AND TRIM(f."{f_path}") != ''
        )""")
    if thumbhash_col:
        thumb_conditions.append(f'(a."{thumbhash_col}" IS NOT NULL AND TRIM(a."{thumbhash_col}") != \'\')')
    if thumb_path_col:
        thumb_conditions.append(f'(a."{thumb_path_col}" IS NOT NULL AND TRIM(a."{thumb_path_col}") != \'\')')

    if thumb_conditions:
        thumb_expr = f"CASE WHEN ({' OR '.join(thumb_conditions)}) THEN 1 ELSE 0 END"
    else:
        thumb_expr = "1"

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT 
            {id_col} AS id,
            {path_col} AS original_path,
            {dur_col} AS duration,
            {', '.join(exif_selects)},
            {encoded_expr} AS has_encoded_video,
            {thumb_expr} AS has_thumbnail
        FROM "{asset_table}" a
        {exif_join}
        {where_clause};
    """

    results = []
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    for r in rows:
        asset_id = str(r[0])
        original_path = r[1] or ""
        duration = r[2]
        fps = r[3]
        width = r[4]
        height = r[5]
        dt_orig = r[6]
        has_exif = bool(r[7]) if len(r) > 7 and r[7] is not None else True
        has_encoded = bool(r[8]) if len(r) > 8 and r[8] is not None else True
        has_thumb = bool(r[9]) if len(r) > 9 and r[9] is not None else True

        missing_reasons = []

        # 1. Metadata check: duration
        if is_duration_empty(duration):
            missing_reasons.append("missing duration")

        # 2. Metadata check: EXIF / FPS / Dimensions / Date
        if exif_table:
            if not has_exif:
                missing_reasons.append("missing EXIF record")
            else:
                if (fps is None or fps == 0) and args.check_fps:
                    missing_reasons.append("missing FPS")
                if (width is None or width == 0 or height is None or height == 0) and args.check_resolution:
                    missing_reasons.append("missing dimensions")
                if dt_orig is None and args.check_date:
                    missing_reasons.append("missing capture date")

        # 3. Encoded video check
        if args.check_encoded and not has_encoded:
            missing_reasons.append("missing encoded video")

        # 4. Thumbnail check
        if args.check_thumbnail and not has_thumb:
            missing_reasons.append("missing thumbnail")

        # If even any one is not available, trigger metadata update for this video
        if missing_reasons:
            results.append({
                "id": asset_id,
                "path": original_path,
                "reasons": missing_reasons,
                "duration": duration
            })

    return results


def scan_videos_api(immich_url: str, api_key: str, args: argparse.Namespace) -> List[Dict[str, Any]]:
    """Scans all videos using Immich REST API (/api/search/metadata)."""
    if not requests:
        print("[!] Error: 'requests' package is required. Install via: pip install requests")
        sys.exit(1)

    url = f"{immich_url.rstrip('/')}/api/search/metadata"
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    page = 1
    page_size = 1000
    missing_assets = []
    total_videos_checked = 0

    print("[*] Fetching videos via Immich API...")

    while True:
        payload = {
            "type": "VIDEO",
            "withExif": True,
            "isVisible": True,
            "page": page,
            "size": page_size
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code != 200:
                print(f"[!] API Search failed on page {page} ({resp.status_code}): {resp.text}")
                break

            data = resp.json()
            assets = data.get("assets", {}).get("items", []) if isinstance(data, dict) else []
            if not assets:
                break

            total_videos_checked += len(assets)
            print(f"    Scanning page {page} ({len(assets)} videos)...", end="\r", flush=True)

            for asset in assets:
                asset_id = asset.get("id")
                original_path = asset.get("originalPath") or asset.get("originalFileName") or ""
                duration = asset.get("duration")
                exif = asset.get("exifInfo")
                encoded_video = asset.get("encodedVideoPath")
                thumbhash = asset.get("thumbhash")
                resize_path = asset.get("resizePath") or asset.get("thumbnailPath")

                missing_reasons = []

                # 1. Duration check
                if is_duration_empty(duration):
                    missing_reasons.append("missing duration")

                # 2. EXIF / Technical metadata check
                if exif is None:
                    missing_reasons.append("missing EXIF record")
                else:
                    fps = exif.get("fps")
                    w = exif.get("exifImageWidth")
                    h = exif.get("exifImageHeight")
                    dt_orig = exif.get("dateTimeOriginal")

                    if (fps is None or fps == 0) and args.check_fps:
                        missing_reasons.append("missing FPS")
                    if (w is None or w == 0 or h is None or h == 0) and args.check_resolution:
                        missing_reasons.append("missing dimensions")
                    if dt_orig is None and args.check_date:
                        missing_reasons.append("missing capture date")

                # 3. Encoded video check
                if args.check_encoded and (not encoded_video or str(encoded_video).strip() == ""):
                    missing_reasons.append("missing encoded video")

                # 4. Thumbnail check
                if args.check_thumbnail and not thumbhash and not resize_path:
                    missing_reasons.append("missing thumbnail")

                # If even any one is not available, queue for metadata update
                if missing_reasons:
                    missing_assets.append({
                        "id": asset_id,
                        "path": original_path,
                        "reasons": missing_reasons,
                        "duration": duration
                    })

            if len(assets) < page_size:
                break
            page += 1

        except Exception as e:
            print(f"\n[!] Error querying Immich API: {e}")
            break

    print(f"\n[*] Checked {total_videos_checked} total video assets via API.")
    return missing_assets


def trigger_metadata_refetch(immich_url: str, api_key: str, asset_ids: List[str], batch_size: int = 100) -> Tuple[int, int]:
    """Triggers the refresh-metadata job for specified asset UUIDs."""
    if not requests:
        print("[!] Error: 'requests' package is required.")
        return 0, len(asset_ids)

    url = f"{immich_url.rstrip('/')}/api/assets/jobs"
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    success_count = 0
    fail_count = 0
    total_batches = (len(asset_ids) + batch_size - 1) // batch_size

    for i in range(0, len(asset_ids), batch_size):
        batch = asset_ids[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        payload = {
            "assetIds": batch,
            "name": "refresh-metadata"
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.status_code in (200, 201, 204):
                success_count += len(batch)
                print(f"    [Batch {batch_num}/{total_batches}] Queued {len(batch)} videos.")
            else:
                fail_count += len(batch)
                print(f"    [Batch {batch_num}/{total_batches}] Failed ({resp.status_code}): {resp.text}")
        except Exception as e:
            fail_count += len(batch)
            print(f"    [Batch {batch_num}/{total_batches}] Request failed: {e}")

    return success_count, fail_count


def main():
    parser = argparse.ArgumentParser(
        description="Search videos in Immich, check for missing metadata, encoded video, or thumbnail, and trigger metadata refetch."
    )
    parser.add_argument("--dry-run", action="store_true", help="Scan only; do not trigger metadata refetch.")
    parser.add_argument("--api-only", action="store_true", help="Force API scanning instead of connecting to PostgreSQL.")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt.")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for triggering asset jobs (default: 100).")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of missing video assets to trigger.")
    parser.add_argument("--quiet", action="store_true", help="Suppress non-essential log output.")

    # Video check options: encoded video and thumbnail
    parser.add_argument("--check-encoded", action="store_true", default=True, help="Flag video if encoded video is missing (default: True).")
    parser.add_argument("--no-check-encoded", dest="check_encoded", action="store_false", help="Ignore missing encoded video.")
    parser.add_argument("--check-thumbnail", action="store_true", default=True, help="Flag video if thumbnail is missing (default: True).")
    parser.add_argument("--no-check-thumbnail", dest="check_thumbnail", action="store_false", help="Ignore missing thumbnail.")

    # Metadata check options
    parser.add_argument("--check-fps", action="store_true", default=True, help="Flag video if FPS is null or 0 (default: True).")
    parser.add_argument("--no-check-fps", dest="check_fps", action="store_false", help="Ignore missing FPS.")
    parser.add_argument("--check-resolution", action="store_true", default=True, help="Flag video if width/height is null/0 (default: True).")
    parser.add_argument("--no-check-resolution", dest="check_resolution", action="store_false", help="Ignore missing resolution.")
    parser.add_argument("--check-date", action="store_true", default=False, help="Flag video if capture date (dateTimeOriginal) is null.")

    # Immich API connection options
    parser.add_argument("--immich-url", type=str, default=None, help="Immich URL (e.g. http://localhost:2283)")
    parser.add_argument("--api-key", type=str, default=None, help="Immich API Key")

    # DB connection options
    parser.add_argument("--db-url", type=str, default=None, help="PostgreSQL connection URL")
    parser.add_argument("--db-host", type=str, default=None, help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=None, help="PostgreSQL port")
    parser.add_argument("--db-user", type=str, default=None, help="PostgreSQL user")
    parser.add_argument("--db-password", type=str, default=None, help="PostgreSQL password")
    parser.add_argument("--db-name", type=str, default=None, help="PostgreSQL database name")

    args = parser.parse_args()

    immich_url = args.immich_url or os.getenv("IMMICH_URL") or "http://localhost:2283"
    api_key = args.api_key or os.getenv("IMMICH_KEY") or os.getenv("IMMICH_API_KEY")

    if not api_key and not args.dry_run:
        print("[!] Error: IMMICH_KEY (or IMMICH_API_KEY) is required to trigger metadata refetch.")
        print("    Please configure it in .env or pass via --api-key.")
        sys.exit(1)

    print("=" * 70)
    print(" Immich Missing Video Metadata, Encoded Video & Thumbnail Detector")
    print("=" * 70)
    if args.dry_run:
        print(" [!] DRY-RUN MODE: No jobs will be queued.")

    missing_videos: List[Dict[str, Any]] = []

    # 1. Try DB scan first unless --api-only is requested
    if not args.api_only:
        conn = get_db_connection(args)
        if conn:
            print("[*] Scanning video assets via PostgreSQL...")
            try:
                missing_videos = scan_videos_db(conn, args)
                print(f"[✓] Database scan complete. Found {len(missing_videos)} videos needing metadata update.")
            finally:
                conn.close()

    # 2. Fall back to API scan if DB was not used or returned no connection
    if not missing_videos and (args.api_only or get_db_connection(args) is None):
        if not api_key:
            print("[!] IMMICH_KEY is required for API scan. Please set IMMICH_KEY in .env.")
            sys.exit(1)
        missing_videos = scan_videos_api(immich_url, api_key, args)

    print("\n" + "-" * 70)
    print(" Scan Results")
    print("-" * 70)
    print(f"  • Videos needing metadata update : {len(missing_videos)}")
    print("-" * 70)

    if not missing_videos:
        print("\n[✓] All videos have complete metadata, encoded video, and thumbnail! Nothing to update.")
        return

    # Aggregate reasons for summary
    reason_counts: Dict[str, int] = {}
    for item in missing_videos:
        for r in item["reasons"]:
            reason_counts[r] = reason_counts.get(r, 0) + 1

    print("\n  Summary of Missing Items:")
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"    - {reason:<24}: {count}")

    # Sample output
    sample_count = min(5, len(missing_videos))
    print(f"\n[i] Sample targets ({sample_count} of {len(missing_videos)}):")
    for item in missing_videos[:sample_count]:
        reasons_str = ", ".join(item["reasons"])
        print(f"  - [{item['id']}] {item['path']} -> ({reasons_str})")

    if args.limit and len(missing_videos) > args.limit:
        print(f"\n[i] Limiting action to first {args.limit} videos (as specified by --limit).")
        missing_videos = missing_videos[:args.limit]

    if args.dry_run:
        print("\n[✓] Dry-run complete. Exiting without triggering jobs.")
        return

    if not args.yes:
        confirm = input(f"\n[?] Trigger metadata update for {len(missing_videos)} videos? [y/N]: ").strip().lower()
        if confirm != "y":
            print("[*] Operation cancelled by user.")
            return

    target_ids = [item["id"] for item in missing_videos]
    print(f"\n[*] Triggering metadata update via Immich API ({immich_url})...")
    success, failed = trigger_metadata_refetch(immich_url, api_key, target_ids, batch_size=args.batch_size)

    print("\n" + "=" * 70)
    print(f" [✓] Completed: {success} queued successfully, {failed} failed.")
    print("=" * 70)


if __name__ == "__main__":
    main()
