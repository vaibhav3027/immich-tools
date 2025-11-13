import os
import requests
import pathlib
from dotenv import load_dotenv

load_dotenv()

# === CONFIG ===
IMMICH_URL = os.getenv("IMMICH_URL")
if not IMMICH_URL:
    raise SystemExit(" -- IMMICH_URL not set in environment --")

API_KEY = os.getenv("IMMICH_KEY")
if not API_KEY:
    raise SystemExit(" -- x - IMMICH_KEY not set in environment - x --")

# === INPUT ===
folder_path = input("- Enter full folder path (e.g. /Volumes/ssd/Vacation): ").rstrip("/")
album_name = input("- Enter album name (leave blank to auto-use folder name): ").strip()

if not album_name:
    album_name = pathlib.PurePath(folder_path).name
    print(f"- | Album name not entered — using folder name: '{album_name}'")

headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}


# === FUNCTION: Fetch all pages ===
def fetch_all_assets(folder_path: str, page_size: int = 1000): # Max page size limit in immich is 1000
    all_assets = []
    page = 1

    print(f"- | Fetching page ", end=" ")
    while True:
        search_body = {
            "page": page,
            "size": page_size,
            "withExif": False,
            "isVisible": True,
            "originalPath": folder_path
        }

        print(f"{page}...", end=" ")
        resp = requests.post(f"{IMMICH_URL}/api/search/metadata", headers=headers, json=search_body)
        if resp.status_code != 200:
            raise SystemExit(f"Failed to fetch page {page}: {resp.status_code} {resp.text}")

        data = resp.json()
        items = data.get("assets", {}).get("items", [])
        if not items:
            break

        all_assets.extend(items)

        if len(items) < page_size:
            break
        page += 1
    print("")

    return all_assets


# === FUNCTION: Get existing albums ===
def get_existing_albums():
    resp = requests.get(f"{IMMICH_URL}/api/albums", headers=headers)
    if resp.status_code != 200:
        raise SystemExit(f"- --x- Failed to get albums: {resp.status_code} {resp.text} -x-- ")
    return resp.json()


# === FUNCTION: Find unique album name ===
def find_unique_album_name(base_name, existing_albums):
    existing_names = [a["albumName"] for a in existing_albums]
    if base_name not in existing_names:
        return base_name

    i = 1
    while True:
        new_name = f"{base_name} {i}"
        if new_name not in existing_names:
            return new_name
        i += 1


# === FETCH ALL ASSETS ===
print(f"- | Searching for assets in '{folder_path}'...")
assets = fetch_all_assets(folder_path)

if not assets:
    raise SystemExit("No assets found for that folder path")

# === FILTER EXACT FILES IN THAT DIRECTORY ===
filtered_assets = []
base_path = pathlib.PurePath(folder_path)

for a in assets:
    asset_path = pathlib.PurePath(a["originalPath"]).parent
    if asset_path == base_path:
        filtered_assets.append(a)

print(f"- | Found {len(filtered_assets)} matching assets directly in folder")
if not filtered_assets:
    raise SystemExit("No exact matches (maybe all files are in subfolders)")


# === CHECK FOR EXISTING ALBUM ===
albums = get_existing_albums()
existing_album = next((a for a in albums if a["albumName"] == album_name), None)

if existing_album:
    choice = input(f"- | - Album '{album_name}' already exists. Add photos to it? (y/n): ").strip().lower()
    if choice == "y":
        album_id = existing_album["id"]
        print(f"- | Using existing album '{album_name}' (ID: {album_id})")
    else:
        unique_name = find_unique_album_name(album_name, albums)
        print(f"- | Creating new album '{unique_name}'...")
        create_album_body = {"albumName": unique_name}
        resp = requests.post(f"{IMMICH_URL}/api/albums", headers=headers, json=create_album_body)
        if resp.status_code not in (200, 201):
            raise SystemExit(f"Failed to create album: {resp.status_code} {resp.text}")
        album_id = resp.json()["id"]
        album_name = unique_name
        print(f"- | Created new album '{album_name}' (ID: {album_id})")
else:
    print(f"- | Creating album '{album_name}'...")
    create_album_body = {"albumName": album_name}
    resp = requests.post(f"{IMMICH_URL}/api/albums", headers=headers, json=create_album_body)
    if resp.status_code not in (200, 201):
        raise SystemExit(f"Failed to create album: {resp.status_code} {resp.text}")
    album_id = resp.json()["id"]
    print(f"- | Album created with ID: {album_id}")


# === ADD ASSETS TO ALBUM ===
asset_ids = [a["id"] for a in filtered_assets]
add_body = {"ids": asset_ids}
print(f"- | Adding {len(asset_ids)} assets to album '{album_name}'...")
resp = requests.put(f"{IMMICH_URL}/api/albums/{album_id}/assets", headers=headers, json=add_body)
if resp.status_code not in (200, 201):
    raise SystemExit(f"Failed to add assets: {resp.status_code} {resp.text}")

print("----------------------xxxxxxxx----------------------")
print(f"- Done! Added {len(asset_ids)} assets to album '{album_name}'")
