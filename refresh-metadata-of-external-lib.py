import os
import requests
import sys
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION FROM ENVIRONMENT ---
# Example: export IMMICH_URL="http://192.168.1.100:2283"
# Example: export IMMICH_KEY="your_admin_api_key_here"
IMMICH_URL = os.environ.get("IMMICH_URL")
IMMICH_KEY = os.environ.get("IMMICH_KEY")
LIBRARY_NAME = "Neha Didi Log"  # Replace with your library's name

if not IMMICH_URL or not IMMICH_KEY:
    print("Error: Please set IMMICH_URL and IMMICH_KEY environment variables.")
    sys.exit(1)

# Ensure URL ends with /api
BASE_API_URL = f"{IMMICH_URL.rstrip('/')}/api"

headers = {
    "Accept": "application/json",
    "x-api-key": IMMICH_KEY,
    "Content-Type": "application/json"
}

def get_library_id(name):
    """Finds the ID of the library with the specified name."""
    try:
        response = requests.get(f"{BASE_API_URL}/libraries", headers=headers)
        response.raise_for_status()
        libraries = response.json()
        for lib in libraries:
            if lib.get('name') == name:
                return lib.get('id')
        return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to Immich: {e}")
        return None

def force_metadata_refresh(library_id):
    """Triggers a scan with flags to recreate/refresh metadata."""
    # refreshAllFiles: Force-reextracts metadata even if the file hasn't changed
    payload = {
        "refreshAllFiles": True,
        "refreshModifiedFiles": True
    }
    
    scan_url = f"{BASE_API_URL}/libraries/{library_id}/scan"
    try:
        response = requests.post(scan_url, headers=headers, json=payload)
        if response.status_code in [200, 201]:
            print(f"Successfully triggered metadata refresh for: {LIBRARY_NAME}")
        else:
            print(f"Error ({response.status_code}): {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    lib_id = get_library_id(LIBRARY_NAME)
    if lib_id:
        print(f"Found Library ID: {lib_id}. Starting scan...")
        force_metadata_refresh(lib_id)
    else:
        print(f"Library '{LIBRARY_NAME}' not found. Check your name or API permissions.")

