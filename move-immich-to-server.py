import os
import subprocess
import sys

# ============================================================
# CONFIGURATION
# ============================================================

# Local Immich
LOCAL_UPLOAD_PATH = "/Volumes/ssd/immich-git/upload/"
LOCAL_PG_USER = "postgres"
LOCAL_PG_DB = "immich"

# IMPORTANT:
# Set this to the EXACT Immich version currently running locally.
#
# Example:
# IMMICH_VERSION = "v2.5.1"
#
# Find your version with:
#   git describe --tags --always
#
# IMMICH_VERSION = "CHANGE_ME"

# Remote server
REMOTE_IP = "80.225.206.109"
REMOTE_USER = "ubuntu"
SSH_KEY_PATH = "/Users/vaibhav/Projects/ssh-keys/oracle-drive-new.key"

REMOTE_APP_DIR = "/home/ubuntu/immich"
REMOTE_DATA_DIR = "/home/ubuntu/immich-data"

# External ML server
EXTERNAL_ML_URL = "http://my-main-server:3003"

# Temporary local files
DUMP_FILE = "/tmp/immich_db_dump.sql.gz"
COMPOSE_FILE = "/tmp/docker-compose.yml"
ENV_FILE = "/tmp/.env"


# ============================================================
# DOCKER COMPOSE
# ============================================================

DOCKER_COMPOSE_YAML = f"""\
name: immich

services:

  immich-server:
    container_name: immich_server
    image: ghcr.io/immich-app/immich-server:{IMMICH_VERSION}

    volumes:
      - {REMOTE_DATA_DIR}:/usr/src/app/upload
      - /etc/localtime:/etc/localtime:ro

    env_file:
      - .env

    ports:
      - "2283:2283"

    depends_on:
      database:
        condition: service_healthy
      redis:
        condition: service_healthy

    restart: always

  redis:
    container_name: immich_redis
    image: docker.io/valkey/valkey:8.0-alpine

    healthcheck:
      test: ["CMD-SHELL", "valkey-cli ping || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5

    restart: always

  database:
    container_name: immich_postgres

    # Keep the PostgreSQL image compatible with the DB
    # used by this Immich version.
    image: docker.io/tensorchord/pgvecto-rs:pg16-v0.2.0

    environment:
      POSTGRES_PASSWORD: '${{DB_PASSWORD}}'
      POSTGRES_USER: '${{DB_USERNAME}}'
      POSTGRES_DB: '${{DB_DATABASE_NAME}}'
      POSTGRES_INITDB_ARGS: '--data-checksums'

    volumes:
      - ./pgdata:/var/lib/postgresql/data

    healthcheck:
      test:
        [
          "CMD-SHELL",
          "pg_isready -d ${{DB_DATABASE_NAME}} -U ${{DB_USERNAME}}"
        ]
      interval: 10s
      timeout: 5s
      retries: 10

    restart: always
"""


# ============================================================
# ENV
# ============================================================

ENV_FILE_CONTENT = f"""\
UPLOAD_LOCATION={REMOTE_DATA_DIR}

DB_PASSWORD=immich_db_password_change_me
DB_USERNAME=postgres
DB_DATABASE_NAME=immich

IMMICH_MACHINE_LEARNING_URL={EXTERNAL_ML_URL}
"""


# ============================================================
# HELPERS
# ============================================================

def run_cmd(cmd, shell=False):
    print(
        f"\n[EXEC] "
        f"{cmd if isinstance(cmd, str) else ' '.join(cmd)}"
    )

    result = subprocess.run(cmd, shell=shell)

    if result.returncode != 0:
        print("\nERROR: Command failed.")
        sys.exit(result.returncode)


def check_version():
    if IMMICH_VERSION == "CHANGE_ME":
        print(
            "\nERROR: IMMICH_VERSION has not been set.\n\n"
            "Find your local Immich version with:\n"
            "  git describe --tags --always\n\n"
            "Then set IMMICH_VERSION in this script."
        )
        sys.exit(1)


# ============================================================
# MAIN
# ============================================================

def main():

    check_version()

    ssh_opts = [
        "-i",
        SSH_KEY_PATH,
        "-o",
        "StrictHostKeyChecking=no",
    ]

    remote = f"{REMOTE_USER}@{REMOTE_IP}"

    print("=" * 70)
    print("IMMICH MIGRATION")
    print("=" * 70)

    print(f"Immich version : {IMMICH_VERSION}")
    print(f"Local upload  : {LOCAL_UPLOAD_PATH}")
    print(f"Remote data   : {REMOTE_DATA_DIR}")
    print()


    # --------------------------------------------------------
    # STEP 1
    # PostgreSQL dump
    # --------------------------------------------------------

    print("--- STEP 1: Dump Local PostgreSQL Database ---")

    dump_cmd = (
        f"pg_dump "
        f"-U {LOCAL_PG_USER} "
        f"-h localhost "
        f"-d {LOCAL_PG_DB} "
        f"--clean "
        f"--if-exists "
        f"--no-owner "
        f"--no-privileges "
        f"| gzip > {DUMP_FILE}"
    )

    run_cmd(dump_cmd, shell=True)

    if not os.path.exists(DUMP_FILE):
        print("ERROR: Database dump was not created.")
        sys.exit(1)

    dump_size = os.path.getsize(DUMP_FILE)

    print(
        f"Database dump created: "
        f"{dump_size / 1024 / 1024:.2f} MB"
    )


    # --------------------------------------------------------
    # STEP 2
    # Remote directories
    # --------------------------------------------------------

    print("\n--- STEP 2: Prepare Remote Directories ---")

    remote_mkdir = (
        f"mkdir -p "
        f"{REMOTE_APP_DIR} "
        f"{REMOTE_DATA_DIR}"
    )

    run_cmd(
        [
            "ssh",
            *ssh_opts,
            remote,
            remote_mkdir,
        ]
    )


    # --------------------------------------------------------
    # STEP 3
    # Write compose + env
    # --------------------------------------------------------

    print("\n--- STEP 3: Create Docker Compose Configuration ---")

    with open(COMPOSE_FILE, "w") as f:
        f.write(DOCKER_COMPOSE_YAML)

    with open(ENV_FILE, "w") as f:
        f.write(ENV_FILE_CONTENT)

    run_cmd(
        [
            "scp",
            *ssh_opts,
            DUMP_FILE,
            COMPOSE_FILE,
            ENV_FILE,
            f"{remote}:{REMOTE_APP_DIR}/",
        ]
    )


    # --------------------------------------------------------
    # STEP 4
    # Rsync media
    # --------------------------------------------------------

    # print(
    #     "\n--- STEP 4: Rsync Immich Media ---"
    # )

    # print(
    #     "Skipping:\n"
    #     "  - thumbs/\n"
    #     "  - encoded-video/\n"
    # )

    # rsync_cmd = [
    #     "rsync",
    #     "-avhP",
    #     "--exclude=.stfolder",
    #     "--exclude=thumbs/",
    #     "--exclude=encoded-video/",
    #     "-e",
    #     f"ssh -i {SSH_KEY_PATH} -o Compression=no",
    #     f"{LOCAL_UPLOAD_PATH.rstrip('/')}/",
    #     f"{remote}:{REMOTE_DATA_DIR}/",
    # ]

    # run_cmd(rsync_cmd)


    # --------------------------------------------------------
    # STEP 5
    # Fresh PostgreSQL
    # --------------------------------------------------------

    print(
        "\n--- STEP 5: Initialize Remote PostgreSQL ---"
    )

    remote_postgres_script = f"""
set -e

cd {REMOTE_APP_DIR}

echo "Stopping existing Immich stack..."

sudo docker compose down

echo "Starting PostgreSQL only..."

sudo docker compose up -d database

echo "Waiting for PostgreSQL to become healthy..."

until sudo docker compose exec -T database \
    pg_isready \
    -U postgres \
    -d immich \
    >/dev/null 2>&1
do
    sleep 2
done

echo "PostgreSQL is ready."

echo "Checking PostgreSQL extensions..."

sudo docker compose exec -T database \
    psql \
    -U postgres \
    -d immich \
    -c "SELECT extname, extversion FROM pg_extension ORDER BY extname;"
"""

    run_cmd(
        [
            "ssh",
            *ssh_opts,
            remote,
            remote_postgres_script,
        ]
    )


    # --------------------------------------------------------
    # STEP 6
    # Restore PostgreSQL
    # --------------------------------------------------------

    print(
        "\n--- STEP 6: Restore PostgreSQL Database ---"
    )

    print(
        "IMPORTANT: PostgreSQL restore will stop at the FIRST error."
    )

    remote_restore_script = f"""
set -e

cd {REMOTE_APP_DIR}

echo "Restoring Immich PostgreSQL database..."

zcat {REMOTE_APP_DIR}/immich_db_dump.sql.gz | \
sudo docker compose exec -T database \
    psql \
    -U postgres \
    -d immich \
    --set ON_ERROR_STOP=1

echo "Database restore completed successfully."
"""

    run_cmd(
        [
            "ssh",
            *ssh_opts,
            remote,
            remote_restore_script,
        ]
    )


    # --------------------------------------------------------
    # STEP 7
    # Permissions
    # --------------------------------------------------------

    print(
        "\n--- STEP 7: Fix Media Permissions ---"
    )

    remote_permissions_script = f"""
set -e

echo "Fixing Immich media permissions..."

sudo chown -R 1000:1000 {REMOTE_DATA_DIR}

echo "Permissions fixed."
"""

    run_cmd(
        [
            "ssh",
            *ssh_opts,
            remote,
            remote_permissions_script,
        ]
    )


    # --------------------------------------------------------
    # STEP 8
    # Start Immich
    # --------------------------------------------------------

    print(
        "\n--- STEP 8: Start Immich ---"
    )

    remote_launch_script = f"""
set -e

cd {REMOTE_APP_DIR}

sudo docker compose up -d

echo ""
echo "Docker containers:"
sudo docker compose ps

echo ""
echo "Migration completed."
"""

    run_cmd(
        [
            "ssh",
            *ssh_opts,
            remote,
            remote_launch_script,
        ]
    )


    # --------------------------------------------------------
    # DONE
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("IMMICH MIGRATION COMPLETED")
    print("=" * 70)
    print()
    print("Open:")
    print("  http://80.225.206.109:2283")
    print()
    print("Check logs with:")
    print(
        "  ssh -i "
        f"{SSH_KEY_PATH} "
        f"{remote} "
        "'cd /home/ubuntu/immich && sudo docker compose logs -f immich-server'"
    )
    print()


if __name__ == "__main__":
    main()