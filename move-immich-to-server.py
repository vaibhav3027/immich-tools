import os
import subprocess
import sys

# Configuration
LOCAL_UPLOAD_PATH = "/Volumes/ssd/immich-git/upload/"
LOCAL_PG_USER = "postgres"  # Change if your local DB user is different
LOCAL_PG_DB = "immich"      # Change if your local DB name is different

REMOTE_IP = "80.225.206.109"
REMOTE_USER = "ubuntu"
SSH_KEY_PATH = "/Users/vaibhav/Projects/ssh-keys/oracle-drive-new.key"

REMOTE_APP_DIR = "/home/ubuntu/immich"
REMOTE_DATA_DIR = "/home/ubuntu/immich-data"
EXTERNAL_ML_URL = "http://my-main-server:3003"  # Replace with your actual ML instance URL

# Docker Compose template without immich-machine-learning service
DOCKER_COMPOSE_YAML = f"""\
name: immich

services:
  immich-server:
    container_name: immich_server
    image: ghcr.io/immich-app/immich-server:release
    volumes:
      - {REMOTE_DATA_DIR}:/usr/src/app/upload
      - /etc/localtime:/etc/localtime:ro
    env_file:
      - .env
    ports:
      - '2283:2283'
    depends_on:
      - database
      - redis
    restart: always

  redis:
    container_name: immich_redis
    image: docker.io/valkey/valkey:8.0-alpine
    healthcheck:
      test: ["CMD-SHELL", "valkey-cli ping || exit 1"]
    restart: always

  database:
    container_name: immich_postgres
    image: docker.io/tensorchord/pgvecto-rs:pg16-v0.2.0
    environment:
      POSTGRES_PASSWORD: '${{DB_PASSWORD}}'
      POSTGRES_USER: '${{DB_USERNAME}}'
      POSTGRES_DB: '${{DB_DATABASE_NAME}}'
      POSTGRES_INITDB_ARGS: '--data-checksums'
    volumes:
      - ./pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -d ${{DB_DATABASE_NAME}} -U ${{DB_USERNAME}}"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: always
"""

ENV_FILE = f"""\
UPLOAD_LOCATION={REMOTE_DATA_DIR}
DB_PASSWORD=immich_db_password_change_me
DB_USERNAME=postgres
DB_DATABASE_NAME=immich
IMMICH_MACHINE_LEARNING_URL={EXTERNAL_ML_URL}
"""

def run_cmd(cmd, shell=False):
    """Executes a local command."""
    print(f"\n[EXEC] {cmd if isinstance(cmd, str) else ' '.join(cmd)}")
    res = subprocess.run(cmd, shell=shell)
    if res.returncode != 0:
        print(f"Error executing command. Exiting.")
        sys.exit(1)

def main():
    ssh_opts = f"-i {SSH_KEY_PATH} -o StrictHostKeyChecking=no"
    remote_ssh = f"ssh {ssh_opts} {REMOTE_USER}@{REMOTE_IP}"

    print("--- STEP 1: Dump Local Postgres Database ---")
    dump_file = "/tmp/immich_db_dump.sql.gz"
    dump_cmd = f"pg_dump -U {LOCAL_PG_USER} -h localhost -d {LOCAL_PG_DB} -c | gzip > {dump_file}"
    run_cmd(dump_cmd, shell=True)

    print("\n--- STEP 2: Create Remote Directories ---")
    run_cmd(f"{remote_ssh} 'mkdir -p {REMOTE_APP_DIR} {REMOTE_DATA_DIR}'", shell=True)

    print("\n--- STEP 3: Transfer Database Dump & Stack Configs ---")
    # Write temporary local files to upload
    with open("/tmp/docker-compose.yml", "w") as f:
        f.write(DOCKER_COMPOSE_YAML)
    with open("/tmp/.env", "w") as f:
        f.write(ENV_FILE)

    run_cmd(f"scp {ssh_opts} {dump_file} /tmp/docker-compose.yml /tmp/.env {REMOTE_USER}@{REMOTE_IP}:{REMOTE_APP_DIR}/", shell=True)

    print("\n--- STEP 4: Rsync Media Files (Skipping thumbs & encoded-video) ---")
    rsync_cmd = [
        "rsync", "-avzP",
        "-e", f"ssh -i {SSH_KEY_PATH} -o Compression=no",
        "--exclude=.stfolder",
        "--exclude=thumbs/",
        "--exclude=encoded-video/",
        f"{LOCAL_UPLOAD_PATH}/",
        f"{REMOTE_USER}@{REMOTE_IP}:{REMOTE_DATA_DIR}/"
    ]
    run_cmd(rsync_cmd)

    print("\n--- STEP 5: Start Postgres & Restore Database on Remote VPS ---")
    remote_restore_script = f"""
    cd {REMOTE_APP_DIR}
    sudo docker-compose down -v
    sudo docker-compose up -d database
    echo "Waiting for Postgres to initialize..."
    sleep 10
    zcat {REMOTE_APP_DIR}/immich_db_dump.sql.gz | sudo docker-compose exec -T database psql -U postgres -d immich
    """
    run_cmd(f"{remote_ssh} '{remote_restore_script}'", shell=True)

    print("\n--- STEP 6: Fix Permissions & Launch Full Docker Stack ---")
    remote_launch_script = f"""
    sudo chown -R 1000:1000 {REMOTE_DATA_DIR}
    cd {REMOTE_APP_DIR}
    docker-compose up -d
    """
    run_cmd(f"{remote_ssh} '{remote_launch_script}'", shell=True)

    print("\n Migration Script Completed Successfully!")

if __name__ == "__main__":
    main()
