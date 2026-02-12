#!/bin/bash
# Migration script for Proxmox VE data
# Run directly on the server machine
#
# Usage: ./migrate-linux.sh
#
# Prerequisites:
# - Go installed (apt install golang-go)
# - Network access to source API and PocketBase

set -e

echo "==========================================================="
echo "   Proxmox VE Data Migration to PocketBase"
echo "==========================================================="

# Configuration - EDIT THESE VALUES
export MIGRATION_SOURCE_URL="https://api.htl-braunau.at/data"
export POCKETBASE_URL="http://db.community-scripts.org"
export POCKETBASE_COLLECTION="telemetry"
export PB_AUTH_COLLECTION="_superusers"
export PB_IDENTITY="db_admin@community-scripts.org"
export PB_PASSWORD="YOUR_PASSWORD_HERE"  # <-- CHANGE THIS!
export REPO_SOURCE="Proxmox VE"
export DATE_UNTIL="2026-02-10"
export BATCH_SIZE="500"

# Optional: Resume from specific page
# export START_PAGE="100"

# Optional: Only import records after this date
# export DATE_FROM="2020-01-01"

echo ""
echo "Configuration:"
echo "  Source:     $MIGRATION_SOURCE_URL"
echo "  Target:     $POCKETBASE_URL"
echo "  Collection: $POCKETBASE_COLLECTION"
echo "  Repo:       $REPO_SOURCE"
echo "  Until:      $DATE_UNTIL"
echo "  Batch:      $BATCH_SIZE"
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Go is not installed. Installing..."
    apt-get update && apt-get install -y golang-go
fi

# Download migrate.go if not present
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATE_GO="$SCRIPT_DIR/migrate.go"

if [ ! -f "$MIGRATE_GO" ]; then
    echo "migrate.go not found in $SCRIPT_DIR"
    echo "Please copy migrate.go to this directory first."
    exit 1
fi

echo "Building migration tool..."
cd "$SCRIPT_DIR"
go build -o migrate migrate.go

echo ""
echo "Starting migration..."
echo "Press Ctrl+C to stop (you can resume later with START_PAGE)"
echo ""

./migrate

echo ""
echo "==========================================================="
echo "   Post-Migration Steps"
echo "==========================================================="
echo ""
echo "1. Connect to PocketBase container:"
echo "   docker exec -it <pocketbase-container> sh"
echo ""
echo "2. Find the table name:"
echo "   sqlite3 /app/pb_data/data.db '.tables'"
echo ""
echo "3. Update timestamps (replace <table> with actual name):"
echo "   sqlite3 /app/pb_data/data.db \"UPDATE <table> SET created = old_created, updated = old_created WHERE old_created IS NOT NULL AND old_created != ''\""
echo ""
echo "4. Verify timestamps:"
echo "   sqlite3 /app/pb_data/data.db \"SELECT created, old_created FROM <table> LIMIT 5\""
echo ""
echo "5. Remove old_created field in PocketBase Admin UI"
echo ""
