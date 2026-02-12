#!/bin/bash
# Post-migration script to fix timestamps in PocketBase
# Run this INSIDE the PocketBase container after migration completes
#
# Usage: ./fix-timestamps.sh

set -e

DB_PATH="/app/pb_data/data.db"

echo "==========================================================="
echo "   Fix Timestamps in PocketBase"
echo "==========================================================="
echo ""

# Check if sqlite3 is available
if ! command -v sqlite3 &> /dev/null; then
    echo "sqlite3 not found. Installing..."
    apk add sqlite 2>/dev/null || apt-get update && apt-get install -y sqlite3
fi

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "Database not found at $DB_PATH"
    echo "Trying alternative paths..."
    
    if [ -f "/pb_data/data.db" ]; then
        DB_PATH="/pb_data/data.db"
    elif [ -f "/pb/pb_data/data.db" ]; then
        DB_PATH="/pb/pb_data/data.db"
    else
        DB_PATH=$(find / -name "data.db" 2>/dev/null | head -1)
    fi
    
    if [ -z "$DB_PATH" ] || [ ! -f "$DB_PATH" ]; then
        echo "Could not find PocketBase database!"
        exit 1
    fi
fi

echo "Database: $DB_PATH"
echo ""

# List tables
echo "Tables in database:"
sqlite3 "$DB_PATH" ".tables"
echo ""

# Find the telemetry table (usually matches collection name)
echo "Looking for telemetry/installations table..."
TABLE_NAME=$(sqlite3 "$DB_PATH" ".tables" | tr ' ' '\n' | grep -E "telemetry|installations" | head -1)

if [ -z "$TABLE_NAME" ]; then
    echo "Could not auto-detect table. Available tables:"
    sqlite3 "$DB_PATH" ".tables"
    echo ""
    read -p "Enter table name: " TABLE_NAME
fi

echo "Using table: $TABLE_NAME"
echo ""

# Check if old_created column exists
HAS_OLD_CREATED=$(sqlite3 "$DB_PATH" "PRAGMA table_info($TABLE_NAME);" | grep -c "old_created" || echo "0")

if [ "$HAS_OLD_CREATED" -eq "0" ]; then
    echo "Column 'old_created' not found in table $TABLE_NAME"
    echo "Migration may not have been run with timestamp preservation."
    exit 1
fi

# Show sample data before update
echo "Sample data BEFORE update:"
sqlite3 "$DB_PATH" "SELECT id, created, old_created FROM $TABLE_NAME WHERE old_created IS NOT NULL AND old_created != '' LIMIT 3;"
echo ""

# Count records to update
COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM $TABLE_NAME WHERE old_created IS NOT NULL AND old_created != '';")
echo "Records to update: $COUNT"
echo ""

read -p "Proceed with timestamp update? [y/N] " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Perform the update
echo "Updating timestamps..."
sqlite3 "$DB_PATH" "UPDATE $TABLE_NAME SET created = old_created, updated = old_created WHERE old_created IS NOT NULL AND old_created != '';"

# Show sample data after update
echo ""
echo "Sample data AFTER update:"
sqlite3 "$DB_PATH" "SELECT id, created, old_created FROM $TABLE_NAME LIMIT 3;"
echo ""

echo "==========================================================="
echo "   Timestamp Update Complete!"
echo "==========================================================="
echo ""
echo "Next steps:"
echo "1. Verify data in PocketBase Admin UI"
echo "2. Remove the 'old_created' field from the collection schema"
echo ""
