#!/bin/bash
# Migration script to import data from the old API to PocketBase
# Usage: ./migrate.sh [POCKETBASE_URL] [COLLECTION_NAME]
#
# Examples:
#   ./migrate.sh                                    # Uses defaults
#   ./migrate.sh http://localhost:8090              # Custom PB URL
#   ./migrate.sh http://localhost:8090 my_telemetry # Custom URL and collection

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
POCKETBASE_URL="${1:-http://localhost:8090}"
POCKETBASE_COLLECTION="${2:-telemetry}"

echo "============================================="
echo "   ProxmoxVED Data Migration Tool"
echo "============================================="
echo ""
echo "This script will migrate telemetry data from:"
echo "  Source: https://api.htl-braunau.at/dev/data"
echo "  Target: $POCKETBASE_URL"
echo "  Collection: $POCKETBASE_COLLECTION"
echo ""

# Check if PocketBase is reachable
echo "🔍 Checking PocketBase connection..."
if ! curl -sf "$POCKETBASE_URL/api/health" >/dev/null 2>&1; then
  echo "❌ Cannot reach PocketBase at $POCKETBASE_URL"
  echo "   Make sure PocketBase is running and the URL is correct."
  exit 1
fi
echo "✅ PocketBase is reachable"
echo ""

# Check source API
echo "🔍 Checking source API..."
SUMMARY=$(curl -sf "https://api.htl-braunau.at/dev/data/summary" 2>/dev/null || echo "")
if [ -z "$SUMMARY" ]; then
  echo "❌ Cannot reach source API"
  exit 1
fi

TOTAL=$(echo "$SUMMARY" | grep -o '"total_entries":[0-9]*' | cut -d: -f2)
echo "✅ Source API is reachable ($TOTAL entries available)"
echo ""

# Confirm migration
read -p "⚠️  Do you want to start the migration? [y/N] " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Migration cancelled."
  exit 0
fi

echo ""
echo "Starting migration..."
echo ""

# Run the Go migration script
cd "$SCRIPT_DIR"
POCKETBASE_URL="$POCKETBASE_URL" POCKETBASE_COLLECTION="$POCKETBASE_COLLECTION" go run migrate.go

echo ""
echo "Migration complete!"
