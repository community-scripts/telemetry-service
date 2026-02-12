#!/bin/sh
# Direct SQLite Import - Pure Shell, FAST batch mode!
# Imports MongoDB Extended JSON directly into PocketBase SQLite
#
# Usage:
#   docker cp import-direct.sh pocketbase:/tmp/
#   docker cp data.json pocketbase:/tmp/
#   docker exec -it pocketbase sh -c "cd /tmp && chmod +x import-direct.sh && ./import-direct.sh"

set -e

JSON_FILE="${1:-/tmp/data.json}"
TABLE="${2:-telemetry}"
REPO="${3:-Proxmox VE}"
DB="${4:-/app/pb_data/data.db}"
BATCH=5000

echo "========================================================="
echo "     Direct SQLite Import (Batch Mode)"
echo "========================================================="
echo "JSON:  $JSON_FILE"
echo "Table: $TABLE"
echo "Repo:  $REPO"
echo "Batch: $BATCH"
echo "---------------------------------------------------------"

# Install jq if missing
command -v jq >/dev/null || apk add --no-cache jq

# Optimize SQLite for bulk
sqlite3 "$DB" "PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA cache_size=100000;"

SQL_FILE="/tmp/batch.sql"
echo "[INFO] Converting JSON to SQL..."
START=$(date +%s)

# Convert entire JSON to SQL file (much faster than line-by-line sqlite3 calls)
{
    echo "BEGIN TRANSACTION;"
    jq -r '.[] | @json' "$JSON_FILE" | while read -r r; do
        CT=$(echo "$r" | jq -r 'if .ct_type|type=="object" then .ct_type["$numberLong"] else .ct_type end // 0')
        DISK=$(echo "$r" | jq -r 'if .disk_size|type=="object" then .disk_size["$numberLong"] else .disk_size end // 0')
        CORE=$(echo "$r" | jq -r 'if .core_count|type=="object" then .core_count["$numberLong"] else .core_count end // 0')
        RAM=$(echo "$r" | jq -r 'if .ram_size|type=="object" then .ram_size["$numberLong"] else .ram_size end // 0')
        OS=$(echo "$r" | jq -r '.os_type // ""' | sed "s/'/''/g")
        OSVER=$(echo "$r" | jq -r '.os_version // ""' | sed "s/'/''/g")
        DIS6=$(echo "$r" | jq -r '.disable_ip6 // "no"' | sed "s/'/''/g")
        APP=$(echo "$r" | jq -r '.nsapp // "unknown"' | sed "s/'/''/g")
        METH=$(echo "$r" | jq -r '.method // ""' | sed "s/'/''/g")
        PVE=$(echo "$r" | jq -r '.pveversion // ""' | sed "s/'/''/g")
        STAT=$(echo "$r" | jq -r '.status // "unknown"')
        [ "$STAT" = "done" ] && STAT="success"
        RID=$(echo "$r" | jq -r '.random_id // ""' | sed "s/'/''/g")
        TYPE=$(echo "$r" | jq -r '.type // "lxc"' | sed "s/'/''/g")
        ERR=$(echo "$r" | jq -r '.error // ""' | sed "s/'/''/g")
        DATE=$(echo "$r" | jq -r 'if .created_at|type=="object" then .created_at["$date"] else .created_at end // ""')
        ID=$(head -c 100 /dev/urandom | tr -dc 'a-z0-9' | head -c 15)
        REPO_ESC=$(echo "$REPO" | sed "s/'/''/g")

        echo "INSERT OR IGNORE INTO $TABLE (id,created,updated,ct_type,disk_size,core_count,ram_size,os_type,os_version,disableip6,nsapp,method,pve_version,status,random_id,type,error,repo_source) VALUES ('$ID','$DATE','$DATE',$CT,$DISK,$CORE,$RAM,'$OS','$OSVER','$DIS6','$APP','$METH','$PVE','$STAT','$RID','$TYPE','$ERR','$REPO_ESC');"
    done
    echo "COMMIT;"
} > "$SQL_FILE"

MID=$(date +%s)
echo "[INFO] SQL generated in $((MID - START))s"
echo "[INFO] Importing into SQLite..."

sqlite3 "$DB" < "$SQL_FILE"

END=$(date +%s)
COUNT=$(wc -l < "$SQL_FILE")
rm -f "$SQL_FILE"

echo "========================================================="
echo "Done! ~$((COUNT - 2)) records in $((END - START)) seconds"
echo "========================================================="
