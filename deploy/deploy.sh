#!/bin/bash
# Deploy code to all VPS machines
# Usage: ./deploy/deploy.sh
#
# Cross-platform (works on Linux, macOS, and Windows Git Bash). Uses tar|ssh
# instead of rsync because Git Bash on Windows doesn't ship rsync. Behavior
# matches "rsync -az --delete" for src/ and config/ — remote dir is wiped and
# repopulated. requirements.txt is overwritten in place.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG="$SCRIPT_DIR/vps_config.json"

PY=$(command -v python || command -v python3)
# tr -d '\r' strips CR that Windows Python emits with \n (CRLF).
USER=$($PY -c "import json,sys; print(json.load(sys.stdin)['vps_user'])" < "$CONFIG" | tr -d '\r')
REMOTE_DIR=$($PY -c "import json,sys; print(json.load(sys.stdin)['remote_dir'])" < "$CONFIG" | tr -d '\r')
IPS=$($PY -c "import json,sys; [print(s['ip']) for s in json.load(sys.stdin)['servers']]" < "$CONFIG" | tr -d '\r')

# Defensive: refuse to operate on an empty/root remote dir.
case "$REMOTE_DIR" in
  ''|/|.|..) echo "Refusing to deploy: remote_dir='$REMOTE_DIR'"; exit 1 ;;
esac

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

echo "=== Deploying to $(echo "$IPS" | wc -l) VPS machines ==="

for ip in $IPS; do
  (
    echo "[$ip] Syncing code..."
    ssh $SSH_OPTS $USER@$ip "mkdir -p $REMOTE_DIR/{src,config,logs} && rm -rf $REMOTE_DIR/src/* $REMOTE_DIR/config/*"

    # Stream local src/ and config/ as a tar over ssh, extracted at REMOTE_DIR.
    tar -cz -C "$PROJECT_DIR" src config | ssh $SSH_OPTS $USER@$ip "tar -xz -C $REMOTE_DIR"

    # requirements.txt: scp it directly, no need for tar.
    scp $SSH_OPTS "$PROJECT_DIR/requirements.txt" "$USER@$ip:$REMOTE_DIR/" > /dev/null

    echo "[$ip] Installing dependencies..."
    ssh $SSH_OPTS $USER@$ip "cd $REMOTE_DIR && /opt/nba/bin/pip install -q -r requirements.txt 2>&1 | tail -1"

    echo "[$ip] Done"
  ) &
done

wait
echo "=== Deploy complete ==="
