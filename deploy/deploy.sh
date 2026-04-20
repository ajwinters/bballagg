#!/bin/bash
# Deploy code to all VPS machines
# Usage: ./deploy/deploy.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG="$SCRIPT_DIR/vps_config.json"

USER=$(python3 -c "import json; print(json.load(open('$CONFIG'))['vps_user'])")
REMOTE_DIR=$(python3 -c "import json; print(json.load(open('$CONFIG'))['remote_dir'])")
IPS=$(python3 -c "import json; [print(s['ip']) for s in json.load(open('$CONFIG'))['servers']]")

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

echo "=== Deploying to $(echo "$IPS" | wc -l) VPS machines ==="

for ip in $IPS; do
  (
    echo "[$ip] Syncing code..."
    ssh $SSH_OPTS $USER@$ip "mkdir -p $REMOTE_DIR/{src,config,logs}"
    rsync -az --delete -e "ssh $SSH_OPTS" \
      "$PROJECT_DIR/src/" "$USER@$ip:$REMOTE_DIR/src/"
    rsync -az --delete -e "ssh $SSH_OPTS" \
      "$PROJECT_DIR/config/" "$USER@$ip:$REMOTE_DIR/config/"
    rsync -az -e "ssh $SSH_OPTS" \
      "$PROJECT_DIR/requirements.txt" "$USER@$ip:$REMOTE_DIR/"

    echo "[$ip] Installing dependencies..."
    ssh $SSH_OPTS $USER@$ip "cd $REMOTE_DIR && /opt/nba/bin/pip install -q -r requirements.txt 2>&1 | tail -1"

    echo "[$ip] Done"
  ) &
done

wait
echo "=== Deploy complete ==="
