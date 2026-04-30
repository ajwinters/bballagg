#!/bin/bash
# Launch queue-based workers on all VPS machines
# Usage: ./deploy/run_queue.sh [--extra-args "..."]
#
# Prerequisites:
#   1. Run deploy.sh first to sync code
#   2. Initialize queue and add jobs:
#      python3 src/job_queue.py init
#      python3 src/job_queue.py add high_priority
#
# Workers pull jobs from the DB queue automatically.
# When a worker finishes an endpoint, it grabs the next one.
# When the queue is empty, workers exit.

set -e

# Force Python to use UTF-8 for stdout/stderr so unicode chars (e.g. → in
# sharded job names) don't crash the local status print on Windows cp1252.
export PYTHONIOENCODING=utf-8

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG="$SCRIPT_DIR/vps_config.json"

PY=$(command -v python || command -v python3)
# tr -d '\r' strips CR that Windows Python emits with \n (CRLF).
USER=$($PY -c "import json,sys; print(json.load(sys.stdin)['vps_user'])" < "$CONFIG" | tr -d '\r')
REMOTE_DIR=$($PY -c "import json,sys; print(json.load(sys.stdin)['remote_dir'])" < "$CONFIG" | tr -d '\r')
PYTHON=$($PY -c "import json,sys; print(json.load(sys.stdin)['vps_python'])" < "$CONFIG" | tr -d '\r')
IPS=($($PY -c "import json,sys; [print(s['ip']) for s in json.load(sys.stdin)['servers']]" < "$CONFIG" | tr -d '\r'))
NUM_VPS=${#IPS[@]}

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o LogLevel=ERROR"

EXTRA_ARGS=""
SKIP_KILL=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --extra-args) EXTRA_ARGS="$2"; shift 2 ;;
    --skip-kill) SKIP_KILL=true; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo "=== Queue-Based Worker Launch ==="
echo "VPS machines: $NUM_VPS"
echo ""

# Show current queue status
echo "=== Current Queue Status ==="
cd "$PROJECT_DIR"
$PY src/job_queue.py status
echo ""

# Step 1: Kill old processes (unless --skip-kill)
if [ "$SKIP_KILL" = false ]; then
  echo "=== Stopping old processes ==="
  for ip in "${IPS[@]}"; do
    ssh $SSH_OPTS $USER@$ip \
      "kill -9 \$(pgrep -f 'nba_data_processor|runner|job_queue' 2>/dev/null) 2>/dev/null; echo 'ok'" 2>/dev/null &
  done
  wait
  echo "Old processes stopped."
  echo ""
fi

# Step 2: Create and upload worker script to each VPS
echo "=== Deploying workers ==="
for vps_idx in $(seq 0 $((NUM_VPS - 1))); do
  VPS_IP=${IPS[$vps_idx]}

  WORKER_FILE=$(mktemp)
  cat > "$WORKER_FILE" << WORKER_EOF
#!/bin/bash
cd $REMOTE_DIR
echo "[\$(date '+%Y-%m-%d %H:%M:%S')] Queue worker starting on $VPS_IP"
$PYTHON src/job_queue.py worker --python-path $PYTHON --worker-ip $VPS_IP
echo "[\$(date '+%Y-%m-%d %H:%M:%S')] Queue worker finished."
WORKER_EOF

  scp $SSH_OPTS "$WORKER_FILE" $USER@$VPS_IP:$REMOTE_DIR/queue_worker.sh > /dev/null
  ssh $SSH_OPTS $USER@$VPS_IP "chmod +x $REMOTE_DIR/queue_worker.sh"
  rm -f "$WORKER_FILE"

  echo "[VPS $((vps_idx+1)) @ $VPS_IP] Worker script deployed."
done

echo ""
echo "=== Launching workers ==="
# Use ssh -f to fork ssh into background after auth (avoids "Connection reset"
# from the &-detached remote process keeping fds open). Run in parallel so one
# host's hiccup doesn't break the rest.
for vps_idx in $(seq 0 $((NUM_VPS - 1))); do
  VPS_IP=${IPS[$vps_idx]}
  (
    ssh -f $SSH_OPTS $USER@$VPS_IP \
      "cd $REMOTE_DIR && nohup bash queue_worker.sh > logs/queue_worker.log 2>&1 < /dev/null &" \
      && echo "[VPS $((vps_idx+1)) @ $VPS_IP] Worker launched." \
      || echo "[VPS $((vps_idx+1)) @ $VPS_IP] LAUNCH FAILED"
  ) &
done
wait

echo ""
echo "All workers launched. They will pull jobs from the queue automatically."
echo "Monitor with: python3 src/job_queue.py status"
echo "         or:  ./deploy/monitor.sh"
