#!/bin/bash
# Monitor job status across all VPS machines
# Usage: ./deploy/monitor.sh [--logs ENDPOINT_NAME]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$SCRIPT_DIR/vps_config.json"

PY=$(command -v python || command -v python3)
# tr -d '\r' strips CR that Windows Python emits with \n (CRLF).
USER=$($PY -c "import json,sys; print(json.load(sys.stdin)['vps_user'])" < "$CONFIG" | tr -d '\r')
REMOTE_DIR=$($PY -c "import json,sys; print(json.load(sys.stdin)['remote_dir'])" < "$CONFIG" | tr -d '\r')
PYTHON=$($PY -c "import json,sys; print(json.load(sys.stdin)['vps_python'])" < "$CONFIG" | tr -d '\r')
IPS=($($PY -c "import json,sys; [print(s['ip']) for s in json.load(sys.stdin)['servers']]" < "$CONFIG" | tr -d '\r'))
NUM_VPS=${#IPS[@]}

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o LogLevel=ERROR"

# If --logs flag, show logs for a specific endpoint
if [ "$1" = "--logs" ] && [ -n "$2" ]; then
  ENDPOINT=$2
  echo "=== Searching for logs of $ENDPOINT ==="
  for vps_idx in $(seq 0 $((NUM_VPS - 1))); do
    VPS_IP=${IPS[$vps_idx]}
    RESULT=$(ssh $SSH_OPTS $USER@$VPS_IP "cat $REMOTE_DIR/logs/${ENDPOINT}.log 2>/dev/null | tail -30" 2>/dev/null)
    if [ -n "$RESULT" ]; then
      echo "[VPS $((vps_idx+1)) @ $VPS_IP] $ENDPOINT:"
      echo "$RESULT"
      echo ""
    fi
  done
  exit 0
fi

echo "=== VPS Fleet Status @ $(date '+%Y-%m-%d %H:%M:%S') ==="
echo ""

TOTAL_COMPLETED=0
TOTAL_FAILED=0
TOTAL_ASSIGNED=0

for vps_idx in $(seq 0 $((NUM_VPS - 1))); do
  VPS_IP=${IPS[$vps_idx]}

  STATUS=$(ssh $SSH_OPTS $USER@$VPS_IP "
    # Check if runner is still going
    RUNNING=\$(pgrep -f 'nba_data_processor' > /dev/null 2>&1 && echo 'yes' || echo 'no')

    # Count completed/failed from runner log
    if [ -f $REMOTE_DIR/logs/runner.log ]; then
      COMPLETED=\$(grep -c 'Completed ' $REMOTE_DIR/logs/runner.log 2>/dev/null || echo 0)
      FAILED=\$(grep -c 'FAILED ' $REMOTE_DIR/logs/runner.log 2>/dev/null || echo 0)
      TOTAL=\$(grep -c 'Starting ' $REMOTE_DIR/logs/runner.log 2>/dev/null || echo 0)
      ASSIGNED=\$(grep -oP '\d+/\K\d+' $REMOTE_DIR/logs/runner.log 2>/dev/null | head -1 || echo '?')
      CURRENT=\$(grep 'Starting ' $REMOTE_DIR/logs/runner.log 2>/dev/null | tail -1 | grep -oP 'Starting \K\S+' || echo 'none')
    else
      COMPLETED=0; FAILED=0; TOTAL=0; ASSIGNED='?'; CURRENT='not started'
    fi

    echo \"\$RUNNING|\$COMPLETED|\$FAILED|\$TOTAL|\$ASSIGNED|\$CURRENT\"
  " 2>/dev/null)

  IFS='|' read -r RUNNING COMPLETED FAILED TOTAL ASSIGNED CURRENT <<< "$STATUS"

  if [ "$RUNNING" = "yes" ]; then
    ICON="RUNNING"
  elif [ "$TOTAL" -gt 0 ] 2>/dev/null; then
    ICON="DONE"
  else
    ICON="IDLE"
  fi

  printf "[VPS %d @ %-15s] %-7s | %s/%s done | %s failed | current: %s\n" \
    $((vps_idx+1)) "$VPS_IP" "$ICON" "$COMPLETED" "$ASSIGNED" "$FAILED" "$CURRENT"

  TOTAL_COMPLETED=$((TOTAL_COMPLETED + COMPLETED))
  TOTAL_FAILED=$((TOTAL_FAILED + FAILED))
  if [[ "$ASSIGNED" =~ ^[0-9]+$ ]]; then
    TOTAL_ASSIGNED=$((TOTAL_ASSIGNED + ASSIGNED))
  fi
done

echo ""
echo "Total: $TOTAL_COMPLETED/$TOTAL_ASSIGNED endpoints completed, $TOTAL_FAILED failed"

# Show queue status if job_queue table exists
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
echo ""
echo "=== Job Queue ==="
cd "$PROJECT_DIR"
$PY src/job_queue.py status 2>/dev/null || echo "(No job queue configured — use '$PY src/job_queue.py init' to set up)"
