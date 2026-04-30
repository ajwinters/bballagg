#!/bin/bash
# Distribute and run endpoint jobs across VPS fleet
# Usage: ./deploy/run_jobs.sh [--masters-first] [--profile PROFILE] [--since-season 2020-21] [--test-mode]
#
# Profiles: high_priority, full (default: full)

set -e

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

# Parse arguments
MASTERS_FIRST=false
PROFILE="full"
EXTRA_ARGS=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --masters-first) MASTERS_FIRST=true; shift ;;
    --profile) PROFILE="$2"; shift 2 ;;
    *) EXTRA_ARGS="$EXTRA_ARGS $1"; shift ;;
  esac
done

# Get endpoint list based on profile
ENDPOINTS=($($PY -c "
import json,sys
data = json.load(sys.stdin)
profile = '$PROFILE'
for name, cfg in data['endpoints'].items():
    if not cfg.get('latest_version'):
        continue
    if profile == 'high_priority' and cfg.get('priority') != 'high':
        continue
    print(name)
" < "$PROJECT_DIR/config/endpoint_config.json" | tr -d '\r'))

echo "=== NBA Data Collection - VPS Fleet ==="
echo "Profile: $PROFILE"
echo "VPS machines: $NUM_VPS"
echo "Endpoints to process: ${#ENDPOINTS[@]}"
echo "Extra args: $EXTRA_ARGS"
echo ""

# Step 1: Kill old processes and clear logs on ALL machines
echo "=== Cleaning up old runs ==="
for ip in "${IPS[@]}"; do
  ssh $SSH_OPTS $USER@$ip \
    "kill -9 \$(pgrep -f 'nba_data_processor|runner.sh' 2>/dev/null) 2>/dev/null; rm -f $REMOTE_DIR/logs/*.log $REMOTE_DIR/runner.sh; echo 'ok'" &
done
wait
echo "All machines cleaned."
echo ""

# Step 2: Masters (if requested)
if [ "$MASTERS_FIRST" = true ]; then
  echo "=== Running master endpoints on ${IPS[0]} ==="
  ssh $SSH_OPTS $USER@${IPS[0]} \
    "cd $REMOTE_DIR && $PYTHON src/nba_data_processor.py --masters-only $EXTRA_ARGS 2>&1" | tail -20
  echo "Masters complete."
  echo ""
fi

# Step 3: Build and upload runner scripts, then launch all at once
echo "=== Distributing ${#ENDPOINTS[@]} endpoints across $NUM_VPS machines ==="

# First, build and upload runner scripts to all machines
for vps_idx in $(seq 0 $((NUM_VPS - 1))); do
  VPS_IP=${IPS[$vps_idx]}

  # Collect endpoints for this VPS via round-robin
  VPS_ENDPOINTS=()
  for i in "${!ENDPOINTS[@]}"; do
    if [ $((i % NUM_VPS)) -eq $vps_idx ]; then
      VPS_ENDPOINTS+=("${ENDPOINTS[$i]}")
    fi
  done

  if [ ${#VPS_ENDPOINTS[@]} -eq 0 ]; then
    echo "[VPS $((vps_idx+1)) @ $VPS_IP] No endpoints assigned, skipping."
    continue
  fi

  echo "[VPS $((vps_idx+1)) @ $VPS_IP] ${#VPS_ENDPOINTS[@]} endpoints: ${VPS_ENDPOINTS[*]}"

  # Write runner script locally then scp it (avoids heredoc/pipe issues)
  RUNNER_FILE=$(mktemp)
  cat > "$RUNNER_FILE" << RUNNER_EOF
#!/bin/bash
cd $REMOTE_DIR
ENDPOINTS=(${VPS_ENDPOINTS[*]})
TOTAL=\${#ENDPOINTS[@]}
for i in "\${!ENDPOINTS[@]}"; do
  EP=\${ENDPOINTS[\$i]}
  echo "[\$(date '+%Y-%m-%d %H:%M:%S')] [\$((i+1))/\$TOTAL] Starting \$EP"
  $PYTHON src/nba_data_processor.py --single-endpoint \$EP $EXTRA_ARGS >> logs/\${EP}.log 2>&1
  EXIT_CODE=\$?
  if [ \$EXIT_CODE -eq 0 ]; then
    echo "[\$(date '+%Y-%m-%d %H:%M:%S')] [\$((i+1))/\$TOTAL] Completed \$EP"
  else
    echo "[\$(date '+%Y-%m-%d %H:%M:%S')] [\$((i+1))/\$TOTAL] FAILED \$EP (exit code \$EXIT_CODE)"
  fi
done
echo "[\$(date '+%Y-%m-%d %H:%M:%S')] All endpoints complete."
RUNNER_EOF

  scp $SSH_OPTS "$RUNNER_FILE" $USER@$VPS_IP:$REMOTE_DIR/runner.sh > /dev/null
  ssh $SSH_OPTS $USER@$VPS_IP "chmod +x $REMOTE_DIR/runner.sh"
  rm -f "$RUNNER_FILE"
done

echo ""
echo "=== Launching all jobs ==="

# Now launch all runners in parallel
for vps_idx in $(seq 0 $((NUM_VPS - 1))); do
  VPS_IP=${IPS[$vps_idx]}
  ssh $SSH_OPTS $USER@$VPS_IP \
    "test -f $REMOTE_DIR/runner.sh && cd $REMOTE_DIR && nohup bash runner.sh > logs/runner.log 2>&1 &" &
done
wait

echo "All jobs launched."
echo "Monitor with: ./deploy/monitor.sh"
