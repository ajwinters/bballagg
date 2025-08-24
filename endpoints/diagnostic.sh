#!/bin/bash
# NBA Deployment Diagnostic Script

echo "=== NBA Deployment Diagnostic ==="
echo "Time: $(date)"
echo "User: $(whoami)"
echo "Working Directory: $(pwd)"
echo

echo "=== File Existence Check ==="
files=(
    "nba_jobs.sh"
    "deployment/slurm_nba_collection.sh" 
    "scripts/get_endpoints.py"
    "collectors/single_endpoint_processor.py"
    "config/database_config.json"
    "config/run_config.json"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file MISSING"
    fi
done
echo

echo "=== Permission Check ==="
echo "nba_jobs.sh: $(ls -la nba_jobs.sh | cut -d' ' -f1)"
echo "slurm script: $(ls -la deployment/slurm_nba_collection.sh | cut -d' ' -f1)"
echo

echo "=== Directory Check ==="
echo "logs/ directory: $(ls -ld logs/ 2>/dev/null || echo 'MISSING')"
echo "config/ directory: $(ls -ld config/ 2>/dev/null || echo 'MISSING')"
echo

echo "=== Python Environment ==="
python --version 2>/dev/null || echo "❌ Python not available"
which python
echo

echo "=== Test Endpoint Retrieval ==="
if python scripts/get_endpoints.py test 2>/dev/null; then
    echo "✅ Endpoint retrieval working"
else
    echo "❌ Endpoint retrieval failed"
    python scripts/get_endpoints.py test
fi
echo

echo "=== SLURM Environment ==="
which sbatch 2>/dev/null || echo "❌ sbatch not found"
which squeue 2>/dev/null || echo "❌ squeue not found"
echo

echo "=== Recent Job History ==="
sacct -u $(whoami) --format=JobID,JobName,State,ExitCode,Submit -S today 2>/dev/null || echo "No job history available"
echo

echo "=== Test Manual SLURM Submission ==="
echo "Testing: sbatch --test-only deployment/slurm_nba_collection.sh test"
sbatch --test-only deployment/slurm_nba_collection.sh test 2>&1 || echo "❌ SLURM test failed"
echo

echo "=== Diagnostic Complete ==="
