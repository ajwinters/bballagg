#!/bin/bash
#SBATCH --job-name=nba_endpoints
#SBATCH --nodes=10
#SBATCH --ntasks=10
#SBATCH --time=24:00:00
#SBATCH --mem=4GB
#SBATCH --array=1-10

# NBA Endpoint Processing - SLURM Array Job
# Each array task processes a different endpoint

# Define endpoints array
endpoints=(
    "BoxScoreAdvancedV3"
    "BoxScoreFourFactorsV3" 
    "BoxScorePlayerTrackV3"
    "BoxScoreMiscV3"
    "PlayerGameLogs"
    "PlayerCareerStats"
    "TeamGameLogs"
    "TeamDashboardByGeneralSplits"
    "BoxScoreMatchupsV3"
    "BoxScoreTraditionalV3"
)

# Get endpoint for this array task
endpoint=${endpoints[$((SLURM_ARRAY_TASK_ID - 1))]}

# Load required modules
module load python/3.12
source /path/to/your/venv/bin/activate

# Set database credentials
export DB_HOST="your-rds-instance.amazonaws.com"
export DB_NAME="thebigone"
export DB_USER="ajwin"
export DB_PASSWORD="your_password"
export DB_PORT="5432"

# Run the processor
echo "Starting processing for endpoint: $endpoint on node: $SLURM_NODEID"
python collectors/endpoint_processor.py \
    --endpoint "$endpoint" \
    --node-id "slurm_node_${SLURM_ARRAY_TASK_ID}" \
    --rate-limit 0.3

echo "Completed processing for endpoint: $endpoint"
