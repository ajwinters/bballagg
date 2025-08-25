#!/bin/bash
# 🎯 SUBMIT HIGH PRIORITY COMPREHENSIVE NBA DATA COLLECTION
# This runs comprehensive processing for all 16 high priority endpoints

echo "🚀 SUBMITTING HIGH PRIORITY COMPREHENSIVE NBA DATA COLLECTION"
echo "=============================================================="

# Show what will be processed
echo "📊 High Priority Endpoints (16 total):"
echo "   Game-based (9): BoxScoreAdvancedV3, BoxScoreFourFactorsV3, BoxScoreMiscV3,"
echo "                   BoxScorePlayerTrackV3, BoxScoreScoringV3, BoxScoreSummaryV2,"
echo "                   BoxScoreTraditionalV3, BoxScoreUsageV3, PlayByPlayV3"
echo "   Player-based (5): CommonPlayerInfo, PlayerGameLog, PlayerDashboardByShootingSplits,"
echo "                     PlayerDashboardByClutch, PlayerGameLogs"  
echo "   Team-based (1): CommonTeamRoster"
echo "   League-based (1): LeagueDashPlayerBioStats"

echo ""
echo "📈 Expected Processing:"
echo "   Missing Game IDs: ~2,396 per game-based endpoint"
echo "   Missing Player IDs: ~571 per player-based endpoint"
echo "   Missing Team IDs: ~30 per team-based endpoint"
echo "   Mode: COMPREHENSIVE (all missing IDs per endpoint)"

echo ""
echo "⚙️  SLURM Configuration:"
echo "   Time limit: 12 hours per endpoint"
echo "   Memory: 4GB per job"
echo "   Rate limit: 0.3 seconds between API calls"
echo "   Failed ID tracking: Enabled"

echo ""
echo "🔥 SUBMITTING JOB..."

# Navigate to endpoints directory
cd endpoints

# Submit the job
sbatch deployment/slurm_nba_collection.sh high_priority

echo ""
echo "✅ Job submitted! Monitor with:"
echo "   ./nba_jobs.sh status"
echo "   ./nba_jobs.sh monitor"
