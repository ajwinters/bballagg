# NBA Data Collection - SLURM Deployment

## Quick Setup

1. **Ensure virtual environment exists in project root**:
   ```bash
   cd /path/to/thebigone
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure database credentials**:
   ```bash
   cd endpoints
   ./setup.sh
   nano config/database_config.json
   ```

3. **Test deployment**:
   ```bash
   ./nba_jobs.sh submit test
   ```

## Environment Setup

- **Virtual Environment**: Uses `../venv/` (project root virtual environment)
- **No module loading required**: SLURM script automatically activates the root venv
- **Simplified approach**: No need to customize cluster modules

## Available Profiles

- `test` - 3 endpoints, 2 hours (BoxScoreAdvancedV3, BoxScoreTraditionalV2, LeagueDashPlayerBioStats)
- `test_player_logs` - PlayerGameLogs endpoint only, 1 hour  
- `high_priority` - All high priority endpoints, 12 hours
- `full` - All endpoints, 24 hours

## Usage Commands

```bash
# Submit jobs
./nba_jobs.sh submit test
./nba_jobs.sh submit high_priority

# Monitor progress
./nba_jobs.sh status
./nba_jobs.sh logs <job_id>

# Cancel jobs
./nba_jobs.sh cancel <job_id>

# View profiles
./nba_jobs.sh profiles
```

## How It Works

1. **Job Submission**: `nba_jobs.sh submit <profile>` submits the SLURM script
2. **Dynamic Configuration**: Script reads `config/run_config.json` for endpoint lists
3. **Array Jobs**: Automatically creates array job with one task per endpoint
4. **Environment**: Activates `../.venv/` from project root
5. **Processing**: Each task processes one complete endpoint (all its dataframes)

## Troubleshooting

- **Environment issues**: Ensure `../.venv/` exists with required packages
- **Database errors**: Check `config/database_config.json` credentials
- **Column name errors**: Fixed with reserved keyword mapping (`to` → `turnovers`)
- **Parameter errors**: Multi-parameter endpoints (like PlayerGameLogs) now supported

## File Structure

```
endpoints/
├── collectors/          # Processing code
├── config/             # Configuration files
├── deployment/         # SLURM scripts
├── logs/              # Job logs
├── scripts/           # Helper scripts
├── nba_jobs.sh        # Job management script
└── setup.sh           # One-time setup
```
