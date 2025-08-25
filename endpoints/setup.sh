#!/bin/bash
# Quick setup for NBA data collection

echo "NBA Data Collection - Quick Setup"
echo "================================="

# Create required directories
mkdir -p logs scripts deployment

# Make scripts executable  
chmod +x deployment/*.sh
chmod +x *.sh 2>/dev/null || true
chmod +x scripts/*.py 2>/dev/null || true

# Check for root virtual environment
PROJECT_ROOT="$(cd .. && pwd)"
if [ -d "$PROJECT_ROOT/.venv" ]; then
    echo "✅ Found virtual environment at $PROJECT_ROOT/.venv"
else
    echo "❌ Virtual environment not found at $PROJECT_ROOT/.venv"
    echo "Please create and activate a virtual environment in the project root:"
    echo "  cd $PROJECT_ROOT"
    echo "  python -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  pip install -r requirements.txt"
    exit 1
fi

# Create database config template if it doesn't exist
if [ ! -f "config/database_config.json" ]; then
    cat > config/database_config.json << 'EOF'
{
    "host": "your-rds-endpoint.amazonaws.com",
    "name": "your_database_name", 
    "user": "your_username",
    "password": "your_password",
    "port": "5432"
}
EOF
    echo "✅ Created database config template: config/database_config.json"
    echo "⚠️  Please edit this file with your actual database credentials"
else
    echo "✅ Database config already exists"
fi

echo ""
echo "Setup complete! Usage:"
echo ""
echo "1. Edit config/database_config.json with your database credentials"
echo "2. Submit a test job: ./nba_jobs.sh submit test"
echo "3. Check status: ./nba_jobs.sh status"
echo ""
echo "Environment:"
echo "  Project root: $PROJECT_ROOT"
echo "  Virtual env: $PROJECT_ROOT/.venv"
echo ""
echo "Available profiles:"
./nba_jobs.sh profiles 2>/dev/null || echo "Run './nba_jobs.sh profiles' to see available profiles"