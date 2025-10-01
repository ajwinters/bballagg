#!/bin/bash
# NBA Data Collection - Linux Cluster Setup Script
# Run this script on the Linux cluster to set up the environment

echo "🚀 NBA Data Collection - Linux Cluster Setup"
echo "============================================"

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: requirements.txt not found"
    echo "Please run this script from the project root directory"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1)
echo "📋 Python version: $PYTHON_VERSION"

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "🏗️  Creating virtual environment..."
    python3 -m venv .venv
    
    if [ $? -eq 0 ]; then
        echo "✅ Virtual environment created successfully"
    else
        echo "❌ Failed to create virtual environment"
        exit 1
    fi
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source .venv/bin/activate

if [ $? -eq 0 ]; then
    echo "✅ Virtual environment activated"
else
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

# Upgrade pip
echo "📦 Upgrading pip..."
python -m pip install --upgrade pip

# Install requirements
echo "📥 Installing requirements..."
python -m pip install -r requirements.txt

if [ $? -eq 0 ]; then
    echo "✅ Requirements installed successfully"
else
    echo "❌ Failed to install requirements"
    exit 1
fi

# Test imports
echo "🧪 Testing imports..."
python -c "
import nba_api.stats.endpoints as endpoints
import pandas as pd
import psycopg2
print('✅ All packages imported successfully')
print(f'   • Pandas: {pd.__version__}')
print('   • NBA API endpoints loaded')
print('   • PostgreSQL driver ready')
"

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 SETUP COMPLETE!"
    echo "================================"
    echo "Your Linux cluster environment is ready for NBA data collection."
    echo ""
    echo "To use the environment:"
    echo "  source .venv/bin/activate"
    echo ""
    echo "To run batch jobs:"
    echo "  ./batching/submit_nba_jobs.sh test"
    echo "  ./batching/submit_nba_jobs.sh production"
else
    echo "❌ Import test failed - please check the installation"
    exit 1
fi