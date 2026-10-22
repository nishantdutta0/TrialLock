#!/usr/bin/env bash
set -e
echo "=== TrialLock Setup ==="
python3 --version || { echo "Python 3 required"; exit 1; }
java -version 2>&1 || echo "WARNING: Java not found. PySpark requires Java."
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi
source venv/bin/activate
pip install -r requirements.txt
echo "Starting TrialLock..."
python -m streamlit run app.py
