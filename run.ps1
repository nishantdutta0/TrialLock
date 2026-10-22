#!/usr/bin/env pwsh
Write-Host "=== TrialLock Setup ==="

$pythonCmd = "python"
try { & $pythonCmd --version } catch { Write-Host "Python not found"; exit 1 }

try { java -version 2>&1 | Out-Null } catch { Write-Host "WARNING: Java not found. PySpark requires Java 17+ to run." }

if (-not (Test-Path "venv")) {
    Write-Host "Creating virtual environment..."
    & $pythonCmd -m venv venv
}
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
Write-Host "Starting TrialLock..."
python -m streamlit run app.py
