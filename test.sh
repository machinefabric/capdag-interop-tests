#!/bin/bash
set -e  # Exit on error

LOGFILE="test.log"
LANGS=${1:-rust,swift,go,python}

echo "========================================"
echo "Test run started: $(date)"
echo "Languages: $LANGS"
echo "Logging to: $LOGFILE"
echo "========================================"

clear

echo "[test.sh] Setting PYTHON_EXECUTABLE..."
export PYTHON_EXECUTABLE=/opt/homebrew/Caskroom/miniforge/base/envs/machinefabric/bin/python

echo "[test.sh] Starting pytest..."
echo "[test.sh] Command: python -m pytest -xvs -s --langs $LANGS --clear"
echo ""

# Redirect all output to both terminal and log file
exec > >(tee "$LOGFILE") 2>&1

# Run pytest
python -u -m pytest -xvs -s --langs "$LANGS" --clear
EXIT_CODE=$?

echo ""
echo "========================================"
echo "Test run finished: $(date)"
echo "Exit code: $EXIT_CODE"
echo "========================================"

# Display throughput matrix if it exists
if [ -f "artifacts/throughput_matrix.json" ]; then
    echo ""
    python3 show_throughput_matrix.py
fi

exit $EXIT_CODE
