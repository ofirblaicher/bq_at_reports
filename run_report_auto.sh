#!/bin/bash
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export HOME="/Users/ofirblaicher"

cd /Users/ofirblaicher/Documents/GitHub/bq_at_reports

# Load secrets from .env
if [ -f .env ]; then
    set -a; source .env; set +a
fi

echo "" | .venv/bin/python3 run_report.py
