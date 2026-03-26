#!/bin/bash
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export HOME="/Users/ofirblaicher"

cd /Users/ofirblaicher/Documents/GitHub/bq_at_reports

# Load secrets from .env
if [ -f .env ]; then
    set -a; source .env; set +a
fi

# Only run once per day — skip if yesterday's report already exists
YESTERDAY=$(date -v-1d +%Y-%m-%d)
if ls *_report_${YESTERDAY}.docx 1>/dev/null 2>&1; then
    echo "Report for ${YESTERDAY} already exists, skipping."
    exit 0
fi

echo "" | .venv/bin/python3 run_report.py
