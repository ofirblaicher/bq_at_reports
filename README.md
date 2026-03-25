# Alert Triage Daily Report

Connects to BigQuery and Langfuse to generate a daily `.docx` report for an account, and posts it to a Slack channel.

---

## Prerequisites

| Tool | Install |
|------|---------|
| Python 3.10+ | [python.org](https://www.python.org/downloads/) |
| Google Cloud CLI (`gcloud`) | `brew install --cask google-cloud-sdk` |
| Cloudflare Tunnel CLI (`cloudflared`) | `brew install cloudflared` |

---

## One-time Setup

### 1. Clone the repo
```bash
git clone https://github.com/ofirblaicher/bq_at_reports.git
cd bq_at_reports
```

### 2. Create a Python virtual environment
```bash
python3 -m venv .venv
.venv/bin/pip install google-cloud-bigquery pandas db-dtypes
```

### 3. Authenticate with Google Cloud
```bash
gcloud auth application-default login
```
Follow the browser prompt and log in with your Torq Google account.

### 4. Authenticate with Langfuse (Cloudflare Access)
```bash
cloudflared access login https://langfuse.us.torqio.dev
```
A browser window will open — log in with your Torq account. This token is cached and refreshed automatically.

### 5. Set environment variables

Add these to your shell profile (`~/.zshrc` or `~/.bashrc`):
```bash
export LANGFUSE_PUBLIC_KEY="your_public_key"
export LANGFUSE_SECRET_KEY="your_secret_key"
export SLACK_BOT_TOKEN="xoxb-..."   # ask the team for this
export SLACK_CHANNEL="C0AN7770N2H"  # #reports channel ID
```
Then reload: `source ~/.zshrc`

> You can find the Langfuse keys in the Langfuse dashboard under **Settings → API Keys**.

---

## Running Manually

```bash
echo "" | .venv/bin/python3 run_report.py
```

The script will prompt for a **BigQuery Account ID** — press Enter to use the default, or type a different account ID.

The `.docx` report is saved in the project folder and automatically posted to the `#reports` Slack channel.

---

## Scheduled Daily Run (macOS)

To run automatically every day at 10:30 AM (even without opening a terminal):

### 1. Create the launcher script
Save `run_report_auto.sh` in the project folder (already included in the repo). Make it executable:
```bash
chmod +x run_report_auto.sh
```

### 2. Register the launchd job
```bash
cp com.ofirblaicher.triage_report.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.ofirblaicher.triage_report.plist
```

To verify it's registered:
```bash
launchctl list | grep triage_report
```

To unload/disable:
```bash
launchctl unload ~/Library/LaunchAgents/com.ofirblaicher.triage_report.plist
```

> **Note:** Your Mac must be on and logged in at 10:30 AM for the job to run.

---

## Slack Setup

The report is posted to `#reports` using a Slack bot. The bot token and channel ID are already configured in `run_report.py`. If you need to reconfigure:

1. Go to [api.slack.com/apps](https://api.slack.com/apps) → **atreports** app
2. **OAuth & Permissions** → ensure `files:write` is under Bot Token Scopes
3. Copy the Bot OAuth Token (`xoxb-...`)
4. In `run_report.py`, update:
   ```python
   SLACK_BOT_TOKEN = "xoxb-..."
   SLACK_CHANNEL   = "C0AN7770N2H"  # #reports channel ID
   ```
5. Invite the bot to the channel in Slack: `/invite @atreports`

---

## Output Files

| File | Description |
|------|-------------|
| `*_report_YYYY-MM-DD.docx` | Generated report (always for yesterday) |
| `langfuse_traces_*.json` | Raw Langfuse trace export |
| `triage_report.log` | Scheduled run logs |
