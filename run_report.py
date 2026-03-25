"""
Alert Triage Daily Report — Multi-Query Edition
================================================
Connects to BigQuery via Application Default Credentials (gcloud CLI),
runs 12 alert-triage queries for YESTERDAY, and generates a styled HTML report.

Usage:
    python run_report.py

Requirements:
    pip install google-cloud-bigquery pandas db-dtypes

Authentication (one-time):
    gcloud auth application-default login
"""

import os
import re
import sys
import json
import subprocess
import tempfile
import urllib.parse
from datetime import datetime, timedelta, timezone, time
from typing import Optional, Any

# Load .env file if present (no external dependencies needed)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

try:
    from google.cloud import bigquery
    import pandas as pd
except ImportError:
    print("Missing dependencies.  Run:  pip install google-cloud-bigquery pandas db-dtypes")
    sys.exit(1)

# ───────────────────────────── Config ────────────────────────────────────────
PROJECT_ID = "stackpulse-production"
BQ_CONN    = "stackpulse-production.us.alert-triage"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EXTERNAL_SQL_FILE = os.path.join(OUTPUT_DIR, "bigquery_external_queries.sql")
DEFAULT_LANGFUSE_BASE_URL = "https://langfuse.us.torqio.dev"

# ───────────────────────────── Dates (always yesterday) ──────────────────────
TODAY     = datetime.now().date()
YESTERDAY = TODAY - timedelta(days=1)

# Two flavours: queries that fully qualify the table vs. bare column names
DATE_RANGE_QUALIFIED = (
    f"alert_triage.alerts.created_at >= '{YESTERDAY}'::timestamp "
    f"AND alert_triage.alerts.created_at < '{TODAY}'::timestamp"
)
DATE_RANGE_BARE = (
    f"created_at >= '{YESTERDAY}'::timestamp "
    f"AND created_at < '{TODAY}'::timestamp"
)

# ───────────────────────────── Severity palette ──────────────────────────────
SEV = {
    "Critical":      {"bg": "#FEE2E2", "fg": "#B91C1C", "bar": "#B91C1C"},
    "High":          {"bg": "#FFEDD5", "fg": "#C2410C", "bar": "#C2410C"},
    "Medium":        {"bg": "#FEF9C3", "fg": "#A16207", "bar": "#A16207"},
    "Low":           {"bg": "#DCFCE7", "fg": "#166534", "bar": "#166534"},
    "Informational": {"bg": "#DBEAFE", "fg": "#1D4ED8", "bar": "#1D4ED8"},
    "Unspecified":   {"bg": "#F1F5F9", "fg": "#475569", "bar": "#475569"},
}

# ───────────────────────────── SQL Templates (Metabase syntax) ───────────────

_SQL = {}

_SQL["alert_processing_funnel"] = """
WITH funnel_data AS (
    SELECT
        COUNT(*) AS total_alerts,
        COUNT(*) FILTER (WHERE alert_triage.alerts.processed_at IS NOT NULL) AS alerts_processed,
        COUNT(*) FILTER (WHERE alert_triage.alerts.verdict IS NOT NULL) AS alerts_with_verdict,
        COUNT(*) FILTER (WHERE EXISTS (
            SELECT 1 FROM alert_triage.enrichments
            WHERE alert_triage.enrichments.alert_id = alert_triage.alerts.id
              AND alert_triage.enrichments.account_id = alert_triage.alerts.account_id
        )) AS alerts_with_enrichment,
        COUNT(*) FILTER (WHERE alert_triage.alerts.case_id IS NOT NULL) AS alerts_with_case
    FROM alert_triage.alerts
    WHERE {{date_range}}
      AND alert_triage.alerts.account_id::text = {{account_id}}
      [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
      [[AND {{verdict}}]]
      [[AND {{severity}}]]
      [[AND {{source}}]]
)
SELECT 'Total Alerts' AS step, 1 AS step_order, total_alerts AS count FROM funnel_data
UNION ALL SELECT 'Processed',    2, alerts_processed        FROM funnel_data
UNION ALL SELECT 'With Verdict', 3, alerts_with_verdict     FROM funnel_data
UNION ALL SELECT 'Enriched',     4, alerts_with_enrichment  FROM funnel_data
UNION ALL SELECT 'Created Case', 5, alerts_with_case        FROM funnel_data
ORDER BY step_order
"""

_SQL["alerts_by_severity"] = """
SELECT
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    alert_triage.alerts.severity_level,
    CASE alert_triage.alerts.severity_level
        WHEN 500 THEN 'Critical'
        WHEN 400 THEN 'High'
        WHEN 300 THEN 'Medium'
        WHEN 200 THEN 'Low'
        WHEN 100 THEN 'Informational'
        ELSE 'Unspecified'
    END AS severity_name,
    DATE_TRUNC('day', alert_triage.alerts.created_at) AS date,
    COUNT(*) AS alert_count
FROM alert_triage.alerts
WHERE {{date_range}}
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    alert_triage.alerts.severity_level,
    DATE_TRUNC('day', alert_triage.alerts.created_at)
ORDER BY date DESC, alert_triage.alerts.severity_level DESC
"""

_SQL["alerts_by_source"] = """
SELECT
    organization_id,
    account_id,
    source,
    DATE_TRUNC('hour', created_at) AS date,
    COUNT(*) AS alert_count
FROM alert_triage.alerts
WHERE {{date_range}}
  [[AND organization_id::text = {{organization_id}}]]
  [[AND account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY organization_id, account_id, source, DATE_TRUNC('hour', created_at)
ORDER BY date ASC, alert_count DESC
"""

_SQL["alerts_by_verdict"] = """
SELECT
    DATE_TRUNC('day', alert_triage.alerts.created_at) AS date,
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    alert_triage.alerts.verdict,
    COUNT(*) AS alert_count
FROM alert_triage.alerts
WHERE {{date_range}}
  AND alert_triage.alerts.verdict IS NOT NULL
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY
    DATE_TRUNC('day', alert_triage.alerts.created_at),
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    alert_triage.alerts.verdict
ORDER BY date ASC, alert_count DESC
"""

_SQL["cases_generated"] = """
SELECT
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    COUNT(DISTINCT alert_triage.alerts.case_id) AS total_cases_generated,
    COUNT(*) FILTER (WHERE alert_triage.alerts.case_id IS NOT NULL) AS alerts_with_cases,
    COUNT(*) AS total_alerts,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE alert_triage.alerts.case_id IS NOT NULL) / NULLIF(COUNT(*), 0),
        2
    ) AS case_generation_rate_pct
FROM alert_triage.alerts
WHERE {{date_range}}
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY alert_triage.alerts.organization_id, alert_triage.alerts.account_id
"""

_SQL["false_positive_ratio"] = """
SELECT
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    COUNT(*) FILTER (WHERE alert_triage.alerts.verdict = 'FalsePositive') AS false_positive_count,
    COUNT(*) AS total_alerts,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE alert_triage.alerts.verdict = 'FalsePositive') / NULLIF(COUNT(*), 0),
        2
    ) AS false_positive_ratio_pct
FROM alert_triage.alerts
WHERE {{date_range}}
  AND alert_triage.alerts.verdict IS NOT NULL
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY alert_triage.alerts.organization_id, alert_triage.alerts.account_id
ORDER BY false_positive_ratio_pct DESC
"""

_SQL["human_feedback"] = """
SELECT
    id AS alert_id,
    pretty_id AS alert_pretty_id,
    name,
    source,
    triage_verdict       AS original_verdict,
    verdict              AS current_verdict,
    triage_confirmation,
    human_verified_at,
    human_verified_by,
    human_comment
FROM alert_triage.alerts
WHERE {{date_range}}
  [[AND organization_id::text = {{organization_id}}]]
  [[AND account_id::text = {{account_id}}]]
  [[AND {{source}}]]
  AND triage_confirmation IN ('Confirmed', 'Declined')
ORDER BY created_at DESC
"""

_SQL["mean_time_to_triage"] = """
SELECT
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    AVG(
        EXTRACT(EPOCH FROM (alert_triage.alerts.processed_at - alert_triage.alerts.created_at)) / 60
    ) AS mttt_minutes
FROM alert_triage.alerts
WHERE {{date_range}}
  AND alert_triage.alerts.processed_at IS NOT NULL
  AND alert_triage.alerts.verdict IS NOT NULL
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY alert_triage.alerts.organization_id, alert_triage.alerts.account_id
"""

_SQL["rule_effectiveness"] = """
SELECT
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    alert_triage.rules.id          AS rule_id,
    alert_triage.rules.name        AS rule_name,
    alert_triage.rules.type        AS rule_type,
    alert_triage.rules.priority,
    alert_triage.rules.enabled,
    COUNT(DISTINCT alert_triage.alerts.id) AS matched_alerts,
    COUNT(DISTINCT alert_triage.alerts.verdict) AS unique_verdicts,
    ROUND(AVG(alert_triage.alerts.severity_level), 0) AS avg_severity_level,
    COUNT(DISTINCT alert_triage.alerts.id) FILTER (WHERE alert_triage.alerts.triage_confirmation = 'Confirmed') AS confirmed_count,
    COUNT(DISTINCT alert_triage.alerts.id) FILTER (WHERE alert_triage.alerts.triage_confirmation = 'Declined')  AS declined_count,
    ROUND(
        100.0 * COUNT(DISTINCT alert_triage.alerts.id) FILTER (WHERE alert_triage.alerts.triage_confirmation = 'Confirmed') /
        NULLIF(COUNT(DISTINCT alert_triage.alerts.id) FILTER (WHERE alert_triage.alerts.triage_confirmation IN ('Confirmed', 'Declined')), 0),
        2
    ) AS accuracy_pct
FROM alert_triage.alerts
INNER JOIN alert_triage.rules ON alert_triage.rules.id = ANY(alert_triage.alerts.matched_rule_ids)
WHERE {{date_range}}
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    alert_triage.rules.id,
    alert_triage.rules.name,
    alert_triage.rules.type,
    alert_triage.rules.priority,
    alert_triage.rules.enabled
ORDER BY matched_alerts DESC
"""

_SQL["triggers_generated"] = """
SELECT
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    COUNT(DISTINCT alert_triage.alerts.id) FILTER (WHERE alert_triage.rules.type = 'VERDICT_BASED_ACTION') AS total_triggers,
    COUNT(DISTINCT alert_triage.alerts.id) AS total_alerts_with_rules,
    ROUND(
        100.0 * COUNT(DISTINCT alert_triage.alerts.id) FILTER (WHERE alert_triage.rules.type = 'VERDICT_BASED_ACTION') /
        NULLIF(COUNT(DISTINCT alert_triage.alerts.id), 0),
        2
    ) AS trigger_rate_pct
FROM alert_triage.alerts
INNER JOIN alert_triage.rules ON alert_triage.rules.id = ANY(alert_triage.alerts.matched_rule_ids)
WHERE {{date_range}}
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY alert_triage.alerts.organization_id, alert_triage.alerts.account_id
"""

_SQL["triggers_over_time"] = """
SELECT
    DATE_TRUNC('day', alert_triage.alerts.created_at) AS date,
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id,
    COUNT(DISTINCT alert_triage.alerts.id) FILTER (WHERE alert_triage.rules.type = 'VERDICT_BASED_ACTION') AS triggers_generated
FROM alert_triage.alerts
INNER JOIN alert_triage.rules ON alert_triage.rules.id = ANY(alert_triage.alerts.matched_rule_ids)
WHERE {{date_range}}
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY
    DATE_TRUNC('day', alert_triage.alerts.created_at),
    alert_triage.alerts.organization_id,
    alert_triage.alerts.account_id
ORDER BY date ASC
"""

_SQL["user_feedback_distribution"] = """
SELECT
    CASE
        WHEN alert_triage.alerts.triage_confirmation = 'Confirmed' THEN 'Confirmed'
        WHEN alert_triage.alerts.triage_confirmation = 'Declined' AND alert_triage.alerts.verdict = 'FalsePositive'  THEN 'Changed to False Positive'
        WHEN alert_triage.alerts.triage_confirmation = 'Declined' AND alert_triage.alerts.verdict = 'TruePositive'   THEN 'Changed to True Positive'
        WHEN alert_triage.alerts.triage_confirmation = 'Declined' AND alert_triage.alerts.verdict = 'Suspicious'     THEN 'Changed to Suspicious'
        WHEN alert_triage.alerts.triage_confirmation = 'Declined' AND alert_triage.alerts.verdict = 'Benign'         THEN 'Changed to Benign'
        WHEN alert_triage.alerts.triage_confirmation = 'Declined' THEN 'Changed to Other'
        ELSE 'No Feedback'
    END AS feedback_action,
    COUNT(*) AS alert_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM alert_triage.alerts
WHERE {{date_range}}
  AND alert_triage.alerts.triage_verdict IS NOT NULL
  [[AND alert_triage.alerts.organization_id::text = {{organization_id}}]]
  [[AND alert_triage.alerts.account_id::text = {{account_id}}]]
  [[AND {{verdict}}]]
  [[AND {{severity}}]]
  [[AND {{source}}]]
GROUP BY feedback_action
ORDER BY alert_count DESC
"""

# ───────────────────────────── Query metadata ────────────────────────────────
# date_range: "qualified" → uses alert_triage.alerts.created_at prefix
#             "bare"      → uses bare created_at column name
QUERY_META = [
    {"key": "alerts_by_severity",        "title": "Alerts by Severity",          "date_range": "qualified"},
    {"key": "alert_processing_funnel",   "title": "Alert Processing Funnel",     "date_range": "qualified"},
    {"key": "mean_time_to_triage",       "title": "Mean Time to Triage",         "date_range": "qualified"},
    {"key": "false_positive_ratio",      "title": "False Positive Ratio",        "date_range": "qualified"},
    {"key": "cases_generated",           "title": "Cases Generated from Alerts", "date_range": "qualified"},
    {"key": "triggers_generated",        "title": "Triggers Generated",          "date_range": "qualified"},
    {"key": "alerts_by_source",          "title": "Alerts by Source Over Time",  "date_range": "bare"},
    {"key": "alerts_by_verdict",         "title": "Alerts by Verdict Over Time", "date_range": "qualified"},
    {"key": "triggers_over_time",        "title": "Triggers Over Time",          "date_range": "qualified"},
    {"key": "rule_effectiveness",        "title": "Rule Effectiveness",          "date_range": "qualified"},
    {"key": "human_feedback",            "title": "Human Feedback",              "date_range": "bare"},
    {"key": "user_feedback_distribution","title": "User Feedback Distribution",  "date_range": "qualified"},
]

# ───────────────────────────── SQL transform ─────────────────────────────────

def transform_sql(sql: str, account_id: str, date_range_type: str) -> str:
    """
    Convert a Metabase-style SQL template to runnable PostgreSQL:
    1. Replace {{date_range}} with yesterday's date filter
    2. Promote [[AND ...account_id...]] optional blocks → required AND
    3. Strip all remaining [[ ]] optional blocks
    4. Replace {{account_id}} with the quoted value
    """
    date_range = DATE_RANGE_QUALIFIED if date_range_type == "qualified" else DATE_RANGE_BARE

    # 1. Date range
    sql = sql.replace("{{date_range}}", date_range)

    # 2. Promote account_id optional block to required
    sql = re.sub(
        r"\[\[(AND\s+(?:alert_triage\.alerts\.)?account_id::text\s*=\s*\{\{account_id\}\})\]\]",
        r"\1",
        sql,
        flags=re.IGNORECASE,
    )

    # 3. Remove all remaining optional blocks (organization_id, verdict, severity, source)
    sql = re.sub(r"\[\[.*?\]\]", "", sql, flags=re.DOTALL)

    # 4. Replace account_id variable
    sql = sql.replace("{{account_id}}", f"'{account_id}'")

    # Clean up blank lines
    sql = re.sub(r"\n[ \t]*\n", "\n", sql).strip().rstrip(";")
    return sql


def wrap_external_query(inner_sql: str) -> str:
    """Wrap a PostgreSQL query in BigQuery EXTERNAL_QUERY."""
    # Escape any accidental triple-quote sequences
    safe = inner_sql.replace('"""', "'''")
    return f'SELECT * FROM EXTERNAL_QUERY(\n  "{BQ_CONN}",\n  """\n{safe}\n  """\n) LIMIT 1000'


def _split_sql_statements(sql_blob: str) -> list[str]:
    """
    Split a SQL script into standalone statements by semicolon.
    This keeps semicolons inside single/double quoted strings intact.
    """
    statements = []
    current = []
    in_single = False
    in_double = False
    escaped = False

    for ch in sql_blob:
        current.append(ch)
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if ch == ";" and not in_single and not in_double:
            stmt = "".join(current).strip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            current = []

    tail = "".join(current).strip().rstrip(";").strip()
    if tail:
        statements.append(tail)
    return statements


def _inject_account_id(sql: str, account_id: str) -> str:
    """Replace account_id filters/placeholders in external SQL."""
    escaped_account = account_id.replace("'", "''")
    sql = sql.replace("{{account_id}}", f"'{escaped_account}'")
    sql = re.sub(
        r"(\baccount_id(?:::text)?\s*=\s*)'[^']*'",
        rf"\1'{escaped_account}'",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _inject_yesterday_timeframe(sql: str) -> str:
    """
    Force external SQL files to use yesterday's full-day window:
      00:00:00 -> 23:59:59
    Replaces common CURRENT_DATE window patterns on created_at columns.
    """
    start_ts = f"{YESTERDAY} 00:00:00"
    end_ts = f"{YESTERDAY} 23:59:59"
    pattern = re.compile(
        r"(?P<col>(?:[a-zA-Z_][\w]*\.)*created_at)\s*>=\s*CURRENT_DATE\s*"
        r"AND\s*(?P=col)\s*<\s*CURRENT_DATE\s*\+\s*INTERVAL\s*'1\s*day'",
        flags=re.IGNORECASE,
    )
    return pattern.sub(
        rf"\g<col> >= '{start_ts}'::timestamp AND \g<col> <= '{end_ts}'::timestamp",
        sql,
    )


def _extract_numbered_queries(sql_blob: str) -> list[dict]:
    """
    Extract query blocks in the form:
      -- N) Title
      <statement ending with ;>
    Returns [{"number": int, "title": str, "statement": str}, ...]
    """
    pattern = re.compile(
        r"--\s*(\d+)\)\s*(.+?)\n(.*?;)(?=\s*--\s*\d+\)|\s*$)",
        flags=re.DOTALL,
    )
    items = []
    for m in pattern.finditer(sql_blob):
        num = int(m.group(1))
        title = m.group(2).strip()
        stmt = m.group(3).strip().rstrip(";").strip()
        if stmt:
            items.append({"number": num, "title": title, "statement": stmt})
    return items


def _query_key_from_title(title: str) -> Optional[str]:
    title_map = {
        "alert processing funnel": "alert_processing_funnel",
        "alerts by severity": "alerts_by_severity",
        "alerts by source over time": "alerts_by_source",
        "alerts by verdict over time": "alerts_by_verdict",
        "cases generated from alerts": "cases_generated",
        "false positive ratio": "false_positive_ratio",
        "human feedback": "human_feedback",
        "mean time to triage": "mean_time_to_triage",
        "rule effectiveness": "rule_effectiveness",
        "triggers generated": "triggers_generated",
        "triggers over time": "triggers_over_time",
        "user feedback distribution": "user_feedback_distribution",
    }
    normalized = re.sub(r"\s+", " ", title.strip().lower())
    return title_map.get(normalized)


def _load_env_file(path: str) -> None:
    """Load KEY=VALUE pairs from .env into process env if unset."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def _parse_langfuse_source(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    if s.upper().startswith("ALERT_SOURCE_"):
        s = s[len("ALERT_SOURCE_") :]
    return s.replace("_", " ").title()


def _extract_host_from_alert_name(name: str) -> str:
    text = str(name or "")
    if " on " in text:
        return text.rsplit(" on ", 1)[-1].strip()
    return ""


def _env_name_from_user_id(user_id: str) -> str:
    """
    Convert Langfuse userId like 'Hard Rock/SHRSS' into report env name.
    Prefer the organization/company prefix before '/'.
    """
    raw = str(user_id or "").strip()
    if not raw:
        return ""
    if "/" in raw:
        left = raw.split("/", 1)[0].strip()
        if left:
            return left
    return raw


def _normalize_primary_assessment(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    compact = re.sub(r"\s+", " ", raw).strip()
    aliases = {
        "true positive – benign": "Anomalous but Benign",
        "true positive - benign": "Anomalous but Benign",
        "anomalous but benign": "Anomalous but Benign",
        "confirmed malicious": "Confirmed Malicious",
        "high-conf. suspicious": "High-Conf. Suspicious",
        "high-confidence suspicious": "High-Conf. Suspicious",
        "policy violation": "Policy Violation",
        "bas (security simulation classification)": "BAS / Simulation",
        "bas(security simulation classification)": "BAS / Simulation",
        "bas / simulation": "BAS / Simulation",
    }
    return aliases.get(compact.lower(), compact)


def _normalize_trace_severity(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    if raw.upper().startswith("ALERT_SEVERITY_"):
        raw = raw[len("ALERT_SEVERITY_") :]
    return raw.replace("_", " ").title()


def _trace_matches_account(trace: dict, account_id: str) -> bool:
    checks = [
        trace.get("metadata", {}).get("account_id"),
        trace.get("output", {}).get("accountId"),
        trace.get("input", {}).get("Workspace", {}).get("ID"),
        trace.get("input", {}).get("workspace", {}).get("ID"),
    ]
    if any(str(v) == account_id for v in checks if v is not None):
        return True

    haystack = json.dumps(
        {
            "metadata": trace.get("metadata", {}),
            "input_workspace": trace.get("input", {}).get("Workspace", {}),
            "input_workspace_lower": trace.get("input", {}).get("workspace", {}),
            "output": trace.get("output", {}),
        },
        ensure_ascii=True,
        default=str,
    )
    return account_id in haystack


def _fetch_langfuse_traces_for_account(
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
    user_ids: Optional[set[str]] = None,
) -> tuple[list[dict], Optional[str]]:
    """
    Fetch all matching traces from Langfuse Public API for [from_dt, to_dt].
    Returns (traces, error_message).
    """
    base_url = os.environ.get("LANGFUSE_BASE_URL", DEFAULT_LANGFUSE_BASE_URL).rstrip("/")
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "").strip()
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "").strip()
    if not public_key or not secret_key:
        return [], "LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY are not set."

    cf_token = os.environ.get("CF_ACCESS_TOKEN", "").strip()
    if not cf_token:
        token_cmd = [
            "cloudflared",
            "access",
            "token",
            "-app",
            base_url,
        ]
        token_run = subprocess.run(token_cmd, capture_output=True, text=True)
        if token_run.returncode != 0:
            msg = token_run.stderr.strip() or token_run.stdout.strip() or "cloudflared token command failed"
            return [], f"Cloudflare token unavailable: {msg}"
        cf_token = token_run.stdout.strip()
        if cf_token:
            os.environ["CF_ACCESS_TOKEN"] = cf_token

    if not cf_token:
        return [], "CF_ACCESS_TOKEN is empty."

    all_matches = []
    seen_ids = set()
    page = 1
    limit = 100
    while page <= 100:
        params = urllib.parse.urlencode(
            {
                "page": page,
                "limit": limit,
                "fromTimestamp": from_dt.isoformat(),
                "toTimestamp": to_dt.isoformat(),
            }
        )
        url = f"{base_url}/api/public/traces?{params}"
        cmd = [
            "curl",
            "-sS",
            "-H",
            f"CF-Access-Token: {cf_token}",
            "-u",
            f"{public_key}:{secret_key}",
            url,
        ]
        run = subprocess.run(cmd, capture_output=True, text=True)
        if run.returncode != 0:
            err = run.stderr.strip() or run.stdout.strip() or "curl failed"
            return [], f"Langfuse traces API call failed: {err}"

        try:
            payload = json.loads(run.stdout or "{}")
        except json.JSONDecodeError:
            return [], "Langfuse traces API returned non-JSON response."

        if isinstance(payload, dict) and payload.get("message") and not payload.get("data"):
            return [], f"Langfuse traces API error: {payload.get('message')}"

        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not rows:
            break

        for trace in rows:
            trace_id = str(trace.get("id") or "")
            if not trace_id or trace_id in seen_ids:
                continue
            seen_ids.add(trace_id)
            if not _trace_matches_account(trace, account_id):
                continue
            if user_ids:
                trace_user = str(trace.get("userId") or "").strip()
                if trace_user not in user_ids:
                    continue
            all_matches.append(trace)

        if len(rows) < limit:
            break
        page += 1

    return all_matches, None


def _list_langfuse_user_ids_for_account(
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
) -> tuple[list[tuple[str, int]], Optional[str]]:
    """
    Return distinct Langfuse trace user IDs for the account and their counts.
    """
    traces, err = _fetch_langfuse_traces_for_account(account_id, from_dt, to_dt, user_ids=None)
    if err:
        return [], err

    counts = {}
    for trace in traces:
        user_id = str(trace.get("userId") or "").strip()
        if not user_id:
            user_id = "(empty)"
        counts[user_id] = counts.get(user_id, 0) + 1

    ordered = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return ordered, None


def _derive_metrics_from_langfuse_traces(
    traces: list[dict], account_id: str, report_date: str
) -> dict:
    """
    Derive report metrics from Langfuse AlertTriagePipeline traces.
    These values are used to enrich/override report sections.
    """
    pipeline = [t for t in traces if str(t.get("name", "")) == "AlertTriagePipeline"]
    alerts = []
    for trace in pipeline:
        output = trace.get("output") or {}
        verdict_obj = output.get("verdict") or {}
        alert_name = str(output.get("name") or trace.get("name") or "")
        source = _parse_langfuse_source(output.get("source") or trace.get("input", {}).get("Source"))
        final_decision = str(verdict_obj.get("final_decision") or "").strip()
        verdict = str(verdict_obj.get("verdict") or "").strip()
        severity = _normalize_trace_severity(output.get("severity") or verdict_obj.get("severity"))
        if not final_decision:
            # Fallback when final_decision is missing in some traces.
            final_decision = verdict
        primary_assessment = _normalize_primary_assessment(str(verdict_obj.get("primary_assessment") or ""))
        host = _extract_host_from_alert_name(alert_name)
        alert_type = str(verdict_obj.get("alert_type") or alert_name).strip()
        alerts.append(
            {
                "trace_id": trace.get("id"),
                "timestamp": trace.get("timestamp"),
                "alert_name": alert_name,
                "source": source,
                "final_decision": final_decision,
                "severity": severity,
                "verdict": verdict,
                "primary_assessment": primary_assessment,
                "host": host,
                "alert_type": alert_type,
            }
        )

    total = len(alerts)

    decision_counts = {"Escalate Immediately": 0, "Escalate for Review": 0, "Close": 0}
    primary_counts = {
        "Confirmed Malicious": 0,
        "High-Conf. Suspicious": 0,
        "Anomalous but Benign": 0,
        "Policy Violation": 0,
        "BAS / Simulation": 0,
    }
    source_counts = {}
    severity_counts = {}
    verdict_counts = {}
    host_counts = {}
    host_type_counts = {}
    host_decision_counts = {}
    host_severity_counts = {}

    user_counts = {}
    for trace in pipeline:
        uid = str(trace.get("userId") or "").strip()
        if uid:
            user_counts[uid] = user_counts.get(uid, 0) + 1

    for alert in alerts:
        decision = alert["final_decision"]
        if decision in decision_counts:
            decision_counts[decision] += 1
        primary = alert["primary_assessment"]
        primary_counts[primary] = primary_counts.get(primary, 0) + 1

        src = alert["source"] or "Unknown"
        source_counts[src] = source_counts.get(src, 0) + 1

        sev = alert["severity"] or "Unknown"
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

        verdict = alert["verdict"] or "Unknown"
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1

        host = alert["host"] or "Various"
        host_counts[host] = host_counts.get(host, 0) + 1
        host_type_counts.setdefault(host, {})
        host_decision_counts.setdefault(host, {})
        host_severity_counts.setdefault(host, {})
        typ = alert["alert_type"] or "Alert activity"
        host_type_counts[host][typ] = host_type_counts[host].get(typ, 0) + 1
        host_decision_counts[host][decision] = host_decision_counts[host].get(decision, 0) + 1
        host_severity_counts[host][sev] = host_severity_counts[host].get(sev, 0) + 1

    tp_benign = (
        primary_counts.get("Anomalous but Benign", 0)
        + primary_counts.get("BAS / Simulation", 0)
    )
    tp_malicious = primary_counts.get("Confirmed Malicious", 0)

    top_source = "CrowdStrike"
    if source_counts:
        top_source = sorted(source_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]
    top_user_id = ""
    if user_counts:
        top_user_id = sorted(user_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]

    trends = []
    repeated_trends = []
    sorted_hosts = sorted(host_counts.items(), key=lambda kv: kv[1], reverse=True)
    for idx, (host, count) in enumerate(sorted_hosts[:7], start=1):
        top_type = "Alert activity"
        type_counts = host_type_counts.get(host, {})
        if type_counts:
            top_type = sorted(type_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]
        top_decision = "Close"
        decision_dist = host_decision_counts.get(host, {})
        if decision_dist:
            top_decision = sorted(decision_dist.items(), key=lambda kv: kv[1], reverse=True)[0][0]
        top_severity = "Unknown"
        severity_dist = host_severity_counts.get(host, {})
        if severity_dist:
            top_severity = sorted(severity_dist.items(), key=lambda kv: kv[1], reverse=True)[0][0]

        level = "benign"
        if top_decision == "Escalate Immediately":
            level = "critical"
        elif top_decision == "Escalate for Review":
            level = "high"
        elif top_severity in {"Critical", "High"}:
            level = "high"
        trends.append(
            {
                "host": host,
                "alerts": count,
                "alert_type": top_type,
                "count": count,
                "level": level,
                "rank": idx,
                "top_decision": top_decision,
                "top_severity": top_severity,
            }
        )
        # Repeated trend = same host with multiple alerts in the period.
        if count >= 2:
            repeated_trends.append(
                {
                    "host": host,
                    "count": count,
                    "top_alert_type": top_type,
                    "top_decision": top_decision,
                    "top_severity": top_severity,
                }
            )

    return {
        "date": report_date,
        "account_id": account_id,
        "total_traces": len(traces),
        "alert_traces": total,
        "alerts": alerts,
        "source": top_source,
        "trace_review_fields": ["final_decision", "severity", "verdict", "primary_assessment"],
        "totals": {"alerts_triaged": total, "feedback_loop": 0},
        "final_decisions": {
            "escalate_immediately": decision_counts["Escalate Immediately"],
            "escalate_for_review": decision_counts["Escalate for Review"],
            "close": decision_counts["Close"],
        },
        "primary_assessment": primary_counts,
        "severity_distribution": severity_counts,
        "verdict_distribution": verdict_counts,
        "alerts_by_verdict": {
            "True Positive — Benign": tp_benign,
            "True Positive — Malicious": tp_malicious,
        },
        "trends": trends,
        "repeated_trends": repeated_trends,
        "top_user_id": top_user_id,
        "user_id_counts": user_counts,
    }


def _alias_external_query_columns(stmt: str, key: str) -> str:
    """
    Wrap EXTERNAL_QUERY inner SQL with explicit column aliases so BigQuery does
    not fail when PostgreSQL returns unnamed duplicate columns (?column?, count, ...).
    """
    aliases_by_key = {
        "alert_processing_funnel": ["step", "step_order", "count"],
        "alerts_by_severity": ["organization_id", "account_id", "severity_level", "severity_name", "date", "alert_count"],
        "alerts_by_source": ["organization_id", "account_id", "source", "date", "alert_count"],
        "alerts_by_verdict": ["date", "organization_id", "account_id", "verdict", "alert_count"],
        "cases_generated": ["organization_id", "account_id", "total_cases_generated", "alerts_with_cases", "total_alerts", "case_generation_rate_pct"],
        "false_positive_ratio": ["organization_id", "account_id", "false_positive_count", "total_alerts", "false_positive_ratio_pct"],
        "human_feedback": ["alert_id", "alert_pretty_id", "name", "source", "original_verdict", "current_verdict", "triage_confirmation", "human_verified_at", "human_verified_by", "human_comment"],
        "mean_time_to_triage": ["organization_id", "account_id", "mttt_minutes"],
        "rule_effectiveness": ["organization_id", "account_id", "rule_id", "rule_name", "rule_type", "priority", "enabled", "matched_alerts", "unique_verdicts", "avg_severity_level", "confirmed_count", "declined_count"],
        "triggers_generated": ["organization_id", "account_id", "total_triggers", "total_alerts_with_rules", "trigger_rate_pct"],
        "triggers_over_time": ["date", "organization_id", "account_id", "triggers_generated"],
        "user_feedback_distribution": ["feedback_action", "alert_count", "percentage"],
    }
    aliases = aliases_by_key.get(key)
    if not aliases:
        return stmt

    m = re.search(
        r'EXTERNAL_QUERY\(\s*"[^"]+"\s*,\s*"""([\s\S]*?)"""\s*\)',
        stmt,
        flags=re.DOTALL,
    )
    if not m:
        return stmt

    inner = m.group(1).strip()
    alias_list = ", ".join(aliases)
    wrapped_inner = f"SELECT * FROM (\n{inner}\n) AS q({alias_list})"
    return stmt[: m.start(1)] + wrapped_inner + stmt[m.end(1):]


def _normalize_external_columns(df: pd.DataFrame, key: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    expected = {
        "alert_processing_funnel": ["step", "step_order", "count"],
        "alerts_by_severity": ["organization_id", "account_id", "severity_level", "severity_name", "date", "alert_count"],
        "alerts_by_source": ["organization_id", "account_id", "source", "date", "alert_count"],
        "alerts_by_verdict": ["date", "organization_id", "account_id", "verdict", "alert_count"],
        "cases_generated": ["organization_id", "account_id", "total_cases_generated", "alerts_with_cases", "total_alerts", "case_generation_rate_pct"],
        "false_positive_ratio": ["organization_id", "account_id", "false_positive_count", "total_alerts", "false_positive_ratio_pct"],
        "human_feedback": ["alert_id", "alert_pretty_id", "name", "source", "original_verdict", "current_verdict", "triage_confirmation", "human_verified_at", "human_verified_by", "human_comment"],
        "mean_time_to_triage": ["organization_id", "account_id", "mttt_minutes"],
        "rule_effectiveness": ["organization_id", "account_id", "rule_id", "rule_name", "rule_type", "priority", "enabled", "matched_alerts", "unique_verdicts", "avg_severity_level", "confirmed_count", "declined_count"],
        "triggers_generated": ["organization_id", "account_id", "total_triggers", "total_alerts_with_rules", "trigger_rate_pct"],
        "triggers_over_time": ["date", "organization_id", "account_id", "triggers_generated"],
        "user_feedback_distribution": ["feedback_action", "alert_count", "percentage"],
    }.get(key)
    if not expected:
        return df

    cols = list(df.columns)
    if len(cols) == len(expected):
        return df.rename(columns={old: new for old, new in zip(cols, expected)})
    return df


def run_external_sql_file(account_id: str, sql_file_path: str) -> dict:
    client = bigquery.Client(project=PROJECT_ID)
    results = {}

    with open(sql_file_path, "r", encoding="utf-8") as fh:
        raw = fh.read()

    numbered = _extract_numbered_queries(raw)
    if numbered:
        query_items = numbered
    else:
        statements = _split_sql_statements(raw)
        query_items = [
            {"number": idx + 1, "title": f"Query #{idx + 1}", "statement": stmt}
            for idx, stmt in enumerate(statements)
        ]

    if not query_items:
        raise ValueError(f"No SQL statements found in file: {sql_file_path}")

    print(f"\nUsing external SQL file: {sql_file_path}")
    print(f"Found {len(query_items)} query statement(s).")

    for idx, item in enumerate(query_items):
        title = item["title"]
        key = _query_key_from_title(title)
        if not key:
            key = QUERY_META[idx]["key"] if idx < len(QUERY_META) else f"query_{item['number']}"
        print(f"  ↳ {title} ...", end=" ", flush=True)

        patched_stmt = _inject_account_id(item["statement"], account_id)
        patched_stmt = _inject_yesterday_timeframe(patched_stmt)
        patched_stmt = _alias_external_query_columns(patched_stmt, key)
        try:
            df = client.query(patched_stmt).to_dataframe()
            results[key] = _normalize_external_columns(df, key)
            print(f"{len(df)} row(s)")
        except Exception as exc:
            print(f"ERROR — {exc}")
            results[key] = pd.DataFrame()

    return results


# ───────────────────────────── Runner ────────────────────────────────────────

def run_all_queries(account_id: str) -> dict:
    client = bigquery.Client(project=PROJECT_ID)
    results = {}
    for meta in QUERY_META:
        key, title, dr = meta["key"], meta["title"], meta["date_range"]
        print(f"  ↳ {title} ...", end=" ", flush=True)
        inner_sql = transform_sql(_SQL[key], account_id, dr)
        bq_sql    = wrap_external_query(inner_sql)
        try:
            df = client.query(bq_sql).to_dataframe()
            results[key] = df
            print(f"{len(df)} row(s)")
        except Exception as exc:
            print(f"ERROR — {exc}")
            results[key] = pd.DataFrame()
    return results


# ───────────────────────────── HTML helpers ──────────────────────────────────

def _sev_badge(name: str) -> str:
    s = SEV.get(str(name), SEV["Unspecified"])
    return (f'<span style="display:inline-block;padding:2px 10px;border-radius:12px;'
            f'font-size:11px;font-weight:600;background:{s["bg"]};color:{s["fg"]}">{name}</span>')


def _kv(label: str, value, color: str = "#1B3A6B") -> str:
    return (f'<div style="background:#fff;border-radius:10px;padding:20px 24px;'
            f'box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:4px solid {color};flex:1;min-width:130px">'
            f'<div style="font-size:32px;font-weight:700;color:{color}">{value}</div>'
            f'<div style="font-size:12px;color:#64748b;margin-top:4px;font-weight:500">{label}</div>'
            f'</div>')


def _section(title: str, body: str) -> str:
    return (f'<div style="background:#fff;border-radius:10px;padding:24px 28px;'
            f'box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:24px">'
            f'<h2 style="font-size:13px;font-weight:700;color:#1B3A6B;text-transform:uppercase;'
            f'letter-spacing:.06em;margin:0 0 16px;padding-bottom:8px;border-bottom:2px solid #DBEAFE">'
            f'{title}</h2>{body}</div>')


def _generic_table(df: pd.DataFrame, highlight_col: str = None) -> str:
    if df is None or df.empty:
        return '<p style="color:#94a3b8;font-style:italic;font-size:13px">No data for this period.</p>'

    ths = "".join(
        f'<th style="padding:10px 12px;font-size:11px;font-weight:600;color:#fff;'
        f'text-align:left;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap">'
        f'{str(col).replace("_", " ").title()}</th>'
        for col in df.columns
    )

    rows_html = ""
    for _, row in df.iterrows():
        cells = ""
        row_bg = ""
        for col in df.columns:
            val = row[col]
            if col == "severity_name" and str(val) in SEV:
                cell = _sev_badge(str(val))
                row_bg = SEV[str(val)]["bg"] + "30"
            elif col == "verdict" and pd.notna(val):
                cell = f'<span style="font-size:12px;font-weight:500">{val}</span>'
            elif col == "enabled":
                cell = ('✅' if val else '❌') if pd.notna(val) else '—'
            elif isinstance(val, float):
                cell = f"{val:.2f}" if pd.notna(val) else "—"
            elif pd.isna(val) if not isinstance(val, str) else False:
                cell = "—"
            else:
                disp = str(val)
                if len(disp) > 60:
                    disp = disp[:57] + "…"
                cell = disp
            cells += (f'<td style="padding:9px 12px;font-size:12px;color:#374151;'
                      f'border-bottom:1px solid #f1f5f9">{cell}</td>')
        rows_html += f'<tr style="background:{row_bg if row_bg else "transparent"}">{cells}</tr>'

    return (f'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">'
            f'<thead><tr style="background:#1B3A6B">{ths}</tr></thead>'
            f'<tbody>{rows_html}</tbody></table></div>')


def _bar_chart(data: dict, colors: list = None) -> str:
    """Pure-CSS horizontal bar chart.  data = {label: value}"""
    if not data:
        return ""
    total = sum(data.values()) or 1
    default_colors = ["#1B3A6B", "#2563EB", "#0EA5E9", "#10B981", "#F59E0B", "#EF4444"]
    html = ""
    for i, (label, val) in enumerate(data.items()):
        color = (colors[i] if colors and i < len(colors) else default_colors[i % len(default_colors)])
        pct = round(val / total * 100, 1)
        html += (f'<div style="margin-bottom:8px">'
                 f'<div style="display:flex;align-items:center;gap:8px">'
                 f'<span style="font-size:12px;color:#374151;min-width:160px">{label}</span>'
                 f'<div style="flex:1;background:#f1f5f9;border-radius:4px;height:16px">'
                 f'<div style="width:{pct}%;background:{color};height:100%;border-radius:4px"></div></div>'
                 f'<span style="font-size:12px;color:#64748b;min-width:80px;text-align:right">'
                 f'{val} ({pct}%)</span></div></div>')
    return html


# ───────────────────────────── Section builders ──────────────────────────────

def _build_kpi_row(r: dict) -> str:
    sev_df  = r.get("alerts_by_severity", pd.DataFrame())
    mttt_df = r.get("mean_time_to_triage", pd.DataFrame())
    fp_df   = r.get("false_positive_ratio", pd.DataFrame())
    cg_df   = r.get("cases_generated", pd.DataFrame())
    tg_df   = r.get("triggers_generated", pd.DataFrame())

    total    = int(sev_df["alert_count"].sum())  if not sev_df.empty and "alert_count"  in sev_df else "—"
    critical = int(sev_df[sev_df["severity_name"] == "Critical"]["alert_count"].sum()) if not sev_df.empty else 0
    high     = int(sev_df[sev_df["severity_name"] == "High"]["alert_count"].sum())     if not sev_df.empty else 0

    mttt_val = "—"
    if not mttt_df.empty and "mttt_minutes" in mttt_df.columns:
        v = mttt_df["mttt_minutes"].mean()
        mttt_val = f"{v:.1f} min" if pd.notna(v) else "—"

    fp_val = "—"
    if not fp_df.empty and "false_positive_ratio_pct" in fp_df.columns:
        v = fp_df["false_positive_ratio_pct"].iloc[0]
        fp_val = f"{v:.1f}%" if pd.notna(v) else "—"

    cg_val = "—"
    if not cg_df.empty and "case_generation_rate_pct" in cg_df.columns:
        v = cg_df["case_generation_rate_pct"].iloc[0]
        cg_val = f"{v:.1f}%" if pd.notna(v) else "—"

    tg_val = "—"
    if not tg_df.empty and "trigger_rate_pct" in tg_df.columns:
        v = tg_df["trigger_rate_pct"].iloc[0]
        tg_val = f"{v:.1f}%" if pd.notna(v) else "—"

    cards = (
        _kv("Total Alerts",        total,    "#1B3A6B") +
        _kv("Critical",            critical, "#B91C1C") +
        _kv("High",                high,     "#C2410C") +
        _kv("Avg MTTT",            mttt_val, "#0EA5E9") +
        _kv("False Positive Rate", fp_val,   "#F59E0B") +
        _kv("Case Gen Rate",       cg_val,   "#10B981") +
        _kv("Trigger Rate",        tg_val,   "#7C3AED")
    )
    return f'<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px">{cards}</div>'


def _build_severity_section(df: pd.DataFrame) -> str:
    if df.empty:
        return _section("Alerts by Severity", '<p style="color:#94a3b8;font-style:italic;font-size:13px">No data.</p>')

    sev_order = ["Critical", "High", "Medium", "Low", "Informational", "Unspecified"]
    grouped   = df.groupby("severity_name")["alert_count"].sum()
    chart_data  = {k: int(grouped[k]) for k in sev_order if k in grouped}
    chart_colors = [SEV[k]["bar"] for k in chart_data]

    body = (f'<div style="margin-bottom:20px">{_bar_chart(chart_data, chart_colors)}</div>'
            + _generic_table(df))
    return _section("Alerts by Severity", body)


def _build_funnel_section(df: pd.DataFrame) -> str:
    if df.empty:
        return _section("Alert Processing Funnel",
                        '<p style="color:#94a3b8;font-style:italic;font-size:13px">No data.</p>')

    total_row = df[df["step"] == "Total Alerts"]
    total = int(total_row["count"].iloc[0]) if not total_row.empty else 1

    steps_html = ""
    for _, row in df.iterrows():
        cnt  = int(row["count"]) if pd.notna(row["count"]) else 0
        pct  = round(cnt / max(total, 1) * 100, 1)
        width = max(pct, 4)
        steps_html += (
            f'<div style="margin-bottom:10px">'
            f'<div style="display:flex;align-items:center;gap:12px">'
            f'<span style="font-size:12px;color:#374151;min-width:130px;font-weight:500">{row["step"]}</span>'
            f'<div style="flex:1;background:#f1f5f9;border-radius:6px;height:22px">'
            f'<div style="width:{width}%;background:#1B3A6B;height:100%;border-radius:6px;'
            f'display:flex;align-items:center;padding-left:8px">'
            f'<span style="color:#fff;font-size:11px;font-weight:600;white-space:nowrap">'
            f'{cnt:,}</span></div></div>'
            f'<span style="font-size:12px;color:#64748b;min-width:48px;text-align:right">'
            f'{pct}%</span></div></div>'
        )
    return _section("Alert Processing Funnel", steps_html)


def _build_feedback_dist_section(df: pd.DataFrame) -> str:
    if df.empty:
        return _section("User Feedback Distribution",
                        '<p style="color:#94a3b8;font-style:italic;font-size:13px">No data.</p>')

    COLORS = ["#10B981", "#EF4444", "#3B82F6", "#F59E0B", "#8B5CF6", "#64748b"]
    chart_data = {str(row["feedback_action"]): int(row["alert_count"])
                  for _, row in df.iterrows() if pd.notna(row.get("feedback_action"))}

    body = (f'<div style="margin-bottom:20px">{_bar_chart(chart_data, COLORS)}</div>'
            + _generic_table(df))
    return _section("User Feedback Distribution", body)


# ───────────────────────────── Full report ───────────────────────────────────

def generate_report(results: dict, account_id: str) -> str:
    now       = datetime.now(timezone.utc)
    date_str  = YESTERDAY.strftime("%B %d, %Y")
    gen_str   = now.strftime("%Y-%m-%d %H:%M UTC")

    # ── Build each block ──
    kpi_row          = _build_kpi_row(results)
    severity_section = _build_severity_section(results.get("alerts_by_severity", pd.DataFrame()))
    funnel_section   = _build_funnel_section(results.get("alert_processing_funnel", pd.DataFrame()))

    source_section   = _section("Alerts by Source Over Time",
                                _generic_table(results.get("alerts_by_source", pd.DataFrame())))
    verdict_section  = _section("Alerts by Verdict Over Time",
                                _generic_table(results.get("alerts_by_verdict", pd.DataFrame())))
    triggers_ot_sec  = _section("Triggers Over Time",
                                _generic_table(results.get("triggers_over_time", pd.DataFrame())))

    cases_section    = _section("Cases Generated from Alerts",
                                _generic_table(results.get("cases_generated", pd.DataFrame())))
    trig_gen_section = _section("Triggers Generated",
                                _generic_table(results.get("triggers_generated", pd.DataFrame())))
    fp_section       = _section("False Positive Ratio",
                                _generic_table(results.get("false_positive_ratio", pd.DataFrame())))
    mttt_section     = _section("Mean Time to Triage",
                                _generic_table(results.get("mean_time_to_triage", pd.DataFrame())))

    feedback_dist    = _build_feedback_dist_section(results.get("user_feedback_distribution", pd.DataFrame()))
    human_fb_section = _section("Human Feedback (Detail)",
                                _generic_table(results.get("human_feedback", pd.DataFrame())))

    rule_section     = _section("Rule Effectiveness",
                                _generic_table(results.get("rule_effectiveness", pd.DataFrame())))

    # ── Group headers ──
    def group_header(title: str) -> str:
        return (f'<div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;'
                f'letter-spacing:.1em;margin:28px 0 12px;padding-left:4px">{title}</div>')

    body = f"""
    {kpi_row}

    {group_header("▸ Alert Overview")}
    {severity_section}
    {funnel_section}

    {group_header("▸ Processing & Quality")}
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
      <div>{mttt_section}</div>
      <div>{fp_section}</div>
    </div>
    {cases_section}

    {group_header("▸ Triggers")}
    {trig_gen_section}
    {triggers_ot_sec}

    {group_header("▸ Over Time")}
    {source_section}
    {verdict_section}

    {group_header("▸ Human Feedback")}
    {feedback_dist}
    {human_fb_section}

    {group_header("▸ Rule Effectiveness")}
    {rule_section}
    """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Alert Triage Report — {date_str}</title>
  <style>
    * {{ box-sizing:border-box;margin:0;padding:0; }}
    body {{ font-family:Arial,sans-serif;background:#F8FAFC;color:#1e293b; }}
    tr:hover {{ background:#f8fafc!important; }}
  </style>
</head>
<body>

<!-- Header -->
<div style="background:#1B3A6B;padding:24px 40px;display:flex;
            justify-content:space-between;align-items:center">
  <div>
    <div style="font-size:10px;color:#93c5fd;letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px">
      Alert Triage</div>
    <div style="font-size:20px;font-weight:700;color:#fff">
      Automated Alert Triage — Daily Report</div>
    <div style="font-size:12px;color:#93c5fd;margin-top:6px">
      Account: <span style="font-weight:600;color:#fff">{account_id}</span></div>
  </div>
  <div style="text-align:right">
    <div style="font-size:24px;font-weight:700;color:#fff">{date_str}</div>
    <div style="font-size:11px;color:#93c5fd;margin-top:4px">Generated {gen_str}</div>
  </div>
</div>

<!-- Body -->
<div style="max-width:1200px;margin:0 auto;padding:28px 24px">
  {body}
  <div style="text-align:center;font-size:11px;color:#94a3b8;padding:20px 0">
    Project: {PROJECT_ID} &nbsp;·&nbsp; Account: {account_id} &nbsp;·&nbsp; Date: {date_str} &nbsp;·&nbsp; Generated {gen_str}
  </div>
</div>

</body>
</html>"""


# ───────────────────────────── DOCX bridge ───────────────────────────────────

def _safe_sum(df, column: str) -> int:
    if df is None or df.empty or column not in df.columns:
        return 0
    try:
        return int(df[column].fillna(0).sum())
    except Exception:
        return 0


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _count_value(df, column: str, value: str) -> int:
    if df is None or df.empty or column not in df.columns:
        return 0
    try:
        return int((df[column].fillna("") == value).sum())
    except Exception:
        return 0


def _normalize_verdict_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _to_jsonable_results(results: dict) -> dict:
    out = {}
    for key, df in results.items():
        if df is None or df.empty:
            out[key] = []
            continue
        safe_df = df.copy()
        # Timestamps/decimals/etc are serialized as strings for portability.
        for col in safe_df.columns:
            safe_df[col] = safe_df[col].apply(lambda x: None if pd.isna(x) else x)
        out[key] = safe_df.to_dict(orient="records")
    return out


def _ensure_node_docx_dependency() -> None:
    """
    Ensure Node dependency needed by generated JS report exists.
    Auto-installs with npm when missing (common after cleaning generated artifacts).
    """
    docx_marker = os.path.join(OUTPUT_DIR, "node_modules", "docx", "package.json")
    if os.path.exists(docx_marker):
        return
    print("[info] Node dependency 'docx' is missing. Running npm install...")
    run = subprocess.run(["npm", "install"], cwd=OUTPUT_DIR, capture_output=True, text=True)
    if run.returncode != 0:
        msg = run.stderr.strip() or run.stdout.strip() or "npm install failed"
        raise RuntimeError(f"Unable to install Node dependencies: {msg}")


def _derive_report_date(results: dict) -> datetime.date:
    for key in ("alerts_by_source", "alerts_by_verdict", "alerts_by_severity", "triggers_over_time"):
        df = results.get(key, pd.DataFrame())
        if df is None or df.empty or "date" not in df.columns:
            continue
        try:
            series = pd.to_datetime(df["date"], errors="coerce").dropna()
            if not series.empty:
                return series.max().date()
        except Exception:
            continue
    return YESTERDAY


def _build_docx_metrics(results: dict, account_id: str, trace_enrichment: Optional[dict] = None) -> dict:
    funnel_df = results.get("alert_processing_funnel", pd.DataFrame())
    severity_df = results.get("alerts_by_severity", pd.DataFrame())
    verdict_df = results.get("alerts_by_verdict", pd.DataFrame())
    feedback_df = results.get("human_feedback", pd.DataFrame())
    source_df = results.get("alerts_by_source", pd.DataFrame())

    tp_malicious = 0
    suspicious = 0
    tp_benign = 0

    alerts_triaged = 0
    if funnel_df is not None and not funnel_df.empty and {"step", "count"} <= set(funnel_df.columns):
        total_row = funnel_df[funnel_df["step"].astype(str).str.lower() == "total alerts"]
        if not total_row.empty:
            alerts_triaged = _safe_int(total_row["count"].iloc[0], 0)
    if alerts_triaged == 0 and severity_df is not None and not severity_df.empty and "alert_count" in severity_df.columns:
        alerts_triaged = _safe_int(severity_df["alert_count"].sum(), 0)

    if verdict_df is not None and not verdict_df.empty and {"verdict", "alert_count"} <= set(verdict_df.columns):
        grouped = verdict_df.groupby("verdict")["alert_count"].sum()
        for verdict_name, raw_count in grouped.items():
            count = _safe_int(raw_count, 0)
            norm = _normalize_verdict_label(verdict_name)
            if "malicious" in norm:
                tp_malicious += count
            elif "suspicious" in norm:
                suspicious += count
            elif "benign" in norm or "falsepositive" in norm:
                tp_benign += count
            elif "truepositive" in norm:
                # Fallback for generic TruePositive labels without suffix.
                tp_malicious += count

    if alerts_triaged == 0:
        alerts_triaged = tp_malicious + suspicious + tp_benign
    feedback_loop = 0 if feedback_df is None else int(len(feedback_df))

    source_name = "CrowdStrike"
    if source_df is not None and not source_df.empty and {"source", "alert_count"} <= set(source_df.columns):
        top = source_df.groupby("source")["alert_count"].sum().sort_values(ascending=False)
        if not top.empty:
            source_name = str(top.index[0])

    trends = []
    if source_df is not None and not source_df.empty and {"source", "alert_count"} <= set(source_df.columns):
        grouped_source = source_df.groupby("source")["alert_count"].sum().sort_values(ascending=False)
        for source, count in grouped_source.head(7).items():
            trends.append(
                {
                    "host": str(source),
                    "alerts": _safe_int(count, 0),
                    "count": _safe_int(count, 0),
                    "alert_type": "Source alert volume",
                    "level": "informational",
                }
            )

    top_host = ""
    analyst_email = ""
    if feedback_df is not None and not feedback_df.empty:
        if "name" in feedback_df.columns:
            host_counts = feedback_df["name"].fillna("").astype(str).str.extract(r" on (.+)$", expand=False).dropna()
            if not host_counts.empty:
                top_host = str(host_counts.value_counts().index[0])
        if "human_verified_by" in feedback_df.columns:
            emails = feedback_df["human_verified_by"].fillna("").astype(str)
            emails = emails[emails.str.strip() != ""]
            if not emails.empty:
                analyst_email = str(emails.value_counts().index[0])

    report_date = _derive_report_date(results)
    metrics = {
        "env_name": "Rocket Companies",
        "date": str(report_date),
        "date_range": report_date.strftime("%b %d, %Y"),
        "source": source_name,
        "totals": {
            "alerts_triaged": alerts_triaged,
            "feedback_loop": feedback_loop,
        },
        "final_decisions": {
            "escalate_immediately": tp_malicious,
            "escalate_for_review": suspicious,
            "close": tp_benign,
        },
        "primary_assessment": {
            "Confirmed Malicious": tp_malicious,
            "High-Conf. Suspicious": suspicious,
            "Anomalous but Benign": tp_benign,
        },
        "alerts_by_verdict": {
            "True Positive — Benign": tp_benign,
            "True Positive — Malicious": tp_malicious,
        },
        "feedback": {
            "human_verified": _count_value(feedback_df, "triage_confirmation", "Confirmed"),
            "verdict_modified": feedback_loop,
            "top_host": top_host,
            "analyst_email": analyst_email,
            "no_overrides_text": "No analyst overrides recorded for this period.",
        },
        "trends": trends,
    }
    if trace_enrichment:
        # Use exported Langfuse trace-derived values when available.
        trace_totals = trace_enrichment.get("totals", {})
        trace_decisions = trace_enrichment.get("final_decisions", {})
        trace_primary = trace_enrichment.get("primary_assessment", {})
        trace_verdict = trace_enrichment.get("alerts_by_verdict", {})
        trace_trends = trace_enrichment.get("trends", [])
        trace_repeated_trends = trace_enrichment.get("repeated_trends", [])
        trace_source = trace_enrichment.get("source")
        trace_triaged = _safe_int(trace_totals.get("alerts_triaged"), 0)
        trace_top_user_id = str(trace_enrichment.get("top_user_id") or "").strip()

        if trace_triaged > 0:
            metrics["totals"]["alerts_triaged"] = trace_triaged
            metrics["totals"]["feedback_loop"] = max(metrics["totals"].get("feedback_loop", 0), _safe_int(trace_totals.get("feedback_loop"), 0))
            metrics["source"] = str(trace_source or metrics.get("source", "CrowdStrike"))

            metrics["final_decisions"]["escalate_immediately"] = _safe_int(
                trace_decisions.get("escalate_immediately"),
                metrics["final_decisions"].get("escalate_immediately", 0),
            )
            metrics["final_decisions"]["escalate_for_review"] = _safe_int(
                trace_decisions.get("escalate_for_review"),
                metrics["final_decisions"].get("escalate_for_review", 0),
            )
            metrics["final_decisions"]["close"] = _safe_int(
                trace_decisions.get("close"),
                metrics["final_decisions"].get("close", 0),
            )

            # Keep default primary buckets plus any additional categories from traces.
            for label, count in trace_primary.items():
                metrics["primary_assessment"][str(label)] = _safe_int(count, 0)

            metrics["alerts_by_verdict"]["True Positive — Benign"] = _safe_int(
                trace_verdict.get("True Positive — Benign"),
                metrics["alerts_by_verdict"].get("True Positive — Benign", 0),
            )
            metrics["alerts_by_verdict"]["True Positive — Malicious"] = _safe_int(
                trace_verdict.get("True Positive — Malicious"),
                metrics["alerts_by_verdict"].get("True Positive — Malicious", 0),
            )

            if isinstance(trace_repeated_trends, list) and trace_repeated_trends:
                metrics["trends"] = [
                    {
                        "host": t.get("host", "Various"),
                        "alerts": _safe_int(t.get("count"), 0),
                        "alert_type": str(t.get("top_alert_type", "Repeated alert trend")),
                        "count": _safe_int(t.get("count"), 0),
                        "level": (
                            "critical"
                            if str(t.get("top_decision", "")) == "Escalate Immediately"
                            else "high"
                            if str(t.get("top_decision", "")) == "Escalate for Review"
                            else "benign"
                        ),
                    }
                    for t in trace_repeated_trends
                ][:7]
            elif isinstance(trace_trends, list) and trace_trends:
                metrics["trends"] = trace_trends
            derived_env = _env_name_from_user_id(trace_top_user_id)
            if derived_env:
                metrics["env_name"] = derived_env

            metrics["langfuse_enrichment"] = {
                "enabled": True,
                "exported_total_traces": _safe_int(trace_enrichment.get("total_traces"), 0),
                "exported_alert_traces": trace_triaged,
                "period_start": f"{YESTERDAY}T00:00:00",
                "period_end": f"{YESTERDAY}T23:59:59",
                "top_user_id": trace_top_user_id,
                "trace_review_fields": trace_enrichment.get("trace_review_fields", []),
                "severity_distribution": trace_enrichment.get("severity_distribution", {}),
                "verdict_distribution": trace_enrichment.get("verdict_distribution", {}),
            }

    return metrics


def _generate_docx_report(results: dict, account_id: str, langfuse_user_ids: Optional[list[str]] = None) -> str:
    local_tz = datetime.now().astimezone().tzinfo
    trace_period_start = datetime.combine(YESTERDAY, time(0, 0, 0), tzinfo=local_tz)
    trace_period_end = datetime.combine(YESTERDAY, time(23, 59, 59), tzinfo=local_tz)

    trace_export_path = None
    trace_enrichment = None
    selected_user_ids = {u.strip() for u in (langfuse_user_ids or []) if str(u).strip()}
    traces, trace_error = _fetch_langfuse_traces_for_account(
        account_id,
        trace_period_start,
        trace_period_end,
        user_ids=selected_user_ids if selected_user_ids else None,
    )
    if trace_error:
        print(f"[warn] Langfuse export skipped: {trace_error}")
    else:
        report_date = str(YESTERDAY)
        trace_export_path = os.path.join(
            OUTPUT_DIR,
            f"langfuse_traces_{report_date}_{account_id}.json",
        )
        payload = {
            "account_id": account_id,
            "from": trace_period_start.isoformat(),
            "to": trace_period_end.isoformat(),
            "total": len(traces),
            "user_id_filter": sorted(selected_user_ids),
            "data": traces,
        }
        with open(trace_export_path, "w", encoding="utf-8") as tf:
            json.dump(payload, tf, ensure_ascii=True, indent=2, default=str)
        print(f"[ok] Exported {len(traces)} Langfuse traces → {trace_export_path}")
        trace_enrichment = _derive_metrics_from_langfuse_traces(traces, account_id, report_date)

    metrics = _build_docx_metrics(results, account_id, trace_enrichment=trace_enrichment)
    if selected_user_ids:
        metrics.setdefault("langfuse_enrichment", {})
        metrics["langfuse_enrichment"]["user_id_filter"] = sorted(selected_user_ids)
    if trace_export_path:
        metrics["langfuse_export_file"] = trace_export_path
    generator = os.path.join(OUTPUT_DIR, "generate_docx_report.py")
    if not os.path.exists(generator):
        raise FileNotFoundError(f"Missing generator script: {generator}")

    # Persist both raw query results and derived metrics for audit/debug/LLM workflows.
    report_date = str(metrics.get("date", YESTERDAY))
    results_snapshot = os.path.join(OUTPUT_DIR, f"query_results_{report_date}.json")
    metrics_snapshot = os.path.join(OUTPUT_DIR, f"report_metrics_{report_date}.json")
    with open(results_snapshot, "w", encoding="utf-8") as rf:
        json.dump(_to_jsonable_results(results), rf, ensure_ascii=True, indent=2, default=str)
    with open(metrics_snapshot, "w", encoding="utf-8") as mf:
        json.dump(metrics, mf, ensure_ascii=True, indent=2, default=str)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(metrics, tmp, ensure_ascii=True, indent=2)
        tmp_metrics_path = tmp.name

    try:
        _ensure_node_docx_dependency()
        cmd = [sys.executable, generator, "--metrics", tmp_metrics_path, "--report-date", str(YESTERDAY), "--out-dir", OUTPUT_DIR]
        run = subprocess.run(cmd, cwd=OUTPUT_DIR, capture_output=True, text=True)
        if run.returncode != 0:
            err = run.stderr.strip() or run.stdout.strip() or "DOCX generator failed"
            if "Cannot find module 'docx'" in err:
                # One retry after restoring node deps.
                _ensure_node_docx_dependency()
                run = subprocess.run(cmd, cwd=OUTPUT_DIR, capture_output=True, text=True)
                if run.returncode == 0:
                    err = ""
                else:
                    err = run.stderr.strip() or run.stdout.strip() or "DOCX generator failed"
            if err:
                raise RuntimeError(err)
    finally:
        try:
            os.remove(tmp_metrics_path)
        except OSError:
            pass

    env_name = str(metrics.get("env_name", "environment")).strip().lower()
    env_slug = re.sub(r"[^a-z0-9]+", "_", env_name).strip("_") or "environment"
    filename = f"{env_slug}_report_{report_date}.docx"
    return os.path.join(OUTPUT_DIR, filename)


# ───────────────────────────── Entry point ───────────────────────────────────

def main():
    _load_env_file(os.path.join(OUTPUT_DIR, ".env"))

    default_account = "6f60849e-1aab-408f-8b00-84e99768d0bd"

    print("\n╔══════════════════════════════════════════╗")
    print("║   Alert Triage Daily Report Generator   ║")
    print("╚══════════════════════════════════════════╝")
    print(f"\n  Report date : {YESTERDAY}  (yesterday)")
    print(f"  Project     : {PROJECT_ID}")
    print(f"\n  BigQuery Account ID  [{default_account}]: ", end="")

    user_input = input().strip()
    account_id = user_input if user_input else default_account

    local_tz = datetime.now().astimezone().tzinfo
    trace_period_start = datetime.combine(YESTERDAY, time(0, 0, 0), tzinfo=local_tz)
    trace_period_end = datetime.combine(YESTERDAY, time(23, 59, 59), tzinfo=local_tz)
    selected_user_ids: list[str] = []
    user_list, user_err = _list_langfuse_user_ids_for_account(account_id, trace_period_start, trace_period_end)
    if user_err:
        print(f"  ! Langfuse user list unavailable: {user_err}")
    elif user_list:
        # Auto-select all known (non-empty) user IDs for this account/day.
        selected_user_ids = sorted({uid for uid, _ in user_list if uid and uid != "(empty)"})
    else:
        print(f"  ! No Langfuse users found for account {account_id} on {YESTERDAY}.")

    # Always run external SQL file without prompting.
    sql_file = DEFAULT_EXTERNAL_SQL_FILE if os.path.exists(DEFAULT_EXTERNAL_SQL_FILE) else ""

    print(f"\n  ✓ Using account: {account_id}")
    if selected_user_ids:
        print(f"  ✓ Langfuse users (auto): {', '.join(selected_user_ids)}")
    else:
        print("  ✓ Langfuse users (auto): all")
    if sql_file:
        print(f"  ✓ SQL file     : {sql_file}")
    else:
        print("  ✓ SQL file     : built-in query templates")
    print(f"  ✓ Date range   : {YESTERDAY} → {TODAY}\n")
    print("Running queries...")

    if sql_file:
        if not os.path.exists(sql_file):
            print(f"  ! SQL file not found: {sql_file}")
            print("  ! Falling back to built-in query templates.\n")
            results = run_all_queries(account_id)
        else:
            results = run_external_sql_file(account_id, sql_file)
    else:
        results = run_all_queries(account_id)

    print("\nBuilding DOCX report...")
    output_path = _generate_docx_report(results, account_id, langfuse_user_ids=selected_user_ids)

    print(f"\n✅ Done!  Report saved →  {output_path}\n")
    _send_to_slack(output_path)


SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL   = os.environ.get("SLACK_CHANNEL", "C0AN7770N2H")


def _send_to_slack(file_path: str) -> None:
    import urllib.request
    if not os.path.exists(file_path):
        print(f"[slack] File not found, skipping: {file_path}")
        return
    filename = os.path.basename(file_path)
    # Step 1: get upload URL
    params = urllib.parse.urlencode({
        "filename": filename,
        "length": os.path.getsize(file_path),
    }).encode()
    req = urllib.request.Request(
        "https://slack.com/api/files.getUploadURLExternal",
        data=params,
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
    )
    with urllib.request.urlopen(req) as r:
        meta = json.loads(r.read())
    if not meta.get("ok"):
        print(f"[slack] getUploadURLExternal failed: {meta.get('error')}")
        return
    upload_url = meta["upload_url"]
    file_id    = meta["file_id"]
    # Step 2: upload file
    with open(file_path, "rb") as f:
        data = f.read()
    upload_req = urllib.request.Request(upload_url, data=data, method="POST")
    upload_req.add_header("Content-Type", "application/octet-stream")
    with urllib.request.urlopen(upload_req) as r:
        pass
    # Step 3: complete upload and share to channel
    complete_body = json.dumps({
        "files": [{"id": file_id}],
        "channel_id": SLACK_CHANNEL,
    }).encode()
    complete_req = urllib.request.Request(
        "https://slack.com/api/files.completeUploadExternal",
        data=complete_body,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(complete_req) as r:
        result = json.loads(r.read())
    if result.get("ok"):
        print(f"[slack] ✅ Report posted to {SLACK_CHANNEL}")
    else:
        print(f"[slack] ❌ completeUpload failed: {result.get('error')}")


if __name__ == "__main__":
    main()
