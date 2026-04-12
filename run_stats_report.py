#!/usr/bin/env python3
"""
run_stats_report.py — Weekly Alert Triage Stats Report

Aggregates metrics across US + EU regions and posts a Slack message.

Usage:
    python run_stats_report.py                    # last 7 days ending today
    python run_stats_report.py --date 2026-04-12  # week ending on specific date
"""

import json
import os
import sys
import urllib.request
from datetime import date, timedelta

# ── Load .env ─────────────────────────────────────────────────────────────────
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

try:
    from google.cloud import bigquery
    import pandas as pd
except ImportError:
    print("Missing dependencies. Run: pip install google-cloud-bigquery pandas db-dtypes")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN    = os.environ.get("SLACK_BOT_TOKEN", "")
STATS_SLACK_CHANNEL = os.environ.get("STATS_SLACK_CHANNEL", os.environ.get("SLACK_CHANNEL", ""))
GRAFANA_BASE_URL   = os.environ.get("GRAFANA_BASE_URL", "https://grafana.torqio.dev")
GRAFANA_API_KEY    = os.environ.get("GRAFANA_API_KEY", "")
GRAFANA_DS_UID     = os.environ.get("GRAFANA_DATASOURCE_UID", "6C3EsgNIz")

REGION_CONFIGS = [
    {
        "region":     "US",
        "bq_project": "stackpulse-production",
        "bq_conn":    "stackpulse-production.us.alert-triage",
    },
    {
        "region":     "EU",
        "bq_project": "torqio-eu-production",
        "bq_conn":    "projects/torqio-eu-production/locations/eu/connections/alert-triage",
    },
]

# Account ID → display name (sourced from global.dim_accounts)
ACCOUNT_NAMES = {
    "bc30896e-c6aa-414f-9750-39473404e0c4": "Carvana",
    "e73c028c-fd24-49b9-9a5b-419426ec4e8d": "GSK",
    "f38cff0e-7917-46ec-acad-40ec6c4af71f": "Content Team",
    "3d01d3fb-f69b-4940-9344-58323d6370ea": "Elron Torq US",
    "89e67b77-a63f-4070-b2a0-6fe260a347f1": "MUFG Research",
    "3d94fd35-c6bc-490e-b137-f37d3b7f055d": "Auto Triage Playground",
    "197be1ce-a84f-4360-9edd-728e7dad2216": "SHRSS",
    "0b1972fe-ba8c-4377-8afc-7bfe481220a3": "Edwards Lifesciences",
    "6f60849e-1aab-408f-8b00-84e99768d0bd": "Rocket Companies",
    "ce0a69d6-fbba-4319-bf00-f657d00758b1": "Citadel Delta",
    "f48ed0cf-b6f6-4381-94c8-cce216012805": "Citadel Haaretz",
}


# ── BQ helpers ────────────────────────────────────────────────────────────────

def _pg(bq_project: str, bq_conn: str, pg_sql: str, label: str = "") -> pd.DataFrame:
    safe = pg_sql.replace('"""', "'''")
    bq_sql = f'SELECT * FROM EXTERNAL_QUERY(\n  "{bq_conn}",\n  """\n{safe}\n  """\n) LIMIT 50000'
    try:
        return bigquery.Client(project=bq_project).query(bq_sql).to_dataframe()
    except Exception as exc:
        print(f"  [warn] {label or bq_project} query failed: {exc}")
        return pd.DataFrame()


def fetch_summary(proj, conn, ws, we) -> dict:
    df = _pg(proj, conn, f"""
SELECT
    COUNT(*)                                                                      AS total_alerts,
    COUNT(*) FILTER (WHERE verdict ILIKE '%true positive%'
                        OR verdict ILIKE '%truepositive%'
                        OR verdict ILIKE '%true_positive%')                      AS true_positives,
    COUNT(*) FILTER (WHERE verdict ILIKE '%false positive%'
                        OR verdict ILIKE '%falsepositive%'
                        OR verdict ILIKE '%false_positive%')                     AS false_positives,
    AVG(EXTRACT(EPOCH FROM (processed_at - created_at)) / 60.0)
        FILTER (WHERE processed_at IS NOT NULL AND verdict IS NOT NULL)          AS avg_mttt_minutes,
    COUNT(*) FILTER (WHERE triage_confirmation = 'Declined')                     AS verdict_changes,
    COUNT(*) FILTER (WHERE triage_confirmation = 'Confirmed')                    AS endorsements,
    COUNT(*) FILTER (WHERE triage_confirmation IN ('Confirmed','Declined'))      AS total_with_feedback
FROM alert_triage.alerts
WHERE created_at >= '{ws}'::timestamp AND created_at < '{we}'::timestamp
""", label=f"{proj}/summary")
    if df.empty:
        return {}
    r = df.iloc[0]
    return {
        "total_alerts":        int(r.get("total_alerts", 0) or 0),
        "true_positives":      int(r.get("true_positives", 0) or 0),
        "false_positives":     int(r.get("false_positives", 0) or 0),
        "avg_mttt_minutes":    float(r.get("avg_mttt_minutes", 0) or 0),
        "verdict_changes":     int(r.get("verdict_changes", 0) or 0),
        "endorsements":        int(r.get("endorsements", 0) or 0),
        "total_with_feedback": int(r.get("total_with_feedback", 0) or 0),
    }


def fetch_escalation(proj, conn, ws, we) -> dict:
    df = _pg(proj, conn, f"""
SELECT
    COUNT(*) FILTER (WHERE verdict ILIKE '%malicious%'
                        OR verdict ILIKE '%suspicious%')                         AS escalated,
    COUNT(*) FILTER (WHERE verdict ILIKE '%false positive%'
                        OR verdict ILIKE '%falsepositive%'
                        OR verdict ILIKE '%false_positive%'
                        OR verdict ILIKE '%benign%')                             AS auto_closed
FROM alert_triage.alerts
WHERE created_at >= '{ws}'::timestamp AND created_at < '{we}'::timestamp
  AND verdict IS NOT NULL
""", label=f"{proj}/escalation")
    if df.empty:
        return {}
    r = df.iloc[0]
    return {
        "escalated":  int(r.get("escalated", 0) or 0),
        "auto_closed": int(r.get("auto_closed", 0) or 0),
    }


def fetch_top_sources(proj, conn, ws, we) -> list:
    df = _pg(proj, conn, f"""
SELECT source, COUNT(*) AS alert_count
FROM alert_triage.alerts
WHERE created_at >= '{ws}'::timestamp AND created_at < '{we}'::timestamp
  AND source IS NOT NULL AND source != ''
GROUP BY source
ORDER BY alert_count DESC
LIMIT 7
""", label=f"{proj}/sources")
    if df.empty:
        return []
    return [{"source": str(r["source"]), "count": int(r["alert_count"])} for _, r in df.iterrows()]


def fetch_by_account(proj, conn, ws, we) -> list:
    df = _pg(proj, conn, f"""
SELECT account_id::text AS account_id, COUNT(*) AS alert_count
FROM alert_triage.alerts
WHERE created_at >= '{ws}'::timestamp AND created_at < '{we}'::timestamp
GROUP BY account_id::text
ORDER BY alert_count DESC
""", label=f"{proj}/by_account")
    if df.empty:
        return []
    return [{"account_id": str(r["account_id"]), "count": int(r["alert_count"])} for _, r in df.iterrows()]


def fetch_rules(proj, conn, ws, we) -> dict:
    df = _pg(proj, conn, f"""
SELECT
    COUNT(*)                                                          AS total,
    COUNT(*) FILTER (WHERE type = 'VERDICT_BASED_ACTION')            AS deterministic,
    COUNT(*) FILTER (WHERE type != 'VERDICT_BASED_ACTION'
                        OR type IS NULL)                             AS guidance
FROM alert_triage.rules
WHERE created_at >= '{ws}'::timestamp AND created_at < '{we}'::timestamp
""", label=f"{proj}/rules")
    if df.empty:
        return {}
    r = df.iloc[0]
    return {
        "total":        int(r.get("total", 0) or 0),
        "deterministic": int(r.get("deterministic", 0) or 0),
        "guidance":     int(r.get("guidance", 0) or 0),
    }


def fetch_enrichment_hit_rate(proj, conn, ws, we) -> dict:
    df = _pg(proj, conn, f"""
SELECT
    COUNT(*)                                                                        AS total,
    COUNT(*) FILTER (WHERE reputation IS NOT NULL
                       AND reputation NOT IN ('Unknown', ''))                       AS successful
FROM alert_triage.enrichments
WHERE enriched_at >= '{ws}'::timestamp AND enriched_at < '{we}'::timestamp
""", label=f"{proj}/enrichment")
    if df.empty:
        return {}
    r = df.iloc[0]
    total      = int(r.get("total", 0) or 0)
    successful = int(r.get("successful", 0) or 0)
    return {
        "total":       total,
        "successful":  successful,
        "hit_rate_pct": round(100.0 * successful / total, 1) if total > 0 else 0.0,
    }


# ── Grafana ───────────────────────────────────────────────────────────────────

def _get_cf_token() -> str:
    """Get a fresh Cloudflare Access token for monitor.torqio.dev."""
    import subprocess
    try:
        return subprocess.check_output(
            ["cloudflared", "access", "token", "--app", GRAFANA_BASE_URL],
            stderr=subprocess.DEVNULL, timeout=10
        ).decode().strip()
    except Exception:
        return ""


def fetch_grafana_rl(week_start: date, week_end: date,
                     prev_start: date, prev_end: date) -> dict:
    """Fetch enrichment lookups (panel-33) and external calls (panel-34) from Grafana."""
    if not GRAFANA_API_KEY or GRAFANA_API_KEY == "your_grafana_api_key_here":
        return {"lookups": None, "external_calls": None}

    import datetime as _dt
    cf_token = _get_cf_token()
    days = (week_end - week_start).days
    range_str = f"{days * 24}h"

    def _ts_ms(d: date) -> str:
        return str(int(_dt.datetime(d.year, d.month, d.day).timestamp() * 1000))

    def _run(expr, from_dt, to_dt):
        payload = json.dumps({
            "queries": [{
                "datasource": {"uid": GRAFANA_DS_UID, "type": "prometheus"},
                "refId": "A",
                "expr": expr,
                "instant": True,
            }],
            "from": _ts_ms(from_dt),
            "to":   _ts_ms(to_dt),
        }).encode()
        headers = {
            "Authorization": f"Bearer {GRAFANA_API_KEY}",
            "Content-Type":  "application/json",
            "User-Agent":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }
        if cf_token:
            headers["CF-Access-Token"] = cf_token
        req = urllib.request.Request(
            f"{GRAFANA_BASE_URL}/api/ds/query",
            data=payload,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
            result = {}
            for frame in data.get("results", {}).get("A", {}).get("frames", []):
                enricher = "total"
                for field in frame.get("schema", {}).get("fields", []):
                    if field.get("labels", {}).get("enricher"):
                        enricher = field["labels"]["enricher"]
                vals = frame.get("data", {}).get("values", [])
                if len(vals) > 1 and vals[1]:
                    result[enricher] = int(vals[1][0])
            return result
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            print(f"  [warn] Grafana query failed: {exc} — {body[:200]}")
        except Exception as exc:
            print(f"  [warn] Grafana query failed: {exc}")
        return {}

    q_lookups = (
        f"sum by (enricher) (max by (pod, enricher, cluster) "
        f"(increase(torq_enrichment_attempts_total[{range_str}])))"
    )
    q_external = (
        f'sum by (enricher) (max by (pod, enricher, cluster) '
        f'(increase(torq_enrichment_cache_total{{result="miss"}}[{range_str}])))'
    )

    return {
        "lookups":        _run(q_lookups,  week_start, week_end),
        "external_calls": _run(q_external, week_start, week_end),
        "days":           days,
    }


# ── Formatting helpers ────────────────────────────────────────────────────────

def _pct(n: int, d: int) -> str:
    return f"{100.0 * n / d:.1f}%" if d else "N/A"

def _change(curr: float, prev: float) -> str:
    if not prev:
        return ""
    c = (curr - prev) / prev * 100
    arrow = "↑" if c >= 0 else "↓"
    return f"  ({arrow}{abs(c):.0f}% vs prev week: {int(prev):,})"


# ── Message builder ───────────────────────────────────────────────────────────

def build_message(week_start, week_end, prev_start,
                  curr, prev, esc, sources, by_account,
                  rules, enrichment, rl) -> str:
    ws = week_start.strftime("%b %d")
    we = (week_end - timedelta(days=1)).strftime("%b %d, %Y")

    total    = curr.get("total_alerts", 0)
    prev_tot = prev.get("total_alerts", 0)
    tp       = curr.get("true_positives", 0)
    fp       = curr.get("false_positives", 0)
    cls      = tp + fp or 1
    mttt     = curr.get("avg_mttt_minutes", 0)
    vc       = curr.get("verdict_changes", 0)
    end      = curr.get("endorsements", 0)
    tfb      = curr.get("total_with_feedback", 0) or 1
    esc_up   = esc.get("escalated", 0)
    esc_cl   = esc.get("auto_closed", 0)
    esc_tot  = esc_up + esc_cl or 1

    lines = [
        "🤖 *Alert Triage — Weekly Stats Report*",
        f"📅 {ws} – {we}",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "📊 *Key Metrics*",
        f"• Total Alerts Triaged: *{total:,}*{_change(total, prev_tot)}",
        f"• True Positive Rate: *{_pct(tp, cls)}*  |  False Positive Rate: *{_pct(fp, cls)}*",
        f"• Avg Time to Triage: *{mttt:.1f} min*",
        f"• Escalation Rate: *{_pct(esc_up, esc_tot)}*  ({esc_up:,} escalated · {esc_cl:,} auto-closed)",
        f"• User Verdict Changes: *{_pct(vc, tfb)}*  |  User Endorsements: *{_pct(end, tfb)}*",
    ]

    if rules.get("total", 0) > 0:
        lines.append(
            f"• Rules Created: *{rules['total']}*"
            f"  ({rules['deterministic']} deterministic · {rules['guidance']} Guidance)"
        )

    if enrichment.get("total", 0) > 0:
        lines.append(
            f"• Enrichment Hit Rate: *{enrichment['hit_rate_pct']}%*"
            f"  ({enrichment['successful']:,} / {enrichment['total']:,})"
        )

    # RL enrichment — dynamic Grafana URLs scoped to the report time range
    _gbase = (
        "https://monitor.torqio.dev/d/alerttriage-enrichment-cache"
        "/alerttriage-observables-enrichment"
        f"?orgId=1&from={week_start.strftime('%Y-%m-%dT00:00:00.000Z')}"
        f"&to={(week_end - timedelta(days=1)).strftime('%Y-%m-%dT23:59:59.000Z')}"
        "&timezone=browser&var-datasource=6C3EsgNIz"
        "&var-cluster=$__all&var-enricher=$__all&var-observable_type=$__all"
    )
    url33 = _gbase + "&viewPanel=panel-33"
    url34 = _gbase + "&viewPanel=panel-34"

    span_label = f"last {rl.get('days', 7)} days"
    lines += ["", f"🔍 *Observable Enrichment ({span_label})*"]

    lookups   = rl.get("lookups") or {}
    ext_calls = rl.get("external_calls") or {}

    if lookups:
        lines.append(f"*Total Lookups by Enricher (incl. cache)* — <{url33}|View panel>")
        lines.append(url33)
        for enricher, count in sorted(lookups.items(), key=lambda x: -x[1]):
            lines.append(f"  • {enricher}: *{count:,}*")

    if ext_calls:
        lines.append(f"*Total External Calls by Enricher* — <{url34}|View panel>")
        lines.append(url34)
        for enricher, count in sorted(ext_calls.items(), key=lambda x: -x[1]):
            lines.append(f"  • {enricher}: *{count:,}*")

    if not lookups and not ext_calls:
        lines.append("• _No data_")

    # Top sources
    if sources:
        lines += ["", "🏆 *Top Alert Sources*"]
        for i, s in enumerate(sources[:7], 1):
            lines.append(f"{i}. {s['source']} — {s['count']:,}")

    # Alerts by customer
    if by_account:
        lines += ["", "👥 *Alerts by Customer*"]
        for a in by_account[:10]:
            name = ACCOUNT_NAMES.get(a["account_id"], a["account_id"])
            lines.append(f"• {name}: {a['count']:,}")

    return "\n".join(lines)


# ── Slack ─────────────────────────────────────────────────────────────────────

def post_to_slack(text: str) -> None:
    if not SLACK_BOT_TOKEN or not STATS_SLACK_CHANNEL:
        print("[slack] Missing token or channel — skipping.")
        return
    payload = json.dumps({
        "channel": STATS_SLACK_CHANNEL,
        "text": text,
        "mrkdwn": True,
    }).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as r:
        result = json.loads(r.read())
    if result.get("ok"):
        print("[slack] ✅ Stats report posted.")
    else:
        print(f"[slack] Error: {result.get('error')}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None,
                        help="Week end date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    today         = date.fromisoformat(args.date) if args.date else date.today()
    week_end      = today
    week_start    = today - timedelta(days=7)
    prev_end      = week_start
    prev_start    = week_start - timedelta(days=7)

    print(f"\n🤖 Alert Triage Stats — {week_start} → {week_end}")
    print(f"   Prev week : {prev_start} → {prev_end}\n")

    # Aggregators
    curr     = {"total_alerts": 0, "true_positives": 0, "false_positives": 0,
                "avg_mttt_minutes": 0.0, "verdict_changes": 0,
                "endorsements": 0, "total_with_feedback": 0}
    prev     = {k: 0 for k in curr}
    esc      = {"escalated": 0, "auto_closed": 0}
    sources  = {}
    by_acct  = {}
    rules    = {"total": 0, "deterministic": 0, "guidance": 0}
    enrich   = {"total": 0, "successful": 0}
    mttt_w   = []   # (value, weight) for weighted avg

    for cfg in REGION_CONFIGS:
        r   = cfg["region"]
        prj = cfg["bq_project"]
        con = cfg["bq_conn"]
        print(f"  [{r}] Querying...")

        cm = fetch_summary(prj, con, week_start, week_end)
        pm = fetch_summary(prj, con, prev_start, prev_end)
        em = fetch_escalation(prj, con, week_start, week_end)
        sr = fetch_top_sources(prj, con, week_start, week_end)
        ba = fetch_by_account(prj, con, week_start, week_end)
        rm = fetch_rules(prj, con, week_start, week_end)
        eh = fetch_enrichment_hit_rate(prj, con, week_start, week_end)

        for k in ["total_alerts", "true_positives", "false_positives",
                  "verdict_changes", "endorsements", "total_with_feedback"]:
            curr[k] += cm.get(k, 0)
            prev[k] += pm.get(k, 0)

        if cm.get("avg_mttt_minutes", 0) > 0:
            mttt_w.append((cm["avg_mttt_minutes"], cm.get("total_alerts", 1)))

        esc["escalated"]  += em.get("escalated", 0)
        esc["auto_closed"] += em.get("auto_closed", 0)

        for s in sr:
            sources[s["source"]] = sources.get(s["source"], 0) + s["count"]
        for a in ba:
            by_acct[a["account_id"]] = by_acct.get(a["account_id"], 0) + a["count"]

        for k in ["total", "deterministic", "guidance"]:
            rules[k] += rm.get(k, 0)

        enrich["total"]      += eh.get("total", 0)
        enrich["successful"] += eh.get("successful", 0)

    # Weighted avg MTTT
    if mttt_w:
        total_w = sum(w for _, w in mttt_w)
        curr["avg_mttt_minutes"] = sum(m * w for m, w in mttt_w) / total_w if total_w else 0

    enrich["hit_rate_pct"] = (
        round(100.0 * enrich["successful"] / enrich["total"], 1)
        if enrich["total"] > 0 else 0.0
    )

    top_sources = sorted(
        [{"source": k, "count": v} for k, v in sources.items()],
        key=lambda x: x["count"], reverse=True
    )[:7]

    top_accounts = sorted(
        [{"account_id": k, "count": v} for k, v in by_acct.items()],
        key=lambda x: x["count"], reverse=True
    )

    rl = fetch_grafana_rl(week_start, week_end, prev_start, prev_end)

    message = build_message(
        week_start, week_end, prev_start,
        curr, prev, esc,
        top_sources, top_accounts,
        rules, enrich, rl,
    )

    print("\n" + message + "\n")
    post_to_slack(message)


if __name__ == "__main__":
    main()
