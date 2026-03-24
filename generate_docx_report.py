#!/usr/bin/env python3
"""
Generate a daily triage .docx report from metrics JSON.

Flow:
1) Load environment variables (.env + process env)
2) Optionally call Anthropic for narrative text
3) Copy and patch report template JS with metrics + narrative
4) Run Node to build the final .docx
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def slugify(value: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return clean or "environment"


def js_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def get_int(payload: dict, *paths: tuple[str, ...], default: int = 0) -> int:
    for path in paths:
        node = payload
        ok = True
        for key in path:
            if not isinstance(node, dict) or key not in node:
                ok = False
                break
            node = node[key]
        if ok:
            try:
                return int(node)
            except (TypeError, ValueError):
                continue
    return default


def get_str(payload: dict, *paths: tuple[str, ...], default: str = "") -> str:
    for path in paths:
        node = payload
        ok = True
        for key in path:
            if not isinstance(node, dict) or key not in node:
                ok = False
                break
            node = node[key]
        if ok and node is not None:
            return str(node)
    return default


def deterministic_summary(metrics: dict) -> str:
    report_date = get_str(metrics, ("date",), default=str(date.today() - timedelta(days=1)))
    env_name = get_str(metrics, ("env_name",), ("environment",), default="the environment")
    source = get_str(metrics, ("source",), default="CrowdStrike")
    total = get_int(metrics, ("totals", "alerts_triaged"), ("totals", "total_alerts"), default=0)
    esc_i = get_int(metrics, ("final_decisions", "escalate_immediately"), default=0)
    esc_r = get_int(metrics, ("final_decisions", "escalate_for_review"), default=0)
    close = get_int(metrics, ("final_decisions", "close"), default=0)
    over = get_int(metrics, ("feedback", "verdict_modified"), ("totals", "feedback_loop"), default=0)
    host = get_str(metrics, ("feedback", "top_host"), default="multiple hosts")
    analyst = get_str(metrics, ("feedback", "analyst_email"), default="the analyst team")

    base = (
        f"On {report_date}, the automated triage system processed {total} {source} alerts for {env_name}. "
        f"{esc_i} alerts were escalated immediately, {esc_r} were escalated for review, and {close} were auto-closed."
    )
    if over > 0:
        base += f" {over} analyst verdict override(s) were recorded on {host} by {analyst}."
    else:
        base += " No analyst overrides were recorded for this period."
    return base


def call_anthropic(skill_text: str, metrics: dict) -> dict | None:
    if os.getenv("AI_PROVIDER", "").lower() != "anthropic":
        return None
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None

    preferred_model = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
    model_candidates = []
    for m in [
        preferred_model,
        "claude-3-7-sonnet-20250219",
        "claude-3-5-sonnet-20241022",
        "claude-3-haiku-20240307",
    ]:
        if m and m not in model_candidates:
            model_candidates.append(m)
    rules_match = re.search(
        r"### Executive Summary \(left column, prose\)([\s\S]*?)### Feedback & Verdict Changes",
        skill_text,
    )
    rules_excerpt = rules_match.group(1).strip() if rules_match else ""

    prompt = {
        "task": (
            "Generate concise daily triage narrative text from metrics. "
            "Do not invent facts. Keep all numbers exactly as provided."
        ),
        "rules_excerpt": rules_excerpt,
        "metrics": metrics,
        "output_schema": {
            "executive_summary": "string, 3-5 sentences, plain text only",
            "trend_insight": "string, max 1 sentence",
            "override_narrative": "string, max 1 sentence",
        },
    }

    base_url = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/")
    body = None
    for model in model_candidates:
        req_payload = {
            "model": model,
            "max_tokens": 700,
            "temperature": 0.2,
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Return JSON only with keys executive_summary, trend_insight, override_narrative.\n\n"
                        + json.dumps(prompt, ensure_ascii=True)
                    ),
                }
            ],
        }
        req = urllib.request.Request(
            f"{base_url}/v1/messages",
            data=json.dumps(req_payload).encode("utf-8"),
            headers={
                "content-type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="ignore")
            if exc.code == 404 and "model" in err_body.lower():
                print(f"[warn] Anthropic model unavailable: {model}. Trying fallback...")
                continue
            print(f"[warn] Anthropic request failed: HTTP {exc.code} {exc.reason}. {err_body[:300]}")
            return None
        except (urllib.error.URLError, TimeoutError) as exc:
            print(f"[warn] Anthropic request failed: {exc}")
            return None
    if body is None:
        print("[warn] Anthropic request failed: no available model found.")
        return None

    text_chunks = []
    for chunk in body.get("content", []):
        if isinstance(chunk, dict) and chunk.get("type") == "text":
            text_chunks.append(chunk.get("text", ""))
    if not text_chunks:
        return None

    combined = "\n".join(text_chunks).strip()
    match = re.search(r"\{[\s\S]*\}", combined)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def replace_or_fail(content: str, pattern: str, repl: str, flags: int = 0) -> str:
    updated, count = re.subn(pattern, lambda _: repl, content, count=1, flags=flags)
    if count != 1:
        raise ValueError(f"Pattern not found or duplicated: {pattern}")
    return updated


def patch_template(content: str, metrics: dict, summary: str, output_docx: Path) -> str:
    env_name = get_str(metrics, ("env_name",), ("environment",), default="Rocket Companies")
    report_date = get_str(metrics, ("date",), default=str(date.today() - timedelta(days=1)))
    date_range = get_str(metrics, ("date_range",), default=report_date)

    alerts_triaged = get_int(metrics, ("totals", "alerts_triaged"), ("totals", "total_alerts"), default=17)
    auto_closed = get_int(metrics, ("final_decisions", "close"), default=10)
    esc_i = get_int(metrics, ("final_decisions", "escalate_immediately"), default=6)
    esc_r = get_int(metrics, ("final_decisions", "escalate_for_review"), default=1)
    feedback_loop = get_int(metrics, ("feedback", "verdict_modified"), ("totals", "feedback_loop"), default=0)
    human_verified = get_int(metrics, ("feedback", "human_verified"), default=0)
    no_override_text = get_str(
        metrics,
        ("feedback", "no_overrides_text"),
        default="No analyst overrides recorded for this period.",
    )

    confirmed_malicious = get_int(metrics, ("primary_assessment", "Confirmed Malicious"), default=4)
    high_conf = get_int(metrics, ("primary_assessment", "High-Conf. Suspicious"), ("primary_assessment", "High-Confidence Suspicious"), default=2)
    anomalous_benign = get_int(metrics, ("primary_assessment", "Anomalous but Benign"), default=11)
    tp_benign = get_int(metrics, ("alerts_by_verdict", "True Positive — Benign"), ("alerts_by_verdict", "True Positive - Benign"), default=11)
    tp_malicious = get_int(metrics, ("alerts_by_verdict", "True Positive — Malicious"), ("alerts_by_verdict", "True Positive - Malicious"), default=6)

    content = replace_or_fail(content, r"text:'Rocket Companies'", f"text:'{js_escape(env_name)}'")
    content = replace_or_fail(content, r"text:'Generated: [^']*'", f"text:'Generated: {js_escape(report_date)}'")
    content = replace_or_fail(content, r"text:'Scope: \d+ alerts'", f"text:'Scope: {alerts_triaged} alerts'")
    content = replace_or_fail(content, r"text:'  \\u2502  [^']*'", f"text:'  \\u2502  {js_escape(date_range)}'")

    def rep_kpi(label: str, value: int) -> None:
        nonlocal content
        pat = rf"kpiCell\('\d+',\s*'{re.escape(label)}'"
        repl = f"kpiCell('{value}', '{label}'"
        content = replace_or_fail(content, pat, repl)

    rep_kpi("Alerts Triaged", alerts_triaged)
    rep_kpi("Auto-Closed", auto_closed)
    rep_kpi("Escalate Immediately", esc_i)
    rep_kpi("Escalate for Review", esc_r)
    rep_kpi("Feedback Loop", feedback_loop)

    def rep_stat(label: str, value: int) -> None:
        nonlocal content
        pat = rf"statRow\('{re.escape(label)}',\s*\d+,"
        repl = f"statRow('{label}', {value},"
        content = replace_or_fail(content, pat, repl)

    rep_stat("Escalate Immediately", esc_i)
    rep_stat("Escalate for Review", esc_r)
    rep_stat("Close", auto_closed)
    rep_stat("Confirmed Malicious", confirmed_malicious)
    rep_stat("High-Conf. Suspicious", high_conf)
    rep_stat("Anomalous but Benign", anomalous_benign)
    content = replace_or_fail(
        content,
        r"statRow\('True Positive (?:—|\\u2014) Benign',\s*\d+,",
        f"statRow('True Positive \\u2014 Benign',   {tp_benign},",
    )
    content = replace_or_fail(
        content,
        r"statRow\('True Positive (?:—|\\u2014) Malicious',\s*\d+,",
        f"statRow('True Positive \\u2014 Malicious', {tp_malicious},",
    )

    # feedback table lines
    content = replace_or_fail(
        content,
        r"feedbackRow\('human_verified',\s*\d+,",
        f"feedbackRow('human_verified',     {human_verified},",
    )
    if feedback_loop > 0:
        content = replace_or_fail(
            content,
            r"feedbackRow\('verdict_modified',[^\n]*",
            f"feedbackRow('verdict_modified',   {feedback_loop}, 'Active',  true,  C.purple, C.purpleBg, C.purple),",
        )
    else:
        content = replace_or_fail(
            content,
            r"feedbackRow\('verdict_modified',[^\n]*",
            "feedbackRow('verdict_modified',   0, '\\u2014', false, C.muted,  C.white,   null),",
        )

    # executive summary block
    summary_js = js_escape(summary)
    exec_pat = (
        r"r\('On March 13, 2026, the automated triage system processed '\),[\s\S]*?"
        r"r\('\. The system delivered high-confidence verdicts on every alert processed\.'\),"
    )
    content = replace_or_fail(content, exec_pat, f"r('{summary_js}'),", flags=re.MULTILINE)

    # If there are no overrides, swap the declined card with muted text.
    if feedback_loop == 0:
        card_anchor = "              // Combined declined card with rubric inline"
        verdict_anchor = "              // ALERTS BY VERDICT"
        card_start = content.find(card_anchor)
        verdict_start = content.find(verdict_anchor, card_start if card_start >= 0 else 0)
        if card_start >= 0 and verdict_start > card_start:
            replacement = (
                "              // Combined declined card with rubric inline\n"
                "              new Paragraph({...sp(20,60),\n"
                f"                children:[r('{js_escape(no_override_text)}',{{size:15,italics:true,color:C.muted}})]\n"
                "              }),\n"
                "              gap(80),\n\n"
            )
            content = content[:card_start] + replacement + content[verdict_start:]

    # Replace repeated trends rows if explicit trend data is provided.
    trends = metrics.get("trends")
    if isinstance(trends, list) and trends:
        section_anchor = "sectionLabel('Repeated Trends')"
        section_idx = content.find(section_anchor)
        if section_idx >= 0:
            header_anchor = "new TableRow({ tableHeader:true, children:["
            header_start = content.find(header_anchor, section_idx)
            header_end = content.find("          ]}),", header_start)
            rows_end = content.find("        ]\n      }),", header_end)
            if header_start >= 0 and header_end > header_start and rows_end > header_end:
                row_lines = []
                for i, t in enumerate(trends[:7], start=1):
                    host = js_escape(str(t.get("host", "Various")))
                    alerts = int(t.get("alerts", t.get("count", 0)))
                    count = int(t.get("count", alerts))
                    alert_type = js_escape(str(t.get("alert_type", "Alert trend")))
                    level = str(t.get("level", "")).lower()
                    color = "C.muted"
                    bg = "C.white"
                    if level in {"critical", "malicious", "escalated"}:
                        color, bg = "C.critical", "C.critBg"
                    elif level in {"high", "policy", "warning"}:
                        color, bg = "C.high", "C.highBg"
                    elif level in {"benign", "slate", "muted"}:
                        color, bg = "C.muted", "C.slBg"
                    row_lines.append(
                        f"          hostRow({i}, '{host}', {alerts}, '{alert_type}', {color}, {bg}),"
                    )
                rows_block = "\n".join(row_lines) + "\n"
                content = content[: header_end + len("          ]}),\n")] + rows_block + content[rows_end:]

    # output file path
    out_js = js_escape(str(output_docx))
    content = replace_or_fail(
        content,
        r"fs\.writeFileSync\('/home/claude/rocket_visual_mar13\.docx',\s*buf\);",
        f"fs.writeFileSync('{out_js}', buf);",
    )

    return content


def main() -> int:
    here = Path(__file__).resolve().parent
    yesterday = str(date.today() - timedelta(days=1))
    default_template = str(here / "rreport_template.js")
    if not Path(default_template).exists():
        fallback = here / "rocket_report_template.js"
        if fallback.exists():
            default_template = str(fallback)

    parser = argparse.ArgumentParser(description="Generate docx daily report from JSON metrics + Anthropic prose")
    parser.add_argument("--metrics", required=True, help="Path to metrics JSON")
    parser.add_argument("--template", default=default_template)
    parser.add_argument("--skill", default=str(here / "DAILY_TRIAGE_REPORT_SKILL.md"))
    parser.add_argument("--out-dir", default=str(here))
    parser.add_argument("--report-date", default=yesterday)
    parser.add_argument("--skip-build", action="store_true", help="Only emit patched JS, do not run node")
    args = parser.parse_args()

    load_env_file(Path(args.out_dir) / ".env")
    load_env_file(here / ".env")

    metrics_path = Path(args.metrics).expanduser().resolve()
    template_path = Path(args.template).expanduser().resolve()
    skill_path = Path(args.skill).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    if "date" not in metrics:
        metrics["date"] = args.report_date
    if "date_range" not in metrics:
        metrics["date_range"] = args.report_date

    skill_text = skill_path.read_text(encoding="utf-8") if skill_path.exists() else ""
    llm_result = call_anthropic(skill_text, metrics)
    summary = get_str(metrics, ("executive_summary",), default=deterministic_summary(metrics))
    if not get_str(metrics, ("executive_summary",), default="") and llm_result and isinstance(llm_result.get("executive_summary"), str):
        summary = llm_result["executive_summary"].strip()

    env_name = get_str(metrics, ("env_name",), ("environment",), default="environment")
    slug = slugify(env_name)
    report_date = get_str(metrics, ("date",), default=args.report_date)
    js_out = out_dir / f"{slug}_report_{report_date}.js"
    docx_out = out_dir / f"{slug}_report_{report_date}.docx"

    original_template = template_path.read_text(encoding="utf-8")
    patched = patch_template(original_template, metrics, summary, docx_out)
    js_out.write_text(patched, encoding="utf-8")
    print(f"[ok] Wrote JS: {js_out}")

    if args.skip_build:
        print("[ok] Skipped Node build (--skip-build).")
        return 0

    cmd = ["node", str(js_out)]
    print(f"[run] {' '.join(cmd)}")
    run = subprocess.run(cmd, cwd=str(out_dir), env=os.environ.copy(), capture_output=True, text=True)
    if run.stdout.strip():
        print(run.stdout.strip())
    if run.returncode != 0:
        if run.stderr.strip():
            print(run.stderr.strip(), file=sys.stderr)
        return run.returncode

    print(f"[ok] Wrote DOCX: {docx_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
