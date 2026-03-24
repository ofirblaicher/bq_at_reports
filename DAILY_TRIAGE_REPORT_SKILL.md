# Daily Alert Triage Report — Skill

## Purpose
Generate or update a daily alert triage `.docx` report for **any environment** from a dashboard screenshot, a trace/alert CSV, a human feedback CSV, or any combination of the three.

The canonical build template is at: `/home/claude/rreport_template.js`
The template is seeded with Rocket Companies / Mar 13 data. Every run replaces all seed values.

---

## LLM provider setup (required for narrative generation)

Set these environment variables before running the report agent:

```bash
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY='YOUR_ANTHROPIC_API_KEY'
```

Rules:
- If `AI_PROVIDER=anthropic`, use Anthropic for all prose generation (Executive Summary, declined-card narrative text, and short trend notes).
- Never send raw CSV files blindly to the model; send only structured aggregates plus minimal evidence rows.
- Keep all numeric values source-of-truth from parsed data. LLM is only for wording, not counting.

---

## What to confirm before starting

| Item | How to get it |
|---|---|
| **Environment name** | Ask if not stated: *"What's the environment or company name?"* |
| **Report date** | From screenshot date filter, or CSV timestamps |
| **Langfuse base URL** | Pattern: `https://langfuse.[domain]/project/[project-id]/sessions/` — ask or omit links |
| At least one of: screenshot / trace CSV / feedback CSV | Ask if none provided |

---

## Trigger phrases
- "update the report", "create the report for [date]", "generate the report"
- "new screenshot / new csv", "daily alert summary", "triage report for [date]"

---

## Input types & how to parse them

### 1. Dashboard screenshot
Read directly from the image:

| Field | Location |
|---|---|
| Date / range | Top filter bar |
| Total alerts | Donut chart center labeled TOTAL |
| Severity breakdown | "Alerts by Severity" donut + legend — read each label and % |
| Feedback distribution | "User Feedback Distribution" donut — No Feedback % / Changed to Other % |
| Alerts by Verdict | "Alerts by Verdict Over Time" scatter — read each series name and count |
| Alert source | Any series label (CrowdStrike, SentinelOne, etc.) |

**Derive raw counts from percentages:** `round(pct / 100 * total)`

**Dashboard total is the authority** — if a trace CSV has more rows than the dashboard total (e.g. duplicate traces, enrichment rows), use the dashboard number.

---

### 2. Trace / alert CSV (the large CSV with `input` and `output` columns)

This CSV uses **triple-nested JSON escaping**. The only correct parse method is **double json.loads**:

```python
import csv, json

with open("file.csv", newline='') as f:
    reader = csv.reader(f)
    headers = next(reader)
    raw_rows = list(reader)

def parse_cell(val):
    v = val.strip()
    if v.startswith('"') and v.endswith('"'):
        v = v[1:-1]
    try:
        inner = json.loads(v)    # decodes the outer escaped JSON string
        return json.loads(inner) # parses the actual JSON object
    except:
        return {}
```

**Do NOT use** `.replace('\\\\"', '"')` — fails on nested Windows paths.
**Do NOT use** `.replace('""', '"')` — wrong escaping level.
**Always use** double `json.loads` above.

From `output` field extract:
- `name` → alert name (host = everything after `" on "` at end)
- `severity` → strip `ALERT_SEVERITY_` prefix, Title case
- `sourceSeverity` → same
- `status` → strip `ALERT_STATUS_` prefix
- `verdict.final_decision`
- `verdict.primary_assessment` — **show ALL distinct values found**, including non-standard ones:
  - `Policy Violation` → orange
  - `BAS(Security Simulation Classification)` → display as `BAS / Simulation`, muted
- `verdict.confidence`
- `verdict.verdict`

From `input` field:
- `RawPayload.event.Hostname`
- `RawPayload.event.Tactic` / `Technique`
- `RawPayload.event.IOARuleName` / `IOARuleGroupName`

**Pipeline filter:** Only use rows where `name` column = `AlertTriagePipeline` for statistics. `CaseHistory.FindCases` rows are enrichment only and have no verdicts.

---

### 3. Human feedback CSV

Standard columns: `alert_id, alert_pretty_id, name, source, original_verdict, current_verdict, triage_confirmation, human_verified_at, human_verified_by, human_comment`

Parse with plain `csv.DictReader` — no special escaping needed.

Extract per row:
- `alert_pretty_id` → `#N`
- `alert_id` → full UUID
- host from `" on HOSTNAME"` suffix of `name`
- `original_verdict`, `current_verdict`
- `triage_confirmation` → Declined / Confirmed
- `human_verified_at` → display as-is
- `human_verified_by` → analyst email
- `human_comment` → verbatim, strip trailing whitespace

**Feedback loop KPI** = total feedback CSV rows.

---

## Data mapping → report sections

### Header bar (navy)
```
Left:   [ENV_NAME]  (small, above title)
        "Automated Alert Triage — Daily Report"
Right:  Generated: [DATE]
        Scope: [N] alerts  |  [DATE RANGE]
```

### KPI Cards (5 cards)
```
Alerts Triaged       = dashboard total (authoritative)
Auto-Closed          = Close count
Escalate Immediately = count
Escalate for Review  = count
Feedback Loop        = feedback CSV row count (0 if no CSV)
```

### Alert Statistics (left column, two side-by-side mini-tables)

**Final Decisions:**
- Escalate Immediately = N (red)
- Escalate for Review  = N (orange)
- Close                = N (green)

**Primary Assessment — show ALL distinct values from trace CSV:**
- Confirmed Malicious              = N (red)     ← always show even if 0
- High-Confidence Suspicious       = N (orange)  ← omit if 0
- Policy Violation                 = N (orange)  ← show if present
- Anomalous but Benign             = N (green)
- BAS / Simulation                 = N (muted)   ← show if present

**Label normalisation — always apply before counting:**
- `"True Positive – Benign"` and `"True Positive - Benign"` → merge into **Anomalous but Benign** (do NOT show as a separate row)
- `"BAS (Security Simulation Classification)"` → display as `BAS / Simulation`

> Adding extra rows: `makeStatTable([...], colWidth)` — just add rows to the array, no width change needed.

### Executive Summary (left column, prose)

Rules:
- State date, env name, total, source
- State escalation + close counts
- If feedback CSV has rows: mention override count, host(s), analyst email
- **Never say "zero analyst intervention"** unless feedback fields are all empty
- 3–5 sentences, no invented details

Template:
> "On [DATE], the automated triage system processed **[N] [SOURCE] alerts** for [ENV_NAME]. **[N] alerts were escalated immediately** — [brief description of what was found]. [N] borderline case(s) were escalated for review, and [N] benign alerts were auto-closed. [If CSV: **[N] analyst verdict override(s)** were recorded on [HOST] — [summary] by **[analyst email]**.]"

### Feedback & Verdict Changes (right column — combined single section)

**Feedback counts table** (3 cols: Feedback Field | Count | Status):
```
human_verified        = rows where triage_confirmation = "Confirmed"
verdict_modified      = total feedback CSV rows  (purple + "Active" if > 0)
verdict_restored      = 0
verification_undone   = 0
```

**Declined card** (purple, only if CSV has Declined rows):
- Title: `⚠ Verdict Declined — [current_verdict]  ([N] confirmed)`
- Rubric table (amber header): Alert # | Original | Updated | Confirmation — one row per CSV row
- `By: [analyst_email]  |  [timestamps]`
- `"[human_comment]"` verbatim, italics
- Langfuse links per alert_id (omit if no URL provided)
- **No Slack links. No "prompt routing" note.**

If no Declined rows → replace card with italic muted text:
`No analyst overrides recorded for this period.`

### Alerts by Verdict (right column, bottom)
- True Positive — Benign    = N (orange / highBg)
- True Positive — Malicious = N (teal `0F7B6C` / bg `D3F0EE`)
- If count = 0: use C.muted / C.white instead of colour
- Omit section entirely if panel absent from screenshot

### Repeated Trends (FULL-WIDTH below both columns)

5-column: **# | Host | Alerts | Alert Type | Count**

- Derive from trace CSV `name` field: extract host (after `" on "`) and alert type pattern
- Sort descending by count
- Max 5–7 rows
- Colours: red = critical/escalated, orange = high/policy, slate = benign
- **Always full-width (CONTENT = 10800 DXA)** — never constrained to LEFT_W
- The section is placed **after** the two-column body table, as a separate top-level element

---

## Build procedure

### Step 1 — Parse inputs
Use double `json.loads` for trace CSV. Use `csv.DictReader` for feedback CSV.
Tally all counts. Note: use dashboard total as authoritative for Alerts Triaged.

### Step 2 — Copy template
```bash
cp /home/claude/rreport_template.js /home/claude/[env_slug]_report_[DATE].js
```

### Step 3 — Apply replacements

**Use positional slice-replace (`c[:start] + new + c[end:]`) wherever string content might be ambiguous or contain special characters. Use `str.replace()` only for simple tokens that appear exactly once.**

```python
# ── IDENTITY ──
"Rocket Companies"                             →  "[ENV_NAME]"
# Note: appears in header TextRun, NOT in exec summary (which uses r('On March..'))

# ── HEADER ──
"Generated: 2026-03-13"                        →  "Generated: [DATE]"
"Scope: 17 alerts"                             →  "Scope: [N] alerts"
# TextRun date: "  \u2502  Mar 13, 2026"        →  "  \u2502  [DATE RANGE]"

# ── KPI CARDS ──
kpiCell('17', 'Alerts Triaged'                 →  kpiCell('[N]', ...
kpiCell('10', 'Auto-Closed'                    →  kpiCell('[N]', ...
kpiCell('6',  'Escalate Immediately'           →  kpiCell('[N]', ...
kpiCell('1',  'Escalate for Review'            →  kpiCell('[N]', ...
kpiCell('2',  'Feedback Loop'                  →  kpiCell('[N]', ...

# ── FINAL DECISIONS ──
statRow('Escalate Immediately', 6, ...         →  [N]
statRow('Escalate for Review',  1, ...         →  [N]
statRow('Close',               10, ...         →  [N]

# ── PRIMARY ASSESSMENT ──
# If categories unchanged: simple count replacement
statRow('Confirmed Malicious',      4, ...     →  [N]
statRow('High-Conf. Suspicious',    2, ...     →  [N]
statRow('Anomalous but Benign',    11, ...     →  [N]
# If new categories needed: positional replace of the entire makeStatTable([...]) block

# ── FEEDBACK ──
feedbackRow('verdict_modified',  2, 'Active', true,  C.purple, C.purpleBg, C.purple)
# → If 0: feedbackRow('verdict_modified',  0, '\u2014', false, C.muted, C.white, null)

# ── EXECUTIVE SUMMARY — ALWAYS use positional slice ──
start = c.find("r('On March 13, 2026")
end   = c.find("<last r() sentence>", start) + len("<last r() sentence>")
c = c[:start] + new_summary_block + c[end:]
# CRITICAL: also replace the date inside the prose explicitly
# The header date and the exec summary date are separate strings

# ── VERDICT CARD — use positional slice ──
# If no feedback: find "// Combined declined card" comment, replace to "// ALERTS BY VERDICT"
# If feedback: find same anchor, replace with updated card content
card_start = c.find("              // Combined declined card")
card_end   = c.find("              // ALERTS BY VERDICT", card_start)
c = c[:card_start] + new_card_or_no_card + c[card_end:]

# ── ALERTS BY VERDICT ──
statRow('True Positive \u2014 Benign',   11, C.high,   C.highBg)  →  [N]
statRow('True Positive \u2014 Malicious', 6, '0F7B6C', 'D3F0EE')  →  [N]

# ── REPEATED TRENDS — full replacement ──
# Anchor: find first data row after tableHeader:true row in the trends table
# End: find closing ] of the rows array
# Replace entire data rows block
# Each row: see template pattern (5 cells: rank, host, count, type, count)

# ── OUTPUT FILENAME ──
"rocket_visual_mar13.docx"  →  "[env_slug]_report_[DATE].docx"
```

### Step 3.5 — LLM prose generation (when provider is enabled)

If `AI_PROVIDER=anthropic` and `ANTHROPIC_API_KEY` is present:
- Build a compact JSON payload from parsed metrics:
  - env name, date, source name, total alerts
  - final decisions counts, primary assessment counts
  - feedback override count + top host(s) + analyst email(s)
  - top repeated trends (max 5)
- Prompt the model to produce:
  - executive summary paragraph (3-5 sentences, factual only)
  - optional single-line trend insight
  - optional override narrative sentence for declined card
- Inject generated prose into the same replacement anchors in Step 3.
- If the LLM call fails or times out, fall back to deterministic template text and continue build.

### Step 4 — Build and validate
```bash
node /home/claude/[env_slug]_report_[DATE].js
python3 /mnt/skills/public/docx/scripts/office/validate.py /home/claude/[env_slug]_report_[DATE].docx
```

### Optional automation command (Anthropic + template patch + build)
```bash
python3 generate_docx_report.py --metrics report_metrics.example.json
```
This command:
- reads `.env` for `AI_PROVIDER` + `ANTHROPIC_API_KEY`
- uses this skill as LLM guidance for narrative prose
- patches `rocket_report_template.js` with metrics + summary
- runs Node and writes `[env_slug]_report_[DATE].docx`

### Step 5 — Output
```bash
cp /home/claude/[env_slug]_report_[DATE].docx /mnt/user-data/outputs/[env_slug]_report_[DATE].docx
```
Call `present_files`.

---

## Common failures & fixes

| Symptom | Root cause | Fix |
|---|---|---|
| JSON parse fails for trace CSV | Wrong unescaping | Use double `json.loads` |
| Exec summary has wrong date | Only header date replaced, not prose `r('On March...')` | Always replace both separately |
| Assessment table missing new categories | `str.replace` can't match multi-line block | Use positional slice to replace entire `makeStatTable([...])` block |
| Repeated Trends cut off | Table constrained to `LEFT_W` | Place as full-width element after body table, width = `CONTENT` |
| `Unexpected token 'new'` syntax error | Missing `,` after `]})` in trend rows | Add `,` after every `]})` preceding another `new TableRow` |
| Replacement ate file header | `end` anchor matched inside `require(...)` | Anchor `end` to a string unique to exec summary, not file-level code |
| Verdict card `str.replace` fails | Card has `\`, `"`, special chars | Use positional find of `// Combined declined card` + slice-replace |
| Dashboard total ≠ CSV rows | CSV includes enrichment/duplicate rows | Dashboard total is authoritative |
| `feedbackRow` still purple after setting to 0 | Replacement string mismatch on multi-param line | Match exact spacing — copy from template grep output |

---

## Missing data handling

| Missing | Action |
|---|---|
| Environment name | Ask before proceeding |
| Langfuse URL | Ask, or omit all session links |
| No feedback CSV | `verdict_modified = 0`, skip card, show "No analyst overrides recorded" |
| No screenshot | Ask for it |
| Severity % only | `round(pct / 100 * total)` |
| Primary assessment not in screenshot | Derive from decisions |
| Alerts by Verdict absent | Omit section entirely |

---

## Template seed values

| Token | Seed | Replace with |
|---|---|---|
| ENV_NAME | `Rocket Companies` | env name |
| Generated | `2026-03-13` | date |
| Scope | `17 alerts` | N alerts |
| Date range | `Mar 13, 2026` | date range |
| Alerts Triaged | `17` | N |
| Auto-Closed | `10` | N |
| Escalate Immediately | `6` | N |
| Escalate for Review | `1` | N |
| Feedback Loop | `2` | N |
| Close (decision) | `10` | N |
| Confirmed Malicious | `4` | N |
| High-Conf. Suspicious | `2` | N |
| Anomalous but Benign | `11` | N |
| verdict_modified | `2` | N |
| Alert #79 ID | `019ce78f-2540-7c07-83b8-d6f3fc038dcd` | new UUID |
| Alert #78 ID | `019ce740-7cf6-7b9d-98c6-f7fe754a6fbe` | new UUID |
| Analyst | `adamlarkin@rocket.com` | analyst |
| Timestamps | `Mar 13  15:53 / 15:56` | new times |
| Comment | `This is a known process for XOME TMs.` | new comment |
| Langfuse base | `https://langfuse.us.torqio.dev/project/cmg7q573p0001wv07rsw58dbu/sessions/` | new base URL |
| TP Benign | `11` | N |
| TP Malicious | `6` | N |
| Trend host 1 | `XMCHELAP408` | new host |
| Trend host 2 | `XMCHELAP507` | new host |
| Trend host 3 | `MACCC02WP0EUHTD8` | new host |
| Trend host 4 | `5CG43966WS` | new host |
| Trend host 5 | `Various` | new host |

---

## Visual design constants — never change

| Rule | Value |
|---|---|
| navy | `1B3A6B` |
| blue | `2563A8` |
| lblue | `DBEAFE` |
| critical | `B91C1C` / bg `FEE2E2` |
| high | `C2410C` / bg `FFEDD5` |
| green | `166534` / bg `DCFCE7` |
| purple | `6B21A8` / bg `F3E8FF` |
| slate | `475569` / bg `F1F5F9` |
| teal (TP Malicious) | `0F7B6C` / bg `D3F0EE` |
| Font | Arial throughout |
| Page | US Letter 12240×15840 DXA, 720 DXA margins |
| Footer | Page X/Y, right-aligned, muted |
| Layout | Navy header → KPI row → 2-col body → full-width Repeated Trends |
| Section labels | ALL CAPS, blue, 3pt bottom border |
| Never include | POC Value Delivered, Slack links, "prompt routing" note |
