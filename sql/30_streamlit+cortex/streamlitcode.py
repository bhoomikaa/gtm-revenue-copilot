import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import date
from typing import List, Optional, Dict, Tuple, Any
import re
import html
import json

# -----------------------------
# Optional chart libraries (graceful fallback)
# -----------------------------
PLOTLY_OK = False
ALTAIR_OK = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

try:
    import altair as alt
    ALTAIR_OK = True
except Exception:
    ALTAIR_OK = False


# -----------------------------
# App Config
# -----------------------------
APP_TITLE = "Revenue Intelligence System"
DB = "GTM_COPILOT"
RAW = f"{DB}.RAW"
MARTS = f"{DB}.MARTS"

# Candidate tables (auto-resolve first existing)
TABLE_CANDIDATES = {
    "ACCOUNTS": [
        f"{RAW}.ACCOUNTS",
        f"{DB}.RAW.ACCOUNTS",
    ],
    "SALES_REPS": [
        f"{RAW}.SALES_REPS",
        f"{DB}.RAW.SALES_REPS",
    ],
    "FCT_MRR": [
        f"{MARTS}.FCT_MRR_COMPLETE",
        f"{MARTS}.FCT_MRR",
    ],
    "FCT_PIPELINE": [
        f"{MARTS}.FCT_PIPELINE",
    ],
    "STAGE_HISTORY": [
        f"{RAW}.OPPORTUNITY_STAGE_HISTORY",
        f"{MARTS}.OPPORTUNITY_STAGE_HISTORY",
    ],
    "SUPPORT_TICKETS": [
        f"{RAW}.SUPPORT_TICKETS",
    ],
    "HEALTH_SNAPSHOT": [
        f"{MARTS}.ACCOUNT_HEALTH",
        f"{MARTS}.METRICS_ACCOUNT_HEALTH",
        f"{MARTS}.FCT_ACCOUNT_HEALTH",
    ],
}

# Thresholds for executive alerts
NRR_ALERT_BELOW = 90.0
GRR_ALERT_BELOW = 90.0
PIPELINE_COVERAGE_ALERT_BELOW = 3.0  # common heuristic (3x). Your dataset may be higher.
ARR_NEGATIVE_TREND_LOOKBACK = 3       # months


# -----------------------------
# Streamlit Page Setup
# -----------------------------
st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# CSS (FIXED + visually appealing, no logos)
# -----------------------------
st.markdown(
    """
<style>
/* Page + container */
[data-testid="stAppViewContainer"] {
  background: linear-gradient(180deg, #f8fafc 0%, #ffffff 32%, #ffffff 100%);
}
.block-container {
  padding-top: 2rem;
  padding-bottom: 3rem;
  max-width: 1320px;
}

/* Sidebar polish */
section[data-testid="stSidebar"] {
  background-color: #fbfbfc;
  border-right: 1px solid rgba(49, 51, 63, 0.10);
}

/* Headings */
h1, h2, h3 {
  letter-spacing: -0.02em;
}

/* Dividers */
.hr {
  height: 1px;
  background: rgba(49, 51, 63, 0.12);
  margin: 1.0rem 0 1.25rem 0;
}

/* Buttons polish */
div.stButton > button, div.stDownloadButton > button {
  border-radius: 12px !important;
  border: 1px solid rgba(49,51,63,0.14) !important;
  padding: 0.55rem 0.9rem !important;
  font-weight: 750 !important;
}
div.stButton > button:hover, div.stDownloadButton > button:hover {
  border-color: rgba(37,99,235,0.35) !important;
  box-shadow: 0 2px 12px rgba(37,99,235,0.12) !important;
}

/* KPI cards */
.kpi {
  border: 1px solid rgba(49, 51, 63, 0.12);
  border-radius: 16px;
  padding: 14px 14px 10px 14px;
  background: #ffffff;
  box-shadow: 0 2px 14px rgba(15, 23, 42, 0.06);
}
.kpi-label {
  font-size: 0.78rem;
  color: rgba(49, 51, 63, 0.70);
  font-weight: 750;
}
.kpi-value {
  font-size: 1.65rem;
  font-weight: 950;
  margin-top: 4px;
  color: rgba(17, 24, 39, 1);
}
.kpi-delta-pos {font-size: 0.82rem; font-weight: 850; color: #0f766e; margin-top: 6px;}
.kpi-delta-neg {font-size: 0.82rem; font-weight: 850; color: #b91c1c; margin-top: 6px;}
.kpi-delta-neutral {font-size: 0.82rem; font-weight: 850; color: rgba(49, 51, 63, 0.65); margin-top: 6px;}

/* Section headings */
.section-title {
  font-size: 1.15rem;
  font-weight: 900;
  margin: 0.25rem 0 0.75rem 0;
}
.subtle {color: rgba(49, 51, 63, 0.70); font-size: 0.92rem;}

/* Table polish */
div[data-testid="stDataFrame"] {
  border-radius: 12px;
  overflow: hidden;
  border: 1px solid rgba(49,51,63,0.10);
}

/* Chat polish */
div[data-testid="stChatMessage"] { border-radius: 14px; }
div[data-testid="stChatMessageContent"] {
  font-size: 0.98rem;
  line-height: 1.45;
}

/* -----------------------------
   Executive Narrative Card
------------------------------*/
.exec-card{
  border: 1px solid rgba(49,51,63,.12);
  border-radius: 18px;
  background: #fff;
  padding: 18px 18px 14px 18px;
  box-shadow: 0 2px 14px rgba(15, 23, 42, 0.06);
}
.exec-headline{
  font-size: 1.25rem;
  font-weight: 950;
  letter-spacing: -0.02em;
  margin-bottom: 10px;
}
.exec-meta{
  font-size: .85rem;
  color: rgba(49,51,63,.65);
  margin-bottom: 14px;
}
.exec-section-title{
  font-size: .95rem;
  font-weight: 900;
  margin: 10px 0 6px 0;
}
.exec-body{
  font-size: .97rem;
  color: rgba(17,24,39,1);
  line-height: 1.5;
}
.exec-grid{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 14px;
  margin-top: 10px;
}
.exec-list{
  margin: 0;
  padding-left: 18px;
}
.exec-list li{ margin: 6px 0; }
.exec-risk{
  border-left: 4px solid #ef4444;
  padding-left: 10px;
  border-radius: 8px;
  background: rgba(239,68,68,0.03);
}
.exec-action{
  border-left: 4px solid #10b981;
  padding-left: 10px;
  border-radius: 8px;
  background: rgba(16,185,129,0.03);
}
@media (max-width: 900px){
  .exec-grid{ grid-template-columns: 1fr; }
}
</style>
""",
    unsafe_allow_html=True,
)


# -----------------------------
# Helpers
# -----------------------------
def _norm_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    s = s.replace("\\n", "\n").replace("\\t", "\t")
    s = s.replace("###", "").replace("##", "").replace("#", "").strip()
    return s


def sql_quote_list(values: List[str]) -> str:
    escaped = [v.replace("'", "''") for v in values]
    return ", ".join([f"'{v}'" for v in escaped])


@st.cache_data(ttl=900, show_spinner=False)
def run_sql(sql: str) -> pd.DataFrame:
    session = get_active_session()
    df = session.sql(sql).to_pandas()
    if df is None:
        return pd.DataFrame()
    df.columns = [c.upper() for c in df.columns]
    return df


def table_exists(fqn: str) -> bool:
    try:
        _ = run_sql(f"select 1 as ok from {fqn} limit 1")
        return True
    except Exception:
        return False


@st.cache_data(ttl=3600, show_spinner=False)
def resolve_tables() -> Dict[str, Optional[str]]:
    resolved: Dict[str, Optional[str]] = {}
    for key, candidates in TABLE_CANDIDATES.items():
        found = None
        for c in candidates:
            try:
                if table_exists(c):
                    found = c
                    break
            except Exception:
                continue
        resolved[key] = found
    return resolved


def fmt_currency(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return "—"
    sign = "-" if x < 0 else ""
    x = abs(float(x))
    if x >= 1_000_000_000:
        return f"{sign}${x/1_000_000_000:,.2f}B"
    if x >= 1_000_000:
        return f"{sign}${x/1_000_000:,.2f}M"
    if x >= 1_000:
        return f"{sign}${x:,.0f}"
    return f"{sign}${x:,.2f}"


def fmt_number(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return "—"
    x = float(x)
    if abs(x) >= 1_000_000_000:
        return f"{x/1_000_000_000:,.2f}B"
    if abs(x) >= 1_000_000:
        return f"{x/1_000_000:,.2f}M"
    if abs(x) >= 1_000:
        return f"{x:,.0f}"
    return f"{x:,.2f}"


def fmt_pct(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return "—"
    return f"{float(x):.2f}%"


def fmt_x(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return "—"
    return f"{float(x):.2f}x"


def kpi_card(label: str, value: str, delta: Optional[float] = None, delta_fmt: str = "number"):
    if delta is None or pd.isna(delta):
        delta_html = '<div class="kpi-delta-neutral">Δ —</div>'
    else:
        if delta_fmt == "currency":
            delta_text = fmt_currency(delta)
        elif delta_fmt == "pct":
            delta_text = fmt_pct(delta)
        elif delta_fmt == "x":
            delta_text = fmt_x(delta)
        else:
            delta_text = fmt_number(delta)

        cls = "kpi-delta-pos" if delta > 0 else "kpi-delta-neg" if delta < 0 else "kpi-delta-neutral"
        sign = "+" if delta > 0 else ""
        delta_html = f'<div class="{cls}">Δ {sign}{delta_text}</div>'

    st.markdown(
        f"""
<div class="kpi">
  <div class="kpi-label">{label}</div>
  <div class="kpi-value">{value}</div>
  {delta_html}
</div>
""",
        unsafe_allow_html=True,
    )


def chart_line(df: pd.DataFrame, x_col: str, y_col: str, title: str, height: int = 320):
    if df is None or df.empty:
        st.info("No data for current filters.")
        return

    if PLOTLY_OK:
        fig = px.line(df, x=x_col, y=y_col)
        fig.update_layout(template="plotly_white", height=height, margin=dict(l=0, r=0, t=45, b=0))
        fig.update_traces(line=dict(width=3))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"line_{x_col}_{y_col}_{title}")
        return

    if ALTAIR_OK:
        c = (
            alt.Chart(df)
            .mark_line(strokeWidth=3)
            .encode(
                x=alt.X(f"{x_col}:T", title=""),
                y=alt.Y(f"{y_col}:Q", title=""),
                tooltip=[alt.Tooltip(f"{x_col}:T"), alt.Tooltip(f"{y_col}:Q")],
            )
            .properties(height=height, title=title)
        )
        st.altair_chart(c, use_container_width=True)
        return

    st.line_chart(df.set_index(x_col)[y_col], height=height)


def chart_multi_line(df: pd.DataFrame, x_col: str, y_cols: List[str], title: str, height: int = 320):
    if df is None or df.empty:
        st.info("No data for current filters.")
        return

    dfl = df[[x_col] + y_cols].copy()
    dfl = dfl.melt(id_vars=[x_col], var_name="SERIES", value_name="VALUE")

    if PLOTLY_OK:
        fig = px.line(dfl, x=x_col, y="VALUE", color="SERIES")
        fig.update_layout(template="plotly_white", height=height, margin=dict(l=0, r=0, t=45, b=0))
        fig.update_traces(line=dict(width=3))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False},
                        key=f"multiline_{x_col}_{'_'.join(y_cols)}_{title}")
        return

    if ALTAIR_OK:
        c = (
            alt.Chart(dfl)
            .mark_line(strokeWidth=3)
            .encode(
                x=alt.X(f"{x_col}:T", title=""),
                y=alt.Y("VALUE:Q", title=""),
                color=alt.Color("SERIES:N", title=""),
                tooltip=[alt.Tooltip(f"{x_col}:T"), alt.Tooltip("SERIES:N"), alt.Tooltip("VALUE:Q")],
            )
            .properties(height=height, title=title)
        )
        st.altair_chart(c, use_container_width=True)
        return

    st.line_chart(df.set_index(x_col)[y_cols], height=height)


def chart_bar(df: pd.DataFrame, x_col: str, y_col: str, title: str, height: int = 340):
    if df is None or df.empty:
        st.info("No data for current filters.")
        return

    if PLOTLY_OK:
        fig = px.bar(df, x=x_col, y=y_col)
        fig.update_layout(template="plotly_white", height=height, margin=dict(l=0, r=0, t=45, b=0))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"bar_{x_col}_{y_col}_{title}")
        return

    if ALTAIR_OK:
        c = (
            alt.Chart(df)
            .mark_bar()
            .encode(
                x=alt.X(f"{x_col}:T", title=""),
                y=alt.Y(f"{y_col}:Q", title=""),
                tooltip=[alt.Tooltip(f"{x_col}:T"), alt.Tooltip(f"{y_col}:Q")],
            )
            .properties(height=height, title=title)
        )
        st.altair_chart(c, use_container_width=True)
        return

    st.bar_chart(df.set_index(x_col)[y_col], height=height)


# -----------------------------
# Cortex Executive Narrative
# -----------------------------
def _metric_for_llm(v: Any, label: str, kind: str) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return f"{label} data not available for selected period"
    try:
        x = float(v)
        if kind == "currency":
            return fmt_currency(x)
        if kind == "pct":
            return fmt_pct(x)
        if kind == "x":
            return fmt_x(x)
        return str(v)
    except Exception:
        s = str(v).strip()
        return s if s else f"{label} data not available for selected period"


def parse_exec_narrative(text: str) -> Dict[str, Any]:
    text = _norm_text(text)
    pattern = re.compile(r"(?im)^\s*(Headline|Executive Summary|Key Risks|Recommended Actions)\s*:\s*$")
    parts = pattern.split(text)

    out: Dict[str, Any] = {"Headline": "", "Executive Summary": "", "Key Risks": "", "Recommended Actions": ""}
    if len(parts) >= 3:
        for i in range(1, len(parts), 2):
            section = parts[i].strip()
            content = parts[i + 1].strip() if i + 1 < len(parts) else ""
            out[section] = content
    else:
        out["Executive Summary"] = text

    def extract_numbered_list(block: str) -> List[str]:
        items: List[str] = []
        for line in block.splitlines():
            m = re.match(r"^\s*\d+\.\s*(.+)$", line.strip())
            if m:
                items.append(m.group(1).strip())
        return items

    out["_risks"] = extract_numbered_list(out.get("Key Risks", ""))
    out["_actions"] = extract_numbered_list(out.get("Recommended Actions", ""))
    return out


def render_exec_narrative(narrative_text: str, filters_text: str = ""):
    d = parse_exec_narrative(narrative_text)

    headline = html.escape(d.get("Headline", "")).strip() or "Executive Narrative"
    summary = html.escape(d.get("Executive Summary", "")).replace("\n", "<br/>").strip()

    risks: List[str] = d.get("_risks", []) or []
    actions: List[str] = d.get("_actions", []) or []

    risks_html = "".join([f"<li>{html.escape(x)}</li>" for x in risks]) or "<li>Not available for selected period</li>"
    actions_html = "".join([f"<li>{html.escape(x)}</li>" for x in actions]) or "<li>Not available for selected period</li>"

    meta = html.escape(filters_text).strip()

    st.markdown(
        f"""
<div class="exec-card">
  <div class="exec-headline">{headline}</div>
  <div class="exec-meta">{meta}</div>

  <div class="exec-section-title">Executive Summary</div>
  <div class="exec-body">{summary}</div>

  <div class="exec-grid">
    <div class="exec-risk">
      <div class="exec-section-title">Key Risks</div>
      <ul class="exec-list exec-body">{risks_html}</ul>
    </div>
    <div class="exec-action">
      <div class="exec-section-title">Recommended Actions</div>
      <ul class="exec-list exec-body">{actions_html}</ul>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=900, show_spinner=False)
def generate_exec_narrative(ctx: Dict) -> str:
    session = get_active_session()

    arr = _metric_for_llm(ctx.get("arr_latest"), "ARR", "currency")
    nrr = _metric_for_llm(ctx.get("nrr_latest"), "NRR", "pct")
    grr = _metric_for_llm(ctx.get("grr_latest"), "GRR", "pct")
    win = _metric_for_llm(ctx.get("win_latest"), "Win Rate", "pct")
    coverage = _metric_for_llm(ctx.get("coverage_ratio"), "Pipeline Coverage", "x")

    retention_interpretable = bool(ctx.get("retention_interpretable", False))
    retention_note = str(ctx.get("retention_note") or "Retention interpretability not provided; treat as NOT fully loaded.")

    prompt = f"""
You are a Chief Revenue Officer writing a board-level executive update.

IMPORTANT:
- Do NOT use markdown symbols like ### or ##
- Do NOT include quotation marks
- Do NOT repeat the prompt
- Write in clean executive prose
- Be concise, structured, and confident

STRICT DATA INTERPRETATION RULES (NON-NEGOTIABLE):
- Retention interpretability flag: {retention_interpretable}
- Retention note: {retention_note}

- If retention_interpretable is FALSE:
  You MUST NOT interpret NRR/GRR as churn or deterioration.
  You MUST say retention is not fully loaded/complete for the selected period.

- If NRR or GRR equals 0.00% AND retention_interpretable is FALSE:
  DO NOT interpret this as churn. Call it a data completeness/boundary issue.

- Only discuss churn if explicitly supported by confirmed retention data (retention_interpretable = TRUE).

Time Window:
{ctx.get('filters_text')}

Key Metrics:
ARR: {arr}
NRR: {nrr}
GRR: {grr}
Win Rate: {win}
Pipeline Coverage: {coverage}

Structure your response EXACTLY as:

Headline:
<1 strong executive sentence>

Executive Summary:
<3–5 sentences explaining overall performance>

Key Risks:
1. ...
2. ...
3. ...

Recommended Actions:
1. ...
2. ...
3. ...
"""

    sql = f"""
    select AI_COMPLETE(
        'mistral-large2',
        $$ {prompt} $$
    ) as RESPONSE;
    """

    try:
        df = session.sql(sql).to_pandas()
        if df is None or df.empty:
            return _norm_text(
                "Headline:\nExecutive narrative unavailable for selected period.\n\n"
                "Executive Summary:\nNo response returned.\n\n"
                "Key Risks:\n1. Not available\n2. Not available\n3. Not available\n\n"
                "Recommended Actions:\n1. Not available\n2. Not available\n3. Not available"
            )
        return _norm_text(df.iloc[0]["RESPONSE"])
    except Exception as e:
        return _norm_text(
            "Headline:\nExecutive narrative unavailable for selected period.\n\n"
            f"Executive Summary:\nError generating narrative: {str(e)}\n\n"
            "Key Risks:\n1. Not available\n2. Not available\n3. Not available\n\n"
            "Recommended Actions:\n1. Not available\n2. Not available\n3. Not available"
        )


# -----------------------------
# Q&A pack + strict retention logic
# -----------------------------
def _df_tail_records(df: pd.DataFrame, n: int, sort_col: str, cols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    d = df.copy()
    if sort_col in d.columns:
        d = d.sort_values(sort_col)
    if cols:
        keep = [c for c in cols if c in d.columns]
        if keep:
            d = d[keep]
    return json.loads(d.tail(n).to_json(orient="records", date_format="iso"))


def assess_pipeline_coverage(ratio: Optional[float], target_x: float = 3.0) -> str:
    if ratio is None or (isinstance(ratio, float) and pd.isna(ratio)):
        return "unknown"
    r = float(ratio)
    if r < target_x:
        return "low"
    if r < target_x * 2:
        return "healthy"
    if r < 10:
        return "strong"
    return "very high (validate denominator)"


@st.cache_data(ttl=900, show_spinner=False)
def retention_data_quality(
    start_date: date,
    end_date: date,
    account_filter: str,
    cohort_month: Optional[date],
    coverage_threshold_pct: float = 90.0,
) -> Dict[str, Any]:
    if cohort_month is None:
        return {
            "cohort_accounts": None,
            "next_month_accounts": None,
            "next_month_coverage_pct": None,
            "retention_interpretable": False,
            "coverage_threshold_pct": float(coverage_threshold_pct),
            "retention_note": "Retention cohort month not available for selected filters.",
        }

    cohort_month_str = str(cohort_month)

    sql = f"""
    with base as (
        select
            m.account_id,
            m.month,
            m.total_mrr
        from {FCT_MRR_TBL} m
        join {ACCOUNTS_TBL} a on a.account_id = m.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
        where m.month >= '{start_date}'
          and m.month <= '{end_date}'
          and {account_filter}
    ),
    cohort as (
        select distinct account_id
        from base
        where month = to_date('{cohort_month_str}')
          and total_mrr > 0
    ),
    nxt as (
        select distinct account_id
        from base
        where month = dateadd(month, 1, to_date('{cohort_month_str}'))
    )
    select
      (select count(*) from cohort) as cohort_accounts,
      (select count(*) from nxt) as next_month_accounts,
      round(100 * (select count(*) from nxt) / nullif((select count(*) from cohort), 0), 2) as next_month_coverage_pct
    ;
    """

    try:
        session = get_active_session()
        df = session.sql(sql).to_pandas()
        if df is None or df.empty:
            raise RuntimeError("No rows returned from retention data-quality query.")

        cohort_accounts = df.loc[0, "COHORT_ACCOUNTS"]
        next_month_accounts = df.loc[0, "NEXT_MONTH_ACCOUNTS"]
        cov = df.loc[0, "NEXT_MONTH_COVERAGE_PCT"]

        interpretable = (cov is not None) and float(cov) >= float(coverage_threshold_pct)

        note = (
            "Retention is interpretable (next-month MRR coverage is high)."
            if interpretable
            else "Retention is NOT interpretable: next-month MRR appears incomplete/not loaded for the selected period."
        )

        return {
            "cohort_accounts": int(cohort_accounts) if cohort_accounts is not None else None,
            "next_month_accounts": int(next_month_accounts) if next_month_accounts is not None else None,
            "next_month_coverage_pct": float(cov) if cov is not None else None,
            "retention_interpretable": bool(interpretable),
            "coverage_threshold_pct": float(coverage_threshold_pct),
            "retention_note": note,
        }

    except Exception as e:
        return {
            "cohort_accounts": None,
            "next_month_accounts": None,
            "next_month_coverage_pct": None,
            "retention_interpretable": False,
            "coverage_threshold_pct": float(coverage_threshold_pct),
            "retention_note": f"Retention data-quality check failed: {str(e)}",
        }


@st.cache_data(ttl=300, show_spinner=False)
def build_qa_pack_json(
    start_d: date,
    end_d: date,
    account_filter: str,
    arr_latest: Optional[float],
    nrr_latest: Optional[float],
    grr_latest: Optional[float],
    win_latest: Optional[float],
    coverage_ratio: Optional[float],
    arr_delta: Optional[float] = None,
    win_delta: Optional[float] = None,
    coverage_open: Optional[float] = None,
    coverage_avg3: Optional[float] = None,
    latest_ret_month: Optional[date] = None,
    pipeline_coverage_target_x: float = 3.0,
) -> str:
    arr_df_local = get_arr_trend(start_d, end_d, account_filter)
    ret_df_local = get_retention_trend(start_d, end_d, account_filter)
    closed_df_local = get_closed_revenue_monthly(start_d, end_d, account_filter)
    coverage_df_local = get_pipeline_coverage(start_d, end_d, account_filter)
    stage_df_local = get_open_pipeline_by_stage(account_filter)
    move_df_local = get_mrr_movement_summary(start_d, end_d, account_filter)
    exp_df_local, con_df_local = get_top_mrr_movers(start_d, end_d, account_filter)

    inferred_ret_month: Optional[date] = None
    try:
        if latest_ret_month is not None:
            inferred_ret_month = latest_ret_month
        elif ret_df_local is not None and not ret_df_local.empty and "MONTH" in ret_df_local.columns and "START_MRR" in ret_df_local.columns:
            tmp = ret_df_local.copy()
            tmp["MONTH"] = pd.to_datetime(tmp["MONTH"])
            valid = tmp[tmp["START_MRR"] > 0]
            if not valid.empty:
                inferred_ret_month = pd.to_datetime(valid["MONTH"].max()).date()
    except Exception:
        inferred_ret_month = latest_ret_month

    dq = retention_data_quality(start_d, end_d, account_filter, inferred_ret_month)

    if coverage_open is None:
        try:
            coverage_open = float(coverage_df_local.iloc[0]["TOTAL_OPEN_PIPELINE"]) if (coverage_df_local is not None and not coverage_df_local.empty) else None
        except Exception:
            coverage_open = None

    if coverage_avg3 is None:
        try:
            coverage_avg3 = float(coverage_df_local.iloc[0]["AVG_3M_CLOSED_REVENUE"]) if (coverage_df_local is not None and not coverage_df_local.empty) else None
        except Exception:
            coverage_avg3 = None

    pack: Dict[str, Any] = {
        "time_window": f"{start_d} to {end_d}",
        "filters": {
            "start_date": str(start_d),
            "end_date": str(end_d),
            "retention_cohort_month": str(inferred_ret_month) if inferred_ret_month else None,
        },
        "benchmarks": {
            "pipeline_coverage_target_x": float(pipeline_coverage_target_x),
        },
        "metrics": {
            "arr_latest": arr_latest,
            "arr_delta_mom": arr_delta,
            "nrr_pct": nrr_latest,
            "grr_pct": grr_latest,
            "win_rate_pct": win_latest,
            "win_rate_delta_mom": win_delta,
            "pipeline_coverage_ratio_x": coverage_ratio,
            "pipeline_coverage_assessment": assess_pipeline_coverage(coverage_ratio, pipeline_coverage_target_x),
            "total_open_pipeline": coverage_open,
            "avg_3m_closed_revenue": coverage_avg3,
        },
        "data_quality": dq,
        "series": {
            "arr_trend_last_12": _df_tail_records(arr_df_local, 12, "MONTH", ["MONTH", "TOTAL_ARR"]),
            "retention_last_12": _df_tail_records(ret_df_local, 12, "MONTH", ["MONTH", "START_MRR", "END_MRR", "NRR_PCT", "GRR_PCT"]),
            "closed_rev_last_12": _df_tail_records(closed_df_local, 12, "CLOSE_MONTH", ["CLOSE_MONTH", "TOTAL_CLOSED_REVENUE", "WIN_RATE_PCT", "AVG_SALES_CYCLE_DAYS"]),
        },
        "pipeline": {
            "coverage_row": json.loads(coverage_df_local.to_json(orient="records"))[:1] if coverage_df_local is not None and not coverage_df_local.empty else [],
            "open_by_stage_top_8": (
                json.loads(stage_df_local.sort_values("OPEN_PIPELINE", ascending=False).head(8).to_json(orient="records"))
                if stage_df_local is not None and not stage_df_local.empty
                else []
            ),
        },
        "mrr_movement": {
            "movement_summary": json.loads(move_df_local.to_json(orient="records")) if move_df_local is not None and not move_df_local.empty else [],
            "top_expansions": (
                json.loads(exp_df_local.sort_values("MRR_DELTA", ascending=False).head(5).to_json(orient="records"))
                if exp_df_local is not None and not exp_df_local.empty
                else []
            ),
            "top_contractions": (
                json.loads(con_df_local.sort_values("MRR_DELTA", ascending=True).head(5).to_json(orient="records"))
                if con_df_local is not None and not con_df_local.empty
                else []
            ),
        },
        "rules": {
            "grounding_rule": "Use ONLY fields in this JSON. If missing/null, say data not available.",
            "retention_rule": (
                "If data_quality.retention_interpretable is false, DO NOT interpret NRR/GRR as churn. "
                "Say retention is not fully loaded/complete for the selected period."
            ),
            "pipeline_rule": (
                "Pipeline coverage is an X multiple. Compare vs benchmarks.pipeline_coverage_target_x. "
                "If assessment is 'very high', advise validating denominator/baseline."
            ),
        },
    }

    return json.dumps(pack, default=str)


@st.cache_data(ttl=900, show_spinner=False)
def cortex_analyst_answer(question: str, pack_json: str) -> str:
    session = get_active_session()

    prompt = f"""
You are a senior GTM analytics leader answering board-level questions.

You MUST use ONLY the JSON pack provided.
You MUST NOT invent numbers.
You MUST NOT assume trends not explicitly supported by the pack.
You MUST NOT interpret missing/boundary data as business deterioration.

STRICT DATA INTERPRETATION RULES (NON-NEGOTIABLE)

1) Retention:
- If data_quality.retention_interpretable is FALSE:
  You MUST NOT interpret NRR/GRR as churn or retention deterioration.
  You MUST say retention is not fully loaded/complete for the selected period.
- If NRR or GRR equals 0.00% AND retention_interpretable is FALSE:
  DO NOT interpret as churn. Call it data completeness/boundary-period.

2) Pipeline:
- Pipeline coverage is an X multiple (metrics.pipeline_coverage_ratio_x).
- Compare vs benchmarks.pipeline_coverage_target_x.
- Do NOT label coverage weak unless it is below the benchmark.

3) Precision:
- Evidence bullets must use exact numbers from JSON (and include the time_window when relevant).
- If a required field is missing, say Data not available for the selected period and list what is missing.

RESPONSE FORMAT (EXACTLY)

Answer:
<2–4 precise sentences grounded in the pack>

Evidence:
- <bullet with exact numbers from JSON>
- <bullet with exact numbers from JSON>
- <bullet with exact numbers from JSON>

What I would check next:
- <analytical next step>
- <analytical next step>
- <analytical next step>

Confidence Level:
<High | Medium | Low> — <one short reason grounded in availability/completeness>

JSON PACK:
{pack_json}

Question:
{question}
"""

    sql = f"""
    select AI_COMPLETE(
        'mistral-large2',
        $$ {prompt} $$
    ) as RESPONSE;
    """

    try:
        df = session.sql(sql).to_pandas()
        if df is None or df.empty:
            return (
                "Answer:\nData not available for the selected period.\n\n"
                "Evidence:\n- No response returned.\n- —\n- —\n\n"
                "What I would check next:\n- Confirm Cortex is enabled and AI_COMPLETE returns output\n- Validate the data pack is non-empty\n- Retry with a narrower question\n\n"
                "Confidence Level:\nLow — no response returned."
            )
        return _norm_text(df.iloc[0]["RESPONSE"])
    except Exception as e:
        return (
            "Answer:\nExecutive Q&A response unavailable.\n\n"
            f"Evidence:\n- Error: {str(e)}\n- —\n- —\n\n"
            "What I would check next:\n- Validate data pack integrity\n- Confirm retention data-quality query executes\n- Retry Cortex call\n\n"
            "Confidence Level:\nLow — execution error."
        )


@st.cache_data(ttl=900, show_spinner=False)
def cortex_agent_run(goal: str, pack_json: str) -> str:
    session = get_active_session()

    prompt = f"""
You are a GTM Analytics Agent. You will solve the user's goal using ONLY the JSON pack.

Hard rules:
- Use ONLY JSON fields. Do not invent numbers.
- Follow the SAME retention strictness:
  If data_quality.retention_interpretable is false, do NOT infer churn.
- Pipeline coverage is an X multiple; compare vs benchmarks.pipeline_coverage_target_x.
- Keep the reasoning chain short, explicit, and checkable.

OUTPUT FORMAT (EXACTLY)

Plan:
1. ...
2. ...
3. ...

Reasoning Chain:
Step 1: <what you checked in JSON + what you found>
Step 2: <what you checked in JSON + what you found>
Step 3: <what you checked in JSON + what you found>

Answer:
<2–4 sentences>

Evidence:
- <exact numbers from JSON>
- <exact numbers from JSON>
- <exact numbers from JSON>

Confidence Level:
<High | Medium | Low> — <one short reason tied to data availability / interpretability>

JSON PACK:
{pack_json}

User goal:
{goal}
"""

    sql = f"""
    select AI_COMPLETE('mistral-large2', $$ {prompt} $$) as RESPONSE;
    """

    try:
        df = session.sql(sql).to_pandas()
        if df is None or df.empty:
            return (
                "Plan:\n1. —\n2. —\n3. —\n\n"
                "Reasoning Chain:\nStep 1: —\nStep 2: —\nStep 3: —\n\n"
                "Answer:\nData not available for the selected period.\n\n"
                "Evidence:\n- —\n- —\n- —\n\n"
                "Confidence Level:\nLow — no response returned."
            )
        return _norm_text(df.iloc[0]["RESPONSE"])
    except Exception as e:
        return (
            "Plan:\n1. —\n2. —\n3. —\n\n"
            "Reasoning Chain:\nStep 1: —\nStep 2: —\nStep 3: —\n\n"
            "Answer:\nAgent output unavailable.\n\n"
            f"Evidence:\n- Error: {str(e)}\n- —\n- —\n\n"
            "Confidence Level:\nLow — execution error."
        )


# -----------------------------
# Resolve tables
# -----------------------------
tables = resolve_tables()

missing_critical = [k for k in ["ACCOUNTS", "FCT_MRR", "FCT_PIPELINE"] if not tables.get(k)]
if missing_critical:
    st.error(
        "Missing required tables to run this app. "
        f"Could not find: {', '.join(missing_critical)}."
    )
    st.stop()

ACCOUNTS_TBL = tables["ACCOUNTS"]
REPS_TBL = tables.get("SALES_REPS")  # optional
FCT_MRR_TBL = tables["FCT_MRR"]
FCT_PIPELINE_TBL = tables["FCT_PIPELINE"]
STAGE_HIST_TBL = tables.get("STAGE_HISTORY")  # optional
SUPPORT_TICKETS_TBL = tables.get("SUPPORT_TICKETS")  # optional
HEALTH_TBL = tables.get("HEALTH_SNAPSHOT")  # optional


# -----------------------------
# Load filter domains
# -----------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def get_filter_domains() -> Dict[str, List[str]]:
    df = run_sql(
        f"""
        select
            distinct
            segment,
            region,
            industry
        from {ACCOUNTS_TBL}
        """
    )
    domains = {
        "SEGMENT": sorted([x for x in df["SEGMENT"].dropna().unique().tolist()]) if "SEGMENT" in df.columns else [],
        "REGION": sorted([x for x in df["REGION"].dropna().unique().tolist()]) if "REGION" in df.columns else [],
        "INDUSTRY": sorted([x for x in df["INDUSTRY"].dropna().unique().tolist()]) if "INDUSTRY" in df.columns else [],
    }

    if REPS_TBL:
        r = run_sql(f"select distinct team, region as rep_region from {REPS_TBL}")
        domains["REP_TEAM"] = sorted([x for x in r["TEAM"].dropna().unique().tolist()]) if "TEAM" in r.columns else []
        domains["REP_REGION"] = sorted([x for x in r["REP_REGION"].dropna().unique().tolist()]) if "REP_REGION" in r.columns else []
    else:
        domains["REP_TEAM"] = []
        domains["REP_REGION"] = []

    return domains


@st.cache_data(ttl=900, show_spinner=False)
def get_mrr_date_bounds() -> Tuple[date, date]:
    df = run_sql(f"select min(month) as min_month, max(month) as max_month from {FCT_MRR_TBL}")
    if df.empty or pd.isna(df.loc[0, "MIN_MONTH"]) or pd.isna(df.loc[0, "MAX_MONTH"]):
        return date(2023, 1, 1), date.today()
    return pd.to_datetime(df.loc[0, "MIN_MONTH"]).date(), pd.to_datetime(df.loc[0, "MAX_MONTH"]).date()


domains = get_filter_domains()
min_month, max_month = get_mrr_date_bounds()


# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.markdown("### Filters")
date_range = st.sidebar.date_input(
    "Date range (month-level)",
    value=(min_month, max_month),
    min_value=min_month,
    max_value=max_month,
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_month, max_month

segments = st.sidebar.multiselect("Segment", domains["SEGMENT"], default=domains["SEGMENT"])
regions = st.sidebar.multiselect("Region", domains["REGION"], default=domains["REGION"])
industries = st.sidebar.multiselect("Industry", domains["INDUSTRY"], default=domains["INDUSTRY"])

rep_teams = []
rep_regions = []
if domains["REP_TEAM"]:
    rep_teams = st.sidebar.multiselect("Owner Rep Team", domains["REP_TEAM"], default=domains["REP_TEAM"])
if domains["REP_REGION"]:
    rep_regions = st.sidebar.multiselect("Owner Rep Region", domains["REP_REGION"], default=domains["REP_REGION"])

st.sidebar.markdown('<div class="hr"></div>', unsafe_allow_html=True)
st.sidebar.markdown(
    """
**What this app shows**
- Revenue & ARR trend (MRR → ARR)
- Retention (NRR / GRR) with correct cohort logic
- Pipeline performance & coverage
- Customer health + account drill-down
""",
    help="All metrics run on Snowflake tables you already built.",
)


# -----------------------------
# Build a shared filter clause
# -----------------------------
def build_account_filter_sql(a_alias: str = "a", r_alias: str = "r") -> str:
    clauses = []

    if segments:
        clauses.append(f"{a_alias}.segment in ({sql_quote_list(segments)})")
    if regions:
        clauses.append(f"{a_alias}.region in ({sql_quote_list(regions)})")
    if industries:
        clauses.append(f"{a_alias}.industry in ({sql_quote_list(industries)})")

    if REPS_TBL:
        if rep_teams:
            clauses.append(f"{r_alias}.team in ({sql_quote_list(rep_teams)})")
        if rep_regions:
            clauses.append(f"{r_alias}.region in ({sql_quote_list(rep_regions)})")

    return " and ".join(clauses) if clauses else "1=1"


ACCOUNT_FILTER = build_account_filter_sql()


# -----------------------------
# Core Metric Queries
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def get_arr_trend(start_d: date, end_d: date, account_filter: str) -> pd.DataFrame:
    sql = f"""
    with base as (
        select
            m.account_id,
            m.month,
            m.total_mrr
        from {FCT_MRR_TBL} m
        join {ACCOUNTS_TBL} a
            on a.account_id = m.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
        where m.month >= '{start_d}'
          and m.month <= '{end_d}'
          and {account_filter}
    )
    select
        month,
        round(sum(total_mrr) * 12, 2) as total_arr
    from base
    group by month
    order by month;
    """
    return run_sql(sql)


@st.cache_data(ttl=600, show_spinner=False)
def get_retention_trend(start_d: date, end_d: date, account_filter: str) -> pd.DataFrame:
    sql = f"""
    with base as (
        select
            m.account_id,
            m.month,
            m.total_mrr
        from {FCT_MRR_TBL} m
        join {ACCOUNTS_TBL} a
            on a.account_id = m.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
        where m.month >= '{start_d}'
          and m.month <= '{end_d}'
          and {account_filter}
    ),
    maxm as (
        select max(month) as max_month from base
    ),
    cur as (
        select b.*
        from base b
        cross join maxm
        where b.month < maxm.max_month
          and b.total_mrr > 0
    ),
    nxt as (
        select account_id, month, total_mrr from base
    )
    select
        cur.month as month,
        round(sum(cur.total_mrr), 2) as start_mrr,
        round(sum(coalesce(nxt.total_mrr, 0)), 2) as end_mrr,
        round(sum(least(coalesce(nxt.total_mrr, 0), cur.total_mrr)), 2) as retained_mrr,
        round(100 * sum(coalesce(nxt.total_mrr, 0)) / nullif(sum(cur.total_mrr), 0), 2) as nrr_pct,
        round(100 * sum(least(coalesce(nxt.total_mrr, 0), cur.total_mrr)) / nullif(sum(cur.total_mrr), 0), 2) as grr_pct
    from cur
    left join nxt
        on nxt.account_id = cur.account_id
       and nxt.month = dateadd(month, 1, cur.month)
    group by cur.month
    order by cur.month;
    """
    return run_sql(sql)


@st.cache_data(ttl=600, show_spinner=False)
def get_closed_revenue_monthly(start_d: date, end_d: date, account_filter: str) -> pd.DataFrame:
    sql = f"""
    with base as (
        select
            p.*,
            date_trunc('month', p.close_date) as close_month
        from {FCT_PIPELINE_TBL} p
        join {ACCOUNTS_TBL} a
            on a.account_id = p.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = p.rep_id" if REPS_TBL else ""}
        where p.is_closed = true
          and p.close_date is not null
          and date_trunc('month', p.close_date) >= '{start_d}'
          and date_trunc('month', p.close_date) <= '{end_d}'
          and {account_filter}
    )
    select
        close_month,
        round(sum(amount), 2) as total_closed_revenue,
        round(sum(case when is_won then amount else 0 end), 2) as total_won_revenue,
        round(100 * sum(case when is_won then 1 else 0 end) / nullif(count(*), 0), 2) as win_rate_pct,
        round(avg(datediff(day, created_date, close_date)), 2) as avg_sales_cycle_days
    from base
    group by close_month
    order by close_month;
    """
    return run_sql(sql)


@st.cache_data(ttl=600, show_spinner=False)
def get_pipeline_coverage(start_d: date, end_d: date, account_filter: str) -> pd.DataFrame:
    sql = f"""
    with open_pipe as (
        select
            round(sum(p.amount), 2) as total_open_pipeline
        from {FCT_PIPELINE_TBL} p
        join {ACCOUNTS_TBL} a
            on a.account_id = p.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = p.rep_id" if REPS_TBL else ""}
        where p.is_closed = false
          and {account_filter}
    ),
    closed as (
        select
            date_trunc('month', p.close_date) as close_month,
            round(sum(p.amount), 2) as total_closed_revenue
        from {FCT_PIPELINE_TBL} p
        join {ACCOUNTS_TBL} a
            on a.account_id = p.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = p.rep_id" if REPS_TBL else ""}
        where p.is_closed = true
          and p.close_date is not null
          and date_trunc('month', p.close_date) >= '{start_d}'
          and date_trunc('month', p.close_date) <= '{end_d}'
          and {account_filter}
        group by 1
    ),
    last3 as (
        select *
        from closed
        order by close_month desc
        limit 3
    ),
    avg3 as (
        select avg(total_closed_revenue) as avg_3m_closed_revenue from last3
    )
    select
        o.total_open_pipeline,
        a.avg_3m_closed_revenue,
        round(o.total_open_pipeline / nullif(a.avg_3m_closed_revenue, 0), 2) as pipeline_coverage_ratio
    from open_pipe o
    cross join avg3 a;
    """
    return run_sql(sql)


@st.cache_data(ttl=600, show_spinner=False)
def get_open_pipeline_by_stage(account_filter: str) -> pd.DataFrame:
    sql = f"""
    select
        current_stage,
        round(sum(amount), 2) as open_pipeline,
        round(sum(amount * probability), 2) as weighted_pipeline,
        count(*) as opp_count
    from {FCT_PIPELINE_TBL} p
    join {ACCOUNTS_TBL} a on a.account_id = p.account_id
    {"left join " + REPS_TBL + " r on r.rep_id = p.rep_id" if REPS_TBL else ""}
    where p.is_closed = false
      and {account_filter}
    group by current_stage
    order by open_pipeline desc;
    """
    return run_sql(sql)


@st.cache_data(ttl=600, show_spinner=False)
def get_mrr_account_month(start_d: date, end_d: date, account_filter: str) -> pd.DataFrame:
    sql = f"""
    select
        m.account_id,
        m.month,
        round(m.total_mrr, 2) as total_mrr
    from {FCT_MRR_TBL} m
    join {ACCOUNTS_TBL} a on a.account_id = m.account_id
    {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
    where m.month >= '{start_d}'
      and m.month <= '{end_d}'
      and {account_filter}
    order by m.account_id, m.month;
    """
    return run_sql(sql)


@st.cache_data(ttl=600, show_spinner=False)
def get_mrr_movement_summary(start_d: date, end_d: date, account_filter: str) -> pd.DataFrame:
    sql = f"""
    with base as (
        select
            m.account_id,
            m.month,
            m.total_mrr
        from {FCT_MRR_TBL} m
        join {ACCOUNTS_TBL} a on a.account_id = m.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
        where m.month >= '{start_d}'
          and m.month <= '{end_d}'
          and {account_filter}
    ),
    lagged as (
        select
            account_id,
            month,
            total_mrr,
            lag(total_mrr) over (partition by account_id order by month) as prev_mrr
        from base
    ),
    labeled as (
        select
            *,
            case
                when prev_mrr is null and total_mrr > 0 then 'New'
                when prev_mrr > 0 and total_mrr = 0 then 'Churn'
                when total_mrr > prev_mrr then 'Expansion'
                when total_mrr < prev_mrr then 'Contraction'
                else 'Flat'
            end as movement_type
        from lagged
    )
    select
        movement_type,
        count(*) as rows_count,
        round(sum(total_mrr - coalesce(prev_mrr, 0)), 2) as net_mrr_change
    from labeled
    group by movement_type
    order by movement_type;
    """
    return run_sql(sql)


@st.cache_data(ttl=600, show_spinner=False)
def get_top_mrr_movers(start_d: date, end_d: date, account_filter: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sql = f"""
    with base as (
        select
            m.account_id,
            m.month,
            m.total_mrr
        from {FCT_MRR_TBL} m
        join {ACCOUNTS_TBL} a on a.account_id = m.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
        where m.month >= '{start_d}'
          and m.month <= '{end_d}'
          and {account_filter}
    ),
    maxm as (select max(month) as max_month from base),
    prevm as (select dateadd(month, -1, max_month) as prev_month from maxm),
    last2 as (
        select b.*
        from base b
        join maxm on b.month in (maxm.max_month, dateadd(month, -1, maxm.max_month))
    ),
    pivoted as (
        select
            account_id,
            max(case when month = (select max_month from maxm) then total_mrr end) as mrr_curr,
            max(case when month = (select prev_month from prevm) then total_mrr end) as mrr_prev
        from last2
        group by account_id
    ),
    scored as (
        select
            p.account_id,
            round(coalesce(p.mrr_curr,0),2) as mrr_curr,
            round(coalesce(p.mrr_prev,0),2) as mrr_prev,
            round(coalesce(p.mrr_curr,0) - coalesce(p.mrr_prev,0),2) as mrr_delta
        from pivoted p
    ),
    enriched as (
        select
            s.*,
            a.account_name,
            a.segment,
            a.region,
            a.industry
        from scored s
        join {ACCOUNTS_TBL} a on a.account_id = s.account_id
    )
    select * from enriched;
    """
    df = run_sql(sql)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    expansions = df.sort_values("MRR_DELTA", ascending=False).head(10)
    contractions = df.sort_values("MRR_DELTA", ascending=True).head(10)
    return expansions, contractions


# -----------------------------
# Health (use existing table if present, else compute fallback)
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def get_health_snapshot(start_d: date, end_d: date, account_filter: str) -> pd.DataFrame:
    if HEALTH_TBL:
        try:
            sql = f"""
            with h as (
                select *
                from {HEALTH_TBL}
            ),
            scoped as (
                select *
                from h
                where month >= '{start_d}' and month <= '{end_d}'
            ),
            maxm as (select max(month) as max_month from scoped)
            select
                s.*
            from scoped s
            join maxm on s.month = maxm.max_month;
            """
            df = run_sql(sql)
            if not df.empty:
                return df
        except Exception:
            pass

    ticket_join = ""
    ticket_cols = "0 as ticket_cnt_90d"
    ticket_ref = "0"
    if SUPPORT_TICKETS_TBL:
        ticket_join = f"""
        left join (
            select
                account_id,
                count(*) as ticket_cnt_90d
            from {SUPPORT_TICKETS_TBL}
            where created_date >= dateadd(day, -90, (select max(month) from base))
            group by account_id
        ) t on t.account_id = b.account_id
        """
        ticket_cols = "coalesce(t.ticket_cnt_90d,0) as ticket_cnt_90d"
        ticket_ref = "coalesce(t.ticket_cnt_90d,0)"

    sql = f"""
    with base as (
        select
            m.account_id,
            m.month,
            m.total_mrr
        from {FCT_MRR_TBL} m
        join {ACCOUNTS_TBL} a on a.account_id = m.account_id
        {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
        where m.month >= '{start_d}'
          and m.month <= '{end_d}'
          and {account_filter}
    ),
    maxm as (select max(month) as max_month from base),
    latest as (
        select
            b.account_id,
            b.month,
            b.total_mrr,
            lag(b.total_mrr) over (partition by b.account_id order by b.month) as prev_mrr,
            avg(b.total_mrr) over (partition by b.account_id order by b.month rows between 2 preceding and current row) as mrr_avg_3m
        from base b
    ),
    snap as (
        select *
        from latest
        join maxm on latest.month = maxm.max_month
    ),
    enriched as (
        select
            b.account_id,
            a.account_name,
            a.segment,
            a.region,
            a.industry,
            round(b.total_mrr,2) as total_mrr,
            round(coalesce(b.prev_mrr,0),2) as prev_mrr,
            round((b.total_mrr - coalesce(b.prev_mrr,0)) / nullif(coalesce(b.prev_mrr,0),0), 4) as mom_mrr_pct,
            round(b.mrr_avg_3m,2) as mrr_avg_3m
        from snap b
        join {ACCOUNTS_TBL} a on a.account_id = b.account_id
    )
    select
        e.*,
        {ticket_cols},
        greatest(0, least(100,
            70
            + case when e.total_mrr > e.prev_mrr then 15 when e.total_mrr < e.prev_mrr then -15 else 0 end
            + case when e.prev_mrr > 0 and e.total_mrr = 0 then -50 else 0 end
            + case when {ticket_ref} >= 8 then -20
                   when {ticket_ref} >= 4 then -10
                   else 0 end
        )) as health_score,
        case
            when e.prev_mrr > 0 and e.total_mrr = 0 then 'Lost'
            when e.total_mrr = 0 and e.prev_mrr = 0 then 'Stable'
            when {ticket_ref} >= 8 then 'High Risk'
            when e.total_mrr < e.prev_mrr then 'At Risk'
            when e.total_mrr > e.prev_mrr then 'Growing'
            else 'Stable'
        end as health_status
    from enriched e
    {ticket_join}
    ;
    """
    return run_sql(sql)


# -----------------------------
# Header
# -----------------------------
st.title(APP_TITLE)
st.caption("Revenue, retention, pipeline, and customer health — powered by your Snowflake marts.")


# Pull datasets (single source of truth per tab)
arr_df = get_arr_trend(start_date, end_date, ACCOUNT_FILTER)
ret_df = get_retention_trend(start_date, end_date, ACCOUNT_FILTER)
closed_df = get_closed_revenue_monthly(start_date, end_date, ACCOUNT_FILTER)
coverage_df = get_pipeline_coverage(start_date, end_date, ACCOUNT_FILTER)
open_stage_df = get_open_pipeline_by_stage(ACCOUNT_FILTER)


# Professional month selection logic (latest valid data)
if not arr_df.empty and "TOTAL_ARR" in arr_df.columns and "MONTH" in arr_df.columns:
    arr_valid = arr_df[arr_df["TOTAL_ARR"] > 0]
    latest_arr_month = pd.to_datetime(arr_valid["MONTH"].max()).date() if not arr_valid.empty else None
else:
    latest_arr_month = None

if not ret_df.empty and "START_MRR" in ret_df.columns and "MONTH" in ret_df.columns:
    ret_valid = ret_df[ret_df["START_MRR"] > 0]
    latest_ret_month = pd.to_datetime(ret_valid["MONTH"].max()).date() if not ret_valid.empty else None
else:
    latest_ret_month = None

if not closed_df.empty and "TOTAL_CLOSED_REVENUE" in closed_df.columns and "CLOSE_MONTH" in closed_df.columns:
    close_valid = closed_df[closed_df["TOTAL_CLOSED_REVENUE"] > 0]
    latest_close_month = pd.to_datetime(close_valid["CLOSE_MONTH"].max()).date() if not close_valid.empty else None
else:
    latest_close_month = None


# ARR latest/prev/delta
arr_latest = None
arr_prev = None
arr_delta = None

if arr_df is not None and not arr_df.empty and "MONTH" in arr_df.columns and "TOTAL_ARR" in arr_df.columns:
    arr_tmp = arr_df.copy()
    arr_tmp["MONTH"] = pd.to_datetime(arr_tmp["MONTH"])
    arr_valid = arr_tmp[arr_tmp["TOTAL_ARR"] > 0]
    if not arr_valid.empty:
        arr_valid = arr_valid.sort_values("MONTH")
        latest_arr_ts = arr_valid["MONTH"].max()
        arr_latest = float(arr_valid[arr_valid["MONTH"] == latest_arr_ts]["TOTAL_ARR"].iloc[0])
        if len(arr_valid) >= 2:
            arr_prev = float(arr_valid.iloc[-2]["TOTAL_ARR"])
            arr_delta = arr_latest - arr_prev


# Retention latest/prev/delta (+ latest_start_mrr)
latest_start_mrr = None
if not ret_df.empty and "MONTH" in ret_df.columns and "START_MRR" in ret_df.columns and "NRR_PCT" in ret_df.columns and "GRR_PCT" in ret_df.columns:
    tmp = ret_df.copy()
    tmp["MONTH"] = pd.to_datetime(tmp["MONTH"])
    ret_valid = tmp[tmp["START_MRR"] > 0]
    if not ret_valid.empty:
        latest_ret_ts = ret_valid["MONTH"].max()
        ret_slice = ret_valid[ret_valid["MONTH"] == latest_ret_ts]
        nrr_latest = float(ret_slice.iloc[0]["NRR_PCT"])
        grr_latest = float(ret_slice.iloc[0]["GRR_PCT"])
        latest_start_mrr = float(ret_slice.iloc[0]["START_MRR"])

        ret_history = ret_valid[ret_valid["MONTH"] <= latest_ret_ts].sort_values("MONTH")
        if len(ret_history) >= 2:
            nrr_prev = float(ret_history.iloc[-2]["NRR_PCT"])
            grr_prev = float(ret_history.iloc[-2]["GRR_PCT"])
        else:
            nrr_prev = None
            grr_prev = None
    else:
        nrr_latest = None
        nrr_prev = None
        grr_latest = None
        grr_prev = None
else:
    nrr_latest = None
    nrr_prev = None
    grr_latest = None
    grr_prev = None


# Win rate latest/prev/delta
win_latest = None
win_prev = None
win_delta = None

if closed_df is not None and not closed_df.empty and "CLOSE_MONTH" in closed_df.columns and "TOTAL_CLOSED_REVENUE" in closed_df.columns and "WIN_RATE_PCT" in closed_df.columns:
    close_tmp = closed_df.copy()
    close_tmp["CLOSE_MONTH"] = pd.to_datetime(close_tmp["CLOSE_MONTH"])
    close_valid = close_tmp[close_tmp["TOTAL_CLOSED_REVENUE"] > 0]
    if not close_valid.empty:
        close_valid = close_valid.sort_values("CLOSE_MONTH")
        latest_close_ts = close_valid["CLOSE_MONTH"].max()
        latest_close_month = latest_close_ts.date()
        latest_row = close_valid[close_valid["CLOSE_MONTH"] == latest_close_ts].iloc[0]
        win_latest = float(latest_row["WIN_RATE_PCT"])
        if len(close_valid) >= 2:
            win_prev = float(close_valid.iloc[-2]["WIN_RATE_PCT"])
            win_delta = win_latest - win_prev


coverage_ratio = float(coverage_df.iloc[0]["PIPELINE_COVERAGE_RATIO"]) if (coverage_df is not None and not coverage_df.empty and "PIPELINE_COVERAGE_RATIO" in coverage_df.columns) else None
coverage_open = float(coverage_df.iloc[0]["TOTAL_OPEN_PIPELINE"]) if (coverage_df is not None and not coverage_df.empty and "TOTAL_OPEN_PIPELINE" in coverage_df.columns) else None
coverage_avg3 = float(coverage_df.iloc[0]["AVG_3M_CLOSED_REVENUE"]) if (coverage_df is not None and not coverage_df.empty and "AVG_3M_CLOSED_REVENUE" in coverage_df.columns) else None


# Smart alert banner
alerts = []
if nrr_latest is not None and nrr_latest < NRR_ALERT_BELOW:
    alerts.append(f"Net Revenue Retention below {NRR_ALERT_BELOW:.0f}% (NRR={nrr_latest:.2f}%).")
if grr_latest is not None and grr_latest < GRR_ALERT_BELOW:
    alerts.append(f"Gross Revenue Retention below {GRR_ALERT_BELOW:.0f}% (GRR={grr_latest:.2f}%).")
if coverage_ratio is not None and coverage_ratio < PIPELINE_COVERAGE_ALERT_BELOW:
    alerts.append(f"Pipeline coverage below {PIPELINE_COVERAGE_ALERT_BELOW:.1f}x (Coverage={coverage_ratio:.2f}x).")

if arr_df is not None and not arr_df.empty and "MONTH" in arr_df.columns and "TOTAL_ARR" in arr_df.columns and len(arr_df) >= ARR_NEGATIVE_TREND_LOOKBACK:
    tail = arr_df.sort_values("MONTH").tail(ARR_NEGATIVE_TREND_LOOKBACK)["TOTAL_ARR"].values
    if len(tail) >= 3 and tail[-1] < tail[0]:
        alerts.append("ARR trending down over recent months.")

if alerts:
    st.warning("  •  " + "  •  ".join(alerts))

st.markdown('<div class="hr"></div>', unsafe_allow_html=True)


def display_retention_value(value, start_mrr):
    if value is None:
        return "Not available"
    if start_mrr is not None and start_mrr > 0 and value == 0:
        return "Data not loaded for selected period"
    return fmt_pct(value)


nrr_delta = (nrr_latest - nrr_prev) if (nrr_latest is not None and nrr_prev is not None) else None
grr_delta = (grr_latest - grr_prev) if (grr_latest is not None and grr_prev is not None) else None

# KPI row
k1, k2, k3, k4, k5 = st.columns([1, 1, 1, 1, 1])
with k1:
    kpi_card("ARR (latest)", fmt_currency(arr_latest), arr_delta, delta_fmt="currency")
with k2:
    kpi_card("NRR (latest complete)", display_retention_value(nrr_latest, latest_start_mrr), nrr_delta, delta_fmt="pct")
with k3:
    kpi_card("GRR (latest complete)", display_retention_value(grr_latest, latest_start_mrr), grr_delta, delta_fmt="pct")
with k4:
    kpi_card("Win Rate (latest close month)", fmt_pct(win_latest), win_delta, delta_fmt="pct")
with k5:
    kpi_card("Pipeline Coverage", fmt_x(coverage_ratio), None, delta_fmt="x")


with st.expander("Executive context (auto-generated)", expanded=False):
    st.write(
        f"""
**Latest months**
- ARR month: **{latest_arr_month}**
- Retention month (complete): **{latest_ret_month}**
- Pipeline close month: **{latest_close_month}**

**Pipeline coverage definition**
- Open pipeline: **{fmt_currency(coverage_open)}**
- Avg closed revenue (last 3 close months in filter): **{fmt_currency(coverage_avg3)}**
- Coverage ratio: **{fmt_x(coverage_ratio)}**
"""
    )


# -----------------------------
# Tabs
# -----------------------------
tab_overview, tab_qa, tab_retention, tab_pipeline, tab_health, tab_accounts, tab_quality, tab_about = st.tabs(
    ["Overview", "Analyst Q&A", "Retention", "Pipeline", "Customer Health", "Account Explorer", "Data Quality", "About"]
)


# -----------------------------
# Overview
# -----------------------------
with tab_overview:
    st.markdown('<div class="section-title">Executive Narrative</div>', unsafe_allow_html=True)

    # Optional: feed interpretability signals if available
    dq_for_exec = retention_data_quality(start_date, end_date, ACCOUNT_FILTER, latest_ret_month)

    ctx = {
        "filters_text": f"{start_date} to {end_date}",
        "arr_latest": arr_latest,
        "nrr_latest": nrr_latest,
        "grr_latest": grr_latest,
        "win_latest": win_latest,
        "coverage_ratio": coverage_ratio,
        "retention_interpretable": bool(dq_for_exec.get("retention_interpretable", False)),
        "retention_note": str(dq_for_exec.get("retention_note", "")),
    }

    col_a, col_b = st.columns([0.82, 0.18])
    with col_b:
        if st.button("Generate / Refresh", use_container_width=True):
            with st.spinner("Generating executive narrative..."):
                st.session_state["exec_narrative"] = generate_exec_narrative(ctx)

    if "exec_narrative" in st.session_state:
        render_exec_narrative(st.session_state["exec_narrative"], filters_text=ctx["filters_text"])
    else:
        st.info("Click **Generate / Refresh** to generate a board-ready narrative for the selected filters.")

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">ARR Trend</div>', unsafe_allow_html=True)
    if arr_df is not None and not arr_df.empty and "MONTH" in arr_df.columns and "TOTAL_ARR" in arr_df.columns:
        df = arr_df[arr_df["TOTAL_ARR"] > 0].copy()
        df["MONTH"] = pd.to_datetime(df["MONTH"])
        chart_line(df, "MONTH", "TOTAL_ARR", "ARR Trend")

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    c1, c2 = st.columns([1.15, 0.85])
    with c1:
        st.markdown('<div class="section-title">Retention Performance</div>', unsafe_allow_html=True)
        if ret_df is not None and not ret_df.empty and "MONTH" in ret_df.columns:
            df = ret_df.copy()
            df["MONTH"] = pd.to_datetime(df["MONTH"])
            cols = [c for c in ["NRR_PCT", "GRR_PCT"] if c in df.columns]
            if cols:
                chart_multi_line(df, "MONTH", cols, "NRR vs GRR (%)")
            else:
                st.info("Retention metrics not available for current filters.")
    with c2:
        st.markdown('<div class="section-title">Pipeline Strength</div>', unsafe_allow_html=True)
        st.write("**Coverage Ratio**")
        st.markdown(f"### {fmt_x(coverage_ratio)}")
        st.caption("Open pipeline ÷ average closed revenue (last 3 close months in filter).")

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Closed Revenue by Month</div>', unsafe_allow_html=True)
    if closed_df is not None and not closed_df.empty and "CLOSE_MONTH" in closed_df.columns and "TOTAL_CLOSED_REVENUE" in closed_df.columns:
        df = closed_df.copy()
        df["CLOSE_MONTH"] = pd.to_datetime(df["CLOSE_MONTH"])
        chart_bar(df, "CLOSE_MONTH", "TOTAL_CLOSED_REVENUE", "Closed Revenue by Month")

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Open Pipeline Breakdown</div>', unsafe_allow_html=True)
    if open_stage_df is not None and not open_stage_df.empty:
        st.dataframe(open_stage_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download open pipeline by stage (CSV)",
            data=open_stage_df.to_csv(index=False),
            file_name="open_pipeline_by_stage.csv",
            mime="text/csv",
        )


# -----------------------------
# Q&A Tab ✅ (ONE input + click-to-fill + strict)
# -----------------------------
with tab_qa:
    st.markdown('<div class="section-title">Cortex Analyst Q&A</div>', unsafe_allow_html=True)
    st.caption("Ask questions about the current filters. Answers are grounded ONLY in metrics computed in this app.")

    pack_json = build_qa_pack_json(
        start_d=start_date,
        end_d=end_date,
        account_filter=ACCOUNT_FILTER,
        arr_latest=arr_latest,
        nrr_latest=nrr_latest,
        grr_latest=grr_latest,
        win_latest=win_latest,
        coverage_ratio=coverage_ratio,
        arr_delta=arr_delta,
        win_delta=win_delta,
        coverage_open=coverage_open,
        coverage_avg3=coverage_avg3,
        latest_ret_month=latest_ret_month,
        pipeline_coverage_target_x=PIPELINE_COVERAGE_ALERT_BELOW,
    )

    if "qa_messages" not in st.session_state:
        st.session_state.qa_messages = []
    if "qa_question" not in st.session_state:
        st.session_state.qa_question = ""

    r1, r2 = st.columns([0.82, 0.18])
    with r2:
        if st.button("Reset Q&A", use_container_width=True):
            st.session_state.qa_messages = []
            st.session_state.qa_question = ""
            st.rerun()

    def _set_q(txt: str):
        st.session_state.qa_question = txt

    def _submit_q():
        q = (st.session_state.get("qa_question") or "").strip()
        if not q:
            return
        st.session_state.qa_messages.append({"role": "user", "content": q})
        ans = cortex_analyst_answer(q, pack_json)
        st.session_state.qa_messages.append({"role": "assistant", "content": ans})
        st.session_state.qa_question = ""

    st.write("**Suggested questions**")
    a1, a2, a3, a4 = st.columns(4)
    a1.button("What changed most MoM?", use_container_width=True,
              on_click=_set_q, args=("What changed most month-over-month, and what drove the change?",))
    a2.button("Why is retention weak?", use_container_width=True,
              on_click=_set_q, args=("Summarize retention performance and whether it is interpretable for this period. If not, explain why.",))
    a3.button("Pipeline risk?", use_container_width=True,
              on_click=_set_q, args=("Is pipeline coverage sufficient relative to targets, and what are the risks?",))
    a4.button("Where to focus?", use_container_width=True,
              on_click=_set_q, args=("Based on these metrics, where should GTM focus in the next 30–60 days?",))

    st.write("**More tests you can click**")
    b1, b2, b3, b4 = st.columns(4)
    b1.button("Win rate trend", use_container_width=True,
              on_click=_set_q, args=("How has win rate changed vs the previous close month, and what should we check?",))
    b2.button("Sales cycle", use_container_width=True,
              on_click=_set_q, args=("What does the average sales cycle suggest, and where could deals be getting stuck?",))
    b3.button("Stage concentration", use_container_width=True,
              on_click=_set_q, args=("Which stages hold most of the open pipeline, and what does that imply?",))
    b4.button("Top movers", use_container_width=True,
              on_click=_set_q, args=("Who are the top expansions and contractions recently, and what patterns do you see?",))

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    for m in st.session_state.qa_messages:
        with st.chat_message(m["role"]):
            st.write(m["content"])

    with st.form("qa_form", clear_on_submit=False):
        st.text_input(
            "Ask a question...",
            key="qa_question",
            placeholder="e.g., Is ARR accelerating or decelerating in the last 3 months?",
        )
        st.form_submit_button("Ask", type="primary", on_click=_submit_q)


# -----------------------------
# Retention Tab
# -----------------------------
with tab_retention:
    st.markdown('<div class="section-title">Retention Time Series</div>', unsafe_allow_html=True)
    if ret_df is not None and not ret_df.empty and "MONTH" in ret_df.columns:
        df = ret_df.copy()
        df["MONTH"] = pd.to_datetime(df["MONTH"])
        cols = [c for c in ["NRR_PCT", "GRR_PCT"] if c in df.columns]
        if cols:
            chart_multi_line(df, "MONTH", cols, "NRR vs GRR (Cohort-Based)")
        st.caption(
            "NRR/GRR are computed using existing-customer cohorts: "
            "accounts with MRR>0 in Month T, compared to the same accounts in Month T+1. "
            "The boundary month is excluded to avoid showing misleading 0%."
        )

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Revenue Movement Summary</div>', unsafe_allow_html=True)
    move_df = get_mrr_movement_summary(start_date, end_date, ACCOUNT_FILTER)
    if move_df is not None and not move_df.empty:
        st.dataframe(move_df, use_container_width=True, hide_index=True)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Top Monthly Movers (latest 2 months)</div>', unsafe_allow_html=True)
    exp_df, con_df = get_top_mrr_movers(start_date, end_date, ACCOUNT_FILTER)
    c1, c2 = st.columns(2)
    with c1:
        st.write("**Top Expansions**")
        if exp_df is not None and not exp_df.empty:
            st.dataframe(exp_df, use_container_width=True, hide_index=True)
        else:
            st.info("No expansion data available.")
    with c2:
        st.write("**Top Contractions**")
        if con_df is not None and not con_df.empty:
            st.dataframe(con_df, use_container_width=True, hide_index=True)
        else:
            st.info("No contraction data available.")


# -----------------------------
# Pipeline Tab
# -----------------------------
with tab_pipeline:
    st.markdown('<div class="section-title">Pipeline Performance (Monthly)</div>', unsafe_allow_html=True)
    if closed_df is not None and not closed_df.empty:
        df = closed_df.copy()
        if "CLOSE_MONTH" in df.columns:
            df["CLOSE_MONTH"] = pd.to_datetime(df["CLOSE_MONTH"])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download pipeline monthly metrics (CSV)",
            data=df.to_csv(index=False),
            file_name="pipeline_monthly_metrics.csv",
            mime="text/csv",
        )
    else:
        st.info("No closed deals found within current date range / filters.")

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Open Pipeline (Stage Funnel)</div>', unsafe_allow_html=True)
    if open_stage_df is not None and not open_stage_df.empty:
        df = open_stage_df.copy()
        if ALTAIR_OK and all(c in df.columns for c in ["CURRENT_STAGE", "OPEN_PIPELINE", "WEIGHTED_PIPELINE", "OPP_COUNT"]):
            c = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    y=alt.Y("CURRENT_STAGE:N", sort="-x", title=""),
                    x=alt.X("OPEN_PIPELINE:Q", title="Open Pipeline ($)"),
                    tooltip=["CURRENT_STAGE:N", "OPEN_PIPELINE:Q", "WEIGHTED_PIPELINE:Q", "OPP_COUNT:Q"],
                )
                .properties(height=360)
            )
            st.altair_chart(c, use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    if STAGE_HIST_TBL:
        st.markdown('<div class="section-title">Stage Dynamics</div>', unsafe_allow_html=True)

        stage_duration_sql = f"""
        with sh as (
            select
                sh.opp_id,
                sh.account_id,
                sh.stage,
                sh.stage_start_date,
                sh.stage_end_date
            from {STAGE_HIST_TBL} sh
            join {ACCOUNTS_TBL} a on a.account_id = sh.account_id
            {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
            where {ACCOUNT_FILTER}
        )
        select
            stage,
            count(distinct opp_id) as deals_reached_stage,
            round(avg(datediff(day, stage_start_date, stage_end_date)), 2) as avg_stage_duration_days
        from sh
        where stage_end_date is not null
        group by stage
        order by deals_reached_stage desc;
        """
        stage_duration_df = run_sql(stage_duration_sql)

        c1, c2 = st.columns(2)
        with c1:
            st.write("**Avg Stage Duration (days)**")
            if stage_duration_df is not None and not stage_duration_df.empty:
                st.dataframe(stage_duration_df, use_container_width=True, hide_index=True)
            else:
                st.info("No stage duration data available.")
        with c2:
            st.write("**Stage Conversion (From → To)**")
            stage_conv_sql = f"""
            with sh as (
                select
                    sh.opp_id,
                    sh.account_id,
                    sh.stage,
                    sh.stage_start_date
                from {STAGE_HIST_TBL} sh
                join {ACCOUNTS_TBL} a on a.account_id = sh.account_id
                {"left join " + REPS_TBL + " r on r.rep_id = a.owner_rep_id" if REPS_TBL else ""}
                where {ACCOUNT_FILTER}
            ),
            ordered as (
                select
                    opp_id,
                    stage as from_stage,
                    lead(stage) over (partition by opp_id order by stage_start_date) as to_stage
                from sh
            ),
            trans as (
                select
                    from_stage,
                    to_stage,
                    count(*) as deals_progressed
                from ordered
                where to_stage is not null
                group by from_stage, to_stage
            ),
            in_stage as (
                select
                    stage as from_stage,
                    count(distinct opp_id) as deals_in_stage
                from sh
                group by stage
            )
            select
                t.from_stage,
                t.to_stage,
                t.deals_progressed,
                i.deals_in_stage,
                round(100 * t.deals_progressed / nullif(i.deals_in_stage, 0), 2) as conversion_rate_pct
            from trans t
            join in_stage i using(from_stage)
            order by conversion_rate_pct desc;
            """
            stage_conv_df = run_sql(stage_conv_sql)

            if stage_conv_df is not None and not stage_conv_df.empty:
                st.dataframe(stage_conv_df, use_container_width=True, hide_index=True)
            else:
                st.info("No stage conversion data available.")
    else:
        st.info("Stage history table not found — pipeline stage dynamics section is disabled.")


# -----------------------------
# Health Tab
# -----------------------------
with tab_health:
    st.markdown('<div class="section-title">Customer Health Snapshot</div>', unsafe_allow_html=True)
    health_df = get_health_snapshot(start_date, end_date, ACCOUNT_FILTER)

    if health_df is None or health_df.empty:
        st.info("Health dataset not available for current filters.")
    else:
        cols_upper = set(health_df.columns)
        status_col = "HEALTH_STATUS" if "HEALTH_STATUS" in cols_upper else None
        score_col = "HEALTH_SCORE" if "HEALTH_SCORE" in cols_upper else None

        if status_col:
            dist = health_df.groupby(status_col).size().reset_index(name="COUNT")
            st.write("**Health distribution**")
            if PLOTLY_OK:
                fig = px.bar(dist, x=status_col, y="COUNT")
                fig.update_layout(template="plotly_white", height=320, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.dataframe(dist, use_container_width=True, hide_index=True)

        st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

        st.write("**At-risk accounts (sorted by score / status)**")
        if score_col:
            view = health_df.sort_values(score_col, ascending=True)
        elif status_col:
            view = health_df.sort_values(status_col, ascending=True)
        else:
            view = health_df

        st.dataframe(view.head(50), use_container_width=True, hide_index=True)
        st.download_button(
            "Download health snapshot (CSV)",
            data=health_df.to_csv(index=False),
            file_name="health_snapshot.csv",
            mime="text/csv",
        )


# -----------------------------
# Account Explorer Tab
# -----------------------------
with tab_accounts:
    st.markdown('<div class="section-title">Account Explorer</div>', unsafe_allow_html=True)

    accounts_df = run_sql(f"select account_id, account_name, segment, region, industry, owner_rep_id, website from {ACCOUNTS_TBL}")
    if accounts_df is None or accounts_df.empty:
        st.info("No accounts available.")
    else:
        accounts_df["ACCOUNT_ID"] = accounts_df["ACCOUNT_ID"].astype(str)
        accounts_df["ACCOUNT_NAME"] = accounts_df["ACCOUNT_NAME"].fillna("").astype(str)
        accounts_df["LABEL"] = accounts_df["ACCOUNT_NAME"] + " (" + accounts_df["ACCOUNT_ID"] + ")"

        selected = st.selectbox("Select an account", options=accounts_df["LABEL"].tolist())
        sel_id = selected.split("(")[-1].replace(")", "").strip()

        acct_rows = accounts_df[accounts_df["ACCOUNT_ID"] == sel_id]
        if acct_rows.empty:
            st.info("Selected account not found.")
        else:
            acct = acct_rows.iloc[0].to_dict()

            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Account", acct.get("ACCOUNT_NAME", "—"))
            p2.metric("Segment", acct.get("SEGMENT", "—"))
            p3.metric("Region", acct.get("REGION", "—"))
            p4.metric("Industry", acct.get("INDUSTRY", "—"))

            st.markdown('<div class="hr"></div>', unsafe_allow_html=True)
            st.markdown('<div class="section-title">MRR Trend</div>', unsafe_allow_html=True)

            mrr_df = get_mrr_account_month(start_date, end_date, "1=1")
            if mrr_df is not None and not mrr_df.empty:
                df = mrr_df[mrr_df["ACCOUNT_ID"].astype(str) == sel_id].copy()
                if not df.empty:
                    df["MONTH"] = pd.to_datetime(df["MONTH"])
                    chart_line(df, "MONTH", "TOTAL_MRR", "MRR by Month", height=300)
                    st.caption("MRR values come from your MARTS.FCT_MRR table.")
                else:
                    st.info("No MRR rows found for this account within the selected range.")
            else:
                st.info("MRR dataset unavailable.")

            st.markdown('<div class="hr"></div>', unsafe_allow_html=True)
            st.markdown('<div class="section-title">Opportunities</div>', unsafe_allow_html=True)

            opp_sql = f"""
            select
                opp_id,
                created_date,
                close_date,
                current_stage,
                probability,
                amount,
                is_closed,
                is_won
            from {FCT_PIPELINE_TBL}
            where account_id = '{sel_id}'
            order by created_date desc
            limit 200;
            """
            opp_df = run_sql(opp_sql)
            if opp_df is not None and not opp_df.empty:
                st.dataframe(opp_df, use_container_width=True, hide_index=True)
            else:
                st.info("No opportunities found for this account.")

            if SUPPORT_TICKETS_TBL:
                st.markdown('<div class="hr"></div>', unsafe_allow_html=True)
                st.markdown('<div class="section-title">Support Tickets</div>', unsafe_allow_html=True)

                t_sql = f"""
                select
                    ticket_id,
                    created_date,
                    status,
                    priority,
                    category,
                    subject
                from {SUPPORT_TICKETS_TBL}
                where account_id = '{sel_id}'
                order by created_date desc
                limit 200;
                """
                t_df = run_sql(t_sql)
                if t_df is not None and not t_df.empty:
                    st.dataframe(t_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No support tickets found for this account (or ticket table empty).")


# -----------------------------
# Data Quality Tab
# -----------------------------
with tab_quality:
    st.markdown('<div class="section-title">Data Quality & Sanity Checks</div>', unsafe_allow_html=True)
    
    checks = []
    checks.append(("MRR date bounds", f"select min(month) as min_month, max(month) as max_month from {FCT_MRR_TBL}"))
    checks.append(("Pipeline close bounds", f"select min(close_date) as min_close, max(close_date) as max_close from {FCT_PIPELINE_TBL}"))
    checks.append(("Row count — Accounts", f"select count(*) as row_count from {ACCOUNTS_TBL}"))
    checks.append(("Row count — FCT_MRR", f"select count(*) as row_count from {FCT_MRR_TBL}"))
    checks.append(("Row count — FCT_PIPELINE", f"select count(*) as row_count from {FCT_PIPELINE_TBL}"))

    if STAGE_HIST_TBL:
        checks.append(("Row count — Stage History", f"select count(*) as row_count from {STAGE_HIST_TBL}"))
    if SUPPORT_TICKETS_TBL:
        checks.append(("Row count — Support Tickets", f"select count(*) as row_count from {SUPPORT_TICKETS_TBL}"))

    for title, q in checks:
        with st.expander(title, expanded=False):
            try:
                df = run_sql(q)
                st.dataframe(df, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Check failed: {e}")

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)
    st.write("**Resolved tables used by this app**")
    resolved_df = pd.DataFrame([{"DATASET": k, "TABLE": v or "NOT FOUND"} for k, v in tables.items()])
    st.dataframe(resolved_df, use_container_width=True, hide_index=True)


# -----------------------------
# About Tab
# -----------------------------
with tab_about:
    st.markdown('<div class="section-title">About this project</div>', unsafe_allow_html=True)
    st.write(
        """
This app is an executive analytics layer built on Snowflake marts:

- Revenue: MRR → ARR trend by month
- Retention: NRR + GRR computed via cohort-based logic (Month T customers tracked to Month T+1)
- Pipeline: closed revenue, win rate, sales cycle, open pipeline + coverage
- Health: customer health snapshot (uses your table if present, otherwise fallback model)

Interview positioning:
This demonstrates end-to-end ownership across ingestion → modeling → metrics → UI delivery,
with data quality checks and drill-down analysis.
"""
    )
    st.caption("Tip: In interviews, open Data Quality tab and explain why boundary months are excluded for NRR/GRR.")
