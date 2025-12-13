from __future__ import annotations

import html
from pathlib import Path
from typing import Optional

import pandas as pd

from moata_pipeline.common.text_utils import safe_filename
from moata_pipeline.common.html_utils import df_to_html_table
# ✅ FIXED: Import dari common/time_utils.py (tidak duplikasi lagi!)
from moata_pipeline.common.time_utils import format_date_for_display


# ❌ REMOVED: def df_to_html_table() - sudah diimport dari common


def img_block(img_name: str, caption: str) -> str:
    return f"""
    <div class="card">
      <img src="{html.escape(img_name)}" alt="{html.escape(caption)}"/>
      <p class="caption">{html.escape(caption)}</p>
    </div>
    """

def build_risk_table(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    is_threshold = df["row_category"] == "Threshold alarm (overflow)"
    is_recency = df["row_category"] == "Data freshness (recency)"
    is_crit = df["is_critical_bool"] == True

    agg = df.groupby("gauge_name").agg(
        total_records=("gauge_name", "size"),
        thresholds=("row_category", lambda s: (s == "Threshold alarm (overflow)").sum()),
        recency=("row_category", lambda s: (s == "Data freshness (recency)").sum()),
        critical=("is_critical_bool", "sum"),
        latest_data=("last_data_dt", "max"),
    )

    # Risk score (same weights as chart)
    agg["risk_score"] = agg["critical"] * 3 + agg["thresholds"] * 2 + agg["recency"] * 1

    agg = agg.sort_values(by=["risk_score", "critical", "thresholds"], ascending=[False, False, False]).head(top_n)

    # Nice formatting
    agg = agg.reset_index().rename(columns={"gauge_name": "Gauge"})
    agg["latest_data"] = agg["latest_data"].dt.strftime("%Y-%m-%d")
    return agg[["Gauge", "risk_score", "critical", "thresholds", "recency", "total_records", "latest_data"]]


def build_report(df: pd.DataFrame, out_dir: Path) -> None:
    total_rows = len(df)
    gauges = df["gauge_name"].nunique()
    traces = df["trace_id"].nunique()

    latest = df["last_data_dt"].max()
    oldest = df["last_data_dt"].min()

    # Tables
    crit = df[df["is_critical_bool"]].copy()
    crit_table = crit[
        ["gauge_name", "trace_name", "alarm_type", "alarm_name", "threshold", "severity", "source", "last_data"]
    ].sort_values(by=["severity", "gauge_name", "trace_name"], ascending=[False, True, True], na_position="last")

    recency = df[df["row_category"] == "Data freshness (recency)"].copy()
    recency_table = (
        recency[["gauge_name", "trace_name", "alarm_type", "alarm_name", "source", "last_data"]]
        .drop_duplicates()
        .sort_values(by=["gauge_name", "trace_name"], ascending=[True, True])
    )

    overflow = df[df["row_category"] == "Threshold alarm (overflow)"].copy()
    overflow_table = (
        overflow[
            ["gauge_name", "trace_name", "alarm_name", "threshold", "threshold_num", "severity", "is_critical", "last_data"]
        ]
        .sort_values(by=["gauge_name", "trace_name", "threshold_num"], ascending=[True, True, True], na_position="last")
        .drop(columns=["threshold_num"])
    )

    # Gauge page links
    gauges_list = sorted([g for g in df["gauge_name"].unique() if str(g).strip() != ""])
    link_rows = []
    for g in gauges_list:
        link = f"07_gauge_pages/{safe_filename(g)}.html"
        link_rows.append({"Gauge": g, "Open": f"<a href='{html.escape(link)}'>View</a>"})
    links_df = pd.DataFrame(link_rows)

    css = """
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; line-height: 1.35; }
      h1 { margin-bottom: 6px; }
      .muted { color: #555; }
      .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }
      .card { border: 1px solid #ddd; border-radius: 10px; padding: 12px; background: #fff; }
      .card img { width: 100%; height: auto; border-radius: 8px; border: 1px solid #eee; }
      .caption { margin: 8px 0 0 0; color: #444; font-size: 0.95em; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #ddd; padding: 6px 8px; font-size: 0.95em; }
      th { background: #f3f3f3; text-align: left; }
      .note { background: #fff7e6; border: 1px solid #ffe0a3; padding: 10px 12px; border-radius: 10px; }
      code { background: #f5f5f5; padding: 1px 5px; border-radius: 6px; }
      .small { font-size: 0.95em; }
    </style>
    """

    # ❌ REMOVED: def dt_fmt() - sekarang menggunakan format_date_for_display dari common

    html_parts = [
        "<html><head><meta charset='utf-8'/>",
        "<title>Alarm Summary Report</title>",
        css,
        "</head><body>",
        "<h1>Alarm Summary Report (Non-technical view)</h1>",
        f"<p class='muted'>Generated from <code>alarm_summary.csv</code>. "
        f"Rows: <b>{total_rows}</b> · Gauges: <b>{gauges}</b> · Traces: <b>{traces}</b> · "
        f"Date range in file: <b>{format_date_for_display(oldest)}</b> to <b>{format_date_for_display(latest)}</b></p>",

        "<div class='note'>"
        "<b>How to read this report:</b><br/>"
        "• <b>Threshold alarm (overflow)</b> rows show trigger levels (e.g., 15mm in 30 min).<br/>"
        "• <b>Data freshness (recency)</b> rows indicate monitoring for missing/stale data (sensor not updating).<br/>"
        "• <b>Critical</b> = flagged as critical in the dataset (higher attention)."
        "</div><br/>",

        "<h2>Quick navigation: one page per gauge</h2>",
        "<p class='muted small'>Click <b>View</b> to open a gauge-specific page with all thresholds and recency flags.</p>",
        links_df.to_html(index=False, escape=False),

        "<h2>Visual overview</h2>",
        "<div class='grid'>",
        img_block("01_records_by_gauge.png", "Which gauges have the most records? (Top gauges)"),
        img_block("02_record_categories.png", "Record categories: threshold vs data freshness vs others"),
        img_block("05_critical_flag.png", "How many records are marked critical vs not"),
        "</div>",

        "<div class='grid'>",
    ]

    for img, cap in [
        ("03_severity_distribution.png", "Severity distribution (if present)"),
        ("04_threshold_hist.png", "Distribution of numeric threshold values"),
    ]:
        if (out_dir / img).exists():
            html_parts.append(img_block(img, cap))
    html_parts.append("</div>")

        # --- Top Risky Gauges (Actionable shortlist) ---
    risk_df = build_risk_table(df, top_n=20)

    html_parts += [
        "<h2>Top Risky Gauges (Actionable shortlist)</h2>",
        "<p class='muted'>This is a simple weighted score: "
        "<code>critical×3 + thresholds×2 + recency×1</code>. "
        "Use it to prioritize which gauges to check first.</p>",
    ]

    # Include risk chart if exists
    if (out_dir / "08_top_risky_gauges.png").exists():
        html_parts += [
            "<div class='grid'>",
            img_block("08_top_risky_gauges.png", "Top gauges by risk score"),
            "</div>",
        ]

    html_parts.append(risk_df.to_html(index=False, escape=True))


    html_parts += [
        "<h2>Threshold ladders (most intuitive)</h2>",
        "<p class='muted'>These charts show the trigger levels per trace for the top gauges. "
        "A longer line means a trace has a range of thresholds (min..max).</p>",
        "<div class='grid'>",
    ]

    for i in range(1, 9):
        img = out_dir / f"06_ladder_gauge_{i}.png"
        if img.exists():
            html_parts.append(img_block(img.name, f"Threshold ladder (Gauge #{i})"))
    html_parts.append("</div>")

    html_parts += [
        "<h2>Actionable tables</h2>",
        df_to_html_table(crit_table, "Critical records (if any)", max_rows=200),
        df_to_html_table(overflow_table, "Overflow threshold configurations (sorted)", max_rows=300),
        df_to_html_table(recency_table, "Data freshness monitoring (recency) flags", max_rows=300),

        "<h2>Full dataset preview</h2>",
        df_to_html_table(
            df.drop(columns=["last_data_dt", "threshold_num"], errors="ignore"),
            "Raw view (first rows)",
            max_rows=120,
        ),

        "<hr/>",
        "<p class='muted'>Tip: You can send this <code>report.html</code> folder to stakeholders. "
        "They only need a browser to open it.</p>",
        "</body></html>",
    ]

    (out_dir / "report.html").write_text("\n".join(html_parts), encoding="utf-8")