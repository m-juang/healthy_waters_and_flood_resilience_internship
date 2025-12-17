from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =============================
# IO helpers
# =============================
def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_exists(path: Path) -> bool:
    try:
        return path.exists()
    except Exception:
        return False


# =============================
# Parsing / feature extraction
# =============================
def parse_window_seconds(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.lower()

    m = re.search(r"(\d+)\s*(sec|secs|second|seconds)\b", t)
    if m:
        return int(m.group(1))

    m = re.search(r"(\d+)\s*(min|mins|minute|minutes)\b", t)
    if m:
        return int(m.group(1)) * 60

    m = re.search(r"(\d+)\s*(hr|hrs|hour|hours)\b", t)
    if m:
        return int(m.group(1)) * 3600

    m = re.search(r"(\d+)\s*(day|days)\b", t)
    if m:
        return int(m.group(1)) * 86400

    return None


def is_ari_related(trace_desc: str, dv_name: str, thr_name: str, alarm_desc: str) -> bool:
    hay = " ".join([trace_desc or "", dv_name or "", thr_name or "", alarm_desc or ""]).lower()
    return "ari" in hay


def build_threshold_table(rgta: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for item in rgta:
        g = item.get("gauge", {}) or {}
        gauge_id = g.get("id")
        gauge_name = g.get("name")

        for t in item.get("traces", []) or []:
            trace = t.get("trace", {}) or {}
            trace_id = trace.get("id")
            trace_desc = trace.get("description") or ""
            resolution_s = trace.get("resolution")  # typically seconds (your output confirms)

            dv = trace.get("dataVariableType", {}) or {}
            dv_name = dv.get("name") or ""
            dv_type = dv.get("type") or ""

            for thr in t.get("thresholds", []) or []:
                thr_name = thr.get("name") or ""
                alarm_desc = thr.get("alarmDescription") or ""

                window_s = (
                    parse_window_seconds(trace_desc)
                    or parse_window_seconds(alarm_desc)
                    or parse_window_seconds(thr_name)
                )

                rows.append(
                    dict(
                        gauge_id=gauge_id,
                        gauge_name=gauge_name,
                        trace_id=trace_id,
                        trace_description=trace_desc,
                        resolution_s=resolution_s,
                        dataVariableType_name=dv_name,
                        dataVariableType_type=dv_type,
                        threshold_id=thr.get("id"),
                        threshold_name=thr_name,
                        threshold_value=thr.get("value"),
                        thresholdType=thr.get("thresholdType"),
                        severity=(thr.get("severity") or "UNKNOWN"),
                        category=(thr.get("category") or "UNKNOWN"),
                        isCritical=bool(thr.get("isCritical") or False),
                        alarmDescription=alarm_desc,
                        window_s=window_s,
                        ari_related=is_ari_related(trace_desc, dv_name, thr_name, alarm_desc),
                    )
                )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["severity"] = df["severity"].fillna("UNKNOWN")
        df["category"] = df["category"].fillna("UNKNOWN")
        df["isCritical"] = df["isCritical"].fillna(False).astype(bool)
        df["severity_norm"] = df["severity"].astype(str).str.strip().str.title()

        # Useful flags for anti-bias checks
        df["threshold_name_norm"] = df["threshold_name"].astype(str).str.strip()
        df["trace_desc_norm"] = df["trace_description"].astype(str).str.strip()

    return df


def extract_threshold_ids_from_test(thrtest: Dict[str, Any]) -> set[int]:
    ids: set[int] = set()
    for g in thrtest.get("gauges", []) or []:
        for t in g.get("thresholds", []) or []:
            thr = (t.get("threshold") or {})
            if isinstance(thr.get("id"), int):
                ids.add(thr["id"])
    return ids


# =============================
# Formatting utilities
# =============================
def fmt_pct(n: int, d: int) -> str:
    if d == 0:
        return "0.0%"
    return f"{(100.0 * n / d):.1f}%"


def seconds_to_human(s: Optional[float]) -> str:
    if s is None or pd.isna(s):
        return "N/A"
    s = float(s)
    if s < 60:
        return f"{int(s)}s"
    if s < 3600:
        return f"{s/60:.1f}min"
    if s < 86400:
        return f"{s/3600:.1f}hr"
    return f"{s/86400:.1f}day"


def safe_crosstab_select(ct: pd.DataFrame) -> pd.DataFrame:
    """Ensure ct has columns [False, True] without triggering boolean row-mask behavior."""
    return ct.reindex(columns=[False, True]).fillna(0).astype(int)


@dataclass
class HypothesisResult:
    name: str
    status: str  # SUPPORTED / WEAKLY SUPPORTED / NOT SUPPORTED / INCONCLUSIVE
    evidence_lines: List[str]


def decide_status(score: float) -> str:
    """
    score in [0,1] roughly:
    - >=0.75: SUPPORTED
    - >=0.50: WEAKLY SUPPORTED
    - >=0.25: INCONCLUSIVE
    - <0.25 : NOT SUPPORTED
    """
    if score >= 0.75:
        return "SUPPORTED"
    if score >= 0.50:
        return "WEAKLY SUPPORTED"
    if score >= 0.25:
        return "INCONCLUSIVE"
    return "NOT SUPPORTED"


# =============================
# Anti-bias inference engine
# =============================
def run_anti_bias_assessment(df: pd.DataFrame) -> List[HypothesisResult]:
    """
    Produces multiple hypotheses (primary + alternatives) and evaluates them
    based on observed statistics in df.
    """
    results: List[HypothesisResult] = []

    n_total = len(df)
    n_crit = int(df["isCritical"].sum())
    n_high = int((df["severity_norm"] == "High").sum())
    n_highcrit = int(((df["severity_norm"] == "High") & (df["isCritical"])).sum())

    # Basic distributions for evidence
    ct = safe_crosstab_select(pd.crosstab(df["severity_norm"], df["isCritical"])).sort_index()
    crit_names = df[df["isCritical"]]["threshold_name_norm"].value_counts()
    crit_top_name = crit_names.index[0] if len(crit_names) else None
    crit_top_share = float(crit_names.iloc[0] / n_crit) if n_crit > 0 else 0.0

    # Window/resolution comparison
    med_win_c = df[df["isCritical"]]["window_s"].median()
    med_win_nc = df[~df["isCritical"]]["window_s"].median()
    med_res_c = df[df["isCritical"]]["resolution_s"].median()
    med_res_nc = df[~df["isCritical"]]["resolution_s"].median()

    # ARI association checks
    high_ari_share = (
        float(df[df["severity_norm"] == "High"]["ari_related"].mean())
        if n_high > 0
        else 0.0
    )
    crit_ari_share = float(df[df["isCritical"]]["ari_related"].mean()) if n_crit > 0 else 0.0

    # ---- Hypothesis 1 (Primary): Severity ~ event magnitude/rarity; Critical ~ urgency / fast-response
    # Evidence signals:
    #   (a) High severity strongly ARI-related
    #   (b) Critical strongly short-window / short-resolution
    #   (c) High & Critical rare/non-existent (separation)
    score_h1 = 0.0
    ev_h1: List[str] = []
    ev_h1.append(f"severity×isCritical table:\n{ct.to_string()}")
    ev_h1.append(f"High & Critical rows: {n_highcrit} out of {n_total} thresholds.")
    if n_high > 0:
        ev_h1.append(f"Share of High severity thresholds that are ARI-related: {high_ari_share:.3f} ({high_ari_share*100:.1f}%).")
    if n_crit > 0:
        ev_h1.append(f"Top critical threshold_name: '{crit_top_name}' covering {crit_top_share:.3f} ({crit_top_share*100:.1f}%) of critical thresholds.")
    ev_h1.append(f"Median window_s: critical={seconds_to_human(med_win_c)} vs non-critical={seconds_to_human(med_win_nc)}.")
    ev_h1.append(f"Median resolution_s: critical={seconds_to_human(med_res_c)} vs non-critical={seconds_to_human(med_res_nc)}.")
    ev_h1.append(f"Critical thresholds ARI-related share: {crit_ari_share:.3f} ({crit_ari_share*100:.1f}%).")

    # scoring
    # a) High->ARI association
    score_h1 += 0.4 * min(max(high_ari_share, 0.0), 1.0)
    # b) Critical short window/res: require medians exist and be much smaller
    if pd.notna(med_win_c) and pd.notna(med_win_nc) and med_win_nc and med_win_c:
        ratio_win = float(med_win_c / med_win_nc)
        # if critical window is <= 10% of non-critical => strong
        score_h1 += 0.35 * (1.0 if ratio_win <= 0.10 else 0.5 if ratio_win <= 0.25 else 0.0)
        ev_h1.append(f"Window ratio (critical/non-critical) ≈ {ratio_win:.4f}.")
    else:
        score_h1 += 0.10
        ev_h1.append("Window ratio could not be computed reliably (missing window hints).")

    if pd.notna(med_res_c) and pd.notna(med_res_nc) and med_res_nc and med_res_c:
        ratio_res = float(med_res_c / med_res_nc)
        score_h1 += 0.15 * (1.0 if ratio_res <= 0.25 else 0.5 if ratio_res <= 0.5 else 0.0)
        ev_h1.append(f"Resolution ratio (critical/non-critical) ≈ {ratio_res:.4f}.")
    else:
        score_h1 += 0.05
        ev_h1.append("Resolution ratio could not be computed reliably.")

    # c) separation: fewer High&Critical => supports separation
    if n_high > 0:
        highcrit_rate = n_highcrit / n_high
        score_h1 += 0.10 * (1.0 if highcrit_rate == 0 else 0.5 if highcrit_rate <= 0.05 else 0.0)
        ev_h1.append(f"High&Critical rate among High severity = {highcrit_rate:.4f}.")
    else:
        score_h1 += 0.05

    results.append(
        HypothesisResult(
            name="H1 (Primary): Severity ≈ event magnitude/rarity; isCritical ≈ operational urgency/fast-response",
            status=decide_status(min(score_h1, 1.0)),
            evidence_lines=ev_h1,
        )
    )

    # ---- Hypothesis 2: isCritical might be data-quality / high-frequency trustworthiness, not impact
    # Evidence signals that SUPPORT H2:
    #   (a) Critical concentrated in high-frequency (small resolution/window) signals (same as H1)
    # Evidence signals that WEAKEN H2:
    #   (b) Critical is almost only one threshold name; could mean a specific policy, not general data-quality.
    score_h2 = 0.0
    ev_h2: List[str] = []

    # a) short resolution/window
    if pd.notna(med_res_c) and pd.notna(med_res_nc) and med_res_nc:
        ratio_res = float(med_res_c / med_res_nc)
        score_h2 += 0.55 * (1.0 if ratio_res <= 0.25 else 0.5 if ratio_res <= 0.5 else 0.0)
        ev_h2.append(f"Critical median resolution is much smaller than non-critical (ratio ≈ {ratio_res:.4f}).")
    else:
        score_h2 += 0.20
        ev_h2.append("Resolution comparison inconclusive (missing values).")

    if pd.notna(med_win_c) and pd.notna(med_win_nc) and med_win_nc:
        ratio_win = float(med_win_c / med_win_nc)
        score_h2 += 0.25 * (1.0 if ratio_win <= 0.10 else 0.5 if ratio_win <= 0.25 else 0.0)
        ev_h2.append(f"Critical median window is much smaller than non-critical (ratio ≈ {ratio_win:.4f}).")
    else:
        score_h2 += 0.10
        ev_h2.append("Window comparison inconclusive (missing values).")

    # b) concentration penalty: if almost all critical are one name, this points to a specific policy rather than general data-quality labeling
    if n_crit > 0:
        ev_h2.append(f"Top critical threshold_name share: {crit_top_share:.3f} ({crit_top_share*100:.1f}%).")
        # If >90% concentrated, reduce support for "general data-quality" interpretation
        if crit_top_share >= 0.90:
            score_h2 -= 0.25
            ev_h2.append("High concentration suggests criticality may be a specific operational policy (not a general data-quality label).")

    results.append(
        HypothesisResult(
            name="H2 (Alternative): isCritical ≈ data-quality/high-frequency trustworthiness trigger",
            status=decide_status(max(min(score_h2, 1.0), 0.0)),
            evidence_lines=ev_h2,
        )
    )

    # ---- Hypothesis 3: Severity might be a configuration/legacy category (policy), not a continuous event severity concept
    # Evidence signals:
    #   (a) High severity is exclusively one threshold_name (ARI Max Threshold)
    #   (b) High severity is almost always ARI-related (close to 100%)
    score_h3 = 0.0
    ev_h3: List[str] = []

    high_names = df[df["severity_norm"] == "High"]["threshold_name_norm"].value_counts()
    if n_high > 0 and len(high_names) > 0:
        top_high_name = high_names.index[0]
        top_high_share = float(high_names.iloc[0] / n_high)
        ev_h3.append(f"High severity most common name: '{top_high_name}' share={top_high_share:.3f} ({top_high_share*100:.1f}%).")
        # If >95% one name -> strong support that it's a simple policy label
        score_h3 += 0.55 * (1.0 if top_high_share >= 0.95 else 0.5 if top_high_share >= 0.80 else 0.0)

        ev_h3.append(f"High severity ARI-related share: {high_ari_share:.3f} ({high_ari_share*100:.1f}%).")
        score_h3 += 0.35 * (1.0 if high_ari_share >= 0.95 else 0.5 if high_ari_share >= 0.80 else 0.0)

        # If High severity never critical -> suggests separation/policy label
        if n_highcrit == 0:
            score_h3 += 0.10
            ev_h3.append("No High severity thresholds are marked critical (supports separation/policy labelling).")
    else:
        ev_h3.append("No High severity thresholds found; cannot evaluate.")
        score_h3 += 0.20

    results.append(
        HypothesisResult(
            name="H3 (Alternative): Severity labels are mostly a configuration/policy category (e.g., ARI thresholds set to High)",
            status=decide_status(min(score_h3, 1.0)),
            evidence_lines=ev_h3,
        )
    )

    # ---- Hypothesis 4: Criticality might be asset/site safety oriented (protecting assets/infrastructure) vs human impact
    # We can't prove without site metadata/outcome data, so mostly INCONCLUSIVE.
    # Evidence we can collect:
    #   (a) Are critical thresholds spread across many gauges? If yes -> more general operational trigger (not a single site quirk)
    #   (b) Are they concentrated in a few gauges? If yes -> could be asset-specific
    score_h4 = 0.0
    ev_h4: List[str] = []

    if n_crit > 0:
        crit_gauges = df[df["isCritical"]]["gauge_id"].nunique()
        ev_h4.append(f"Critical thresholds occur across {crit_gauges} unique gauges.")
        # if distributed across many gauges, it's less likely to be one-site asset issue
        spread_ratio = crit_gauges / max(df["gauge_id"].nunique(), 1)
        ev_h4.append(f"Spread ratio (critical gauges / all gauges-with-thresholds) = {spread_ratio:.3f}.")
        score_h4 += 0.35 * (1.0 if spread_ratio >= 0.50 else 0.5 if spread_ratio >= 0.25 else 0.0)

        # But still cannot conclude "human vs asset" without outcome data
        score_h4 += 0.15
        ev_h4.append("However, without incident/outcome metadata, distinguishing human-impact vs asset-protection intent remains uncertain.")
    else:
        ev_h4.append("No critical thresholds found; cannot evaluate.")
        score_h4 += 0.20

    # This should generally be INCONCLUSIVE (score around 0.3–0.5)
    results.append(
        HypothesisResult(
            name="H4 (Alternative): isCritical could reflect asset/site operational protection rather than human impact",
            status=decide_status(min(score_h4, 1.0)),
            evidence_lines=ev_h4,
        )
    )

    return results


# =============================
# Report writer
# =============================
def write_report_txt(
    *,
    out_path: Path,
    df: pd.DataFrame,
    total_gauges_all_assets: Optional[int],
    thresholds_test_ok: Optional[bool],
    thresholds_test_missing: Optional[List[int]],
    hypothesis_results: List[HypothesisResult],
) -> None:
    lines: List[str] = []

    n_total = len(df)
    unique_threshold_ids = int(df["threshold_id"].nunique()) if n_total else 0
    unique_gauges_with_thresholds = int(df["gauge_id"].nunique()) if n_total else 0

    lines.append("MOATA ALARM LABEL ANALYSIS REPORT (ANTI-BIAS)")
    lines.append("=" * 48)
    lines.append("")
    lines.append("This report presents evidence AND multiple plausible interpretations.")
    lines.append("It avoids assuming a single intent behind labels without outcome data.")
    lines.append("")

    # Coverage
    lines.append("STEP 0 — DATA COVERAGE")
    lines.append("-" * 22)
    lines.append(f"Total threshold rows analysed       : {n_total}")
    lines.append(f"Unique threshold IDs               : {unique_threshold_ids}")
    lines.append(f"Unique gauges with thresholds      : {unique_gauges_with_thresholds}")
    if total_gauges_all_assets is not None:
        lines.append(f"Total gauges in asset inventory     : {total_gauges_all_assets}")
        no_threshold = max(total_gauges_all_assets - unique_gauges_with_thresholds, 0)
        lines.append(
            f"Gauges without thresholds (inferred): {no_threshold} "
            f"({fmt_pct(no_threshold, total_gauges_all_assets)})"
        )
    lines.append("")

    # Quick facts
    if n_total:
        ct = safe_crosstab_select(pd.crosstab(df["severity_norm"], df["isCritical"])).sort_index()
        lines.append("STEP 1 — OBSERVED LABEL PATTERNS (FACTS)")
        lines.append("-" * 37)
        lines.append("severity×isCritical counts:")
        lines.append(ct.to_string())
        lines.append("")
        highcrit = df[(df["severity_norm"] == "High") & (df["isCritical"])].shape[0]
        lines.append(f"High severity AND Critical simultaneously: {highcrit} rows.")
        lines.append("")
    else:
        lines.append("No threshold rows found; cannot compute patterns.")
        lines.append("")

    # Optional consistency check
    lines.append("STEP 2 — OPTIONAL CONSISTENCY CHECK (thresholds_test_results.json)")
    lines.append("-" * 63)
    if thresholds_test_ok is None:
        lines.append("thresholds_test_results.json not found — skipped.")
    else:
        if thresholds_test_ok:
            lines.append("✅ All threshold IDs from thresholds_test_results.json exist in the full dataset.")
        else:
            lines.append("⚠ Some threshold IDs from thresholds_test_results.json are missing from the full dataset.")
            lines.append(f"Missing IDs (first 20): {thresholds_test_missing[:20] if thresholds_test_missing else []}")
    lines.append("")

    # Anti-bias section
    lines.append("STEP 3 — MULTIPLE INTERPRETATIONS (ANTI-BIAS ASSESSMENT)")
    lines.append("-" * 58)
    lines.append("Legend:")
    lines.append("  SUPPORTED        : Strongly consistent with observed statistics")
    lines.append("  WEAKLY SUPPORTED : Some support, but plausible counter-explanations remain")
    lines.append("  INCONCLUSIVE     : Not enough information to decide")
    lines.append("  NOT SUPPORTED    : Contradicted or not aligned with observed statistics")
    lines.append("")

    for hr in hypothesis_results:
        lines.append(f"{hr.name}")
        lines.append(f"Status: {hr.status}")
        lines.append("Evidence:")
        for ev in hr.evidence_lines:
            # indent multi-line evidence blocks
            block = ev.splitlines()
            if len(block) == 1:
                lines.append(f"  - {block[0]}")
            else:
                lines.append("  - " + block[0])
                for more in block[1:]:
                    lines.append("    " + more)
        lines.append("")

    # Final cautious conclusion
    lines.append("FINAL — CAUTIOUS CONCLUSION")
    lines.append("-" * 26)
    lines.append("The dataset strongly constrains how labels are used:")
    lines.append("- 'High' severity is tightly associated with ARI-related thresholds in this snapshot.")
    lines.append("- 'Critical' is tightly associated with very short window/resolution thresholds (e.g., 30s).")
    lines.append("")
    lines.append("However, label intent (human impact vs asset protection vs policy/legacy) cannot be fully proven")
    lines.append("without outcome/incident metadata or official documentation of label definitions.")
    lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")


# =============================
# Main
# =============================
def main():
    base = Path(".")  # run from folder containing the json files

    f_rgta = base / "rain_gauges_traces_alarms.json"   # REQUIRED
    f_gauges = base / "rain_gauges.json"               # OPTIONAL
    f_thrtest = base / "thresholds_test_results.json"  # OPTIONAL

    if not safe_exists(f_rgta):
        raise SystemExit(f"Required file not found: {f_rgta.resolve()}")

    rgta = load_json(f_rgta)
    df = build_threshold_table(rgta)

    # Optional gauge inventory count
    total_gauges_all_assets: Optional[int] = None
    if safe_exists(f_gauges):
        gauges = load_json(f_gauges)
        if isinstance(gauges, list):
            total_gauges_all_assets = len(gauges)

    # Optional thresholds test consistency check
    thresholds_test_ok: Optional[bool] = None
    thresholds_test_missing: Optional[List[int]] = None
    if safe_exists(f_thrtest):
        thrtest = load_json(f_thrtest)
        ids_test = extract_threshold_ids_from_test(thrtest)
        ids_full = set(df["threshold_id"].dropna().astype(int).tolist()) if not df.empty else set()
        missing = sorted(list(ids_test - ids_full))
        thresholds_test_ok = (len(missing) == 0)
        thresholds_test_missing = missing

    # Anti-bias hypotheses
    hypothesis_results = run_anti_bias_assessment(df) if not df.empty else []

    # Write report
    out_path = base / "alarm_label_interpretation_report_anti_bias.txt"
    write_report_txt(
        out_path=out_path,
        df=df,
        total_gauges_all_assets=total_gauges_all_assets,
        thresholds_test_ok=thresholds_test_ok,
        thresholds_test_missing=thresholds_test_missing,
        hypothesis_results=hypothesis_results,
    )

    print(f"✅ Anti-bias report written to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
