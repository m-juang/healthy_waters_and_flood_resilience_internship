from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


# =====================
# CONFIG
# =====================
INPUT_CSV = Path("outputs/ari_alarm_validation_by_ari_trace.csv")
OUT_DIR = Path("outputs/figures")


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input not found: {INPUT_CSV.resolve()}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)

    # --- coerce numeric ---
    df["max_ari_value"] = pd.to_numeric(df["max_ari_value"], errors="coerce")
    df["threshold"] = pd.to_numeric(df["threshold"], errors="coerce")

    # --- exceedance ---
    df["exceed_by"] = df["max_ari_value"] - df["threshold"]

    # --- top 10 exceedances ---
    top = (
        df.dropna(subset=["exceed_by"])
          .sort_values("exceed_by", ascending=False)
          .head(10)
    )

    # =========================
    # TOP EXCEEDANCE PLOT
    # =========================
    plt.figure(figsize=(10, 5))
    plt.barh(top["gauge_name"], top["exceed_by"])
    plt.gca().invert_yaxis()
    plt.axvline(0, linestyle="--", linewidth=1)

    plt.title("Top 10 ARI Exceedances (Max TP108 ARI)")
    plt.xlabel("Exceedance above threshold (ARI years)")
    plt.ylabel("Gauge name")

    plt.tight_layout()
    plt.savefig(OUT_DIR / "top_exceedances.png", dpi=200)
    plt.close()

    print("DONE")
    print("- outputs/figures/top_exceedances.png")


if __name__ == "__main__":
    main()
