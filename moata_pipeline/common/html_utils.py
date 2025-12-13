from __future__ import annotations

import html
import pandas as pd


def df_to_html_table(df: pd.DataFrame, title: str, max_rows: int = 50) -> str:
    """
    Convert a DataFrame to an HTML table with a title.
    
    Args:
        df: DataFrame to convert
        title: Title to display above the table
        max_rows: Maximum number of rows to display
    
    Returns:
        HTML string with title and table
    """
    if df.empty:
        return f"<h3>{html.escape(title)}</h3><p><em>No rows.</em></p>"
    
    view = df.head(max_rows).copy()
    return (
        f"<h3>{html.escape(title)}</h3>"
        f"<p><em>Showing first {min(len(view), max_rows)} rows.</em></p>"
        + view.to_html(index=False, escape=True)
    )