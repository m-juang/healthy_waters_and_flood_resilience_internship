"""
HTML Utilities Module

Provides utilities for generating HTML content, including tables, headers,
and formatted text.

Functions:
    df_to_html_table: Convert DataFrame to HTML table with title
    create_html_page: Create complete HTML page with header/footer
    escape_html: Safely escape HTML special characters
    create_dashboard_section: Create dashboard section with title

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import html
from typing import Optional, List, Dict, Any

import pandas as pd


# Version info
__version__ = "1.0.0"


# =============================================================================
# HTML Generation Functions
# =============================================================================

def df_to_html_table(
    df: pd.DataFrame,
    title: str,
    max_rows: int = 50,
    show_index: bool = False,
    classes: str = "dataframe",
) -> str:
    """
    Convert DataFrame to HTML table with title.
    
    Args:
        df: DataFrame to convert
        title: Title to display above the table
        max_rows: Maximum number of rows to display (default: 50)
        show_index: Whether to show DataFrame index (default: False)
        classes: CSS classes for the table (default: "dataframe")
        
    Returns:
        HTML string with title and table
        
    Example:
        >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        >>> html = df_to_html_table(df, "My Data")
        >>> print(html)
        <h3>My Data</h3>
        <p><em>Showing first 2 rows.</em></p>
        <table class="dataframe">...</table>
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df must be a DataFrame, got {type(df).__name__}")
    
    if df.empty:
        return f"<h3>{html.escape(title)}</h3><p><em>No rows.</em></p>"
    
    # Limit to max_rows
    view = df.head(max_rows).copy()
    total_rows = len(df)
    shown_rows = len(view)
    
    # Build HTML
    parts = [
        f"<h3>{html.escape(title)}</h3>",
    ]
    
    # Row count message
    if total_rows > max_rows:
        parts.append(
            f"<p><em>Showing first {shown_rows} of {total_rows} rows.</em></p>"
        )
    else:
        parts.append(
            f"<p><em>Showing {shown_rows} row{'s' if shown_rows != 1 else ''}.</em></p>"
        )
    
    # Table HTML
    parts.append(view.to_html(index=show_index, escape=True, classes=classes))
    
    return "\n".join(parts)


def create_html_page(
    title: str,
    content: str,
    css: Optional[str] = None,
    javascript: Optional[str] = None,
) -> str:
    """
    Create complete HTML page with header, content, and footer.
    
    Args:
        title: Page title (appears in browser tab)
        content: Main HTML content
        css: Optional CSS styles
        javascript: Optional JavaScript code
        
    Returns:
        Complete HTML page string
        
    Example:
        >>> html = create_html_page(
        ...     title="My Report",
        ...     content="<h1>Hello World</h1>",
        ...     css="body { font-family: Arial; }"
        ... )
    """
    css_block = f"<style>{css}</style>" if css else ""
    js_block = f"<script>{javascript}</script>" if javascript else ""
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html.escape(title)}</title>
    {css_block}
</head>
<body>
    {content}
    {js_block}
</body>
</html>"""


def escape_html(text: str) -> str:
    """
    Safely escape HTML special characters.
    
    Converts <, >, &, ", ' to their HTML entity equivalents.
    
    Args:
        text: Text to escape
        
    Returns:
        Escaped text safe for HTML
        
    Example:
        >>> escape_html("<script>alert('xss')</script>")
        '&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;'
    """
    return html.escape(str(text))


def create_dashboard_section(
    title: str,
    content: str,
    section_id: Optional[str] = None,
    collapsible: bool = False,
) -> str:
    """
    Create dashboard section with title and content.
    
    Args:
        title: Section title
        content: Section HTML content
        section_id: Optional HTML ID for the section
        collapsible: Whether section can be collapsed (default: False)
        
    Returns:
        HTML section string
        
    Example:
        >>> section = create_dashboard_section(
        ...     title="Statistics",
        ...     content="<p>Total: 100</p>",
        ...     section_id="stats"
        ... )
    """
    id_attr = f' id="{html.escape(section_id)}"' if section_id else ""
    class_attr = ' class="dashboard-section collapsible"' if collapsible else ' class="dashboard-section"'
    
    html_parts = [
        f"<div{class_attr}{id_attr}>",
        f"  <h2>{html.escape(title)}</h2>",
        f"  <div class='section-content'>",
        f"    {content}",
        f"  </div>",
        f"</div>",
    ]
    
    return "\n".join(html_parts)


def create_summary_box(
    title: str,
    value: Any,
    description: Optional[str] = None,
    color: str = "blue",
) -> str:
    """
    Create summary statistic box.
    
    Args:
        title: Statistic title
        value: Statistic value
        description: Optional description text
        color: Box color theme (blue, green, red, yellow)
        
    Returns:
        HTML summary box string
        
    Example:
        >>> box = create_summary_box(
        ...     title="Total Gauges",
        ...     value=42,
        ...     description="Active Auckland gauges"
        ... )
    """
    desc_html = f"<p class='summary-desc'>{html.escape(description)}</p>" if description else ""
    
    return f"""
<div class="summary-box summary-{html.escape(color)}">
    <div class="summary-title">{html.escape(title)}</div>
    <div class="summary-value">{html.escape(str(value))}</div>
    {desc_html}
</div>
"""


def create_alert_box(
    message: str,
    alert_type: str = "info",
    dismissible: bool = False,
) -> str:
    """
    Create alert/notification box.
    
    Args:
        message: Alert message
        alert_type: Type of alert (info, success, warning, error)
        dismissible: Whether alert can be dismissed (default: False)
        
    Returns:
        HTML alert box string
        
    Example:
        >>> alert = create_alert_box(
        ...     "Data collection complete!",
        ...     alert_type="success"
        ... )
    """
    valid_types = ["info", "success", "warning", "error"]
    if alert_type not in valid_types:
        alert_type = "info"
    
    dismiss_button = (
        '<button class="alert-close" onclick="this.parentElement.style.display=\'none\'">×</button>'
        if dismissible else ""
    )
    
    return f"""
<div class="alert alert-{html.escape(alert_type)}">
    {dismiss_button}
    {html.escape(message)}
</div>
"""


def create_progress_bar(
    value: float,
    max_value: float = 100.0,
    label: Optional[str] = None,
    show_percentage: bool = True,
) -> str:
    """
    Create progress bar HTML.
    
    Args:
        value: Current progress value
        max_value: Maximum value (default: 100)
        label: Optional label text
        show_percentage: Show percentage text (default: True)
        
    Returns:
        HTML progress bar string
        
    Example:
        >>> bar = create_progress_bar(75, label="Processing")
        >>> # Creates 75% progress bar
    """
    if max_value <= 0:
        max_value = 100.0
    
    percentage = min(100.0, max(0.0, (value / max_value) * 100))
    
    label_html = f"<span class='progress-label'>{html.escape(label)}</span>" if label else ""
    percent_html = f"<span class='progress-percent'>{percentage:.1f}%</span>" if show_percentage else ""
    
    return f"""
<div class="progress-container">
    {label_html}
    <div class="progress-bar">
        <div class="progress-fill" style="width: {percentage}%">
            {percent_html}
        </div>
    </div>
</div>
"""


def create_data_table(
    data: List[Dict[str, Any]],
    columns: Optional[List[str]] = None,
    title: Optional[str] = None,
    max_rows: int = 100,
) -> str:
    """
    Create HTML table from list of dictionaries.
    
    Args:
        data: List of dictionaries with data
        columns: Optional list of column names (default: use all keys)
        title: Optional table title
        max_rows: Maximum rows to display
        
    Returns:
        HTML table string
        
    Example:
        >>> data = [{"name": "A", "value": 1}, {"name": "B", "value": 2}]
        >>> table = create_data_table(data, title="Results")
    """
    if not data:
        no_data = "<p><em>No data available.</em></p>"
        return f"<h3>{html.escape(title)}</h3>{no_data}" if title else no_data
    
    # Convert to DataFrame and use df_to_html_table
    df = pd.DataFrame(data)
    
    if columns:
        # Filter to specified columns
        available_cols = [c for c in columns if c in df.columns]
        df = df[available_cols]
    
    return df_to_html_table(
        df,
        title=title or "Data Table",
        max_rows=max_rows,
    )