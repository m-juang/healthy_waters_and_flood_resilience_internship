"""
UI Components Module

Reusable UI components for the Rain Monitoring GUI.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-02
Version: 1.0.0
"""

from __future__ import annotations

import customtkinter as ctk
from typing import Callable, Dict, List, Optional, Tuple

from .config import darken_color


# =============================================================================
# Decorative Elements
# =============================================================================

def add_background_decoration(parent: ctk.CTkFrame, colors: Dict[str, str]) -> None:
    """
    Add subtle decorative circles to background.
    
    Args:
        parent: Parent frame to add decorations to
        colors: Color dictionary with 'background', 'deco1', 'deco2' keys
    """
    # Top-left large circle
    deco1 = ctk.CTkFrame(
        parent,
        width=350,
        height=350,
        corner_radius=175,
        fg_color=colors["deco1"],
        bg_color=colors["background"]
    )
    deco1.place(x=-50, y=-80, anchor="nw")
    
    # Bottom-right circle
    deco2 = ctk.CTkFrame(
        parent,
        width=250,
        height=250,
        corner_radius=125,
        fg_color=colors["deco2"],
        bg_color=colors["background"]
    )
    deco2.place(relx=1.05, rely=0.7, anchor="e")
    
    # Bottom-left circle  
    deco3 = ctk.CTkFrame(
        parent,
        width=200,
        height=200,
        corner_radius=100,
        fg_color=colors["deco1"],
        bg_color=colors["background"]
    )
    deco3.place(relx=-0.02, rely=1.02, anchor="sw")


# =============================================================================
# Header Components
# =============================================================================

def create_header(
    parent: ctk.CTkFrame,
    title: str,
    subtitle: Optional[str],
    colors: Dict[str, str],
    height: int = 80,
    fg_color: Optional[str] = None,
) -> ctk.CTkFrame:
    """
    Create a header frame with title and optional subtitle.
    
    Args:
        parent: Parent widget
        title: Main title text
        subtitle: Optional subtitle text
        colors: Color dictionary
        height: Header height in pixels
        fg_color: Override foreground color (default: primary)
        
    Returns:
        Header frame widget
    """
    header = ctk.CTkFrame(
        parent,
        fg_color=fg_color or colors["primary"],
        corner_radius=12,
        height=height
    )
    header.grid(row=0, column=0, sticky="ew")
    header.grid_propagate(False)
    
    title_label = ctk.CTkLabel(
        header,
        text=title,
        font=ctk.CTkFont(size=24, weight="bold"),
        text_color="white"
    )
    
    if subtitle:
        title_label.place(relx=0.5, rely=0.4, anchor="center")
        subtitle_label = ctk.CTkLabel(
            header,
            text=subtitle,
            font=ctk.CTkFont(size=12),
            text_color=colors["header_subtitle"]
        )
        subtitle_label.place(relx=0.5, rely=0.72, anchor="center")
    else:
        title_label.place(relx=0.5, rely=0.5, anchor="center")
    
    return header


def create_theme_toggle(
    parent: ctk.CTkFrame,
    is_dark_mode: bool,
    colors: Dict[str, str],
    command: Callable,
    hover_color: Optional[str] = None,
) -> ctk.CTkButton:
    """
    Create a theme toggle button.
    
    Args:
        parent: Parent widget
        is_dark_mode: Current theme state
        colors: Color dictionary
        command: Callback function for toggle
        hover_color: Override hover color
        
    Returns:
        Theme toggle button widget
    """
    toggle_icon = "🌙" if not is_dark_mode else "☀️"
    toggle_text = "Dark" if not is_dark_mode else "Light"
    
    theme_toggle = ctk.CTkButton(
        parent,
        text=f"{toggle_icon}  {toggle_text}",
        font=ctk.CTkFont(size=12),
        fg_color="transparent",
        hover_color=hover_color or colors["primary_light"],
        text_color="white",
        corner_radius=12,
        width=90,
        height=32,
        command=command
    )
    theme_toggle.place(relx=0.96, rely=0.5, anchor="e")
    
    return theme_toggle


# =============================================================================
# Card Components
# =============================================================================

def create_pipeline_card(
    parent: ctk.CTkFrame,
    column: int,
    icon: str,
    title: str,
    subtitle: str,
    features: List[str],
    color: str,
    colors: Dict[str, str],
    command: Callable,
) -> ctk.CTkFrame:
    """
    Create a pipeline selection card.
    
    Args:
        parent: Parent widget
        column: Grid column for placement
        icon: Emoji icon for the card
        title: Card title
        subtitle: Card subtitle
        features: List of feature descriptions
        color: Accent color for the card
        colors: Color dictionary
        command: Callback for start button
        
    Returns:
        Card frame widget
    """
    card = ctk.CTkFrame(parent, fg_color=colors["surface"], corner_radius=12)
    card.grid(row=0, column=column, sticky="nsew", padx=10, pady=5)
    
    # Color strip at top
    strip = ctk.CTkFrame(card, fg_color=color, corner_radius=12, height=4)
    strip.pack(fill="x")
    
    # Content
    content = ctk.CTkFrame(card, fg_color="transparent")
    content.pack(fill="both", expand=True, padx=20, pady=15)
    
    # Icon and title in same row
    header_frame = ctk.CTkFrame(content, fg_color="transparent")
    header_frame.pack(fill="x", pady=(0, 8))
    
    icon_label = ctk.CTkLabel(
        header_frame,
        text=icon,
        font=ctk.CTkFont(size=32)
    )
    icon_label.pack(side="left", padx=(0, 10))
    
    title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
    title_frame.pack(side="left", fill="x", expand=True)
    
    title_label = ctk.CTkLabel(
        title_frame,
        text=title,
        font=ctk.CTkFont(size=18, weight="bold"),
        text_color=colors["text"],
        anchor="w"
    )
    title_label.pack(anchor="w")
    
    subtitle_label = ctk.CTkLabel(
        title_frame,
        text=subtitle,
        font=ctk.CTkFont(size=12, weight="bold"),
        text_color=color,
        anchor="w"
    )
    subtitle_label.pack(anchor="w")
    
    # Features list
    features_frame = ctk.CTkFrame(content, fg_color="transparent")
    features_frame.pack(fill="x", pady=(5, 10))
    
    for feature in features:
        feature_label = ctk.CTkLabel(
            features_frame,
            text=f"✓  {feature}",
            font=ctk.CTkFont(size=12),
            text_color=colors["text_secondary"],
            anchor="w"
        )
        feature_label.pack(anchor="w", pady=1)
    
    # Start button
    start_btn = ctk.CTkButton(
        content,
        text="Start Pipeline  →",
        font=ctk.CTkFont(size=13, weight="bold"),
        fg_color=color,
        hover_color=darken_color(color),
        corner_radius=12,
        height=38,
        command=command
    )
    start_btn.pack(fill="x", pady=(10, 5))
    
    return card


# =============================================================================
# Step Components
# =============================================================================

def create_step_row(
    parent: ctk.CTkFrame,
    number: str,
    name: str,
    description: str,
    color: str,
    colors: Dict[str, str],
    command: Callable,
    is_last: bool = False,
) -> ctk.CTkFrame:
    """
    Create a pipeline step row.
    
    Args:
        parent: Parent widget
        number: Step number (e.g., "1")
        name: Step name
        description: Step description
        color: Accent color for the step
        colors: Color dictionary
        command: Callback for run button
        is_last: Whether this is the last step (no separator)
        
    Returns:
        Step row frame widget
    """
    row_frame = ctk.CTkFrame(parent, fg_color="transparent")
    row_frame.pack(fill="x", padx=20, pady=10)
    
    # Number badge
    badge = ctk.CTkFrame(row_frame, fg_color=color, corner_radius=12, width=50, height=50)
    badge.pack(side="left", padx=(5, 20))
    badge.pack_propagate(False)
    
    badge_label = ctk.CTkLabel(
        badge,
        text=number,
        font=ctk.CTkFont(size=20, weight="bold"),
        text_color="white"
    )
    badge_label.place(relx=0.5, rely=0.5, anchor="center")
    
    # Info
    info_frame = ctk.CTkFrame(row_frame, fg_color="transparent")
    info_frame.pack(side="left", fill="x", expand=True)
    
    name_label = ctk.CTkLabel(
        info_frame,
        text=name,
        font=ctk.CTkFont(size=16, weight="bold"),
        text_color=colors["text"],
        anchor="w"
    )
    name_label.pack(anchor="w")
    
    desc_label = ctk.CTkLabel(
        info_frame,
        text=description,
        font=ctk.CTkFont(size=12),
        text_color=colors["text_secondary"],
        anchor="w"
    )
    desc_label.pack(anchor="w")
    
    # Run button
    run_btn = ctk.CTkButton(
        row_frame,
        text="Run",
        font=ctk.CTkFont(size=13, weight="bold"),
        fg_color=color,
        hover_color=darken_color(color),
        corner_radius=12,
        width=80,
        height=36,
        command=command
    )
    run_btn.pack(side="right", padx=(10, 5))
    
    # Separator
    if not is_last:
        separator = ctk.CTkFrame(parent, fg_color=colors["border"], height=1)
        separator.pack(fill="x", padx=40, pady=5)
    
    return row_frame


# =============================================================================
# Dialog Components
# =============================================================================

def show_date_selection_dialog(
    parent: ctk.CTk,
    title: str,
    options: List[Tuple[str, str, str]],
    colors: Dict[str, str],
) -> Optional[str]:
    """
    Show a date/option selection dialog.
    
    Args:
        parent: Parent window
        title: Dialog title
        options: List of (button_text, value, description) tuples
        colors: Color dictionary
        
    Returns:
        Selected value or None if cancelled
    """
    dialog = ctk.CTkToplevel(parent)
    dialog.title(title)
    dialog.geometry("500x420")
    dialog.transient(parent)
    dialog.grab_set()
    dialog.configure(fg_color=colors["background"])
    
    # Center dialog
    dialog.update_idletasks()
    x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
    y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
    dialog.geometry(f"+{x}+{y}")
    
    # Title
    title_label = ctk.CTkLabel(
        dialog,
        text=title,
        font=ctk.CTkFont(size=20, weight="bold"),
        text_color=colors["text"]
    )
    title_label.pack(pady=(25, 5))
    
    # Subtitle
    subtitle_label = ctk.CTkLabel(
        dialog,
        text="Choose an option below",
        font=ctk.CTkFont(size=12),
        text_color=colors["text_secondary"]
    )
    subtitle_label.pack(pady=(0, 20))
    
    # Selection variable
    selection = {"value": None}
    
    # Options container
    options_frame = ctk.CTkFrame(dialog, fg_color="transparent")
    options_frame.pack(fill="both", expand=True, padx=25)
    
    # Option buttons
    for btn_text, btn_value, btn_desc in options:
        btn_frame = ctk.CTkFrame(options_frame, fg_color=colors["surface"], corner_radius=12)
        btn_frame.pack(fill="x", pady=6)
        
        # Make the frame clickable
        def make_click_handler(v):
            return lambda e: [selection.update({"value": v}), dialog.destroy()]
        
        btn_frame.bind("<Button-1>", make_click_handler(btn_value))
        btn_frame.configure(cursor="hand2")
        
        # Content inside frame
        content_frame = ctk.CTkFrame(btn_frame, fg_color="transparent")
        content_frame.pack(fill="x", padx=18, pady=14)
        content_frame.bind("<Button-1>", make_click_handler(btn_value))
        
        # Text container
        text_container = ctk.CTkFrame(content_frame, fg_color="transparent")
        text_container.pack(fill="x")
        text_container.bind("<Button-1>", make_click_handler(btn_value))
        
        # Title
        btn_title = ctk.CTkLabel(
            text_container,
            text=btn_text,
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=colors["text"],
            anchor="w"
        )
        btn_title.pack(fill="x")
        btn_title.bind("<Button-1>", make_click_handler(btn_value))
        
        # Description
        btn_desc_label = ctk.CTkLabel(
            text_container,
            text=btn_desc,
            font=ctk.CTkFont(size=12),
            text_color=colors["text_secondary"],
            anchor="w"
        )
        btn_desc_label.pack(fill="x", pady=(4, 0))
        btn_desc_label.bind("<Button-1>", make_click_handler(btn_value))
        
        # Hover effect
        def make_hover_enter(frame):
            return lambda e: frame.configure(fg_color=colors["border"])
        def make_hover_leave(frame):
            return lambda e: frame.configure(fg_color=colors["surface"])
        
        btn_frame.bind("<Enter>", make_hover_enter(btn_frame))
        btn_frame.bind("<Leave>", make_hover_leave(btn_frame))
    
    # Cancel button
    cancel_btn = ctk.CTkButton(
        dialog,
        text="Cancel",
        font=ctk.CTkFont(size=13),
        fg_color="transparent",
        hover_color=colors["border"],
        text_color=colors["text_secondary"],
        corner_radius=12,
        height=35,
        command=dialog.destroy
    )
    cancel_btn.pack(pady=15)
    
    # Wait for dialog
    parent.wait_window(dialog)
    
    return selection["value"]