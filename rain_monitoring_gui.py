#!/usr/bin/env python3
"""
Rain Monitoring System GUI - Professional Edition

Interactive GUI with modern design for running rain gauge and rain radar pipelines.
Features: professional color system, modern layout, emoji support, enhanced UX.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 2.1.0 (Professional Color System)

Usage:
    python rain_monitoring_gui.py
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext, font
import subprocess
import threading
import sys
from pathlib import Path
from datetime import datetime


class ModernButton(tk.Canvas):
    """Modern gradient button with hover effects."""

    def __init__(self, parent, text, command, color="#1E3A5F", scale=1.0, **kwargs):
        self.scale = scale
        self.command = command
        self.base_color = color
        self.hover_color = self._adjust_color(color, 1.08)
        self.text = text

        # Better button sizing with stable minimums
        self.width = max(240, int(round(260 * scale)))
        self.height = max(44, int(round(48 * scale)))

        super().__init__(
            parent,
            height=self.height,
            width=self.width,
            highlightthickness=0,
            **kwargs,
        )

        self.draw_button(self.base_color)

        self.bind("<Button-1>", lambda e: self.command())
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)

    def draw_button(self, color):
        """Draw gradient button."""
        self.delete("all")

        # Slight top highlight for depth (still professional)
        top_color = self._adjust_color(color, 1.10)
        bottom_color = color

        margin = max(4, int(round(6 * self.scale)))
        x0, y0 = margin, margin
        x1, y1 = self.width - margin, self.height - margin

        steps = max(12, int(round(18 * self.scale)))
        denom = max(1, steps - 1)
        for i in range(steps):
            shade = self._interpolate_color(top_color, bottom_color, i / denom)
            y_start = y0 + i * (y1 - y0) / steps
            y_end = y0 + (i + 1) * (y1 - y0) / steps
            self.create_rectangle(x0, y_start, x1, y_end, fill=shade, outline="")

        # Border: slightly darker than fill for crispness
        border_w = max(1, int(round(2 * self.scale)))
        border_color = self._adjust_color(color, 0.78)
        self.create_rectangle(x0, y0, x1, y1, outline=border_color, width=border_w)

        text_size = max(12, int(round(13 * self.scale)))
        self.create_text(
            self.width // 2,
            self.height // 2,
            text=self.text,
            font=("Segoe UI", text_size, "bold"),
            fill="white",
        )

    def _on_enter(self, event):
        self.draw_button(self.hover_color)
        self.config(cursor="hand2")

    def _on_leave(self, event):
        self.draw_button(self.base_color)
        self.config(cursor="")

    def _adjust_color(self, color, factor):
        """Lighten/darken by scaling RGB (keeps hue stable, reduces neon)."""
        color = color.lstrip("#")
        rgb = tuple(int(color[i : i + 2], 16) for i in (0, 2, 4))
        rgb = tuple(max(0, min(255, int(c * factor))) for c in rgb)
        return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"

    def _interpolate_color(self, color1, color2, fraction):
        c1 = color1.lstrip("#")
        c2 = color2.lstrip("#")
        rgb1 = tuple(int(c1[i : i + 2], 16) for i in (0, 2, 4))
        rgb2 = tuple(int(c2[i : i + 2], 16) for i in (0, 2, 4))
        rgb = tuple(int(rgb1[i] + (rgb2[i] - rgb1[i]) * fraction) for i in range(3))
        return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"


class RainMonitoringGUI:
    """Main GUI application for Rain Monitoring System."""

    def __init__(self, root):
        self.root = root
        self.root.title("Auckland Council - Rain Monitoring System")

        # Detect screen size and set responsive dimensions
        self.detect_screen_size()

        self.root.geometry(f"{self.window_width}x{self.window_height}")
        self.root.resizable(True, True)

        # ---------------------------------------------------------------------
        # PROFESSIONAL COLOR SYSTEM (Color Theory Applied)
        #
        # Approach:
        # - Neutral base (slate/gray) -> reduces visual fatigue, improves hierarchy
        # - Primary (navy) -> trust/authority
        # - Accent (teal) -> modern highlight without being loud
        # - Semantic colors are muted (professional dashboards)
        # ---------------------------------------------------------------------
        self.colors = {
            # Core brand
            "primary": "#1E3A5F",     # Navy (trust, authority)
            "primary_2": "#274C77",   # Slightly lighter navy for gradients/sections
            "accent": "#0F766E",      # Teal accent (modern, calm)

            # Backgrounds & surfaces (neutral)
            "bg_light": "#F7F8FA",    # near-white, softer than pure white
            "bg_medium": "#EEF2F6",   # light slate
            "surface": "#FFFFFF",     # cards

            # Text
            "text_dark": "#111827",   # near-black (good contrast)
            "text_light": "#475569",  # slate for secondary text
            "text_muted": "#64748B",  # muted label text

            # Borders / dividers
            "border": "#D7DEE7",

            # Semantic (muted but clear)
            "success": "#166534",     # deep green
            "warning": "#B45309",     # amber-brown
            "danger":  "#B91C1C",     # deep red

            # Pipeline accents (distinct but professional)
            "gauge": "#1D4ED8",       # blue (data/measurement)
            "radar": "#6D28D9",       # purple (spatial/remote sensing)
        }

        # State variables
        self.current_pipeline = None
        self.current_step = 0
        self.output_dir = None

        self.setup_fonts()
        self.setup_styles()

        self.root.configure(bg=self.colors["bg_light"])
        self.create_main_menu()

    # ----------------------------
    # UI scaling helpers
    # ----------------------------
    def ui(self, value: float, min_v: int | None = None, max_v: int | None = None) -> int:
        v = int(round(value * self.scale))
        if min_v is not None:
            v = max(min_v, v)
        if max_v is not None:
            v = min(max_v, v)
        return v

    def detect_screen_size(self):
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()

        width_scale = screen_width / 1920
        height_scale = screen_height / 1080
        self.scale = min(width_scale, height_scale)
        self.scale = max(0.75, min(1.15, self.scale))

        self.window_width = int(min(980 * self.scale, screen_width * 0.68))
        self.window_height = int(min(740 * self.scale, screen_height * 0.72))

        self.pad_xs = self.ui(6, 6)
        self.pad_s = self.ui(10, 8)
        self.pad_m = self.ui(16, 12)
        self.pad_l = self.ui(24, 16)
        self.pad_xl = self.ui(34, 20)

        self.header_height = self.ui(96, 84, 120)
        self.card_width = self.ui(300, 280, 360)
        self.card_height = self.ui(360, 340, 430)

        try:
            self.root.tk.call("tk", "scaling", 1.0)
        except Exception:
            pass

    def setup_fonts(self):
        try:
            font.Font(family="Segoe UI", size=12)
            self.font_family = "Segoe UI"
        except Exception:
            self.font_family = "Arial"

        def f(sz: int, min_sz: int) -> int:
            return max(min_sz, int(round(sz * self.scale)))

        self.fonts = {
            "title": (self.font_family, f(24, 20), "bold"),
            "subtitle": (self.font_family, f(14, 12)),
            "heading": (self.font_family, f(18, 15), "bold"),
            "button": (self.font_family, f(13, 12), "bold"),
            "body": (self.font_family, f(11, 11)),
            "small": (self.font_family, f(9, 10)),
        }

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use("clam")

        # Make ttk theme match our neutral background
        style.configure(".", background=self.colors["bg_light"])

        btn_pad_x = self.ui(14, 12, 18)
        btn_pad_y = self.ui(10, 8, 14)

        # Primary button style for execution window close etc.
        style.configure(
            "Modern.TButton",
            font=self.fonts["button"],
            padding=(btn_pad_x, btn_pad_y),
            relief="flat",
            borderwidth=0,
            background=self.colors["primary"],
            foreground="white",
            focusthickness=0,
            focuscolor="none",
        )
        style.map(
            "Modern.TButton",
            background=[
                ("active", self.colors["primary_2"]),
                ("pressed", self.colors["primary_2"]),
            ],
            foreground=[("disabled", self.colors["text_muted"])],
        )

        # Step buttons: neutral surface with accent hover
        step_pad_x = self.ui(12, 10, 16)
        step_pad_y = self.ui(8, 7, 12)

        style.configure(
            "Step.TButton",
            font=self.fonts["body"],
            padding=(step_pad_x, step_pad_y),
            relief="flat",
            borderwidth=0,
            background=self.colors["bg_medium"],
            foreground=self.colors["text_dark"],
        )
        style.map(
            "Step.TButton",
            background=[
                ("active", "#E3EAF2"),
                ("pressed", "#DCE6F1"),
            ],
            foreground=[
                ("active", self.colors["text_dark"]),
            ],
        )

        # Progressbar: more “corporate”
        style.configure(
            "Horizontal.TProgressbar",
            troughcolor=self.colors["bg_medium"],
            bordercolor=self.colors["border"],
            background=self.colors["accent"],
            lightcolor=self.colors["accent"],
            darkcolor=self.colors["accent"],
        )

        # LabelFrame styling
        style.configure(
            "TLabelframe",
            background=self.colors["surface"],
            foreground=self.colors["text_dark"],
        )
        style.configure(
            "TLabelframe.Label",
            background=self.colors["surface"],
            foreground=self.colors["text_dark"],
            font=self.fonts["body"],
        )

    # ----------------------------
    # Main Menu
    # ----------------------------
    def create_main_menu(self):
        for widget in self.root.winfo_children():
            widget.destroy()

        main_container = tk.Frame(self.root, bg=self.colors["bg_light"])
        main_container.pack(fill=tk.BOTH, expand=True)

        header = tk.Frame(main_container, bg=self.colors["primary"], height=self.header_height)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        title = tk.Label(
            header,
            text="🌧️ Rain Monitoring System",
            font=self.fonts["title"],
            bg=self.colors["primary"],
            fg="white",
        )
        title.pack(pady=(self.ui(20, 14), self.ui(4, 2)))

        subtitle = tk.Label(
            header,
            text="Auckland Council • Healthy Waters & Flood Resilience",
            font=self.fonts["subtitle"],
            bg=self.colors["primary"],
            fg="#DCE7F3",
        )
        subtitle.pack()

        content = tk.Frame(main_container, bg=self.colors["bg_light"])
        content.pack(fill=tk.BOTH, expand=True, padx=self.pad_xl, pady=self.pad_l)

        welcome_card = tk.Frame(content, bg=self.colors["surface"], relief="flat", bd=0)
        welcome_card.pack(fill=tk.X, pady=(0, self.pad_m))
        welcome_card.config(highlightbackground=self.colors["border"], highlightthickness=1)

        welcome_text = tk.Label(
            welcome_card,
            text="Select a pipeline to begin your analysis",
            font=self.fonts["heading"],
            bg=self.colors["surface"],
            fg=self.colors["text_dark"],
        )
        welcome_text.pack(pady=self.pad_m)

        info = tk.Label(
            welcome_card,
            text=(
                "Choose between point-based gauge monitoring or spatial radar analysis.\n"
                "Each pipeline guides you through data collection, analysis, and visualization."
            ),
            font=self.fonts["body"],
            bg=self.colors["surface"],
            fg=self.colors["text_light"],
            justify=tk.CENTER,
        )
        info.pack(pady=(0, self.pad_m))

        cards_frame = tk.Frame(content, bg=self.colors["bg_light"])
        cards_frame.pack(expand=True)

        gauge_card = self.create_pipeline_card(
            cards_frame,
            "🌊 Rain Gauge Pipeline",
            "Point-Based Monitoring",
            "~200 active rain gauges\nReal-time ARI calculation\nAlarm validation",
            self.colors["gauge"],
            lambda: self.start_pipeline("gauge"),
        )
        gauge_card.grid(row=0, column=0, padx=self.pad_m, pady=self.pad_s)

        radar_card = self.create_pipeline_card(
            cards_frame,
            "📡 Rain Radar Pipeline",
            "Spatial Coverage",
            "QPE catchment analysis\nPixel-level rainfall data\nSpatial visualization",
            self.colors["radar"],
            lambda: self.start_pipeline("radar"),
        )
        radar_card.grid(row=0, column=1, padx=self.pad_m, pady=self.pad_s)

        footer_height = self.ui(48, 44, 60)
        footer = tk.Frame(main_container, bg=self.colors["bg_medium"], height=footer_height)
        footer.pack(fill=tk.X, side=tk.BOTTOM)
        footer.pack_propagate(False)

        footer_text = tk.Label(
            footer,
            text=f"Version 2.1.0 • {datetime.now().strftime('%Y-%m-%d')} • COMPSCI 778 Internship",
            font=self.fonts["small"],
            bg=self.colors["bg_medium"],
            fg=self.colors["text_muted"],
        )
        footer_text.pack(expand=True)

    def create_pipeline_card(self, parent, title, subtitle, description, color, command):
        card = tk.Frame(parent, bg=self.colors["surface"], relief="flat", bd=0, width=self.card_width, height=self.card_height)
        card.config(highlightbackground=self.colors["border"], highlightthickness=1)
        card.pack_propagate(False)
        card.grid_propagate(False)

        header_strip_height = self.ui(6, 6, 10)
        header = tk.Frame(card, bg=color, height=header_strip_height)
        header.pack(fill=tk.X)

        content_padding = self.ui(22, 18, 30)
        content = tk.Frame(card, bg=self.colors["surface"])
        content.pack(fill=tk.BOTH, expand=True, padx=content_padding, pady=self.ui(18, 14, 26))

        # Extract emoji and text
        if "🌊" in title:
            icon_text = "🌊"
            title_text = title.replace("🌊", "").strip()
        elif "📡" in title:
            icon_text = "📡"
            title_text = title.replace("📡", "").strip()
        else:
            icon_text = ""
            title_text = title

        title_frame = tk.Frame(content, bg=self.colors["surface"])
        title_frame.pack(pady=(self.pad_s, self.ui(4, 2)))

        if icon_text:
            icon_size = self.ui(20, 18, 26)
            icon_label = tk.Label(
                title_frame,
                text=icon_text,
                font=("Segoe UI Emoji", icon_size),
                bg=self.colors["surface"],
                fg=self.colors["text_dark"],
            )
            icon_label.pack(side=tk.LEFT, padx=(0, self.ui(6, 4)))

        title_font = (self.font_family, self.ui(15, 14, 18), "bold")
        title_label = tk.Label(
            title_frame,
            text=title_text,
            font=title_font,
            bg=self.colors["surface"],
            fg=self.colors["text_dark"],
        )
        title_label.pack(side=tk.LEFT)

        subtitle_font = (self.font_family, self.ui(11, 10, 13), "bold")
        subtitle_label = tk.Label(
            content,
            text=subtitle,
            font=subtitle_font,
            bg=self.colors["surface"],
            fg=color,
            wraplength=self.card_width - (content_padding * 2),
        )
        subtitle_label.pack(pady=(0, self.ui(12, 10, 16)))

        desc_font = (self.font_family, self.ui(10, 10, 12))
        desc_label = tk.Label(
            content,
            text=description,
            font=desc_font,
            bg=self.colors["surface"],
            fg=self.colors["text_light"],
            justify=tk.LEFT,
            wraplength=self.card_width - (content_padding * 2),
        )
        desc_label.pack(pady=(0, self.ui(16, 12, 20)))

        btn = ModernButton(content, "Start Pipeline →", command, color=color, scale=self.scale, bg=self.colors["surface"])
        btn.pack(pady=(self.ui(8, 6), 0))

        def on_enter(e):
            card.config(highlightbackground=color, highlightthickness=2)

        def on_leave(e):
            card.config(highlightbackground=self.colors["border"], highlightthickness=1)

        for widget in [card, content, title_frame, title_label, subtitle_label, desc_label]:
            widget.bind("<Enter>", on_enter)
            widget.bind("<Leave>", on_leave)

        return card

    # ----------------------------
    # Pipeline Steps Menu
    # ----------------------------
    def start_pipeline(self, pipeline_type):
        self.current_pipeline = pipeline_type
        self.current_step = 0
        self.show_step_menu()

    def show_step_menu(self):
        for widget in self.root.winfo_children():
            widget.destroy()

        main_container = tk.Frame(self.root, bg=self.colors["bg_light"])
        main_container.pack(fill=tk.BOTH, expand=True)

        pipeline_name = "Rain Gauge" if self.current_pipeline == "gauge" else "Rain Radar"
        pipeline_icon = "🌊" if self.current_pipeline == "gauge" else "📡"
        pipeline_color = self.colors["gauge"] if self.current_pipeline == "gauge" else self.colors["radar"]

        header_height = self.ui(102, 90, 130)
        header = tk.Frame(main_container, bg=pipeline_color, height=header_height)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        title_frame = tk.Frame(header, bg=pipeline_color)
        title_frame.pack(pady=(self.ui(20, 14), self.ui(4, 2)))

        icon_size = self.ui(24, 20, 28)
        icon_label = tk.Label(
            title_frame,
            text=pipeline_icon,
            font=("Segoe UI Emoji", icon_size),
            bg=pipeline_color,
            fg="white",
        )
        icon_label.pack(side=tk.LEFT, padx=(0, self.ui(8, 6)))

        title = tk.Label(
            title_frame,
            text=f"{pipeline_name} Pipeline",
            font=self.fonts["title"],
            bg=pipeline_color,
            fg="white",
        )
        title.pack(side=tk.LEFT)

        progress = tk.Label(
            header,
            text="Ready to begin • Step 0 of 5",
            font=self.fonts["subtitle"],
            bg=pipeline_color,
            fg="#E6EEF8",
        )
        progress.pack()

        content = tk.Frame(main_container, bg=self.colors["bg_light"])
        content.pack(fill=tk.BOTH, expand=True, padx=self.pad_xl, pady=self.pad_m)

        steps_card = tk.Frame(content, bg=self.colors["surface"])
        steps_card.pack(fill=tk.BOTH, expand=True)
        steps_card.config(highlightbackground=self.colors["border"], highlightthickness=1)

        card_header = tk.Label(
            steps_card,
            text="Pipeline Steps",
            font=self.fonts["heading"],
            bg=self.colors["surface"],
            fg=self.colors["text_dark"],
        )
        card_header.pack(pady=self.pad_m, padx=self.pad_m, anchor="w")

        steps = [
            ("1", "Retrieve Data", "Collect data from Moata API", self.run_retrieve, self.colors["accent"]),
            ("2", "Analyze Data", "Filter gauges and calculate ARI", self.run_analyze, self.colors["success"]),
            ("3", "Visualize Results", "Generate HTML dashboards", self.run_visualize, self.colors["warning"]),
            ("4", "Validate Alarms", "Compare with historical data (Optional)", self.run_validate, self.colors["text_muted"]),
            ("5", "Visualize Validation", "Create validation dashboard (Optional)", self.run_visualize_validation, self.colors["text_muted"]),
        ]

        circle_size = self.ui(54, 48, 64)
        left_margin = self.ui(22, 18, 30)

        for i, (number, name, desc, command, color) in enumerate(steps):
            step_frame = tk.Frame(steps_card, bg=self.colors["surface"])
            step_frame.pack(fill=tk.X, padx=(left_margin, self.pad_m), pady=self.pad_s)

            circle_frame = tk.Frame(step_frame, bg=self.colors["surface"])
            circle_frame.pack(side=tk.LEFT, padx=(0, self.ui(18, 14)))

            circle = tk.Canvas(circle_frame, width=circle_size, height=circle_size, bg=self.colors["surface"], highlightthickness=0)
            circle.pack()

            padding = self.ui(4, 3, 6)
            circle.create_oval(
                padding,
                padding,
                circle_size - padding,
                circle_size - padding,
                fill=color,
                outline="",
                width=0,
            )

            number_size = self.ui(20, 16, 24)
            circle.create_text(
                circle_size // 2,
                circle_size // 2,
                text=number,
                font=(self.font_family, number_size, "bold"),
                fill="white",
            )

            info_frame = tk.Frame(step_frame, bg=self.colors["surface"])
            info_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, pady=self.ui(2, 2))

            name_font = (self.font_family, self.ui(14, 12, 16), "bold")
            name_label = tk.Label(
                info_frame,
                text=name,
                font=name_font,
                bg=self.colors["surface"],
                fg=self.colors["text_dark"],
                anchor="w",
            )
            name_label.pack(fill=tk.X, pady=(0, self.ui(2, 2)))

            desc_font = (self.font_family, self.ui(10, 10, 12))
            desc_label = tk.Label(
                info_frame,
                text=desc,
                font=desc_font,
                bg=self.colors["surface"],
                fg=self.colors["text_light"],
                anchor="w",
            )
            desc_label.pack(fill=tk.X)

            button_frame = tk.Frame(step_frame, bg=self.colors["surface"])
            button_frame.pack(side=tk.RIGHT, padx=(self.ui(10, 8), 0))

            run_btn = ttk.Button(button_frame, text="Run →", style="Step.TButton", command=command)
            run_btn.pack()
            try:
                run_btn.configure(width=max(8, int(round(10 * self.scale))))
            except Exception:
                pass

            if i < len(steps) - 1:
                sep = tk.Frame(steps_card, bg=self.colors["bg_medium"], height=1)
                sep.pack(fill=tk.X, padx=self.ui(54, 40), pady=self.ui(6, 4))

        back_frame = tk.Frame(main_container, bg=self.colors["bg_light"])
        back_frame.pack(fill=tk.X, padx=self.pad_xl, pady=(0, self.pad_m))

        back_btn = tk.Label(
            back_frame,
            text="← Back to Main Menu",
            font=self.fonts["body"],
            fg=pipeline_color,
            bg=self.colors["bg_light"],
            cursor="hand2",
        )
        back_btn.pack(anchor="w")
        back_btn.bind("<Button-1>", lambda e: self.create_main_menu())

    # ----------------------------
    # Steps actions (scripts)
    # ----------------------------
    def run_retrieve(self):
        script = "retrieve_rain_gauges.py" if self.current_pipeline == "gauge" else "retrieve_rain_radar.py"
        title = "Step 1: Retrieve Data"
        self.show_execution_window(title, script, [], self.on_retrieve_complete)

    def on_retrieve_complete(self, success):
        if success:
            messagebox.showinfo("Success", "✅ Data collection complete!\n\nReady to proceed to Analysis.")
            self.show_step_menu()
        else:
            messagebox.showerror("Error", "❌ Data collection failed!\n\nCheck the logs for details.")

    def run_analyze(self):
        output_dir = filedialog.askdirectory(
            title="Select output directory for analysis results",
            initialdir=str(Path.cwd() / "outputs"),
        )
        if not output_dir:
            messagebox.showwarning("Cancelled", "Analysis cancelled - no output directory selected")
            return

        self.output_dir = output_dir
        script = "analyze_rain_gauges.py" if self.current_pipeline == "gauge" else "analyze_rain_radar.py"
        title = "Step 2: Analyze Data"
        args = ["--output-dir", output_dir]
        self.show_execution_window(title, script, args, self.on_analyze_complete)

    def on_analyze_complete(self, success):
        if success:
            messagebox.showinfo(
                "Success",
                f"✅ Analysis complete!\n\nResults saved to:\n{self.output_dir}\n\nReady to proceed to Visualization.",
            )
            self.show_step_menu()
        else:
            messagebox.showerror("Error", "❌ Analysis failed!\n\nCheck the logs for details.")

    def run_visualize(self):
        output_dir = filedialog.askdirectory(
            title="Select output directory for visualizations",
            initialdir=str(Path.cwd() / "outputs"),
        )
        if not output_dir:
            messagebox.showwarning("Cancelled", "Visualization cancelled - no output directory selected")
            return

        self.output_dir = output_dir
        script = "visualize_rain_gauges.py" if self.current_pipeline == "gauge" else "visualize_rain_radar.py"
        title = "Step 3: Visualize Results"
        args = ["--output", output_dir]
        self.show_execution_window(title, script, args, self.on_visualize_complete)

    def on_visualize_complete(self, success):
        if success:
            result = messagebox.askquestion(
                "Success",
                f"✅ Visualization complete!\n\nDashboard saved to:\n{self.output_dir}\n\nWould you like to open the dashboard?",
            )
            if result == "yes":
                self.open_dashboard()
            self.show_step_menu()
        else:
            messagebox.showerror("Error", "❌ Visualization failed!\n\nCheck the logs for details.")

    def open_dashboard(self):
        import webbrowser

        if self.current_pipeline == "gauge":
            dashboard_path = Path(self.output_dir) / "report.html"
        else:
            dashboard_path = Path(self.output_dir) / "radar_dashboard.html"

        if dashboard_path.exists():
            webbrowser.open(dashboard_path.as_uri())
        else:
            messagebox.showwarning("Not Found", f"Dashboard file not found:\n{dashboard_path}")

    def run_validate(self):
        input_file = filedialog.askopenfilename(
            title="Select historical alarm events CSV",
            initialdir=str(Path.cwd() / "data" / "inputs"),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not input_file:
            messagebox.showwarning("Cancelled", "Validation cancelled - no input file selected")
            return

        output_dir = filedialog.askdirectory(
            title="Select output directory for validation results",
            initialdir=str(Path.cwd() / "outputs"),
        )
        if not output_dir:
            messagebox.showwarning("Cancelled", "Validation cancelled - no output directory selected")
            return

        self.output_dir = output_dir
        script = (
            "validate_ari_alarms_rain_gauges.py"
            if self.current_pipeline == "gauge"
            else "validate_ari_alarms_rain_radar.py"
        )
        title = "Step 4: Validate Alarms"
        args = ["--input", input_file, "--output", str(Path(output_dir) / "ari_alarm_validation.csv")]
        self.show_execution_window(title, script, args, self.on_validate_complete)

    def on_validate_complete(self, success):
        if success:
            messagebox.showinfo(
                "Success",
                f"✅ Validation complete!\n\nResults saved to:\n{self.output_dir}\n\nReady to visualize validation results.",
            )
            self.show_step_menu()
        else:
            messagebox.showerror("Error", "❌ Validation failed!\n\nCheck the logs for details.")

    def run_visualize_validation(self):
        input_file = filedialog.askopenfilename(
            title="Select validation results CSV",
            initialdir=str(Path.cwd() / "outputs"),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not input_file:
            messagebox.showwarning("Cancelled", "Visualization cancelled - no input file selected")
            return

        output_dir = filedialog.askdirectory(
            title="Select output directory for validation visualization",
            initialdir=str(Path.cwd() / "outputs"),
        )
        if not output_dir:
            messagebox.showwarning("Cancelled", "Visualization cancelled - no output directory selected")
            return

        self.output_dir = output_dir
        script = (
            "visualize_ari_alarms_rain_gauges.py"
            if self.current_pipeline == "gauge"
            else "visualize_ari_alarms_rain_radar.py"
        )
        title = "Step 5: Visualize Validation"
        args = ["--input", input_file, "--output", output_dir]
        self.show_execution_window(title, script, args, self.on_visualize_validation_complete)

    def on_visualize_validation_complete(self, success):
        if success:
            result = messagebox.askquestion(
                "Success",
                f"✅ Validation visualization complete!\n\nDashboard saved to:\n{self.output_dir}\n\nWould you like to open the dashboard?",
            )
            if result == "yes":
                self.open_validation_dashboard()

            messagebox.showinfo(
                "Pipeline Complete!",
                f"🎉 All steps completed!\n\n{'Rain Gauge' if self.current_pipeline == 'gauge' else 'Rain Radar'} pipeline finished successfully.",
            )
            self.show_step_menu()
        else:
            messagebox.showerror("Error", "❌ Validation visualization failed!\n\nCheck the logs for details.")

    def open_validation_dashboard(self):
        import webbrowser

        dashboard_path = Path(self.output_dir) / "validation_dashboard.html"
        if dashboard_path.exists():
            webbrowser.open(dashboard_path.as_uri())
        else:
            messagebox.showwarning("Not Found", f"Validation dashboard not found:\n{dashboard_path}")

    # ----------------------------
    # Execution window
    # ----------------------------
    def show_execution_window(self, title, script, args, callback):
        exec_window = tk.Toplevel(self.root)
        exec_window.title(title)

        exec_width = self.ui(900, 820, 1100)
        exec_height = self.ui(650, 560, 900)
        exec_window.geometry(f"{exec_width}x{exec_height}")
        exec_window.configure(bg=self.colors["bg_light"])

        header_height = self.ui(82, 72, 110)
        header = tk.Frame(exec_window, bg=self.colors["primary"], height=header_height)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        title_label = tk.Label(
            header,
            text=title,
            font=self.fonts["heading"],
            bg=self.colors["primary"],
            fg="white",
        )
        title_label.pack(pady=(self.ui(18, 14), self.ui(4, 2)))

        script_label = tk.Label(
            header,
            text=f"Running: {script}",
            font=self.fonts["body"],
            bg=self.colors["primary"],
            fg="#DCE7F3",
        )
        script_label.pack()

        progress_frame = tk.Frame(exec_window, bg=self.colors["surface"])
        progress_frame.pack(fill=tk.X, padx=self.pad_m, pady=self.pad_m)
        progress_frame.config(highlightbackground=self.colors["border"], highlightthickness=1)

        progress_width = exec_width - (self.pad_m * 4)
        progress = ttk.Progressbar(progress_frame, mode="indeterminate", length=progress_width)
        progress.pack(pady=self.pad_s)
        progress.start(8)

        output_frame = tk.LabelFrame(
            exec_window,
            text=" Console Output ",
            bg=self.colors["surface"],
            fg=self.colors["text_dark"],
            font=self.fonts["body"],
            relief="flat",
            bd=1,
        )
        output_frame.pack(fill=tk.BOTH, expand=True, padx=self.pad_m, pady=(0, self.pad_m))

        console_font_size = self.ui(9, 9, 12)
        output_text = scrolledtext.ScrolledText(
            output_frame,
            wrap=tk.WORD,
            font=("Consolas", console_font_size),
            bg="#0B1220",          # darker navy console (more premium than pure black)
            fg="#E5E7EB",
            insertbackground="white",
            relief="flat",
        )
        output_text.pack(fill=tk.BOTH, expand=True, padx=self.pad_s, pady=self.pad_s)

        status_frame = tk.Frame(exec_window, bg=self.colors["surface"])
        status_frame.pack(fill=tk.X, padx=self.pad_m, pady=(0, self.pad_m))
        status_frame.config(highlightbackground=self.colors["border"], highlightthickness=1)

        status_label = tk.Label(
            status_frame,
            text="⏳ Running...",
            font=self.fonts["body"],
            bg=self.colors["surface"],
            fg=self.colors["accent"],
        )
        status_label.pack(pady=self.pad_s)

        def run_script():
            try:
                if getattr(sys, "frozen", False):
                    base_dir = Path(sys.executable).parent
                    venv_python = base_dir / ".venv" / "Scripts" / "python.exe"
                    python_exe = str(venv_python) if venv_python.exists() else "python"
                    script_path = base_dir / script
                    if not script_path.exists():
                        script_path = Path(sys._MEIPASS) / script
                else:
                    python_exe = sys.executable
                    script_path = Path(script)

                cmd = [python_exe, str(script_path)] + args
                output_text.insert(tk.END, f"$ {' '.join(cmd)}\n{'='*80}\n\n")

                startupinfo = None
                creationflags = 0
                if sys.platform == "win32":
                    startupinfo = subprocess.STARTUPINFO()
                    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    creationflags = subprocess.CREATE_NO_WINDOW

                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    encoding="utf-8",
                    errors="replace",
                    startupinfo=startupinfo,
                    creationflags=creationflags,
                    cwd=str(Path(script_path).parent) if getattr(sys, "frozen", False) else None,
                )

                for line in process.stdout:
                    output_text.insert(tk.END, line)
                    output_text.see(tk.END)
                    exec_window.update()

                return_code = process.wait()
                progress.stop()

                if return_code == 0:
                    status_label.config(text="✅ Completed Successfully!", fg=self.colors["success"])
                    output_text.insert(tk.END, f"\n{'='*80}\n✅ SUCCESS - Exit code 0\n")
                    success = True
                else:
                    status_label.config(text=f"❌ Failed (Exit Code {return_code})", fg=self.colors["danger"])
                    output_text.insert(tk.END, f"\n{'='*80}\n❌ ERROR - Exit code {return_code}\n")
                    success = False

                close_btn = ttk.Button(
                    status_frame,
                    text="Close Window",
                    style="Modern.TButton",
                    command=lambda: [exec_window.destroy(), callback(success)],
                )
                close_btn.pack(pady=self.pad_m)

            except Exception as e:
                progress.stop()
                status_label.config(text=f"❌ Exception: {str(e)}", fg=self.colors["danger"])
                output_text.insert(tk.END, f"\n❌ EXCEPTION: {str(e)}\n")
                close_btn = ttk.Button(
                    status_frame,
                    text="Close Window",
                    style="Modern.TButton",
                    command=lambda: [exec_window.destroy(), callback(False)],
                )
                close_btn.pack(pady=self.pad_m)

        thread = threading.Thread(target=run_script, daemon=True)
        thread.start()


def main():
    root = tk.Tk()
    app = RainMonitoringGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
