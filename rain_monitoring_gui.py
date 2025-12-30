#!/usr/bin/env python3
"""
Rain Monitoring System GUI - Modern Edition (CustomTkinter)

Beautiful modern GUI for running rain gauge and rain radar pipelines.
Built with CustomTkinter for a professional, modern appearance.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-30
Version: 3.0.0 (CustomTkinter Modern UI)

Requirements:
    pip install customtkinter

Usage:
    python rain_monitoring_gui_modern.py
"""

from __future__ import annotations

import customtkinter as ctk
from tkinter import filedialog, messagebox
import subprocess
import threading
import sys
import os
import time
from pathlib import Path
from datetime import datetime

# Set appearance and theme
ctk.set_appearance_mode("dark")  # "light", "dark", or "system"
ctk.set_default_color_theme("blue")


class ModernApp(ctk.CTk):
    """Main application window."""
    
    def __init__(self):
        super().__init__()
        
        # Window configuration
        self.title("Auckland Council - Rain Monitoring System")
        self.geometry("1000x700")
        self.minsize(900, 650)
        
        # Theme state
        self.is_dark_mode = True
        
        # Color palettes
        self.light_colors = {
            "primary": "#1E3A5F",
            "primary_light": "#2D4A6F",
            "accent": "#0EA5E9",
            "success": "#10B981",
            "warning": "#F59E0B",
            "danger": "#EF4444",
            "surface": "#FFFFFF",
            "background": "#F8FAFC",
            "text": "#1E293B",
            "text_secondary": "#64748B",
            "border": "#E2E8F0",
            "header_subtitle": "#B8C5D6",
            # Pipeline colors
            "gauge": "#0EA5E9",
            "radar": "#8B5CF6",
            # Step colors - Professional muted palette
            "step1": "#475569",  # Slate
            "step2": "#475569",  # Slate
            "step3": "#475569",  # Slate
            "step4": "#64748B",  # Lighter slate (optional)
            "step5": "#64748B",  # Lighter slate (optional)
        }
        
        self.dark_colors = {
            "primary": "#0F172A",
            "primary_light": "#1E293B",
            "accent": "#38BDF8",
            "success": "#34D399",
            "warning": "#FBBF24",
            "danger": "#F87171",
            "surface": "#1E293B",
            "background": "#0F172A",
            "text": "#F1F5F9",
            "text_secondary": "#94A3B8",
            "border": "#334155",
            "header_subtitle": "#94A3B8",
            # Pipeline colors (brighter for dark mode)
            "gauge": "#38BDF8",
            "radar": "#A78BFA",
            # Step colors - Professional muted palette for dark mode
            "step1": "#64748B",  # Slate
            "step2": "#64748B",  # Slate
            "step3": "#64748B",  # Slate
            "step4": "#475569",  # Darker slate (optional)
            "step5": "#475569",  # Darker slate (optional)
        }
        
        # Start with dark mode
        self.colors = self.dark_colors.copy()
        
        # State
        self.current_pipeline = None
        self.output_dir = None
        
        # Configure grid
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Show main menu
        self.show_main_menu()
    
    def toggle_theme(self):
        """Toggle between dark and light mode."""
        self.is_dark_mode = not self.is_dark_mode
        
        if self.is_dark_mode:
            self.colors = self.dark_colors.copy()
            ctk.set_appearance_mode("dark")
        else:
            self.colors = self.light_colors.copy()
            ctk.set_appearance_mode("light")
        
        # Refresh current view
        if self.current_pipeline:
            self.show_pipeline_steps()
        else:
            self.show_main_menu()
    
    def clear_window(self):
        """Clear all widgets from window."""
        for widget in self.winfo_children():
            widget.destroy()
    
    def _add_background_decoration(self, parent):
        """Add subtle decorative elements to background."""
        # Get decoration colors based on theme
        if self.is_dark_mode:
            deco_color1 = "#1a2536"  # Subtle lighter than background
            deco_color2 = "#151d2b"  # Very subtle
        else:
            deco_color1 = "#e8eef4"
            deco_color2 = "#dde5ed"
        
        # Top-right large circle
        deco1 = ctk.CTkFrame(
            parent,
            width=350,
            height=350,
            corner_radius=175,
            fg_color=deco_color1,
            bg_color=self.colors["background"]
        )
        deco1.place(x=-50, y=-80, anchor="nw")
        
        # Bottom-right circle
        deco2 = ctk.CTkFrame(
            parent,
            width=250,
            height=250,
            corner_radius=125,
            fg_color=deco_color2,
            bg_color=self.colors["background"]
        )
        deco2.place(relx=1.05, rely=0.7, anchor="e")
        
        # Bottom-left circle  
        deco3 = ctk.CTkFrame(
            parent,
            width=200,
            height=200,
            corner_radius=100,
            fg_color=deco_color1,
            bg_color=self.colors["background"]
        )
        deco3.place(relx=-0.02, rely=1.02, anchor="sw")
    
    def show_main_menu(self):
        """Display the main menu."""
        self.clear_window()
        
        # Main container
        main_frame = ctk.CTkFrame(self, fg_color=self.colors["background"], corner_radius=0)
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        
        # Decorative background elements
        self._add_background_decoration(main_frame)
        
        # Header - more compact
        header = ctk.CTkFrame(main_frame, fg_color=self.colors["primary"], corner_radius=0, height=80)
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False)
        
        title_label = ctk.CTkLabel(
            header,
            text="🌧️  MOATA-INGEST",
            font=ctk.CTkFont(size=24, weight="bold"),
            text_color="white"
        )
        title_label.place(relx=0.5, rely=0.4, anchor="center")
        
        subtitle_label = ctk.CTkLabel(
            header,
            text="Auckland Council  •  Healthy Waters & Flood Resilience",
            font=ctk.CTkFont(size=12),
            text_color=self.colors["header_subtitle"]
        )
        subtitle_label.place(relx=0.5, rely=0.72, anchor="center")
        
        # Theme toggle button
        toggle_icon = "🌙" if not self.is_dark_mode else "☀️"
        toggle_text = "Dark" if not self.is_dark_mode else "Light"
        
        theme_toggle = ctk.CTkButton(
            header,
            text=f"{toggle_icon}  {toggle_text}",
            font=ctk.CTkFont(size=12),
            fg_color="transparent",
            hover_color=self.colors["primary_light"],
            text_color="white",
            corner_radius=8,
            width=90,
            height=32,
            command=self.toggle_theme
        )
        theme_toggle.place(relx=0.96, rely=0.5, anchor="e")
        
        # Content area - reduced padding
        content = ctk.CTkFrame(main_frame, fg_color="transparent")
        content.grid(row=1, column=0, sticky="nsew", padx=40, pady=20)
        content.grid_columnconfigure((0, 1), weight=1)
        content.grid_rowconfigure(1, weight=1)
        
        # Welcome message - more compact
        welcome_frame = ctk.CTkFrame(content, fg_color=self.colors["surface"], corner_radius=10)
        welcome_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 15))
        
        welcome_title = ctk.CTkLabel(
            welcome_frame,
            text="Select a Pipeline to Begin",
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color=self.colors["text"]
        )
        welcome_title.pack(pady=(15, 5))
        
        welcome_desc = ctk.CTkLabel(
            welcome_frame,
            text="Choose between point-based gauge monitoring or spatial radar analysis.",
            font=ctk.CTkFont(size=12),
            text_color=self.colors["text_secondary"]
        )
        welcome_desc.pack(pady=(0, 15))
        
        # Pipeline cards container
        cards_frame = ctk.CTkFrame(content, fg_color="transparent")
        cards_frame.grid(row=1, column=0, columnspan=2, sticky="nsew")
        cards_frame.grid_columnconfigure((0, 1), weight=1)
        cards_frame.grid_rowconfigure(0, weight=1)
        
        # Rain Gauge Card
        self.create_pipeline_card(
            cards_frame,
            column=0,
            icon="🌊",
            title="Rain Gauge Pipeline",
            subtitle="Point-Based Monitoring",
            features=["Retrieve rain gauges data", "Alarm configuration", "ARI alarm validation", "Dashboards"],
            color=self.colors["gauge"],
            command=lambda: self.start_pipeline("gauge")
        )
        
        # Rain Radar Card
        self.create_pipeline_card(
            cards_frame,
            column=1,
            icon="📡",
            title="Rain Radar Pipeline",
            subtitle="Spatial Coverage",
            features=["Pixel-level rainfall data", "ARI calcuation", "Alarm validation", "Dashboards"],
            color=self.colors["radar"],
            command=lambda: self.start_pipeline("radar")
        )
        
        # Footer - more compact
        footer = ctk.CTkFrame(main_frame, fg_color=self.colors["border"], corner_radius=0, height=32)
        footer.grid(row=2, column=0, sticky="ew")
        footer.grid_propagate(False)
        
        footer_text = ctk.CTkLabel(
            footer,
            text=f"Version 3.0.0  •  {datetime.now().strftime('%Y-%m-%d')}  •  COMPSCI 778 Internship",
            font=ctk.CTkFont(size=10),
            text_color=self.colors["text_secondary"]
        )
        footer_text.place(relx=0.5, rely=0.5, anchor="center")
    
    def create_pipeline_card(self, parent, column, icon, title, subtitle, features, color, command):
        """Create a pipeline selection card."""
        card = ctk.CTkFrame(parent, fg_color=self.colors["surface"], corner_radius=12)
        card.grid(row=0, column=column, sticky="nsew", padx=10, pady=5)
        
        # Color strip at top
        strip = ctk.CTkFrame(card, fg_color=color, corner_radius=0, height=4)
        strip.pack(fill="x")
        
        # Content - more compact padding
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
            text_color=self.colors["text"],
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
        
        # Features list - compact
        features_frame = ctk.CTkFrame(content, fg_color="transparent")
        features_frame.pack(fill="x", pady=(5, 10))
        
        for feature in features:
            feature_label = ctk.CTkLabel(
                features_frame,
                text=f"✓  {feature}",
                font=ctk.CTkFont(size=12),
                text_color=self.colors["text_secondary"],
                anchor="w"
            )
            feature_label.pack(anchor="w", pady=1)
        
        # Start button - smaller
        start_btn = ctk.CTkButton(
            content,
            text="Start Pipeline  →",
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color=color,
            hover_color=self._darken_color(color),
            corner_radius=8,
            height=38,
            command=command
        )
        start_btn.pack(fill="x", pady=(10, 5))
    
    def start_pipeline(self, pipeline_type):
        """Start a pipeline."""
        self.current_pipeline = pipeline_type
        self.show_pipeline_steps()
    
    def show_pipeline_steps(self):
        """Display the pipeline steps screen."""
        self.clear_window()
        
        pipeline_name = "Rain Gauge" if self.current_pipeline == "gauge" else "Rain Radar"
        pipeline_icon = "🌊" if self.current_pipeline == "gauge" else "📡"
        pipeline_color = self.colors["gauge"] if self.current_pipeline == "gauge" else self.colors["radar"]
        
        # Main container
        main_frame = ctk.CTkFrame(self, fg_color=self.colors["background"], corner_radius=0)
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        
        # Decorative background elements
        self._add_background_decoration(main_frame)
        
        # Header
        header = ctk.CTkFrame(main_frame, fg_color=pipeline_color, corner_radius=0, height=90)
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False)
        
        title_label = ctk.CTkLabel(
            header,
            text=f"{pipeline_icon}  {pipeline_name} Pipeline",
            font=ctk.CTkFont(size=26, weight="bold"),
            text_color="white"
        )
        title_label.place(relx=0.5, rely=0.5, anchor="center")
        
        # Theme toggle button in pipeline header
        toggle_icon = "🌙" if not self.is_dark_mode else "☀️"
        toggle_text = "Dark" if not self.is_dark_mode else "Light"
        
        theme_toggle = ctk.CTkButton(
            header,
            text=f"{toggle_icon}  {toggle_text}",
            font=ctk.CTkFont(size=12),
            fg_color="transparent",
            hover_color=self._darken_color(pipeline_color),
            text_color="white",
            corner_radius=8,
            width=90,
            height=32,
            command=self.toggle_theme
        )
        theme_toggle.place(relx=0.96, rely=0.5, anchor="e")
        
        # Content
        content = ctk.CTkScrollableFrame(main_frame, fg_color="transparent")
        content.grid(row=1, column=0, sticky="nsew", padx=40, pady=20)
        
        # Steps card
        steps_card = ctk.CTkFrame(content, fg_color=self.colors["surface"], corner_radius=16)
        steps_card.pack(fill="x", pady=(0, 15))
        
        card_title = ctk.CTkLabel(
            steps_card,
            text="Pipeline Steps",
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color=self.colors["text"]
        )
        card_title.pack(anchor="w", padx=25, pady=(20, 15))
        
        # Define steps
        steps = [
            ("1", "Retrieve Data", "Collect data from Moata API", self.run_retrieve, self.colors["step1"]),
            ("2", "Analyze Data", "Filter gauges and calculate ARI", self.run_analyze, self.colors["step2"]),
            ("3", "Visualize Results", "Generate HTML dashboards", self.run_visualize, self.colors["step3"]),
            ("4", "Validate Alarms", "Compare with historical data (Optional)", self.run_validate, self.colors["step4"]),
            ("5", "Visualize Validation", "Create validation dashboard (Optional)", self.run_visualize_validation, self.colors["step5"]),
        ]
        
        for i, (number, name, desc, command, color) in enumerate(steps):
            self.create_step_row(steps_card, number, name, desc, command, color, is_last=(i == len(steps) - 1))
        
        # Back button
        back_btn = ctk.CTkButton(
            content,
            text="←  Back to Main Menu",
            font=ctk.CTkFont(size=13),
            fg_color="transparent",
            hover_color=self.colors["border"],
            text_color=pipeline_color,
            corner_radius=8,
            height=40,
            command=self.show_main_menu
        )
        back_btn.pack(anchor="w", pady=(10, 0))
    
    def create_step_row(self, parent, number, name, desc, command, color, is_last=False):
        """Create a step row."""
        row_frame = ctk.CTkFrame(parent, fg_color="transparent")
        row_frame.pack(fill="x", padx=20, pady=10)
        
        # Number badge
        badge = ctk.CTkFrame(row_frame, fg_color=color, corner_radius=25, width=50, height=50)
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
            text_color=self.colors["text"],
            anchor="w"
        )
        name_label.pack(anchor="w")
        
        desc_label = ctk.CTkLabel(
            info_frame,
            text=desc,
            font=ctk.CTkFont(size=12),
            text_color=self.colors["text_secondary"],
            anchor="w"
        )
        desc_label.pack(anchor="w")
        
        # Run button
        run_btn = ctk.CTkButton(
            row_frame,
            text="Run",
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color=color,
            hover_color=self._darken_color(color),
            corner_radius=8,
            width=80,
            height=36,
            command=command
        )
        run_btn.pack(side="right", padx=(10, 5))
        
        # Separator
        if not is_last:
            separator = ctk.CTkFrame(parent, fg_color=self.colors["border"], height=1)
            separator.pack(fill="x", padx=40, pady=5)
    
    def run_retrieve(self):
        """Run retrieve step."""
        if self.current_pipeline == "gauge":
            script = "retrieve_rain_gauges.py"
            self.show_execution_window("Step 1: Retrieve Data", script, [], self.on_retrieve_complete)
        else:
            self.show_date_selection_dialog(
                title="Select Data to Retrieve",
                options=[
                    ("📅  Last 24 Hours (Current)", "current", "Retrieve radar data from past 24 hours"),
                    ("📆  Specific Historical Date", "date", "Retrieve radar data for a specific 24h period"),
                ],
                callback=self._handle_retrieve_selection
            )
    
    def _handle_retrieve_selection(self, selection):
        """Handle retrieve date selection."""
        if not selection:
            return
        
        script = "retrieve_rain_radar.py"
        args = []
        
        if selection == "date":
            date_str = ctk.CTkInputDialog(
                text="Enter date in YYYY-MM-DD format:",
                title="Enter Date"
            ).get_input()
            if not date_str:
                return
            args = ["--date", date_str]
        
        self.show_execution_window("Step 1: Retrieve Data", script, args, self.on_retrieve_complete)
    
    def on_retrieve_complete(self, success):
        """Handle retrieve completion."""
        if success:
            messagebox.showinfo("Success", "✅ Data collection complete!\n\nReady to proceed to Analysis.")
        else:
            messagebox.showerror("Error", "❌ Data collection failed!\n\nCheck the logs for details.")
        self.show_pipeline_steps()
    
    def run_analyze(self):
        """Run analyze step."""
        self.show_date_selection_dialog(
            title="Select Data to Analyze",
            options=[
                ("🔍  Auto-Detect Most Recent", "auto", "Automatically find and analyze the latest data"),
                ("📅  Current Data (Last 24h)", "current", "Analyze data from most recent retrieve"),
                ("📆  Specific Historical Date", "date", "Choose a specific date to analyze"),
            ],
            callback=self._handle_analyze_selection
        )
    
    def _handle_analyze_selection(self, selection):
        """Handle analyze date selection."""
        if not selection:
            return
        
        script = "analyze_rain_gauges.py" if self.current_pipeline == "gauge" else "analyze_rain_radar.py"
        args = []
        
        if selection == "current":
            args = ["--current"]
        elif selection == "date":
            date_str = ctk.CTkInputDialog(
                text="Enter date in YYYY-MM-DD format:",
                title="Enter Date"
            ).get_input()
            if not date_str:
                return
            args = ["--date", date_str]
        
        self.show_execution_window("Step 2: Analyze Data", script, args, self.on_analyze_complete)
    
    def on_analyze_complete(self, success):
        """Handle analyze completion."""
        if success:
            if self.current_pipeline == "gauge":
                self.output_dir = str(Path.cwd() / "outputs" / "rain_gauges" / "analyzed")
            else:
                self.output_dir = str(Path.cwd() / "outputs" / "rain_radar" / "analyzed")
            messagebox.showinfo("Success", f"✅ Analysis complete!\n\nResults saved to:\n{self.output_dir}")
        else:
            messagebox.showerror("Error", "❌ Analysis failed!\n\nCheck the logs for details.")
        self.show_pipeline_steps()
    
    def run_visualize(self):
        """Run visualize step."""
        result = messagebox.askokcancel(
            "Auto-Detection",
            "The visualize script will automatically:\n\n"
            "• Find the latest analyzed JSON\n"
            "• Generate HTML dashboard\n"
            "• Save to outputs folder\n\n"
            "Continue?"
        )
        
        if not result:
            return
        
        script = "visualize_rain_gauges.py" if self.current_pipeline == "gauge" else "visualize_rain_radar.py"
        self.show_execution_window("Step 3: Visualize Results", script, [], self.on_visualize_complete)
    
    def on_visualize_complete(self, success):
        """Handle visualize completion."""
        if success:
            # Find output directory
            if self.current_pipeline == "gauge":
                base_dir = Path.cwd() / "outputs" / "rain_gauges" / "visualizations"
            else:
                base_dir = Path.cwd() / "outputs" / "rain_radar" / "visualizations"
            
            html_files = []
            if base_dir.exists():
                html_files = list(base_dir.glob("**/*.html"))
            
            if html_files:
                most_recent = max(html_files, key=lambda p: p.stat().st_mtime)
                self.output_dir = str(most_recent.parent)
            else:
                self.output_dir = str(base_dir)
            
            result = messagebox.askyesno(
                "Success",
                f"✅ Visualization complete!\n\nDashboard saved to:\n{self.output_dir}\n\nOpen dashboard now?"
            )
            if result:
                self.open_dashboard()
        else:
            messagebox.showerror("Error", "❌ Visualization failed!\n\nCheck the logs for details.")
        self.show_pipeline_steps()
    
    def open_dashboard(self):
        """Open the generated dashboard."""
        import webbrowser
        
        dashboard_dir = Path(self.output_dir)
        html_files = list(dashboard_dir.glob("**/*.html"))
        
        if html_files:
            dashboard_path = max(html_files, key=lambda p: p.stat().st_mtime)
            webbrowser.open(dashboard_path.as_uri())
        else:
            messagebox.showwarning("Not Found", f"No dashboard HTML files found in:\n{dashboard_dir}")
    
    def run_validate(self):
        """Run validate step."""
        input_file = filedialog.askopenfilename(
            title="Select historical alarm events CSV",
            initialdir=str(Path.cwd() / "data" / "inputs"),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not input_file:
            return
        
        output_dir = filedialog.askdirectory(
            title="Select output directory for validation results",
            initialdir=str(Path.cwd() / "outputs"),
        )
        if not output_dir:
            return
        
        self.output_dir = output_dir
        script = "validate_ari_alarms_rain_gauges.py" if self.current_pipeline == "gauge" else "validate_ari_alarms_rain_radar.py"
        args = ["--input", input_file, "--output", str(Path(output_dir) / "ari_alarm_validation.csv")]
        self.show_execution_window("Step 4: Validate Alarms", script, args, self.on_validate_complete)
    
    def on_validate_complete(self, success):
        """Handle validate completion."""
        if success:
            messagebox.showinfo("Success", f"✅ Validation complete!\n\nResults saved to:\n{self.output_dir}")
        else:
            messagebox.showerror("Error", "❌ Validation failed!\n\nCheck the logs for details.")
        self.show_pipeline_steps()
    
    def run_visualize_validation(self):
        """Run visualize validation step."""
        input_file = filedialog.askopenfilename(
            title="Select validation results CSV",
            initialdir=str(Path.cwd() / "outputs"),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not input_file:
            return
        
        output_dir = filedialog.askdirectory(
            title="Select output directory for validation visualization",
            initialdir=str(Path.cwd() / "outputs"),
        )
        if not output_dir:
            return
        
        self.output_dir = output_dir
        script = "visualize_ari_alarms_rain_gauges.py" if self.current_pipeline == "gauge" else "visualize_ari_alarms_rain_radar.py"
        args = ["--input", input_file, "--output", output_dir]
        self.show_execution_window("Step 5: Visualize Validation", script, args, self.on_visualize_validation_complete)
    
    def on_visualize_validation_complete(self, success):
        """Handle visualize validation completion."""
        if success:
            result = messagebox.askyesno(
                "Success",
                f"✅ Validation visualization complete!\n\nDashboard saved to:\n{self.output_dir}\n\nOpen dashboard now?"
            )
            if result:
                import webbrowser
                dashboard_path = Path(self.output_dir) / "validation_dashboard.html"
                if dashboard_path.exists():
                    webbrowser.open(dashboard_path.as_uri())
            
            messagebox.showinfo("Pipeline Complete!", f"🎉 All steps completed!\n\n{self.current_pipeline.title()} pipeline finished successfully.")
        else:
            messagebox.showerror("Error", "❌ Validation visualization failed!\n\nCheck the logs for details.")
        self.show_pipeline_steps()
    
    def show_date_selection_dialog(self, title, options, callback):
        """Show a date/option selection dialog."""
        dialog = ctk.CTkToplevel(self)
        dialog.title(title)
        dialog.geometry("500x420")
        dialog.transient(self)
        dialog.grab_set()
        dialog.configure(fg_color=self.colors["background"])
        
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
            text_color=self.colors["text"]
        )
        title_label.pack(pady=(25, 5))
        
        # Subtitle
        subtitle_label = ctk.CTkLabel(
            dialog,
            text="Choose an option below",
            font=ctk.CTkFont(size=12),
            text_color=self.colors["text_secondary"]
        )
        subtitle_label.pack(pady=(0, 20))
        
        # Selection variable
        selection = {"value": None}
        
        # Options container
        options_frame = ctk.CTkFrame(dialog, fg_color="transparent")
        options_frame.pack(fill="both", expand=True, padx=25)
        
        # Option buttons
        for btn_text, btn_value, btn_desc in options:
            btn_frame = ctk.CTkFrame(options_frame, fg_color=self.colors["surface"], corner_radius=10)
            btn_frame.pack(fill="x", pady=6)
            
            # Make the frame clickable
            def make_click_handler(v):
                return lambda e: [selection.update({"value": v}), dialog.destroy()]
            
            btn_frame.bind("<Button-1>", make_click_handler(btn_value))
            btn_frame.configure(cursor="hand2")
            
            # Content inside frame - using grid for better alignment
            content_frame = ctk.CTkFrame(btn_frame, fg_color="transparent")
            content_frame.pack(fill="x", padx=18, pady=14)
            content_frame.bind("<Button-1>", make_click_handler(btn_value))
            
            # Icon and text container
            text_container = ctk.CTkFrame(content_frame, fg_color="transparent")
            text_container.pack(fill="x")
            text_container.bind("<Button-1>", make_click_handler(btn_value))
            
            # Title - bold, left aligned
            btn_title = ctk.CTkLabel(
                text_container,
                text=btn_text,
                font=ctk.CTkFont(size=14, weight="bold"),
                text_color=self.colors["text"],
                anchor="w"
            )
            btn_title.pack(fill="x")
            btn_title.bind("<Button-1>", make_click_handler(btn_value))
            
            # Description - smaller, muted color, left aligned
            btn_desc_label = ctk.CTkLabel(
                text_container,
                text=btn_desc,
                font=ctk.CTkFont(size=12),
                text_color=self.colors["text_secondary"],
                anchor="w"
            )
            btn_desc_label.pack(fill="x", pady=(4, 0))
            btn_desc_label.bind("<Button-1>", make_click_handler(btn_value))
            
            # Hover effect
            def make_hover_enter(frame):
                return lambda e: frame.configure(fg_color=self.colors["border"])
            def make_hover_leave(frame):
                return lambda e: frame.configure(fg_color=self.colors["surface"])
            
            btn_frame.bind("<Enter>", make_hover_enter(btn_frame))
            btn_frame.bind("<Leave>", make_hover_leave(btn_frame))
        
        # Cancel button
        cancel_btn = ctk.CTkButton(
            dialog,
            text="Cancel",
            font=ctk.CTkFont(size=13),
            fg_color="transparent",
            hover_color=self.colors["border"],
            text_color=self.colors["text_secondary"],
            corner_radius=8,
            height=35,
            command=dialog.destroy
        )
        cancel_btn.pack(pady=15)
        
        # Wait for dialog
        self.wait_window(dialog)
        
        # Call callback with selection
        callback(selection["value"])
    
    def show_execution_window(self, title, script, args, callback):
        """Show script execution window."""
        exec_window = ctk.CTkToplevel(self)
        exec_window.title(title)
        exec_window.geometry("900x600")
        exec_window.transient(self)
        
        # Header
        header = ctk.CTkFrame(exec_window, fg_color=self.colors["primary"], corner_radius=0, height=70)
        header.pack(fill="x")
        header.pack_propagate(False)
        
        title_label = ctk.CTkLabel(
            header,
            text=title,
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color="white"
        )
        title_label.pack(pady=(15, 5))
        
        script_label = ctk.CTkLabel(
            header,
            text=f"Running: {script}",
            font=ctk.CTkFont(size=12),
            text_color="#B8C5D6"
        )
        script_label.pack()
        
        # Progress bar
        progress = ctk.CTkProgressBar(exec_window, mode="indeterminate", height=6)
        progress.pack(fill="x", padx=20, pady=15)
        progress.start()
        
        # Console output
        console_frame = ctk.CTkFrame(exec_window, fg_color=self.colors["surface"], corner_radius=10)
        console_frame.pack(fill="both", expand=True, padx=20, pady=(0, 15))
        
        console_label = ctk.CTkLabel(
            console_frame,
            text="Console Output",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=self.colors["text_secondary"]
        )
        console_label.pack(anchor="w", padx=15, pady=(10, 5))
        
        console_text = ctk.CTkTextbox(
            console_frame,
            font=ctk.CTkFont(family="Consolas", size=11),
            fg_color="#1E293B",
            text_color="#E2E8F0",
            corner_radius=8
        )
        console_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Status bar
        status_frame = ctk.CTkFrame(exec_window, fg_color=self.colors["surface"], corner_radius=10, height=50)
        status_frame.pack(fill="x", padx=20, pady=(0, 15))
        status_frame.pack_propagate(False)
        
        status_label = ctk.CTkLabel(
            status_frame,
            text="⏳ Running...",
            font=ctk.CTkFont(size=13),
            text_color=self.colors["accent"]
        )
        status_label.pack(side="left", padx=15, pady=10)
        
        # Run script in thread
        def run_script():
            try:
                python_exe = sys.executable
                script_path = Path(script)
                
                cmd = [python_exe, "-u", str(script_path)] + args
                console_text.insert("end", f"$ {' '.join(cmd)}\n{'='*70}\n\n")
                console_text.see("end")
                
                env = os.environ.copy()
                env['PYTHONIOENCODING'] = 'utf-8'
                env['PYTHONUNBUFFERED'] = '1'
                
                startupinfo = None
                creationflags = 0
                if sys.platform == "win32":
                    startupinfo = subprocess.STARTUPINFO()
                    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    creationflags = subprocess.CREATE_NO_WINDOW
                
                process = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=env,
                    startupinfo=startupinfo,
                    creationflags=creationflags,
                )
                
                # Set timeout based on script type
                if "retrieve" in script.lower():
                    timeout_seconds = 3600
                elif "radar" in script.lower():
                    timeout_seconds = 1800
                else:
                    timeout_seconds = 300
                
                start_time = time.time()
                
                # Read output in real-time
                while True:
                    ret = process.poll()
                    
                    if process.stdout:
                        try:
                            line = process.stdout.readline()
                            if line:
                                if not any(x in line for x in ["UnicodeEncodeError", "charmap_encode"]):
                                    console_text.insert("end", line)
                                    console_text.see("end")
                        except:
                            pass
                    
                    elapsed = int(time.time() - start_time)
                    try:
                        status_label.configure(text=f"⏳ Running... ({elapsed}s elapsed)")
                    except:
                        process.kill()
                        return
                    
                    if elapsed > timeout_seconds:
                        process.kill()
                        status_label.configure(text=f"⏱️ Timeout after {timeout_seconds}s", text_color=self.colors["danger"])
                        console_text.insert("end", f"\n{'='*70}\n⏱️ TIMEOUT\n")
                        break
                    
                    if ret is not None:
                        try:
                            remaining = process.stdout.read()
                            if remaining:
                                console_text.insert("end", remaining)
                        except:
                            pass
                        break
                    
                    time.sleep(0.1)
                
                progress.stop()
                return_code = process.returncode if process.returncode is not None else -1
                
                # Read stderr
                stderr_data = process.stderr.read() if process.stderr else ""
                if stderr_data:
                    # Filter unicode errors
                    filtered_lines = [l for l in stderr_data.split('\n') 
                                     if not any(x in l for x in ["charmap_encode", "UnicodeEncodeError"])]
                    if filtered_lines:
                        console_text.insert("end", "\n=== Errors ===\n")
                        console_text.insert("end", "\n".join(filtered_lines))
                
                if return_code == 0:
                    status_label.configure(text="✅ Completed Successfully!", text_color=self.colors["success"])
                    console_text.insert("end", f"\n{'='*70}\n✅ SUCCESS\n")
                    success = True
                else:
                    status_label.configure(text=f"❌ Failed (Exit Code {return_code})", text_color=self.colors["danger"])
                    console_text.insert("end", f"\n{'='*70}\n❌ ERROR - Exit code {return_code}\n")
                    success = False
                
                console_text.see("end")
                
                # Add close button
                close_btn = ctk.CTkButton(
                    status_frame,
                    text="Close",
                    font=ctk.CTkFont(size=13, weight="bold"),
                    fg_color=self.colors["primary"],
                    corner_radius=8,
                    width=100,
                    command=lambda: [exec_window.destroy(), callback(success)]
                )
                close_btn.pack(side="right", padx=15, pady=8)
                
            except Exception as e:
                progress.stop()
                status_label.configure(text=f"❌ Exception: {str(e)}", text_color=self.colors["danger"])
                console_text.insert("end", f"\n❌ EXCEPTION: {str(e)}\n")
                
                close_btn = ctk.CTkButton(
                    status_frame,
                    text="Close",
                    font=ctk.CTkFont(size=13, weight="bold"),
                    fg_color=self.colors["primary"],
                    corner_radius=8,
                    width=100,
                    command=lambda: [exec_window.destroy(), callback(False)]
                )
                close_btn.pack(side="right", padx=15, pady=8)
        
        thread = threading.Thread(target=run_script, daemon=True)
        thread.start()
    
    def _darken_color(self, hex_color, factor=0.85):
        """Darken a hex color."""
        hex_color = hex_color.lstrip('#')
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        rgb = tuple(max(0, int(c * factor)) for c in rgb)
        return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"


def main():
    app = ModernApp()
    app.mainloop()


if __name__ == "__main__":
    main()