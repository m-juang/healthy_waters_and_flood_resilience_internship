"""
Rain Gauge Pipeline Module

Implements the rain gauge data processing pipeline.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-14
Version: 1.2.0 - Added robust browser opening and updated validation dashboard method
"""

from __future__ import annotations

import customtkinter as ctk
from tkinter import filedialog, messagebox
from pathlib import Path
from typing import Callable, List, Tuple, TYPE_CHECKING

from .base import BasePipeline
from ..components import show_date_selection_dialog

if TYPE_CHECKING:
    from ..main import ModernApp


class GaugePipeline(BasePipeline):
    """
    Rain Gauge Pipeline implementation.
    
    Steps:
    1. Retrieve - Collect gauge metadata & alarm configs
    2. Analyze - Filter active gauges and generate summary
    3. Visualize - Generate HTML dashboard
    4. Validate - Fetch timeseries & validate alarms
    5. Visualize Validation - Create validation dashboard
    """
    
    @property
    def name(self) -> str:
        return "Rain Gauge"
    
    @property
    def icon(self) -> str:
        return "💧"
    
    @property
    def color_key(self) -> str:
        return "gauge"
    
    @property
    def subtitle(self) -> str:
        return "Point-Based Monitoring"
    
    @property
    def features(self) -> List[str]:
        return [
            "Retrieve rain gauges data",
            "Alarm configuration",
            "ARI alarm validation",
            "Dashboards"
        ]
    
    def get_steps(self) -> List[Tuple[str, str, str, Callable, str]]:
        """Get rain gauge pipeline steps."""
        return [
            ("1", "Retrieve Data", "Collect gauge metadata & alarm configs from Moata API",
             self.run_retrieve, self.app.colors["step1"]),
            ("2", "Analyze Data", "Filter active gauges and generate summary",
             self.run_analyze, self.app.colors["step2"]),
            ("3", "Visualize Results", "Generate HTML dashboard from analysis",
             self.run_visualize, self.app.colors["step3"]),
            ("4", "Validate Alarms", "Fetch timeseries data & validate historical alarms",
             self.run_validate, self.app.colors["step4"]),
            ("5", "Visualize Validation", "Create validation dashboard (Optional)",
             self.run_visualize_validation, self.app.colors["step5"]),
        ]
    
    # =========================================================================
    # Step Implementations
    # =========================================================================
    
    def run_retrieve(self) -> None:
        """Run retrieve step with date selection."""
        # If dates were pre-filled from CLI, use them directly
        if self.initial_start_time and self.initial_end_time:
            duration_days = (self.initial_end_time - self.initial_start_time).days
            if duration_days == 1:
                # Single date
                self._run_retrieve_with_date(
                    self.initial_start_time.strftime('%Y-%m-%d')
                )
            else:
                # Date range
                self._run_retrieve_with_range(
                    self.initial_start_time.strftime('%Y-%m-%d'),
                    self.initial_end_time.strftime('%Y-%m-%d')
                )
            return
        
        # Otherwise show date selection dialog
        selection = show_date_selection_dialog(
            self.app,
            title="Select Data to Retrieve",
            options=[
                ("📅  Current (Real-time Last 24h)", "current",
                 "Retrieve gauge data from past 24 hours"),
                ("📆  Specific Historical Date", "date",
                 "Retrieve gauge data for a specific 24h period"),
                ("📊  Date Range", "range",
                 "Retrieve gauge data for multiple days"),
            ],
            colors=self.app.colors,
        )
        
        if not selection:
            return
        
        script = "scripts/gauge/retrieve.py"
        args = []
        
        if selection == "date":
            date_str = ctk.CTkInputDialog(
                text="Enter date in YYYY-MM-DD format:",
                title="Enter Date"
            ).get_input()
            if not date_str:
                return
            self._run_retrieve_with_date(date_str)
            return
        
        elif selection == "range":
            start_str = ctk.CTkInputDialog(
                text="Enter START date in YYYY-MM-DD format:",
                title="Enter Start Date"
            ).get_input()
            if not start_str:
                return
            
            end_str = ctk.CTkInputDialog(
                text="Enter END date in YYYY-MM-DD format:",
                title="Enter End Date"
            ).get_input()
            if not end_str:
                return
            
            self._run_retrieve_with_range(start_str, end_str)
            return
        
        # selection == "current" - no args (default: last 24h)
        self.app.executor.execute(
            "Step 1: Retrieve Data",
            script,
            args,
            self._on_retrieve_complete
        )
    
    def _run_retrieve_with_date(self, date_str: str) -> None:
        """Run retrieve with specified single date."""
        script = "scripts/gauge/retrieve.py"
        args = ["--date", date_str]
        self.app.selected_date = date_str
        
        self.app.executor.execute(
            "Step 1: Retrieve Data",
            script,
            args,
            self._on_retrieve_complete
        )
    
    def _run_retrieve_with_range(self, start_str: str, end_str: str) -> None:
        """Run retrieve with specified date range."""
        script = "scripts/gauge/retrieve.py"
        args = ["--start", start_str, "--end", end_str]
        self.app.selected_date = f"{start_str} to {end_str}"
        
        self.app.executor.execute(
            "Step 1: Retrieve Data",
            script,
            args,
            self._on_retrieve_complete
        )
    
    def _on_retrieve_complete(self, success: bool) -> None:
        """Handle retrieve completion."""
        if success:
            messagebox.showinfo(
                "Success",
                "✅ Data collection complete!\n\nReady to proceed to Analysis."
            )
        else:
            messagebox.showerror(
                "Error",
                "❌ Data collection failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def run_analyze(self) -> None:
        """Run analyze step with date selection."""
        selection = show_date_selection_dialog(
            self.app,
            title="Select Data to Analyze",
            options=[
                ("🔍  Auto-Detect Most Recent", "auto",
                 "Automatically find the latest data (prefers historical)"),
                ("📅  Current (Last 24h)", "current",
                 "Analyze real-time data from outputs/rain_gauges/raw/"),
                ("📆  Specific Historical Date", "date",
                 "Choose a specific date to analyze"),
            ],
            colors=self.app.colors,
        )
        
        if not selection:
            return
        
        script = "scripts/gauge/analyze.py"
        args = []
        
        if selection == "current":
            args = ["--current"]
            self.app.selected_date = None
        elif selection == "date":
            date_str = ctk.CTkInputDialog(
                text="Enter date in YYYY-MM-DD format:",
                title="Enter Date"
            ).get_input()
            if not date_str:
                return
            args = ["--date", date_str]
            self.app.selected_date = date_str
        else:
            self.app.selected_date = None
        
        self.app.executor.execute(
            "Step 2: Analyze Data",
            script,
            args,
            self._on_analyze_complete
        )
    
    def _on_analyze_complete(self, success: bool) -> None:
        """Handle analyze completion."""
        if success:
            output_dir = str(self.paths.rain_gauges_analyze_dir)
            messagebox.showinfo(
                "Success",
                f"✅ Analysis complete!\n\nResults saved to:\n{output_dir}"
            )
        else:
            messagebox.showerror(
                "Error",
                "❌ Analysis failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def run_visualize(self) -> None:
        """Run visualize step with date selection."""
        selection = show_date_selection_dialog(
            self.app,
            title="Select Data to Visualize",
            options=[
                ("🔍  Auto-Detect Most Recent", "auto",
                "Automatically find the latest data (prefers historical)"),
                
                ("📅  Current (Last 24h)", "current",
                "Visualize real-time data from outputs/rain_gauges/analyze/"),
                
                ("📆  Specific Historical Date", "date",
                "Choose a specific date to visualize"),
            ],
            colors=self.app.colors,
        )
        
        if not selection:
            return
        
        script = "scripts/gauge/visualize.py"
        args = []
        
        if selection == "current":
            args = ["--current"]
            self.app.selected_date = None
        elif selection == "date":
            date_str = ctk.CTkInputDialog(
                text="Enter date in YYYY-MM-DD format:",
                title="Enter Date"
            ).get_input()
            if not date_str:
                return
            args = ["--date", date_str]
            self.app.selected_date = date_str
        else:  # auto
            self.app.selected_date = None
        
        self.app.executor.execute(
            "Step 3: Visualize Results",
            script,
            args,
            self._on_visualize_complete
        )
    
    def _on_visualize_complete(self, success: bool) -> None:
        """Handle visualize completion."""
        if success:
            base_dir = self.paths.rain_gauges_viz_dir
            
            html_files = []
            if base_dir.exists():
                html_files = list(base_dir.glob("**/*.html"))
            
            if html_files:
                most_recent = max(html_files, key=lambda p: p.stat().st_mtime)
                self.app.output_dir = str(most_recent.parent)
            else:
                self.app.output_dir = str(base_dir)
            
            result = messagebox.askyesno(
                "Success",
                f"✅ Visualization complete!\n\n"
                f"Dashboard saved to:\n{self.app.output_dir}\n\n"
                f"Open dashboard now?"
            )
            if result:
                self._open_dashboard()
        else:
            messagebox.showerror(
                "Error",
                "❌ Visualization failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def _open_dashboard(self) -> None:
        """Open the generated dashboard."""
        import webbrowser
        import os
        import sys
        import subprocess
        
        dashboard_dir = Path(self.app.output_dir)
        html_files = list(dashboard_dir.glob("**/*.html"))
        
        if not html_files:
            messagebox.showwarning(
                "Not Found",
                f"No dashboard HTML files found in:\n{dashboard_dir}"
            )
            return
        
        dashboard_path = max(html_files, key=lambda p: p.stat().st_mtime)
        abs_path = str(dashboard_path.resolve())
        
        print(f"DEBUG: Attempting to open: {abs_path}")
        
        # Try multiple methods in order
        success = False
        error_msg = ""
        
        # Method 1: os.startfile (Windows - most reliable)
        if sys.platform == 'win32':
            try:
                print("DEBUG: Trying os.startfile...")
                os.startfile(abs_path)
                success = True
                print("DEBUG: os.startfile SUCCESS")
                return
            except Exception as e:
                error_msg += f"os.startfile: {e}\n"
                print(f"DEBUG: os.startfile failed: {e}")
        
        # Method 2: webbrowser.open
        if not success:
            try:
                print("DEBUG: Trying webbrowser.open...")
                webbrowser.open(dashboard_path.as_uri())
                success = True
                print("DEBUG: webbrowser.open SUCCESS")
                return
            except Exception as e:
                error_msg += f"webbrowser: {e}\n"
                print(f"DEBUG: webbrowser.open failed: {e}")
        
        # Method 3: subprocess (platform-specific)
        if not success:
            try:
                if sys.platform == 'win32':
                    print("DEBUG: Trying subprocess with cmd /c start...")
                    subprocess.run(['cmd', '/c', 'start', '', abs_path], shell=True)
                    success = True
                elif sys.platform == 'darwin':
                    print("DEBUG: Trying subprocess with open...")
                    subprocess.run(['open', abs_path])
                    success = True
                else:
                    print("DEBUG: Trying subprocess with xdg-open...")
                    subprocess.run(['xdg-open', abs_path])
                    success = True
                print("DEBUG: subprocess SUCCESS")
                return
            except Exception as e:
                error_msg += f"subprocess: {e}\n"
                print(f"DEBUG: subprocess failed: {e}")
        
        # All methods failed
        if not success:
            messagebox.showerror(
                "Cannot Open Browser",
                f"Could not open dashboard automatically.\n\n"
                f"Please open manually:\n{abs_path}\n\n"
                f"Errors:\n{error_msg}"
            )
    
    def run_validate(self) -> None:
        """Run validate step with file selection."""
        input_file = filedialog.askopenfilename(
            title="Select historical alarm events CSV",
            initialdir=str(Path.cwd() / "data" / "inputs"),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not input_file:
            return
        
        output_dir = filedialog.askdirectory(
            title="Select output directory for validation results",
            initialdir=str(self.paths.outputs_root),
        )
        if not output_dir:
            return
        
        self.app.output_dir = output_dir
        script = "scripts/gauge/validate.py"
        args = [
            "--input", input_file,
            "--output", str(Path(output_dir) / "ari_alarm_validation.csv")
        ]
        
        self.app.executor.execute(
            "Step 4: Validate Alarms",
            script,
            args,
            self._on_validate_complete
        )
    
    def _on_validate_complete(self, success: bool) -> None:
        """Handle validate completion."""
        if success:
            messagebox.showinfo(
                "Success",
                f"✅ Validation complete!\n\nResults saved to:\n{self.app.output_dir}"
            )
        else:
            messagebox.showerror(
                "Error",
                "❌ Validation failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def run_visualize_validation(self) -> None:
        """Run visualize validation step."""
        input_file = filedialog.askopenfilename(
            title="Select validation results CSV",
            initialdir=str(self.paths.outputs_root),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not input_file:
            return
        
        output_dir = filedialog.askdirectory(
            title="Select output directory for validation visualization",
            initialdir=str(self.paths.outputs_root),
        )
        if not output_dir:
            return
        
        self.app.output_dir = output_dir
        script = "scripts/gauge/visualize_validation.py"
        args = ["--input", input_file, "--output", output_dir]
        
        self.app.executor.execute(
            "Step 5: Visualize Validation",
            script,
            args,
            self._on_visualize_validation_complete
        )
    
    def _open_validation_dashboard(self) -> None:
        """Open the validation dashboard."""
        import webbrowser
        import os
        import sys
        import subprocess
        
        dashboard_dir = Path(self.app.output_dir)
        html_files = list(dashboard_dir.glob("**/*.html"))
        
        if not html_files:
            messagebox.showwarning(
                "Not Found",
                f"No dashboard HTML files found in:\n{dashboard_dir}"
            )
            return
        
        dashboard_path = max(html_files, key=lambda p: p.stat().st_mtime)
        abs_path = str(dashboard_path.resolve())
        
        # Try os.startfile first (most reliable on Windows)
        if sys.platform == 'win32':
            try:
                os.startfile(abs_path)
                return
            except Exception:
                pass
        
        # Fallback to webbrowser
        try:
            webbrowser.open(dashboard_path.as_uri())
        except Exception as e:
            messagebox.showerror(
                "Cannot Open Browser",
                f"Could not open dashboard automatically.\n\n"
                f"Please open manually:\n{abs_path}\n\n"
                f"Error: {e}"
            )
    
    def _on_visualize_validation_complete(self, success: bool) -> None:
        """Handle visualize validation completion."""
        if success:
            result = messagebox.askyesno(
                "Success",
                f"✅ Validation visualization complete!\n\n"
                f"Dashboard saved to:\n{self.app.output_dir}\n\n"
                f"Open dashboard now?"
            )
            
            if result:
                # Use the robust method
                self._open_validation_dashboard()
            
            # Show completion message AFTER dashboard interaction
            messagebox.showinfo(
                "Pipeline Complete!",
                "🎉 All steps completed!\n\nGauge pipeline finished successfully."
            )
        else:
            messagebox.showerror(
                "Error",
                "❌ Validation visualization failed!\n\nCheck the logs for details."
            )
        
        self.app.show_pipeline_steps()