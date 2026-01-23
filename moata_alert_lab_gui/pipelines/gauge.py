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
            ("6", "Check Alarms", "Check rainfall alarms in last 24 hours with verification",
             self.run_check_alarms, self.app.colors["step1"]),
        ]
    
    # =========================================================================
    # Step Implementations
    # =========================================================================
    
    def run_retrieve(self) -> None:
        """Run retrieve step - simple date picker."""
        date_str = self._get_date_from_user("Select Date to Retrieve")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 1: Retrieve Data",
            "scripts/gauge/retrieve.py",
            ["--date", date_str],
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
        """Run analyze step - simple date picker."""
        date_str = self._get_date_from_user("Select Date to Analyze")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 2: Analyze Data",
            "scripts/gauge/analyze.py",
            ["--date", date_str],
            self._on_analyze_complete
        )
    
    def _on_analyze_complete(self, success: bool) -> None:
        """Handle analyze completion."""
        if success:
            messagebox.showinfo(
                "Success",
                "✅ Analysis complete!\n\nReady to proceed to Visualization."
            )
        else:
            messagebox.showerror(
                "Error",
                "❌ Analysis failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def run_visualize(self) -> None:
        """Run visualize step - simple date picker."""
        date_str = self._get_date_from_user("Select Date to Visualize")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 3: Visualize Results",
            "scripts/gauge/visualize.py",
            ["--date", date_str],
            self._on_visualize_complete
        )
    
    def _on_visualize_complete(self, success: bool) -> None:
        """Handle visualize completion."""
        if success:
            # Update output_dir to the visualization directory
            from moata_pipeline.common.paths import get_paths
            paths = get_paths()
            if self.app.selected_date:
                viz_dir = paths.get_gauge_viz_dir(self.app.selected_date)
                if viz_dir.exists():
                    self.app.output_dir = str(viz_dir)
                    print(f"Updated output_dir to: {self.app.output_dir}")
            
            result = messagebox.askyesno(
                "Success",
                "✅ Visualization complete!\n\n"
                "Open dashboard now?"
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
        
        print(f"[DEBUG] Attempting to open dashboard: {abs_path}")
        
        # Try multiple methods in order
        error_msg = ""
        
        # Method 1: os.startfile (Windows - most reliable)
        if sys.platform == 'win32':
            try:
                print("[DEBUG] Trying os.startfile...")
                os.startfile(abs_path)
                print("[DEBUG] os.startfile called successfully")
                return
            except Exception as e:
                error_msg += f"os.startfile: {e}\n"
                print(f"[DEBUG] os.startfile failed: {e}")
        
        # Method 2: webbrowser.open with file URI
        try:
            print("[DEBUG] Trying webbrowser.open with file URI...")
            file_uri = dashboard_path.as_uri()
            print(f"File URI: {file_uri}")
            result = webbrowser.open(file_uri)
            print(f"[DEBUG] webbrowser.open returned: {result}")
            if result:
                return
        except Exception as e:
            error_msg += f"webbrowser (uri): {e}\n"
            print(f"[DEBUG] webbrowser.open (uri) failed: {e}")
        
        # Method 3: webbrowser.open with file path directly
        try:
            print("[DEBUG] Trying webbrowser.open with path...")
            result = webbrowser.open(abs_path)
            print(f"[DEBUG] webbrowser.open (path) returned: {result}")
            if result:
                return
        except Exception as e:
            error_msg += f"webbrowser (path): {e}\n"
            print(f"[DEBUG] webbrowser.open (path) failed: {e}")
        
        # Method 4: subprocess (platform-specific)
        try:
            if sys.platform == 'win32':
                print("[DEBUG] Trying subprocess with explorer...")
                subprocess.Popen(['explorer', abs_path])
                print("[DEBUG] subprocess explorer called successfully")
                return
        except Exception as e:
            error_msg += f"subprocess explorer: {e}\n"
            print(f"[DEBUG] subprocess explorer failed: {e}")
        
        # Method 5: cmd /c start
        try:
            if sys.platform == 'win32':
                print("[DEBUG] Trying cmd /c start...")
                subprocess.Popen(f'cmd /c start "" "{abs_path}"', shell=True)
                print("[DEBUG] cmd /c start called successfully")
                return
        except Exception as e:
            error_msg += f"subprocess cmd: {e}\n"
            print(f"[DEBUG] subprocess cmd failed: {e}")
        
        # All methods failed - show path to user
        messagebox.showerror(
            "Cannot Open Browser",
            f"Could not open dashboard automatically.\n\n"
            f"Please open manually:\n{abs_path}\n\n"
            f"Errors:\n{error_msg}"
        )
    
    def run_validate(self) -> None:
        """Run validate step - simple date picker."""
        date_str = self._get_date_from_user("Select Date to Validate")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 4: Validate Alarms",
            "scripts/gauge/validate.py",
            ["--date", date_str],
            self._on_validate_complete
        )
    
    def _on_validate_complete(self, success: bool) -> None:
        """Handle validate completion."""
        if success:
            messagebox.showinfo(
                "Success",
                "✅ Validation complete!\n\nReady to proceed to Visualization."
            )
        else:
            messagebox.showerror(
                "Error",
                "❌ Validation failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def run_visualize_validation(self) -> None:
        """Run visualize validation step - simple date picker."""
        date_str = self._get_date_from_user("Select Date to Visualize Validation")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 5: Visualize Validation",
            "scripts/gauge/visualize_validation.py",
            [],
            self._on_visualize_validation_complete
        )
    
    def _open_validation_dashboard(self) -> None:
        """Open the validation dashboard."""
        import webbrowser
        import os
        import sys
        import subprocess
        from moata_pipeline.common.paths import get_paths
        paths = get_paths()
        
        dashboard_dir = Path(self.app.output_dir) if self.app.output_dir else None
        
        # If no output_dir, try to find it from selected_date
        if not dashboard_dir or not dashboard_dir.exists():
            if self.app.selected_date:
                # Look for validation files in analysis directory
                analyze_dir = paths.get_gauge_analyze_dir(self.app.selected_date)
                if analyze_dir.exists():
                    dashboard_dir = analyze_dir
                    self.app.output_dir = str(dashboard_dir)
        
        # Always look for validation dashboard in outputs/rain_gauges/validation
        validation_dir = paths.outputs_root / "rain_gauges" / "validation"
        html_files = []
        if validation_dir.exists():
            html_files = list(validation_dir.glob("**/*.html"))
        # If not found, search all validation subfolders in outputs/rain_gauges
        if not html_files:
            rg_dir = paths.outputs_root / "rain_gauges"
            html_files = list(rg_dir.glob("**/validation/*.html"))
        if not html_files:
            messagebox.showwarning(
                "Not Found",
                f"No dashboard HTML files found in any validation folder under:\n{paths.outputs_root / 'rain_gauges'}"
            )
            return
        dashboard_path = max(html_files, key=lambda p: p.stat().st_mtime)
        abs_path = str(dashboard_path.resolve())
        
        print(f"[DEBUG] Attempting to open validation dashboard: {abs_path}")
        
        # Try os.startfile first (most reliable on Windows)
        if sys.platform == 'win32':
            try:
                print("[DEBUG] Trying os.startfile...")
                os.startfile(abs_path)
                print("[DEBUG] os.startfile called successfully")
                return
            except Exception as e:
                print(f"os.startfile failed: {e}")
        
        # Fallback to webbrowser with file URI
        try:
            print("[DEBUG] Trying webbrowser.open...")
            result = webbrowser.open(dashboard_path.as_uri())
            print(f"[DEBUG] webbrowser.open returned: {result}")
            if result:
                return
        except Exception as e:
            print(f"webbrowser.open failed: {e}")
        
        # Fallback to subprocess explorer
        try:
            if sys.platform == 'win32':
                print("[DEBUG] Trying subprocess explorer...")
                subprocess.Popen(['explorer', abs_path])
                print("[DEBUG] subprocess explorer called successfully")
                return
        except Exception as e:
            print(f"subprocess explorer failed: {e}")
        
        # Fallback to cmd /c start
        try:
            if sys.platform == 'win32':
                print("[DEBUG] Trying cmd /c start...")
                subprocess.Popen(f'cmd /c start "" "{abs_path}"', shell=True)
                print("[DEBUG] cmd /c start called successfully")
                return
        except Exception as e:
            print(f"cmd /c start failed: {e}")
        
        messagebox.showerror(
            "Cannot Open Browser",
            f"Could not open dashboard automatically.\n\n"
            f"Please open manually:\n{abs_path}"
        )
    
    def _on_visualize_validation_complete(self, success: bool) -> None:
        """Handle visualize validation completion."""
        if success:
            # Update output_dir to the analysis directory (where validation outputs go)
            from moata_pipeline.common.paths import get_paths
            paths = get_paths()
            if self.app.selected_date:
                analyze_dir = paths.get_gauge_analyze_dir(self.app.selected_date)
                if analyze_dir.exists():
                    self.app.output_dir = str(analyze_dir)
                    print(f"Updated output_dir to: {self.app.output_dir}")
            
            result = messagebox.askyesno(
                "Success",
                "✅ Validation visualization complete!\n\n"
                "Open dashboard now?"
            )
            if result:
                self._open_validation_dashboard()
        else:
            messagebox.showerror(
                "Error",
                "❌ Validation visualization failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def run_check_alarms(self) -> None:
        """Run check alarms step - datetime picker for end time."""
        datetime_str = self._get_datetime_from_user("Select End Time for Alarm Check")
        if not datetime_str:
            return
        
        self.app.executor.execute(
            "Step 6: Check Alarms",
            "scripts/gauge/check_alarms.py",
            ["--datetime", datetime_str],
            self._on_check_alarms_complete
        )
    
    def _get_datetime_from_user(self, title: str) -> str:
        """Show datetime picker dialog and return datetime string."""
        from datetime import datetime
        
        dialog = ctk.CTkToplevel(self.app)
        dialog.title(title)
        dialog.geometry("350x200")
        dialog.transient(self.app)
        dialog.grab_set()
        
        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (175)
        y = (dialog.winfo_screenheight() // 2) - (100)
        dialog.geometry(f"+{x}+{y}")
        
        result = {"value": None}
        
        # Date frame
        date_frame = ctk.CTkFrame(dialog)
        date_frame.pack(pady=10, padx=20, fill="x")
        
        ctk.CTkLabel(date_frame, text="Date (YYYY-MM-DD):").pack(side="left")
        date_entry = ctk.CTkEntry(date_frame, width=120)
        date_entry.insert(0, datetime.now().strftime("%Y-%m-%d"))
        date_entry.pack(side="right")
        
        # Time frame
        time_frame = ctk.CTkFrame(dialog)
        time_frame.pack(pady=10, padx=20, fill="x")
        
        ctk.CTkLabel(time_frame, text="Time (HH:MM) NZDT:").pack(side="left")
        time_entry = ctk.CTkEntry(time_frame, width=120)
        time_entry.insert(0, datetime.now().strftime("%H:%M"))
        time_entry.pack(side="right")
        
        def on_ok():
            date_val = date_entry.get().strip()
            time_val = time_entry.get().strip()
            result["value"] = f"{date_val} {time_val}"
            dialog.destroy()
        
        def on_cancel():
            dialog.destroy()
        
        # Buttons
        btn_frame = ctk.CTkFrame(dialog)
        btn_frame.pack(pady=20)
        
        ctk.CTkButton(btn_frame, text="OK", command=on_ok, width=80).pack(side="left", padx=10)
        ctk.CTkButton(btn_frame, text="Cancel", command=on_cancel, width=80).pack(side="left", padx=10)
        
        dialog.wait_window()
        return result["value"]
    
    def _on_check_alarms_complete(self, success: bool) -> None:
        """Handle check alarms completion."""
        if success:
            result = messagebox.askyesno(
                "Success",
                "✅ Alarm check complete!\n\n"
                "Open alarm dashboard now?"
            )
            if result:
                self._open_alarm_dashboard()
        else:
            messagebox.showerror(
                "Error",
                "❌ Alarm check failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def _open_alarm_dashboard(self) -> None:
        """Open the alarm dashboard."""
        import webbrowser
        import os
        import sys
        import subprocess
        
        # Look for most recent alarm dashboard
        from moata_pipeline.common.paths import get_paths
        paths = get_paths()
        
        # Try outputs/rain_gauges/alarms first
        dashboard_dir = paths.outputs_root / "rain_gauges" / "alarms"
        html_files = list(dashboard_dir.glob("**/alarm_dashboard.html"))
        
        if not html_files:
            # Try broader search in rain_gauges
            dashboard_dir = paths.outputs_root / "rain_gauges"
            html_files = list(dashboard_dir.glob("**/alarm_dashboard.html"))
        
        if not html_files:
            messagebox.showwarning(
                "Not Found",
                f"No alarm dashboard found in:\n{dashboard_dir}\n\n"
                f"Run Check Alarms step first."
            )
            return
        
        dashboard_path = max(html_files, key=lambda p: p.stat().st_mtime)
        abs_path = str(dashboard_path.resolve())
        
        print(f"[DEBUG] Attempting to open alarm dashboard: {abs_path}")
        
        # Try os.startfile first (most reliable on Windows)
        if sys.platform == 'win32':
            try:
                print("[DEBUG] Trying os.startfile...")
                os.startfile(abs_path)
                print("[DEBUG] os.startfile called successfully")
                return
            except Exception as e:
                print(f"os.startfile failed: {e}")
        
        # Fallback to webbrowser
        try:
            print("[DEBUG] Trying webbrowser.open...")
            result = webbrowser.open(dashboard_path.as_uri())
            print(f"[DEBUG] webbrowser.open returned: {result}")
            if result:
                return
        except Exception as e:
            print(f"webbrowser.open failed: {e}")
        
        # Fallback to subprocess explorer
        try:
            if sys.platform == 'win32':
                print("[DEBUG] Trying subprocess explorer...")
                subprocess.Popen(['explorer', abs_path])
                print("[DEBUG] subprocess explorer called successfully")
                return
        except Exception as e:
            print(f"subprocess explorer failed: {e}")
        
        # Fallback to cmd /c start
        try:
            if sys.platform == 'win32':
                print("[DEBUG] Trying cmd /c start...")
                subprocess.Popen(f'cmd /c start "" "{abs_path}"', shell=True)
                print("[DEBUG] cmd /c start called successfully")
                return
        except Exception as e:
            print(f"cmd /c start failed: {e}")
        
        messagebox.showerror(
            "Cannot Open Browser",
            f"Could not open dashboard automatically.\n\n"
            f"Please open manually:\n{abs_path}"
        )