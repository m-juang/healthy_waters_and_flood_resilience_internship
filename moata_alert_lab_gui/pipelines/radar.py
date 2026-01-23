"""
Rain Radar Pipeline Module

Implements the rain radar data processing pipeline.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-23
Version: 1.5.0 - Fixed visualize_alarms to use --no-open and GUI handles browser opening
"""

from __future__ import annotations
import customtkinter as ctk
from tkinter import filedialog, messagebox
from pathlib import Path
from typing import Callable, List, Tuple, TYPE_CHECKING

from .base import BasePipeline

if TYPE_CHECKING:
    from ..main import ModernApp


class RadarPipeline(BasePipeline):
    """
    Rain Radar Pipeline implementation.
    
    Steps:
    1. Retrieve - Collect QPE radar data for specific date
    2. Analyze - Calculate ARI for each catchment
    3. Visualize - Generate HTML dashboard
    4. Check Alarms - Detect alarms at each timestamp in 24h period
    5. Visualize Alarms - Show when/where alarms occurred
    """
    
    @property
    def name(self) -> str:
        return "Rain Radar"
    
    @property
    def icon(self) -> str:
        return "📡"
    
    @property
    def color_key(self) -> str:
        return "radar"
    
    @property
    def subtitle(self) -> str:
        return "Spatial Coverage"
    
    @property
    def features(self) -> List[str]:
        return [
            "Pixel-level rainfall data",
            "ARI calculation",
            "Alarm validation",
            "Dashboards"
        ]
    
    def get_steps(self) -> List[Tuple[str, str, str, Callable, str]]:
        """Get rain radar pipeline steps."""
        return [
            ("1", "Retrieve Data", "Collect QPE radar data for specific date",
             self.run_retrieve, self.app.colors["step1"]),
            ("2", "Analyze Data", "Calculate ARI for each catchment",
             self.run_analyze, self.app.colors["step2"]),
            ("3", "Visualize Results", "Generate HTML dashboard from analysis",
             self.run_visualize, self.app.colors["step3"]),
            ("4", "Check Alarms", "Detect alarms at each timestamp in 24h period",
             self.run_check_alarms, self.app.colors["step4"]),
            ("5", "Visualize Alarms", "Show when and where alarms occurred",
             self.run_visualize_alarms, self.app.colors["step5"]),
        ]
    
    # =========================================================================
    # Step Implementations
    # =========================================================================
    
    def run_retrieve(self) -> None:
        """Run retrieve step - simple date picker."""
        # Get date from user
        date_str = self._get_date_from_user("Select Date to Retrieve")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 1: Retrieve Data",
            "scripts/radar/retrieve.py",
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
            "scripts/radar/analyze.py",
            ["--date", date_str],
            self._on_analyze_complete
        )
    
    def _on_analyze_complete(self, success: bool) -> None:
        """Handle analyze completion."""
        if success:
            messagebox.showinfo(
                "Success",
                "✅ Analysis complete!\n\nReady for visualization."
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
            "scripts/radar/visualize.py",
            ["--date", date_str, "--no-open"],
            self._on_visualize_complete
        )

    def _on_visualize_complete(self, success: bool) -> None:
        """Handle visualize completion."""
        if success:
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
        """Open the generated analysis dashboard."""
        import webbrowser
        import os
        import sys
        import subprocess
        
        # Use PipelinePaths to get the correct visualization directory
        # based on the selected date
        if hasattr(self.app, 'selected_date') and self.app.selected_date:
            from moata_pipeline.common.paths import PipelinePaths
            paths = PipelinePaths.for_date(self.app.selected_date)
            dashboard_dir = paths.rain_radar_viz_dir
        elif hasattr(self.app, 'output_dir') and self.app.output_dir:
            dashboard_dir = Path(self.app.output_dir)
        else:
            # Fallback: search in default outputs directory
            dashboard_dir = Path("outputs/rain_radar")
        
        # Search for HTML files
        html_files = []
        if dashboard_dir.exists():
            html_files = list(dashboard_dir.glob("**/*.html"))
        
        if not html_files:
            # Try broader search
            broader_search = Path("outputs/rain_radar")
            if broader_search.exists():
                html_files = list(broader_search.glob("**/*.html"))
        
        if not html_files:
            messagebox.showwarning(
                "Not Found",
                f"No dashboard HTML files found in:\n{dashboard_dir}\n\n"
                f"Make sure visualization has completed successfully."
            )
            return
        
        dashboard_path = max(html_files, key=lambda p: p.stat().st_mtime)
        abs_path = str(dashboard_path.resolve())
        
        # Try multiple methods in order
        success = False
        error_msg = ""
        
        # Method 1: os.startfile (Windows - most reliable)
        if sys.platform == 'win32':
            try:
                os.startfile(abs_path)
                success = True
                return
            except Exception as e:
                error_msg += f"os.startfile: {e}\n"
        
        # Method 2: webbrowser.open
        if not success:
            try:
                webbrowser.open(dashboard_path.as_uri())
                success = True
                return
            except Exception as e:
                error_msg += f"webbrowser: {e}\n"
        
        # Method 3: subprocess (platform-specific)
        if not success:
            try:
                if sys.platform == 'win32':
                    subprocess.run(['cmd', '/c', 'start', '', abs_path], shell=True)
                    success = True
                elif sys.platform == 'darwin':
                    subprocess.run(['open', abs_path])
                    success = True
                else:
                    subprocess.run(['xdg-open', abs_path])
                    success = True
                return
            except Exception as e:
                error_msg += f"subprocess: {e}\n"
        
        # All methods failed
        if not success:
            messagebox.showerror(
                "Cannot Open Browser",
                f"Could not open dashboard automatically.\n\n"
                f"Please open manually:\n{abs_path}\n\n"
                f"Errors:\n{error_msg}"
            )
    
    def run_check_alarms(self) -> None:
        """Run alarm timeline check step - simple date picker."""
        date_str = self._get_date_from_user("Select Date to Check Alarms")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 4: Check Alarms",
            "scripts/alarms/check_alarm_timeline.py",
            ["--date", date_str],
            self._on_check_alarms_complete
        )
    
    def _on_check_alarms_complete(self, success: bool) -> None:
        """Handle alarm timeline check completion."""
        if success:
            # Search for alarm output files
            search_root = self.paths.rain_radar_dir
            
            summary_files = []
            if search_root.exists():
                summary_files = list(search_root.glob("**/alarms/alarm_summary.txt"))
            
            if summary_files:
                most_recent = max(summary_files, key=lambda p: p.stat().st_mtime)
                self.app.output_dir = str(most_recent.parent)
            else:
                self.app.output_dir = str(search_root)
            
            messagebox.showinfo(
                "Success",
                f"✅ Alarm timeline check complete!\n\n"
                f"Output saved to:\n{self.app.output_dir}\n\n"
                f"Files:\n"
                f"• alarm_timeline.csv\n"
                f"• alarm_events.csv\n"
                f"• alarm_summary.txt\n\n"
                f"Ready to proceed to Visualize Alarms."
            )
        else:
            messagebox.showerror(
                "Error",
                "❌ Alarm timeline check failed!\n\nCheck the logs for details."
            )
        
        self.app.show_pipeline_steps()
    
    def run_visualize_alarms(self) -> None:
        """Run visualize alarms step - show detection results."""
        date_str = self._get_date_from_user("Select Date to Visualize Alarms")
        if not date_str:
            return
        
        self.app.selected_date = date_str
        self.app.executor.execute(
            "Step 5: Visualize Alarms",
            "scripts/radar/visualize_alarms.py",
            ["--date", date_str, "--no-open"],
            self._on_visualize_alarms_complete
        )
    
    def _on_visualize_alarms_complete(self, success: bool) -> None:
        """Handle visualize alarms completion."""
        if success:
            result = messagebox.askyesno(
                "Success",
                "✅ Alarm visualization complete!\n\n"
                "Dashboard created with timeline and statistics.\n\n"
                "Open dashboard now?"
            )
            if result:
                self._open_alarms_dashboard()
        else:
            messagebox.showerror(
                "Error",
                "❌ Alarm visualization failed!\n\nCheck the logs for details."
            )
        self.app.show_pipeline_steps()
    
    def _open_alarms_dashboard(self) -> None:
        """Open the generated alarms dashboard."""
        import webbrowser
        import os
        import sys
        import subprocess
        
        # Use PipelinePaths to get the correct visualization directory
        if hasattr(self.app, 'selected_date') and self.app.selected_date:
            from moata_pipeline.common.paths import PipelinePaths
            paths = PipelinePaths.for_date(self.app.selected_date)
            dashboard_path = paths.rain_radar_viz_dir / "alarms_dashboard.html"
        else:
            messagebox.showwarning(
                "Not Found",
                "No date selected. Cannot locate dashboard."
            )
            return
        
        if not dashboard_path.exists():
            # Try to find the most recent alarms_dashboard.html
            search_root = Path("outputs/rain_radar")
            if search_root.exists():
                html_files = list(search_root.glob("**/alarms_dashboard.html"))
                if html_files:
                    dashboard_path = max(html_files, key=lambda p: p.stat().st_mtime)
                else:
                    messagebox.showwarning(
                        "Not Found",
                        f"Dashboard not found:\n{dashboard_path}\n\n"
                        f"Make sure visualization has completed successfully."
                    )
                    return
            else:
                messagebox.showwarning(
                    "Not Found",
                    f"Dashboard not found:\n{dashboard_path}\n\n"
                    f"Make sure visualization has completed successfully."
                )
                return
        
        abs_path = str(dashboard_path.resolve())
        
        # Try multiple methods in order
        success = False
        error_msg = ""
        
        # Method 1: os.startfile (Windows - most reliable)
        if sys.platform == 'win32':
            try:
                os.startfile(abs_path)
                success = True
                return
            except Exception as e:
                error_msg += f"os.startfile: {e}\n"
        
        # Method 2: webbrowser.open
        if not success:
            try:
                webbrowser.open(dashboard_path.as_uri())
                success = True
                return
            except Exception as e:
                error_msg += f"webbrowser: {e}\n"
        
        # Method 3: subprocess (platform-specific)
        if not success:
            try:
                if sys.platform == 'win32':
                    subprocess.run(['cmd', '/c', 'start', '', abs_path], shell=True)
                    success = True
                elif sys.platform == 'darwin':
                    subprocess.run(['open', abs_path])
                    success = True
                else:
                    subprocess.run(['xdg-open', abs_path])
                    success = True
                return
            except Exception as e:
                error_msg += f"subprocess: {e}\n"
        
        # All methods failed
        if not success:
            messagebox.showerror(
                "Cannot Open Browser",
                f"Could not open dashboard automatically.\n\n"
                f"Please open manually:\n{abs_path}\n\n"
                f"Errors:\n{error_msg}"
            )