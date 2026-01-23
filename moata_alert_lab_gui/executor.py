"""
Script Executor Module

Handles subprocess execution with real-time output display.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2026-01-22
Version: 1.1.0 - Added alarm script timeout detection
"""

from __future__ import annotations

import customtkinter as ctk
import subprocess
import threading
import sys
import os
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .config import (
    TIMEOUT_RETRIEVE,
    TIMEOUT_RADAR,
    TIMEOUT_ALARMS,
    TIMEOUT_DEFAULT,
)


def get_timeout_for_script(script: str) -> int:
    """
    Get appropriate timeout based on script type.
    
    Args:
        script: Script filename or path
        
    Returns:
        Timeout in seconds
    """
    script_lower = script.lower()
    
    # Check for alarm-related scripts (highest priority - longest running)
    if "alarm" in script_lower or "alarms" in script_lower:
        return TIMEOUT_ALARMS
    
    # Check for retrieve scripts
    if "retrieve" in script_lower:
        return TIMEOUT_RETRIEVE
    
    # Check for radar scripts
    if "radar" in script_lower:
        return TIMEOUT_RADAR
    
    return TIMEOUT_DEFAULT


class ScriptExecutor:
    """
    Executes Python scripts with real-time output display.
    
    Shows a popup window with:
    - Progress bar
    - Console output
    - Status updates
    - Close button on completion
    """
    
    def __init__(
        self,
        parent: ctk.CTk,
        colors: Dict[str, str],
    ):
        """
        Initialize executor.
        
        Args:
            parent: Parent window
            colors: Color dictionary for theming
        """
        self.parent = parent
        self.colors = colors
    
    def execute(
        self,
        title: str,
        script: str,
        args: List[str],
        callback: Callable[[bool], None],
    ) -> None:
        """
        Execute a script and show progress window.
        
        Args:
            title: Window title
            script: Script filename to execute
            args: Command-line arguments
            callback: Callback function called with success status
        """
        exec_window = ctk.CTkToplevel(self.parent)
        exec_window.title(title)
        exec_window.geometry("900x600")
        exec_window.transient(self.parent)
        
        # Header
        header = ctk.CTkFrame(
            exec_window,
            fg_color=self.colors["primary"],
            corner_radius=0,
            height=70
        )
        header.pack(fill="x")
        header.pack_propagate(False)
        
        title_label = ctk.CTkLabel(
            header,
            text=title,
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color="white"
        )
        title_label.pack(pady=(15, 5))
        
        # Show timeout info
        timeout_seconds = get_timeout_for_script(script)
        timeout_minutes = timeout_seconds // 60
        script_label = ctk.CTkLabel(
            header,
            text=f"Running: {script} (timeout: {timeout_minutes} min)",
            font=ctk.CTkFont(size=12),
            text_color="#B8C5D6"
        )
        script_label.pack()
        
        # Progress bar
        progress = ctk.CTkProgressBar(exec_window, mode="indeterminate", height=6)
        progress.pack(fill="x", padx=20, pady=15)
        progress.start()
        
        # Console output
        console_frame = ctk.CTkFrame(
            exec_window,
            fg_color=self.colors["surface"],
            corner_radius=10
        )
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
        status_frame = ctk.CTkFrame(
            exec_window,
            fg_color=self.colors["surface"],
            corner_radius=10,
            height=50
        )
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
            success = self._run_subprocess(
                script=script,
                args=args,
                exec_window=exec_window,
                console_text=console_text,
                status_label=status_label,
                progress=progress,
                status_frame=status_frame,
                callback=callback,
            )
        
        thread = threading.Thread(target=run_script, daemon=True)
        thread.start()
    
    def _run_subprocess(
        self,
        script: str,
        args: List[str],
        exec_window: ctk.CTkToplevel,
        console_text: ctk.CTkTextbox,
        status_label: ctk.CTkLabel,
        progress: ctk.CTkProgressBar,
        status_frame: ctk.CTkFrame,
        callback: Callable[[bool], None],
    ) -> bool:
        """
        Run the subprocess and handle output.
        
        Returns:
            True if successful, False otherwise
        """
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
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )
            
            timeout_seconds = get_timeout_for_script(script)
            start_time = time.time()
            
            # Read output line by line
            while True:
                # Check if window still exists
                try:
                    exec_window.winfo_exists()
                except:
                    process.kill()
                    return False
                
                ret = process.poll()
                
                # Read available output
                if process.stdout:
                    try:
                        line = process.stdout.readline()
                        if line:
                            if not any(x in line for x in ["UnicodeEncodeError", "charmap_encode"]):
                                console_text.insert("end", line)
                                console_text.see("end")
                                exec_window.update_idletasks()
                    except Exception:
                        pass
                
                # Update elapsed time with better formatting
                elapsed = int(time.time() - start_time)
                elapsed_min = elapsed // 60
                elapsed_sec = elapsed % 60
                timeout_min = timeout_seconds // 60
                
                try:
                    if elapsed_min > 0:
                        status_label.configure(
                            text=f"⏳ Running... ({elapsed_min}m {elapsed_sec}s / {timeout_min}m timeout)"
                        )
                    else:
                        status_label.configure(
                            text=f"⏳ Running... ({elapsed_sec}s elapsed)"
                        )
                except:
                    process.kill()
                    return False
                
                # Check timeout
                if elapsed > timeout_seconds:
                    process.kill()
                    try:
                        status_label.configure(
                            text=f"⏱️ Timeout after {timeout_min} minutes",
                            text_color=self.colors["danger"]
                        )
                        console_text.insert("end", f"\n{'='*70}\n⏱️ TIMEOUT\n")
                        console_text.insert("end", f"\nScript exceeded {timeout_min} minute timeout.\n")
                        console_text.insert("end", f"Consider running from command line for longer operations.\n")
                    except:
                        pass
                    break
                
                # Check if process finished
                if ret is not None:
                    try:
                        remaining = process.stdout.read()
                        if remaining:
                            console_text.insert("end", remaining)
                            console_text.see("end")
                    except:
                        pass
                    break
                
                time.sleep(0.05)
            
            # Stop progress bar
            try:
                progress.stop()
            except:
                pass
            
            return_code = process.returncode if process.returncode is not None else -1
            
            # Update status based on result
            try:
                if return_code == 0:
                    status_label.configure(
                        text="✅ Completed Successfully!",
                        text_color=self.colors["success"]
                    )
                    console_text.insert("end", f"\n{'='*70}\n✅ SUCCESS\n")
                    success = True
                else:
                    status_label.configure(
                        text=f"❌ Failed (Exit Code {return_code})",
                        text_color=self.colors["danger"]
                    )
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
            except:
                pass
            
            return success
            
        except Exception as e:
            try:
                progress.stop()
                status_label.configure(
                    text=f"❌ Exception: {str(e)}",
                    text_color=self.colors["danger"]
                )
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
            except:
                pass
            
            return False