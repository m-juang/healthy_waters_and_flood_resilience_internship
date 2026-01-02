"""
Rain Monitoring System GUI - Main Module

Main application window and entry point.

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2025-01-02
Version: 2.0.0 (Modular architecture)
"""

from __future__ import annotations

import customtkinter as ctk
from datetime import datetime
from typing import Optional

from .config import (
    APP_TITLE,
    APP_VERSION,
    APP_SUBTITLE,
    APP_FOOTER,
    WINDOW_WIDTH,
    WINDOW_HEIGHT,
    MIN_WIDTH,
    MIN_HEIGHT,
    LIGHT_COLORS,
    DARK_COLORS,
    darken_color,
)
from .components import (
    add_background_decoration,
    create_header,
    create_theme_toggle,
    create_pipeline_card,
    create_step_row,
)
from .executor import ScriptExecutor
from .pipelines import GaugePipeline, RadarPipeline

# Set appearance and theme
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class ModernApp(ctk.CTk):
    """
    Main application window.
    
    Manages:
    - Theme switching (dark/light mode)
    - Navigation between main menu and pipeline views
    - Pipeline instances for gauge and radar
    - Script execution via executor
    """
    
    def __init__(self):
        super().__init__()
        
        # Window configuration
        self.title(APP_TITLE)
        self.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
        self.minsize(MIN_WIDTH, MIN_HEIGHT)
        
        # Theme state
        self.is_dark_mode = True
        self.colors = DARK_COLORS.copy()
        
        # Application state
        self.current_pipeline: Optional[str] = None
        self.output_dir: Optional[str] = None
        self.selected_date: Optional[str] = None
        
        # Initialize executor
        self.executor = ScriptExecutor(self, self.colors)
        
        # Initialize pipelines
        self.pipelines = {
            "gauge": GaugePipeline(self),
            "radar": RadarPipeline(self),
        }
        
        # Configure grid
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Show main menu
        self.show_main_menu()
    
    def toggle_theme(self) -> None:
        """Toggle between dark and light mode."""
        self.is_dark_mode = not self.is_dark_mode
        
        if self.is_dark_mode:
            self.colors = DARK_COLORS.copy()
            ctk.set_appearance_mode("dark")
        else:
            self.colors = LIGHT_COLORS.copy()
            ctk.set_appearance_mode("light")
        
        # Update executor colors
        self.executor.colors = self.colors
        
        # Refresh current view
        if self.current_pipeline:
            self.show_pipeline_steps()
        else:
            self.show_main_menu()
    
    def clear_window(self) -> None:
        """Clear all widgets from window."""
        for widget in self.winfo_children():
            widget.destroy()
    
    def show_main_menu(self) -> None:
        """Display the main menu."""
        self.clear_window()
        self.current_pipeline = None
        
        # Main container
        main_frame = ctk.CTkFrame(self, fg_color=self.colors["background"], corner_radius=0)
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        
        # Decorative background elements
        add_background_decoration(main_frame, self.colors)
        
        # Header
        header = create_header(
            main_frame,
            title="🌧 MOATA-RETRIEVER",
            subtitle=APP_SUBTITLE,
            colors=self.colors,
            height=80,
        )
        
        # Theme toggle
        create_theme_toggle(header, self.is_dark_mode, self.colors, self.toggle_theme)
        
        # Content area
        content = ctk.CTkFrame(main_frame, fg_color="transparent")
        content.grid(row=1, column=0, sticky="nsew", padx=40, pady=20)
        content.grid_columnconfigure((0, 1), weight=1)
        content.grid_rowconfigure(1, weight=1)
        
        # Welcome message
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
        gauge = self.pipelines["gauge"]
        create_pipeline_card(
            cards_frame,
            column=0,
            icon=gauge.icon,
            title=f"{gauge.name} Pipeline",
            subtitle=gauge.subtitle,
            features=gauge.features,
            color=gauge.color,
            colors=self.colors,
            command=lambda: self.start_pipeline("gauge")
        )
        
        # Rain Radar Card
        radar = self.pipelines["radar"]
        create_pipeline_card(
            cards_frame,
            column=1,
            icon=radar.icon,
            title=f"{radar.name} Pipeline",
            subtitle=radar.subtitle,
            features=radar.features,
            color=radar.color,
            colors=self.colors,
            command=lambda: self.start_pipeline("radar")
        )
        
        # Footer
        footer = ctk.CTkFrame(main_frame, fg_color=self.colors["border"], corner_radius=0, height=32)
        footer.grid(row=2, column=0, sticky="ew")
        footer.grid_propagate(False)
        
        footer_text = ctk.CTkLabel(
            footer,
            text=f"Version {APP_VERSION}  •  {datetime.now().strftime('%Y-%m-%d')}  •  {APP_FOOTER}",
            font=ctk.CTkFont(size=10),
            text_color=self.colors["text_secondary"]
        )
        footer_text.place(relx=0.5, rely=0.5, anchor="center")
    
    def start_pipeline(self, pipeline_type: str) -> None:
        """
        Start a pipeline.
        
        Args:
            pipeline_type: "gauge" or "radar"
        """
        self.current_pipeline = pipeline_type
        self.selected_date = None  # Reset date for new pipeline
        self.show_pipeline_steps()
    
    def show_pipeline_steps(self) -> None:
        """Display the pipeline steps screen."""
        self.clear_window()
        
        pipeline = self.pipelines[self.current_pipeline]
        
        # Main container
        main_frame = ctk.CTkFrame(self, fg_color=self.colors["background"], corner_radius=0)
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        
        # Decorative background elements
        add_background_decoration(main_frame, self.colors)
        
        # Header
        header = ctk.CTkFrame(
            main_frame,
            fg_color=pipeline.color,
            corner_radius=0,
            height=90
        )
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False)
        
        title_label = ctk.CTkLabel(
            header,
            text=f"{pipeline.icon}  {pipeline.name} Pipeline",
            font=ctk.CTkFont(size=26, weight="bold"),
            text_color="white"
        )
        title_label.place(relx=0.5, rely=0.5, anchor="center")
        
        # Theme toggle
        create_theme_toggle(
            header,
            self.is_dark_mode,
            self.colors,
            self.toggle_theme,
            hover_color=darken_color(pipeline.color)
        )
        
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
        
        # Create step rows
        steps = pipeline.get_steps()
        for i, (number, name, desc, command, color) in enumerate(steps):
            create_step_row(
                steps_card,
                number=number,
                name=name,
                description=desc,
                color=color,
                colors=self.colors,
                command=command,
                is_last=(i == len(steps) - 1)
            )
        
        # Back button
        back_btn = ctk.CTkButton(
            content,
            text="←  Back to Main Menu",
            font=ctk.CTkFont(size=13),
            fg_color="transparent",
            hover_color=self.colors["border"],
            text_color=pipeline.color,
            corner_radius=8,
            height=40,
            command=self.show_main_menu
        )
        back_btn.pack(anchor="w", pady=(10, 0))


def main():
    """Application entry point."""
    app = ModernApp()
    app.mainloop()


if __name__ == "__main__":
    main()