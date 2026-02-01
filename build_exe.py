#!/usr/bin/env python3
"""
Build Script for MOATA AlertLab Executable

Creates a standalone executable using PyInstaller.

Usage:
    python build_exe.py

Requirements:
    pip install pyinstaller

Output:
    dist/MOATA_AlertLab.exe

Author: Auckland Council Internship Team (COMPSCI 778)
Created: 2026-02-01
"""

import subprocess
import sys
import shutil
from pathlib import Path


def check_pyinstaller():
    """Check if PyInstaller is installed."""
    try:
        import PyInstaller
        print(f"[OK] PyInstaller {PyInstaller.__version__} found")
        return True
    except ImportError:
        print("[!] PyInstaller not found")
        print("    Installing PyInstaller...")
        subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller"], check=True)
        return True


def clean_build():
    """Clean previous build artifacts."""
    print("\n[1/4] Cleaning previous build...")
    
    dirs_to_clean = ['dist']  # Skip 'build' folder to avoid permission issues
    for dir_name in dirs_to_clean:
        dir_path = Path(dir_name)
        if dir_path.exists():
            try:
                shutil.rmtree(dir_path)
                print(f"      Removed {dir_name}/")
            except PermissionError:
                print(f"      [WARN] Could not remove {dir_name}/ (in use)")
    
    # Clean .pyc files
    for pyc in Path('.').rglob('*.pyc'):
        try:
            pyc.unlink()
        except PermissionError:
            pass
    
    print("      Done!")


def build_executable():
    """Build the executable using PyInstaller."""
    print("\n[2/4] Building executable...")
    print("      This may take several minutes...")
    
    result = subprocess.run(
        [sys.executable, "-m", "PyInstaller", "moata_alertlab.spec", "--noconfirm"],
        capture_output=False
    )
    
    if result.returncode != 0:
        print("\n[ERROR] Build failed!")
        return False
    
    print("      Done!")
    return True


def copy_required_files():
    """Copy required files to dist folder."""
    print("\n[3/4] Copying required files...")
    
    dist_dir = Path('dist')
    
    # Create outputs folder (empty, for runtime use)
    outputs_dir = dist_dir / 'outputs'
    outputs_dir.mkdir(exist_ok=True)
    (outputs_dir / 'rain_gauges').mkdir(exist_ok=True)
    (outputs_dir / 'rain_radar').mkdir(exist_ok=True)
    print("      Created outputs/ folder structure")
    
    # Create data folder
    data_dir = dist_dir / 'data'
    data_dir.mkdir(exist_ok=True)
    
    # Copy .env.example if exists
    env_example = Path('.env.example')
    if env_example.exists():
        shutil.copy(env_example, dist_dir / '.env.example')
        print("      Copied .env.example")
    
    # Create empty .env template
    env_template = dist_dir / '.env.template'
    with open(env_template, 'w') as f:
        f.write("""# MOATA AlertLab Configuration
# Rename this file to .env and fill in your credentials

# Moata API Credentials (required)
MOATA_CLIENT_ID=your_client_id_here
MOATA_CLIENT_SECRET=your_client_secret_here

# Optional settings
# MOATA_PROJECT_ID=594
# MOATA_REQUESTS_PER_SECOND=2.0
""")
    print("      Created .env.template")
    
    print("      Done!")
    return True


def show_summary():
    """Show build summary."""
    print("\n[4/4] Build Summary")
    print("=" * 60)
    
    exe_path = Path('dist/MOATA_AlertLab.exe')
    if exe_path.exists():
        size_mb = exe_path.stat().st_size / (1024 * 1024)
        print(f"[OK] Executable created successfully!")
        print(f"     Location: {exe_path.absolute()}")
        print(f"     Size: {size_mb:.1f} MB")
        print()
        print("To run the application:")
        print(f"  1. Copy the 'dist' folder to your desired location")
        print(f"  2. Create a .env file with your API credentials")
        print(f"  3. Run MOATA_AlertLab.exe")
    else:
        print("[ERROR] Executable not found!")
        print("        Check build output for errors.")
    
    print("=" * 60)


def main():
    """Main build process."""
    print("=" * 60)
    print("MOATA AlertLab - Build Executable")
    print("=" * 60)
    
    # Check PyInstaller
    if not check_pyinstaller():
        return 1
    
    # Clean previous build
    clean_build()
    
    # Build executable
    if not build_executable():
        return 1
    
    # Copy required files
    copy_required_files()
    
    # Show summary
    show_summary()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
