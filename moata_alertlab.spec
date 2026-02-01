# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller Spec File for MOATA AlertLab

Creates a single executable containing:
- GUI Application
- All pipeline scripts
- Data files (tp108_stats.csv, raingauge_ari_alarms.csv)
- Database template

Build command:
    pyinstaller moata_alertlab.spec

Author: Auckland Council Internship Team (COMPSCI 778)
Created: 2026-02-01
"""

import sys
from pathlib import Path

block_cipher = None

# Project root
project_root = Path('.').resolve()

# Collect all Python files from scripts/
scripts_data = []
for script_dir in ['scripts/gauge', 'scripts/radar', 'scripts/alarms']:
    script_path = project_root / script_dir
    if script_path.exists():
        for py_file in script_path.glob('*.py'):
            # Include as data file so they can be executed
            scripts_data.append((str(py_file), script_dir))

# Add the main scripts folder files
scripts_root = project_root / 'scripts'
for py_file in scripts_root.glob('*.py'):
    scripts_data.append((str(py_file), 'scripts'))

# Data files to include
data_files = [
    # Static data files
    ('data/inputs/tp108_stats.csv', 'data/inputs'),
    ('data/inputs/raingauge_ari_alarms.csv', 'data/inputs'),
]

# Hidden imports that PyInstaller might miss
hidden_imports = [
    'pandas',
    'numpy',
    'requests',
    'customtkinter',
    'matplotlib',
    'matplotlib.backends.backend_tkagg',
    'shapely',
    'shapely.geometry',
    'pyproj',
    'PIL',
    'PIL._tkinter_finder',
    'tkinter',
    'tkinter.messagebox',
    'tkinter.filedialog',
    'sqlite3',
    'json',
    'csv',
    'datetime',
    'pathlib',
    'subprocess',
    'threading',
    'queue',
    'logging',
    'argparse',
    'urllib3',
    'certifi',
    'dotenv',
    'dateutil',
    'pytz',
    'tzdata',
    # Project modules
    'moata_pipeline',
    'moata_pipeline.common',
    'moata_pipeline.common.database',
    'moata_pipeline.common.config',
    'moata_pipeline.common.paths',
    'moata_pipeline.collect',
    'moata_pipeline.analyze',
    'moata_pipeline.viz',
    'moata_pipeline.alarms',
    'moata_pipeline.moata',
    'moata_alert_lab_gui',
    'moata_alert_lab_gui.pipelines',
    'moata_alert_lab_gui.pipelines.gauge',
    'moata_alert_lab_gui.pipelines.radar',
]

a = Analysis(
    ['moata_alert_lab.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=data_files + scripts_data,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'pytest',
        'sphinx',
        'setuptools',
        'wheel',
        'pip',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='MOATA_AlertLab',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # Set to True if you want console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # Add icon path here: icon='assets/icon.ico'
)
