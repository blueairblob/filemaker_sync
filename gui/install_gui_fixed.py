#!/bin/env python3
# FILE: install_gui_fixed.py
"""
Complete Installation Script for Fixed FileMaker Sync GUI
This script will install missing dependencies and set up the corrected GUI files
"""

import sys
import os
import subprocess
from pathlib import Path

def install_missing_dependency():
    """Install the missing tomli-w dependency"""
    print("Installing missing dependency: tomli-w")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'tomli-w'])
        print("✓ tomli-w installed successfully")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install tomli-w")
        print("Please run manually: pip install tomli-w")
        return False

def create_gui_directory():
    """Create the gui directory if it doesn't exist"""
    gui_dir = Path('gui')
    if not gui_dir.exists():
        gui_dir.mkdir()
        print("✓ Created gui/ directory")
    else:
        print("✓ gui/ directory exists")
    return gui_dir

def create_init_file(gui_dir):
    """Create __init__.py file in gui directory"""
    init_file = gui_dir / '__init__.py'
    if not init_file.exists():
        with open(init_file, 'w') as f:
            f.write('# GUI Package for FileMaker Sync\n')
        print("✓ Created gui/__init__.py")
    else:
        print("✓ gui/__init__.py exists")

def instructions_for_user():
    """Print instructions for the user"""
    print("\n" + "="*60)
    print("INSTALLATION COMPLETE!")
    print("="*60)
    print("\n📋 Next Steps:")
    print("\n1. Copy the following files to your project:")
    print("   • gui/gui_widgets.py (from artifact above)")
    print("   • gui/gui_operations.py (from artifact above)")  
    print("   • gui/filemaker_gui.py (from artifact above)")
    print("\n2. Make sure your config.toml file is properly configured")
    print("\n3. Run the GUI with:")
    print("   python gui/filemaker_gui.py")
    print("\n🔧 If you still get connection errors:")
    print("   • Verify your config.toml has correct database settings")
    print("   • Test the CLI first: python filemaker_extract_refactored.py --info-only")
    print("   • Check that FileMaker Pro is running and ODBC is enabled")
    print("\n💡 The Activity section has been removed as requested.")
    print("   Click the clock icon (🕒) in the header to view activity logs.")

def main():
    """Main installation function"""
    print("FileMaker Sync GUI - Fixed Version Installation")
    print("=" * 50)
    
    # Check Python version
    if sys.version_info < (3, 9):
        print("❌ Error: Python 3.9 or higher is required")
        sys.exit(1)
    
    print(f"✓ Python {sys.version_info.major}.{sys.version_info.minor}")
    
    # Install missing dependency
    print("\nInstalling dependencies...")
    if not install_missing_dependency():
        print("\n⚠ Dependency installation failed, but you can continue.")
        print("You may need to install tomli-w manually for config editing.")
    
    # Create GUI directory structure
    print("\nSetting up directory structure...")
    gui_dir = create_gui_directory()
    create_init_file(gui_dir)
    
    # Check for existing files
    print("\nChecking existing files...")
    required_files = [
        'config.toml',
        'filemaker_extract_refactored.py',
        'config_manager.py',
        'database_connections.py',
        'data_exporter.py',
        'gui/gui_logging.py',
        'gui/gui_logviewer.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"✓ {file_path}")
        else:
            missing_files.append(file_path)
            print(f"⚠ {file_path} - MISSING")
    
    if missing_files:
        print(f"\n⚠ Missing files: {len(missing_files)}")
        print("These files should be present from your existing installation.")
    
    # Create directories
    directories = ['logs', 'exports', 'exports/images/jpg', 'exports/images/webp']
    print("\nCreating required directories...")
    for directory in directories:
        dir_path = Path(directory)
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✓ Created {directory}")
        else:
            print(f"✓ {directory} exists")
    
    # Show final instructions
    instructions_for_user()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInstallation cancelled by user")
    except Exception as e:
        print(f"\nInstallation error: {e}")
        sys.exit(1)