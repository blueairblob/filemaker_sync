#!/usr/bin/env python3
"""
Setup and Launch Script for Enhanced FileMaker Sync GUI
Handles initialization, dependency checking, and GUI launch
"""

import sys
import os
from pathlib import Path
import subprocess
import importlib.util

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 9):
        print("❌ Error: Python 3.9 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    
    print(f"✓ Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    return True

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        'pandas', 'pyodbc', 'sqlalchemy', 'pillow', 'tqdm', 'tomli'
    ]
    
    missing = []
    for package in required_packages:
        spec = importlib.util.find_spec(package)
        if spec is None:
            missing.append(package)
        else:
            print(f"✓ {package}")
    
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        print("To install missing packages, run:")
        print(f"pip install {' '.join(missing)}")
        return False
    
    return True

def check_files():
    """Check if required files are present"""
    required_files = [
        'config.toml',
        'filemaker_extract_refactored.py',
        'config_manager.py',
        'database_connections.py',
        'data_exporter.py',
        'gui/gui_logging.py',
        'gui/gui_widgets.py',
        'gui/gui_operations.py',
        'gui/gui_logviewer.py'
    ]
    
    missing = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing.append(file_path)
        else:
            print(f"✓ {file_path}")
    
    if missing:
        print(f"❌ Missing files: {', '.join(missing)}")
        return False
    
    return True

def create_directories():
    """Create required directories"""
    directories = ['logs', 'exports', 'exports/images', 'exports/images/jpg', 'exports/images/webp']
    
    for directory in directories:
        dir_path = Path(directory)
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✓ Created directory: {directory}")
        else:
            print(f"✓ Directory exists: {directory}")

def check_config():
    """Check if config.toml exists and is valid"""
    config_file = Path('config.toml')
    if not config_file.exists():
        print("❌ config.toml not found")
        print("Create a config.toml file with your database settings")
        
        # Offer to create a sample config
        response = input("Would you like to create a sample config.toml? (y/n): ")
        if response.lower() == 'y':
            create_sample_config()
        return False
    
    try:
        import tomli
        with open(config_file, 'rb') as f:
            config = tomli.load(f)
        
        # Check for required sections
        required_sections = ['database', 'export']
        for section in required_sections:
            if section not in config:
                print(f"❌ Missing config section: {section}")
                return False
        
        print("✓ config.toml is valid")
        return True
        
    except Exception as e:
        print(f"❌ Error reading config.toml: {e}")
        return False

def create_sample_config():
    """Create a sample configuration file"""
    sample_config = '''# FileMaker Sync Tool Configuration

[gui]
api_host = "http://localhost"
api_port = 5000
api_timeout = 5000
api_health_check_interval = 30000

[export]
path = './exports'
prefix = 'filemaker'
image_formats_supported = ['jpg', 'webp']
image_path = 'images'

[database]

  [database.source] 
    host = '127.0.0.1'
    dsn  = 'FileMaker_DSN'  # Your FileMaker ODBC DSN name
    user = 'your_username'
    pwd  = 'your_password' 
    port = ''
    type = 'odbc'
    name = ['fmp', 'FileMaker Pro']
    schema = ['FileMaker_Tables', 'FileMaker_Fields', 'FileMaker_BaseTableFields']

  [database.target]
    dsn = 'postgres'
    db = 'supabase'
    dt = '%Y%m%d %H:%M:%S'
    type = 'url'
    host = 'your-project.supabase.co'  # Your Supabase host
    schema = ['migration_schema', 'target_schema']
    mig_schema = 0
    tgt_schema = 1
    user = 'migration_user'
    
  [database.target.migration_schema.pk]
    your_table1 = ['column1']
    your_table2 = ['column1', 'column2']

  [database.target.supabase]
    name = ['supabase', 'Supabase']
    user = 'postgres.your_project_id'
    pwd = 'your_password'
    port = '5432'
'''
    
    try:
        with open('config.toml', 'w') as f:
            f.write(sample_config)
        print("✓ Sample config.toml created")
        print("Please edit config.toml with your actual database settings")
        return True
    except Exception as e:
        print(f"❌ Error creating sample config: {e}")
        return False

def launch_gui():
    """Launch the GUI application"""
    try:
        # Import and run the GUI
        from gui.gui_filemaker import main as gui_main
        print("🚀 Launching FileMaker Sync Dashboard...")
        gui_main()
    except ImportError as e:
        print(f"❌ Error importing GUI modules: {e}")
        print("Make sure all GUI files are present in the gui/ directory")
        return False
    except Exception as e:
        print(f"❌ Error launching GUI: {e}")
        return False

def main():
    """Main setup and launch function"""
    print("FileMaker Sync GUI - Setup and Launch")
    print("=" * 40)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    print("\nChecking dependencies...")
    if not check_dependencies():
        print("\nPlease install missing dependencies and try again.")
        sys.exit(1)
    
    print("\nChecking required files...")
    if not check_files():
        print("\nPlease ensure all required files are present.")
        sys.exit(1)
    
    print("\nCreating directories...")
    create_directories()
    
    print("\nChecking configuration...")
    if not check_config():
        print("\nPlease configure config.toml and try again.")
        sys.exit(1)
    
    print("\n" + "=" * 40)
    print("✓ All checks passed! Ready to launch GUI")
    print("=" * 40)
    
    # Launch the GUI
    launch_gui()

if __name__ == "__main__":
    main()