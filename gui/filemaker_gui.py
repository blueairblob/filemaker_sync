#!/usr/bin/env python3
"""
Enhanced FileMaker Sync GUI - Main Application
Complete modular replacement for filemaker_gui.py
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os
import sys
from pathlib import Path
from datetime import datetime
import threading
import subprocess

# Import our modules
from gui_logging import LogManager, LogLevel
from gui_widgets import StatusCard, MigrationOverview, QuickActions, RecentActivity, StatusBar
from gui_operations import OperationManager, ConnectionTester, StatusManager
from gui_logviewer import LogViewerWindow, LogStatsWindow

class ConfigurationWindow:
    """Configuration management window"""
    
    def __init__(self, parent, config: dict, on_save_callback=None):
        self.parent = parent
        self.config = config.copy()
        self.on_save_callback = on_save_callback
        
        self.window = tk.Toplevel(parent)
        self.window.title("Configuration Settings")
        self.window.geometry("600x500")
        self.window.transient(parent)
        self.window.grab_set()
        
        self.create_widgets()
        self.load_config_values()
    
    def create_widgets(self):
        """Create configuration interface"""
        main_frame = ttk.Frame(self.window)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Create notebook for different config sections
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill='both', expand=True, pady=(0, 10))
        
        # Source Database tab
        source_frame = ttk.Frame(notebook)
        notebook.add(source_frame, text="Source Database")
        self.create_source_tab(source_frame)
        
        # Target Database tab
        target_frame = ttk.Frame(notebook)
        notebook.add(target_frame, text="Target Database")
        self.create_target_tab(target_frame)
        
        # Export Settings tab
        export_frame = ttk.Frame(notebook)
        notebook.add(export_frame, text="Export Settings")
        self.create_export_tab(export_frame)
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x')
        
        ttk.Button(button_frame, text="Cancel", command=self.window.destroy).pack(side='right', padx=(5, 0))
        ttk.Button(button_frame, text="Save", command=self.save_config).pack(side='right')
        ttk.Button(button_frame, text="Test Connections", command=self.test_connections).pack(side='left')
    
    def create_source_tab(self, parent):
        """Create source database configuration tab"""
        # FileMaker settings
        fm_frame = ttk.LabelFrame(parent, text="FileMaker Pro Settings", padding=10)
        fm_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(fm_frame, text="DSN Name:").grid(row=0, column=0, sticky='w', pady=2)
        self.fm_dsn_var = tk.StringVar()
        ttk.Entry(fm_frame, textvariable=self.fm_dsn_var, width=30).grid(row=0, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        ttk.Label(fm_frame, text="Username:").grid(row=1, column=0, sticky='w', pady=2)
        self.fm_user_var = tk.StringVar()
        ttk.Entry(fm_frame, textvariable=self.fm_user_var, width=30).grid(row=1, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        ttk.Label(fm_frame, text="Password:").grid(row=2, column=0, sticky='w', pady=2)
        self.fm_pwd_var = tk.StringVar()
        ttk.Entry(fm_frame, textvariable=self.fm_pwd_var, width=30, show='*').grid(row=2, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        fm_frame.columnconfigure(1, weight=1)
    
    def create_target_tab(self, parent):
        """Create target database configuration tab"""
        # Supabase settings
        sb_frame = ttk.LabelFrame(parent, text="Supabase Settings", padding=10)
        sb_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(sb_frame, text="Host:").grid(row=0, column=0, sticky='w', pady=2)
        self.sb_host_var = tk.StringVar()
        ttk.Entry(sb_frame, textvariable=self.sb_host_var, width=40).grid(row=0, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        ttk.Label(sb_frame, text="Database:").grid(row=1, column=0, sticky='w', pady=2)
        self.sb_db_var = tk.StringVar()
        ttk.Entry(sb_frame, textvariable=self.sb_db_var, width=40).grid(row=1, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        ttk.Label(sb_frame, text="Username:").grid(row=2, column=0, sticky='w', pady=2)
        self.sb_user_var = tk.StringVar()
        ttk.Entry(sb_frame, textvariable=self.sb_user_var, width=40).grid(row=2, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        ttk.Label(sb_frame, text="Password:").grid(row=3, column=0, sticky='w', pady=2)
        self.sb_pwd_var = tk.StringVar()
        ttk.Entry(sb_frame, textvariable=self.sb_pwd_var, width=40, show='*').grid(row=3, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        ttk.Label(sb_frame, text="Port:").grid(row=4, column=0, sticky='w', pady=2)
        self.sb_port_var = tk.StringVar()
        ttk.Entry(sb_frame, textvariable=self.sb_port_var, width=40).grid(row=4, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        sb_frame.columnconfigure(1, weight=1)
    
    def create_export_tab(self, parent):
        """Create export settings configuration tab"""
        # Export settings
        exp_frame = ttk.LabelFrame(parent, text="Export Settings", padding=10)
        exp_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(exp_frame, text="Export Path:").grid(row=0, column=0, sticky='w', pady=2)
        path_frame = ttk.Frame(exp_frame)
        path_frame.grid(row=0, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        self.export_path_var = tk.StringVar()
        ttk.Entry(path_frame, textvariable=self.export_path_var, width=35).pack(side='left', fill='x', expand=True)
        ttk.Button(path_frame, text="Browse", command=self.browse_export_path, width=8).pack(side='right', padx=(5, 0))
        
        ttk.Label(exp_frame, text="Prefix:").grid(row=1, column=0, sticky='w', pady=2)
        self.export_prefix_var = tk.StringVar()
        ttk.Entry(exp_frame, textvariable=self.export_prefix_var, width=30).grid(row=1, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        # Image formats
        ttk.Label(exp_frame, text="Image Formats:").grid(row=2, column=0, sticky='w', pady=2)
        format_frame = ttk.Frame(exp_frame)
        format_frame.grid(row=2, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        self.jpg_var = tk.BooleanVar()
        self.webp_var = tk.BooleanVar()
        ttk.Checkbutton(format_frame, text="JPG", variable=self.jpg_var).pack(side='left')
        ttk.Checkbutton(format_frame, text="WebP", variable=self.webp_var).pack(side='left', padx=(10, 0))
        
        exp_frame.columnconfigure(1, weight=1)
    
    def load_config_values(self):
        """Load current configuration values into widgets"""
        # Source database
        self.fm_dsn_var.set(self.config.get('filemaker_dsn', ''))
        self.fm_user_var.set(self.config.get('filemaker_user', ''))
        self.fm_pwd_var.set(self.config.get('filemaker_password', ''))
        
        # Target database
        self.sb_host_var.set(self.config.get('supabase_host', ''))
        self.sb_db_var.set(self.config.get('supabase_db', ''))
        self.sb_user_var.set(self.config.get('supabase_user', ''))
        self.sb_pwd_var.set(self.config.get('supabase_password', ''))
        self.sb_port_var.set(self.config.get('supabase_port', '5432'))
        
        # Export settings
        self.export_path_var.set(self.config.get('export_path', ''))
        self.export_prefix_var.set(self.config.get('export_prefix', 'rat'))
        
        # Image formats
        formats = self.config.get('image_formats', ['jpg'])
        self.jpg_var.set('jpg' in formats)
        self.webp_var.set('webp' in formats)
    
    def browse_export_path(self):
        """Browse for export path"""
        path = filedialog.askdirectory(title="Select Export Directory")
        if path:
            self.export_path_var.set(path)
    
    def save_config(self):
        """Save configuration changes"""
        # Update config dictionary
        self.config.update({
            'filemaker_dsn': self.fm_dsn_var.get(),
            'filemaker_user': self.fm_user_var.get(),
            'filemaker_password': self.fm_pwd_var.get(),
            'supabase_host': self.sb_host_var.get(),
            'supabase_db': self.sb_db_var.get(),
            'supabase_user': self.sb_user_var.get(),
            'supabase_password': self.sb_pwd_var.get(),
            'supabase_port': self.sb_port_var.get(),
            'export_path': self.export_path_var.get(),
            'export_prefix': self.export_prefix_var.get(),
            'image_formats': []
        })
        
        # Add selected image formats
        if self.jpg_var.get():
            self.config['image_formats'].append('jpg')
        if self.webp_var.get():
            self.config['image_formats'].append('webp')
        
        # Call save callback
        if self.on_save_callback:
            self.on_save_callback(self.config)
        
        messagebox.showinfo("Success", "Configuration saved successfully!")
        self.window.destroy()
    
    def test_connections(self):
        """Test both database connections"""
        messagebox.showinfo("Test Connections", "Connection testing not implemented in config window.")

class DiagnosticEngine:
    """Enhanced diagnostic system for troubleshooting"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
    
    def run_full_diagnostics(self) -> dict:
        """Run comprehensive system diagnostics"""
        self.log_manager.log(LogLevel.INFO, "Diagnostics", "Starting full system diagnostics")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'overall_health': 'unknown',
            'checks': {}
        }
        
        # Run individual checks
        checks = [
            ('python_version', self.check_python_version),
            ('dependencies', self.check_dependencies),
            ('config_files', self.check_config_files),
            ('directories', self.check_directories),
            ('file_permissions', self.check_file_permissions),
            ('database_files', self.check_database_files)
        ]
        
        total_passed = 0
        for check_name, check_func in checks:
            try:
                result = check_func()
                results['checks'][check_name] = result
                if result['status'] == 'pass':
                    total_passed += 1
            except Exception as e:
                results['checks'][check_name] = {
                    'status': 'error',
                    'message': f"Check failed: {e}",
                    'severity': 'high'
                }
        
        # Determine overall health
        pass_rate = total_passed / len(checks)
        if pass_rate >= 0.9:
            results['overall_health'] = 'excellent'
        elif pass_rate >= 0.7:
            results['overall_health'] = 'good'
        elif pass_rate >= 0.5:
            results['overall_health'] = 'fair'
        else:
            results['overall_health'] = 'poor'
        
        self.log_manager.log(LogLevel.INFO, "Diagnostics", f"Diagnostics completed - Health: {results['overall_health']}")
        return results
    
    def check_python_version(self) -> dict:
        """Check Python version compatibility"""
        import sys
        version = sys.version_info
        
        if version.major == 3 and version.minor >= 9:
            return {
                'status': 'pass',
                'message': f"Python {version.major}.{version.minor}.{version.micro} - Compatible",
                'severity': 'info'
            }
        else:
            return {
                'status': 'fail',
                'message': f"Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.9+",
                'severity': 'high'
            }
    
    def check_dependencies(self) -> dict:
        """Check required Python packages"""
        required = ['pandas', 'pyodbc', 'sqlalchemy', 'pillow', 'tqdm', 'tomli']
        missing = []
        
        for package in required:
            try:
                __import__(package)
            except ImportError:
                missing.append(package)
        
        if not missing:
            return {
                'status': 'pass',
                'message': f"All {len(required)} required packages are installed",
                'severity': 'info'
            }
        else:
            return {
                'status': 'fail',
                'message': f"Missing packages: {', '.join(missing)}",
                'severity': 'high',
                'recommendation': f"Install with: pip install {' '.join(missing)}"
            }
    
    def check_config_files(self) -> dict:
        """Check configuration files"""
        config_file = Path('config.toml')
        
        if not config_file.exists():
            return {
                'status': 'fail',
                'message': "config.toml not found",
                'severity': 'high',
                'recommendation': "Create config.toml file with database settings"
            }
        
        try:
            import tomli
            with open(config_file, 'rb') as f:
                config = tomli.load(f)
            
            required_sections = ['database.source', 'database.target', 'export']
            missing_sections = []
            
            for section in required_sections:
                keys = section.split('.')
                current = config
                for key in keys:
                    if key not in current:
                        missing_sections.append(section)
                        break
                    current = current[key]
            
            if missing_sections:
                return {
                    'status': 'fail',
                    'message': f"Missing config sections: {', '.join(missing_sections)}",
                    'severity': 'medium'
                }
            else:
                return {
                    'status': 'pass',
                    'message': "Configuration file is valid",
                    'severity': 'info'
                }
        
        except Exception as e:
            return {
                'status': 'fail',
                'message': f"Config file error: {e}",
                'severity': 'high'
            }
    
    def check_directories(self) -> dict:
        """Check required directories"""
        required_dirs = ['logs', 'exports', 'exports/images/jpg', 'exports/images/webp']
        missing_dirs = []
        
        for dir_path in required_dirs:
            if not Path(dir_path).exists():
                missing_dirs.append(dir_path)
        
        if missing_dirs:
            return {
                'status': 'warning',
                'message': f"Missing directories: {', '.join(missing_dirs)}",
                'severity': 'low',
                'recommendation': "Directories will be created automatically when needed"
            }
        else:
            return {
                'status': 'pass',
                'message': "All required directories exist",
                'severity': 'info'
            }
    
    def check_file_permissions(self) -> dict:
        """Check file system permissions"""
        test_file = Path('permission_test.tmp')
        
        try:
            # Test write permissions
            with open(test_file, 'w') as f:
                f.write('test')
            
            # Test read permissions
            with open(test_file, 'r') as f:
                content = f.read()
            
            # Clean up
            test_file.unlink()
            
            return {
                'status': 'pass',
                'message': "File system permissions are adequate",
                'severity': 'info'
            }
        
        except Exception as e:
            return {
                'status': 'fail',
                'message': f"Permission error: {e}",
                'severity': 'medium',
                'recommendation': "Run as administrator or check file permissions"
            }
    
    def check_database_files(self) -> dict:
        """Check for database-related files"""
        core_files = ['filemaker_extract_refactored.py', 'config_manager.py', 'database_connections.py']
        missing_files = []
        
        for file_path in core_files:
            if not Path(file_path).exists():
                missing_files.append(file_path)
        
        if missing_files:
            return {
                'status': 'fail',
                'message': f"Missing core files: {', '.join(missing_files)}",
                'severity': 'high'
            }
        else:
            return {
                'status': 'pass',
                'message': "All core database files are present",
                'severity': 'info'
            }

class FileMakerSyncGUI:
    """Enhanced FileMaker Sync GUI - Main Application Class"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("FileMaker Sync Dashboard")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        
        # Initialize core systems
        self.log_manager = LogManager()
        self.operation_manager = OperationManager(self.log_manager)
        self.connection_tester = ConnectionTester(self.operation_manager)
        self.status_manager = StatusManager(self.operation_manager)
        self.diagnostic_engine = DiagnosticEngine(self.log_manager)
        
        # Configuration
        self.config_file = Path("sync_config.json")
        self.config = self.load_config()
        
        # Windows
        self.log_viewer_window = None
        self.log_stats_window = None
        self.config_window = None
        
        # Initialize GUI
        self.create_widgets()
        self.setup_bindings()
        self.setup_callbacks()
        
        # Start auto-refresh
        self.auto_refresh()
        
        # Log startup
        self.log_manager.log(LogLevel.INFO, "Application", "FileMaker Sync Dashboard started")
    
    def create_widgets(self):
        """Create the main dashboard layout"""
        # Configure root
        self.root.configure(bg='#f0f0f0')
        
        # Create menu bar
        self.create_menu_bar()
        
        # Main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Header
        self.create_header(main_container)
        
        # Connection status row
        self.create_connection_status(main_container)
        
        # Main content area
        self.create_main_content(main_container)
        
        # Status bar
        self.status_bar = StatusBar(main_container, self.log_manager.session_id)
        self.status_bar.pack(side='bottom', fill='x', pady=(10, 0))
    
    def create_header(self, parent):
        """Create header section"""
        header_frame = ttk.Frame(parent)
        header_frame.pack(fill='x', pady=(0, 20))
        
        title_label = ttk.Label(header_frame, text="FileMaker Sync Dashboard", 
                               font=('Arial', 20, 'bold'))
        title_label.pack(side='left')
        
        subtitle_label = ttk.Label(header_frame, 
                                  text="Monitor and manage your FileMaker to Supabase migration",
                                  font=('Arial', 10))
        subtitle_label.pack(side='left', padx=(20, 0))
    
    def create_connection_status(self, parent):
        """Create connection status section"""
        conn_frame = ttk.Frame(parent)
        conn_frame.pack(fill='x', pady=(0, 20))
        
        # FileMaker status card
        fm_card_frame = ttk.LabelFrame(conn_frame, text=" FileMaker Pro ", padding=10)
        fm_card_frame.pack(side='left', fill='x', expand=True, padx=(0, 10))
        
        self.fm_status_card = StatusCard(fm_card_frame, "FileMaker Pro")
        self.fm_status_card.pack(fill='x')
        
        # Target status card
        target_card_frame = ttk.LabelFrame(conn_frame, text=" Supabase Target ", padding=10)
        target_card_frame.pack(side='right', fill='x', expand=True, padx=(10, 0))
        
        self.target_status_card = StatusCard(target_card_frame, "Supabase Target")
        self.target_status_card.pack(fill='x')
    
    def create_main_content(self, parent):
        """Create main content area"""
        content_frame = ttk.Frame(parent)
        content_frame.pack(fill='both', expand=True)
        
        # Left side - Migration Overview (2/3 width)
        left_frame = ttk.LabelFrame(content_frame, text=" Migration Overview ", padding=10)
        left_frame.pack(side='left', fill='both', expand=True, padx=(0, 10))
        
        self.migration_overview = MigrationOverview(left_frame)
        self.migration_overview.pack(fill='both', expand=True)
        
        # Right side - Actions and Activity (1/3 width)
        right_frame = ttk.Frame(content_frame)
        right_frame.pack(side='right', fill='both', padx=(10, 0))
        
        # Quick Actions
        actions_frame = ttk.LabelFrame(right_frame, text=" Quick Actions ", padding=10)
        actions_frame.pack(fill='x', pady=(0, 10))
        
        self.quick_actions = QuickActions(actions_frame)
        self.quick_actions.pack(fill='x')
        
        # Recent Activity
        activity_frame = ttk.LabelFrame(right_frame, text=" Recent Activity ", padding=10)
        activity_frame.pack(fill='both', expand=True, pady=(10, 0))
        
        self.recent_activity = RecentActivity(activity_frame)
        self.recent_activity.pack(fill='both', expand=True)
        
        # Configure right frame width
        right_frame.configure(width=350)
        right_frame.pack_propagate(False)
    
    def create_menu_bar(self):
        """Create the menu bar"""
        self.menubar = tk.Menu(self.root)
        self.root.config(menu=self.menubar)
        
        # File menu
        file_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Configuration...", command=self.open_configuration)
        file_menu.add_separator()
        file_menu.add_command(label="Export Configuration", command=self.export_configuration)
        file_menu.add_command(label="Import Configuration", command=self.import_configuration)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # Tools menu
        tools_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="View Logs", command=self.open_log_viewer)
        tools_menu.add_command(label="Log Statistics", command=self.open_log_stats)
        tools_menu.add_separator()
        tools_menu.add_command(label="Run Diagnostics", command=self.run_diagnostics)
        tools_menu.add_separator()
        tools_menu.add_command(label="Open Export Folder", command=self.open_export_folder)
        tools_menu.add_command(label="Open Log Folder", command=self.open_log_folder)
        
        # Help menu
        help_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.show_about)
    
    def setup_bindings(self):
        """Set up event bindings"""
        # Connection test buttons
        self.fm_status_card.test_button.configure(command=self.test_filemaker_connection)
        self.target_status_card.test_button.configure(command=self.test_target_connection)
        
        # Refresh button
        self.migration_overview.refresh_button.configure(command=self.refresh_migration_status)
        
        # Quick action buttons
        actions = self.quick_actions.action_buttons
        actions['Full Sync'].configure(command=lambda: self.run_operation('full_sync'))
        actions['Incremental Sync'].configure(command=lambda: self.run_operation('incremental_sync'))
        actions['Export to Files'].configure(command=lambda: self.run_operation('export_files'))
        actions['Export Images'].configure(command=lambda: self.run_operation('export_images'))
        actions['Test Connections'].configure(command=self.test_all_connections)
        actions['View Logs'].configure(command=self.open_log_viewer)
    
    def setup_callbacks(self):
        """Set up callbacks for real-time updates"""
        # Log manager callbacks
        self.log_manager.add_callback(self.on_new_log_entry)
        
        # Operation manager callbacks
        self.operation_manager.add_operation_callback(self.on_operation_status)
    
    def on_new_log_entry(self, log_entry):
        """Handle new log entries"""
        # Add to recent activity
        self.recent_activity.add_activity(f"[{log_entry.level}] {log_entry.component}: {log_entry.message}")
        
        # Update status if it's an error
        if log_entry.level in ['ERROR', 'CRITICAL']:
            self.root.after(0, self.update_status_indicator)
    
    def on_operation_status(self, status, operation, result=None):
        """Handle operation status updates"""
        if status == 'start':
            self.quick_actions.show_progress(operation.replace('_', ' ').title())
        elif status == 'complete':
            self.quick_actions.hide_progress()
            # Refresh status after operation
            self.root.after(1000, self.refresh_migration_status)
    
    def load_config(self) -> dict:
        """Load configuration from file"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.log_manager.log(LogLevel.WARNING, "Config", f"Error loading config: {e}")
        return {}
    
    def save_config(self, config: dict = None):
        """Save configuration to file"""
        try:
            config_to_save = config or self.config
            with open(self.config_file, 'w') as f:
                json.dump(config_to_save, f, indent=4)
            self.log_manager.log(LogLevel.INFO, "Config", "Configuration saved successfully")
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Config", f"Error saving config: {e}")
    
    # Connection testing methods
    def test_filemaker_connection(self):
        """Test FileMaker connection"""
        self.connection_tester.test_filemaker_connection(self.on_connection_test_complete)
    
    def test_target_connection(self):
        """Test target database connection"""
        self.connection_tester.test_target_connection(self.on_connection_test_complete)
    
    def test_all_connections(self):
        """Test both connections"""
        self.connection_tester.test_all_connections(self.on_connection_test_complete)
    
    def on_connection_test_complete(self, connection_type, status):
        """Handle connection test completion"""
        def update_ui():
            if connection_type == 'filemaker':
                self.fm_status_card.update_status(status['connected'], status['message'])
            elif connection_type == 'target':
                self.target_status_card.update_status(status['connected'], status['message'])
            
            # Update button states
            fm_status = self.connection_tester.connection_status['filemaker']
            target_status = self.connection_tester.connection_status['target']
            self.quick_actions.update_button_states(
                fm_status['connected'], 
                target_status['connected']
            )
            
            self.update_status_indicator()
        
        self.root.after(0, update_ui)
    
    # Operation methods
    def run_operation(self, operation: str):
        """Run a migration operation"""
        if self.operation_manager.is_operation_running:
            messagebox.showwarning("Operation Running", 
                                 "Another operation is already running. Please wait.")
            return
        
        # Confirm operation
        if not messagebox.askyesno("Confirm Operation", 
                                  f"Are you sure you want to run {operation.replace('_', ' ')}?"):
            return
        
        self.operation_manager.run_operation_async(operation)
    
    def refresh_migration_status(self):
        """Refresh migration status data"""
        def on_status_complete(success, data):
            def update_ui():
                if success:
                    self.migration_overview.update_overview(data)
                    # Update connection status from the data
                    conn_status = data.get('connection_status', {})
                    if 'filemaker' in conn_status:
                        self.connection_tester.connection_status['filemaker'] = conn_status['filemaker']
                    if 'target' in conn_status:
                        self.connection_tester.connection_status['target'] = conn_status['target']
                    
                    self.update_connection_displays()
                else:
                    self.recent_activity.add_activity(f"Failed to refresh status: {data}")
            
            self.root.after(0, update_ui)
        
        self.status_manager.refresh_migration_status(on_status_complete)
    
    def update_connection_displays(self):
        """Update connection status displays"""
        fm_status = self.connection_tester.connection_status['filemaker']
        target_status = self.connection_tester.connection_status['target']
        
        self.fm_status_card.update_status(fm_status['connected'], fm_status['message'])
        self.target_status_card.update_status(target_status['connected'], target_status['message'])
        
        self.quick_actions.update_button_states(
            fm_status['connected'], 
            target_status['connected']
        )
    
    def update_status_indicator(self):
        """Update the overall status indicator"""
        # Count recent errors
        recent_logs = self.log_manager.get_recent_logs(limit=100)
        error_count = len([log for log in recent_logs if log.level in ['ERROR', 'CRITICAL']])
        
        self.status_bar.update_health(error_count)
    
    # Menu actions
    def open_configuration(self):
        """Open configuration window"""
        if self.config_window is None:
            self.config_window = ConfigurationWindow(self.root, self.config, self.on_config_saved)
        else:
            self.config_window.window.lift()
    
    def on_config_saved(self, new_config):
        """Handle configuration save"""
        self.config = new_config
        self.save_config(new_config)
        self.config_window = None
        self.log_manager.log(LogLevel.INFO, "Config", "Configuration updated via GUI")
    
    def export_configuration(self):
        """Export configuration to file"""
        filename = filedialog.asksaveasfilename(
            title="Export Configuration",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'w') as f:
                    json.dump(self.config, f, indent=4)
                messagebox.showinfo("Success", f"Configuration exported to {filename}")
                self.log_manager.log(LogLevel.INFO, "Config", f"Configuration exported to {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export configuration: {e}")
                self.log_manager.log(LogLevel.ERROR, "Config", f"Export failed: {e}")
    
    def import_configuration(self):
        """Import configuration from file"""
        filename = filedialog.askopenfilename(
            title="Import Configuration",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'r') as f:
                    imported_config = json.load(f)
                
                # Confirm import
                if messagebox.askyesno("Confirm Import", 
                                     "This will replace your current configuration. Continue?"):
                    self.config = imported_config
                    self.save_config()
                    messagebox.showinfo("Success", "Configuration imported successfully!")
                    self.log_manager.log(LogLevel.INFO, "Config", f"Configuration imported from {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to import configuration: {e}")
                self.log_manager.log(LogLevel.ERROR, "Config", f"Import failed: {e}")
    
    def open_log_viewer(self):
        """Open log viewer window"""
        if self.log_viewer_window is None:
            self.log_viewer_window = LogViewerWindow(self.root, self.log_manager)
            # Clean up reference when window is closed
            def on_close():
                self.log_viewer_window = None
            self.log_viewer_window.window.protocol("WM_DELETE_WINDOW", on_close)
        else:
            self.log_viewer_window.window.lift()
    
    def open_log_stats(self):
        """Open log statistics window"""
        if self.log_stats_window is None:
            self.log_stats_window = LogStatsWindow(self.root, self.log_manager)
            # Clean up reference when window is closed
            def on_close():
                self.log_stats_window = None
            self.log_stats_window.window.protocol("WM_DELETE_WINDOW", on_close)
        else:
            self.log_stats_window.window.lift()
    
    def run_diagnostics(self):
        """Run system diagnostics"""
        self.log_manager.log(LogLevel.INFO, "Diagnostics", "Starting system diagnostics from GUI")
        
        def run_diag():
            results = self.diagnostic_engine.run_full_diagnostics()
            self.root.after(0, lambda: self.show_diagnostic_results(results))
        
        threading.Thread(target=run_diag, daemon=True).start()
    
    def show_diagnostic_results(self, results):
        """Show diagnostic results in a popup window"""
        diag_window = tk.Toplevel(self.root)
        diag_window.title("System Diagnostics")
        diag_window.geometry("700x500")
        diag_window.transient(self.root)
        
        # Create scrollable text widget
        from tkinter import scrolledtext
        text_widget = scrolledtext.ScrolledText(diag_window, wrap=tk.WORD)
        text_widget.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Format results
        health_colors = {
            'excellent': 'green',
            'good': 'blue',
            'fair': 'orange',
            'poor': 'red'
        }
        
        report = f"""System Diagnostic Report
========================
Timestamp: {results['timestamp']}
Overall Health: {results['overall_health'].upper()}

Detailed Results:
"""
        
        for check_name, check_result in results['checks'].items():
            status_icon = "✓" if check_result['status'] == 'pass' else "✗" if check_result['status'] == 'fail' else "⚠"
            report += f"\n{status_icon} {check_name.replace('_', ' ').title()}\n"
            report += f"   Status: {check_result['status'].upper()}\n"
            report += f"   Message: {check_result['message']}\n"
            
            if 'recommendation' in check_result:
                report += f"   Recommendation: {check_result['recommendation']}\n"
        
        report += f"\n\nFor detailed logs, check Tools → View Logs"
        
        text_widget.insert('1.0', report)
        text_widget.configure(state='disabled')
        
        # Button frame
        button_frame = ttk.Frame(diag_window)
        button_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        ttk.Button(button_frame, text="Close", command=diag_window.destroy).pack(side='right')
        ttk.Button(button_frame, text="Export Report", 
                  command=lambda: self.export_diagnostic_report(results)).pack(side='right', padx=(0, 10))
    
    def export_diagnostic_report(self, results):
        """Export diagnostic report to file"""
        filename = filedialog.asksaveasfilename(
            title="Export Diagnostic Report",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            try:
                if filename.endswith('.json'):
                    with open(filename, 'w') as f:
                        json.dump(results, f, indent=2)
                else:
                    # Export as text
                    with open(filename, 'w') as f:
                        f.write(f"System Diagnostic Report\n")
                        f.write(f"========================\n")
                        f.write(f"Timestamp: {results['timestamp']}\n")
                        f.write(f"Overall Health: {results['overall_health']}\n\n")
                        
                        for check_name, check_result in results['checks'].items():
                            f.write(f"{check_name}: {check_result['status']} - {check_result['message']}\n")
                
                messagebox.showinfo("Success", f"Diagnostic report exported to {filename}")
                self.log_manager.log(LogLevel.INFO, "Diagnostics", f"Report exported to {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export report: {e}")
    
    def open_export_folder(self):
        """Open export folder in file explorer"""
        export_path = Path(self.config.get('export_path', './exports'))
        if export_path.exists():
            if sys.platform == 'win32':
                os.startfile(export_path)
            elif sys.platform == 'darwin':
                subprocess.run(['open', export_path])
            else:
                subprocess.run(['xdg-open', export_path])
        else:
            messagebox.showinfo("Not Found", f"Export directory not found: {export_path}")
    
    def open_log_folder(self):
        """Open log folder in file explorer"""
        log_path = self.log_manager.log_dir
        if log_path.exists():
            if sys.platform == 'win32':
                os.startfile(log_path)
            elif sys.platform == 'darwin':
                subprocess.run(['open', log_path])
            else:
                subprocess.run(['xdg-open', log_path])
        else:
            messagebox.showinfo("Not Found", f"Log directory not found: {log_path}")
    
    def show_about(self):
        """Show about dialog"""
        about_text = """FileMaker Sync Dashboard
Version 2.0.0

A comprehensive tool for migrating and synchronizing data 
from FileMaker Pro databases to Supabase.

Features:
• Real-time migration monitoring
• Advanced logging and diagnostics
• Connection testing and validation
• Export capabilities (DDL, DML, Images)
• Professional dashboard interface

Built with Python and tkinter.
"""
        messagebox.showinfo("About FileMaker Sync", about_text)
    
    def auto_refresh(self):
        """Automatically refresh status every 30 seconds"""
        self.refresh_migration_status()
        self.update_status_indicator()
        self.root.after(30000, self.auto_refresh)  # 30 seconds
    
    def on_closing(self):
        """Handle application close"""
        if self.operation_manager.is_operation_running:
            if messagebox.askokcancel("Quit", 
                                    "An operation is running. Do you want to stop it and quit?"):
                self.log_manager.log(LogLevel.INFO, "Application", "Application closed by user")
                self.root.destroy()
        else:
            self.log_manager.log(LogLevel.INFO, "Application", "Application closed normally")
            self.root.destroy()


def main():
    """Main entry point"""
    root = tk.Tk()
    
    # Set window icon if available
    try:
        root.iconbitmap('icon.ico')
    except:
        pass
    
    # Create the application
    app = FileMakerSyncGUI(root)
    
    # Handle window close
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    
    # Center window on screen
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    # Start the GUI
    root.mainloop()


if __name__ == "__main__":
    main()