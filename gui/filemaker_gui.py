#!/usr/bin/env python3
# FILE: gui/filemaker_gui.py
"""
Complete Working FileMaker Sync GUI with Debug Configuration
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
from gui_logging import LogManager, LogLevel, PerformanceLogger
from gui_widgets import StatusCard, MigrationOverview, QuickActions, StatusBar
from gui_operations import OperationManager, ConnectionTester, StatusManager
from gui_logviewer import LogViewerWindow, LogStatsWindow

class ConfigurationWindow:
    """Enhanced configuration management window with debug options"""
    
    def __init__(self, parent, config_file: str = 'config.toml', on_save_callback=None):
        self.parent = parent
        self.config_file = Path(config_file)
        self.on_save_callback = on_save_callback
        self.window = None
        self._destroyed = False
        
        self.create_window()
        self.create_widgets()
        self.load_config_values()
    
    def create_window(self):
        """Create the configuration window"""
        self.window = tk.Toplevel(self.parent)
        self.window.title("Configuration Settings")
        self.window.geometry("650x600")
        self.window.minsize(500, 500)
        
        # Set window management
        self.window.protocol("WM_DELETE_WINDOW", self.close_window)
        self.window.transient(self.parent)
        
        # Center on parent
        self.center_on_parent()
        
        # Bring to front
        self.window.lift()
        self.window.focus_force()
    
    def center_on_parent(self):
        """Center window on parent"""
        try:
            self.window.update_idletasks()
            
            parent_x = self.parent.winfo_rootx()
            parent_y = self.parent.winfo_rooty()
            parent_width = self.parent.winfo_width()
            parent_height = self.parent.winfo_height()
            
            window_width = self.window.winfo_width()
            window_height = self.window.winfo_height()
            
            x = parent_x + (parent_width - window_width) // 2
            y = parent_y + (parent_height - window_height) // 2
            
            self.window.geometry(f"+{x}+{y}")
        except:
            pass
    
    def close_window(self):
        """Close the window properly"""
        if self._destroyed:
            return
        
        self._destroyed = True
        
        try:
            if self.window and self.window.winfo_exists():
                self.window.destroy()
        except:
            pass
    
    def create_widgets(self):
        """Create configuration interface with debug options"""
        if self._destroyed:
            return
        
        main_frame = ttk.Frame(self.window)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Info label
        info_label = ttk.Label(main_frame, 
                              text="Configure connection settings and application behavior.",
                              font=('Arial', 9), foreground='gray', wraplength=600)
        info_label.pack(pady=(0, 10))
        
        # Create notebook for different config sections
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill='both', expand=True, pady=(0, 10))
        
        # Source Database tab
        source_frame = ttk.Frame(notebook)
        notebook.add(source_frame, text="FileMaker")
        self.create_source_tab(source_frame)
        
        # Target Database tab
        target_frame = ttk.Frame(notebook)
        notebook.add(target_frame, text="Supabase")
        self.create_target_tab(target_frame)
        
        # Export Settings tab
        export_frame = ttk.Frame(notebook)
        notebook.add(export_frame, text="Export")
        self.create_export_tab(export_frame)
        
        # Debug & Logging tab
        debug_frame = ttk.Frame(notebook)
        notebook.add(debug_frame, text="Debug & Logging")
        self.create_debug_tab(debug_frame)
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x')
        
        ttk.Button(button_frame, text="Cancel", command=self.close_window).pack(side='right', padx=(5, 0))
        ttk.Button(button_frame, text="Save", command=self.save_config).pack(side='right')
        ttk.Button(button_frame, text="Open File", command=self.open_config_file).pack(side='left')
    
    def create_source_tab(self, parent):
        """Create source database configuration tab"""
        fm_frame = ttk.LabelFrame(parent, text="FileMaker Pro Connection", padding=10)
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
        sb_frame = ttk.LabelFrame(parent, text="Supabase Connection", padding=10)
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
        exp_frame = ttk.LabelFrame(parent, text="Export Settings", padding=10)
        exp_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(exp_frame, text="Export Path:").grid(row=0, column=0, sticky='w', pady=2)
        path_frame = ttk.Frame(exp_frame)
        path_frame.grid(row=0, column=1, padx=(10, 0), pady=2, sticky='ew')
        
        self.export_path_var = tk.StringVar()
        ttk.Entry(path_frame, textvariable=self.export_path_var, width=35).pack(side='left', fill='x', expand=True)
        ttk.Button(path_frame, text="Browse", command=self.browse_export_path, width=8).pack(side='right', padx=(5, 0))
        
        ttk.Label(exp_frame, text="File Prefix:").grid(row=1, column=0, sticky='w', pady=2)
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
    
    def create_debug_tab(self, parent):
        """Create debug and logging configuration tab"""
        # Logging Level Section
        log_frame = ttk.LabelFrame(parent, text="Logging Configuration", padding=10)
        log_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(log_frame, text="Log Level:").grid(row=0, column=0, sticky='w', pady=2)
        self.log_level_var = tk.StringVar()
        log_combo = ttk.Combobox(log_frame, textvariable=self.log_level_var, 
                                values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                                width=15, state="readonly")
        log_combo.grid(row=0, column=1, padx=(10, 0), pady=2, sticky='w')
        
        ttk.Label(log_frame, text="Console Output:").grid(row=1, column=0, sticky='w', pady=2)
        self.console_logging_var = tk.BooleanVar()
        ttk.Checkbutton(log_frame, text="Show logs in console", variable=self.console_logging_var).grid(row=1, column=1, padx=(10, 0), pady=2, sticky='w')
        
        ttk.Label(log_frame, text="Max Log Entries:").grid(row=2, column=0, sticky='w', pady=2)
        self.max_log_entries_var = tk.StringVar()
        ttk.Entry(log_frame, textvariable=self.max_log_entries_var, width=10).grid(row=2, column=1, padx=(10, 0), pady=2, sticky='w')
        
        # Debug Options Section
        debug_frame = ttk.LabelFrame(parent, text="Debug Options", padding=10)
        debug_frame.pack(fill='x', padx=10, pady=10)
        
        self.debug_mode_var = tk.BooleanVar()
        ttk.Checkbutton(debug_frame, text="Enable Debug Mode", variable=self.debug_mode_var, 
                       command=self.on_debug_mode_change).grid(row=0, column=0, sticky='w', pady=2)
        
        self.verbose_sql_var = tk.BooleanVar()
        ttk.Checkbutton(debug_frame, text="Log SQL Queries", variable=self.verbose_sql_var).grid(row=1, column=0, sticky='w', pady=2)
        
        self.debug_connections_var = tk.BooleanVar()
        ttk.Checkbutton(debug_frame, text="Debug Database Connections", variable=self.debug_connections_var).grid(row=2, column=0, sticky='w', pady=2)
    
    def on_debug_mode_change(self):
        """Handle debug mode checkbox change"""
        if self.debug_mode_var.get():
            self.log_level_var.set("DEBUG")
            self.console_logging_var.set(True)
        else:
            self.log_level_var.set("INFO")
    
    def load_config_values(self):
        """Load current configuration values from TOML file"""
        try:
            import tomli
            if self.config_file.exists():
                with open(self.config_file, 'rb') as f:
                    config = tomli.load(f)
                
                # Source database
                source = config.get('database', {}).get('source', {})
                self.fm_dsn_var.set(source.get('dsn', ''))
                self.fm_user_var.set(source.get('user', ''))
                self.fm_pwd_var.set(source.get('pwd', ''))
                
                # Target database
                target = config.get('database', {}).get('target', {})
                db_type = target.get('db', 'supabase')
                target_db = target.get(db_type, {})
                
                self.sb_host_var.set(target.get('host', ''))
                self.sb_db_var.set(target.get('dsn', ''))
                self.sb_user_var.set(target_db.get('user', ''))
                self.sb_pwd_var.set(target_db.get('pwd', ''))
                self.sb_port_var.set(target_db.get('port', '5432'))
                
                # Export settings
                export = config.get('export', {})
                self.export_path_var.set(export.get('path', './exports'))
                self.export_prefix_var.set(export.get('prefix', 'rat'))
                
                # Image formats
                formats = export.get('image_formats_supported', ['jpg'])
                self.jpg_var.set('jpg' in formats)
                self.webp_var.set('webp' in formats)
                
                # Debug settings
                debug_settings = config.get('debug', {})
                self.log_level_var.set(debug_settings.get('log_level', 'INFO'))
                self.console_logging_var.set(debug_settings.get('console_logging', False))
                self.max_log_entries_var.set(str(debug_settings.get('max_log_entries', 1000)))
                self.debug_mode_var.set(debug_settings.get('debug_mode', False))
                self.verbose_sql_var.set(debug_settings.get('verbose_sql', False))
                self.debug_connections_var.set(debug_settings.get('debug_connections', False))
                
        except Exception as e:
            messagebox.showwarning("Config Load Error", f"Could not load config.toml: {e}")
            # Set defaults
            self.log_level_var.set("INFO")
            self.console_logging_var.set(False)
            self.max_log_entries_var.set("1000")
            self.debug_mode_var.set(False)
    
    def browse_export_path(self):
        """Browse for export path"""
        path = filedialog.askdirectory(title="Select Export Directory")
        if path:
            self.export_path_var.set(path)
    
    def save_config(self):
        """Save configuration changes to TOML file"""
        try:
            # Build image formats list
            formats = []
            if self.jpg_var.get():
                formats.append('jpg')
            if self.webp_var.get():
                formats.append('webp')
            
            # Enhanced TOML configuration with debug settings
            config_content = f"""# FileMaker Sync Configuration
# Updated: {datetime.now().isoformat()}

[database.source]
dsn = "{self.fm_dsn_var.get()}"
user = "{self.fm_user_var.get()}"
pwd = "{self.fm_pwd_var.get()}"
host = "127.0.0.1"
port = ""
type = "odbc"
name = ["fmp", "FileMaker Pro"]
schema = ["FileMaker_Tables", "FileMaker_Fields", "FileMaker_BaseTableFields"]

[database.target]
dsn = "{self.sb_db_var.get()}"
db = "supabase"
dt = "%Y%m%d %H:%M:%S"
type = "url"
host = "{self.sb_host_var.get()}"
schema = ["rat_migration", "rat"]
mig_schema = 0
tgt_schema = 1
user = "migration_user"

[database.target.supabase]
name = ["supabase", "Supabase"]
user = "{self.sb_user_var.get()}"
pwd = "{self.sb_pwd_var.get()}"
port = "{self.sb_port_var.get()}"

[export]
path = "{self.export_path_var.get()}"
prefix = "{self.export_prefix_var.get()}"
image_formats_supported = {formats}
image_path = "images"

[debug]
log_level = "{self.log_level_var.get()}"
console_logging = {str(self.console_logging_var.get()).lower()}
max_log_entries = {self.max_log_entries_var.get()}
debug_mode = {str(self.debug_mode_var.get()).lower()}
verbose_sql = {str(self.verbose_sql_var.get()).lower()}
debug_connections = {str(self.debug_connections_var.get()).lower()}
"""
            
            with open(self.config_file, 'w') as f:
                f.write(config_content)
            
            if self.on_save_callback:
                self.on_save_callback()
            
            messagebox.showinfo("Success", "Configuration saved successfully!\n\nRestart the application for debug settings to take full effect.")
            self.close_window()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save configuration: {e}")
    
    def open_config_file(self):
        """Open config.toml in default editor"""
        try:
            if sys.platform == 'win32':
                os.startfile(self.config_file)
            elif sys.platform == 'darwin':
                subprocess.run(['open', self.config_file])
            else:
                subprocess.run(['xdg-open', self.config_file])
        except Exception as e:
            messagebox.showerror("Error", f"Could not open config file: {e}")


class FileMakerSyncGUI:
    """FileMaker Sync GUI with configurable debug support"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("FileMaker Sync Dashboard")
        self.root.geometry("1000x650")
        self.root.minsize(800, 550)
        
        # Load configuration first
        self.config = self.load_configuration()
        
        # Initialize core systems with configuration
        self.log_manager = LogManager(config=self.config)
        self.operation_manager = OperationManager(self.log_manager)
        self.connection_tester = ConnectionTester(self.operation_manager)
        self.status_manager = StatusManager(self.operation_manager)
        
        # Child window references
        self.child_windows = {}
        
        # Initialize GUI
        self.create_widgets()
        self.setup_bindings()
        self.setup_callbacks()
        
        # Start auto-refresh
        self.auto_refresh()
        
        # Log startup
        self.log_manager.log(LogLevel.INFO, "Application", "FileMaker Sync Dashboard started")
    
    def load_configuration(self) -> dict:
        """Load configuration from TOML file with defaults"""
        config_file = Path('config.toml')
        default_config = {
            'debug': {
                'log_level': 'INFO',
                'console_logging': False,
                'max_log_entries': 1000,
                'debug_mode': False,
                'verbose_sql': False,
                'debug_connections': False
            }
        }
        
        if not config_file.exists():
            return default_config
        
        try:
            import tomli
            with open(config_file, 'rb') as f:
                config = tomli.load(f)
            
            # Merge with defaults
            merged_config = default_config.copy()
            if 'debug' in config:
                merged_config['debug'].update(config['debug'])
            
            # Include other config sections
            for key, value in config.items():
                if key != 'debug':
                    merged_config[key] = value
            
            return merged_config
            
        except Exception as e:
            print(f"Error loading config: {e}, using defaults")
            return default_config
    
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
        header_frame.pack(fill='x', pady=(0, 15))
        
        title_label = ttk.Label(header_frame, text="FileMaker Sync Dashboard", 
                               font=('Arial', 18, 'bold'))
        title_label.pack(side='left')
        
        subtitle_text = "Monitor and manage your FileMaker to Supabase migration"
        if self.config.get('debug', {}).get('debug_mode', False):
            log_level = self.config.get('debug', {}).get('log_level', 'INFO')
            subtitle_text += f" | Log Level: {log_level}"
        
        subtitle_label = ttk.Label(header_frame, text=subtitle_text, font=('Arial', 9))
        subtitle_label.pack(side='left', padx=(20, 0))
        
        # Activity log button
        activity_button = ttk.Button(header_frame, text="🕒", width=3, 
                                    command=self.open_log_viewer)
        activity_button.pack(side='right', padx=(0, 10))
        
        ttk.Label(header_frame, text="Activity Log", font=('Arial', 8)).pack(side='right')
    
    def create_connection_status(self, parent):
        """Create connection status section"""
        conn_frame = ttk.Frame(parent)
        conn_frame.pack(fill='x', pady=(0, 15))
        
        # Configure style for larger fonts
        style = ttk.Style()
        style.configure('Large.TLabelframe.Label', font=('Arial', 12, 'bold'))
        
        # FileMaker status card
        fm_card_frame = ttk.LabelFrame(conn_frame, text="FileMaker Pro", 
                                      style='Large.TLabelframe', padding=8)
        fm_card_frame.pack(side='left', fill='x', expand=True, padx=(0, 8))
        
        self.fm_status_card = StatusCard(fm_card_frame, "FileMaker Pro")
        self.fm_status_card.pack(fill='x')
        
        # Target status card
        target_card_frame = ttk.LabelFrame(conn_frame, text="Supabase Target", 
                                          style='Large.TLabelframe', padding=8)
        target_card_frame.pack(side='right', fill='x', expand=True, padx=(8, 0))
        
        self.target_status_card = StatusCard(target_card_frame, "Supabase Target")
        self.target_status_card.pack(fill='x')
    
    def create_main_content(self, parent):
        """Create main content area"""
        content_frame = ttk.Frame(parent)
        content_frame.pack(fill='both', expand=True)
        
        # Configure style for larger section fonts
        style = ttk.Style()
        style.configure('Large.TLabelframe.Label', font=('Arial', 12, 'bold'))
        
        # Migration Overview
        migration_frame = ttk.LabelFrame(content_frame, text="Migration Overview", 
                                        style='Large.TLabelframe', padding=5)
        migration_frame.pack(fill='both', expand=True, pady=(0, 8))
        
        self.migration_overview = MigrationOverview(migration_frame)
        self.migration_overview.pack(fill='both', expand=True)
        
        # Quick Actions
        actions_frame = ttk.LabelFrame(content_frame, text="Quick Actions", 
                                      style='Large.TLabelframe', padding=5)
        actions_frame.pack(fill='x')
        
        self.quick_actions = QuickActions(actions_frame)
        self.quick_actions.pack(fill='x')
    
    def create_menu_bar(self):
        """Create the menu bar"""
        self.menubar = tk.Menu(self.root)
        self.root.config(menu=self.menubar)
        
        # File menu
        file_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Configuration...", command=self.open_configuration)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # Tools menu
        tools_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="View Activity Logs", command=self.open_log_viewer)
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
        if log_entry.level in ['ERROR', 'CRITICAL']:
            self.root.after(0, self.update_status_indicator)
    
    def on_operation_status(self, status, operation, result=None):
        """Handle operation status updates"""
        def update_ui():
            if status == 'start':
                self.quick_actions.show_progress(operation.replace('_', ' ').title())
            elif status == 'complete':
                self.quick_actions.hide_progress()
                self.root.after(1000, self.refresh_migration_status)
        
        self.root.after(0, update_ui)
    
    # Connection testing methods
    def test_filemaker_connection(self):
        """Test FileMaker connection"""
        self.log_manager.log(LogLevel.INFO, "GUI", "Testing FileMaker connection from GUI")
        self.connection_tester.test_filemaker_connection(self.on_connection_test_complete)
    
    def test_target_connection(self):
        """Test target database connection"""
        self.log_manager.log(LogLevel.INFO, "GUI", "Testing target connection from GUI")
        self.connection_tester.test_target_connection(self.on_connection_test_complete)
    
    def test_all_connections(self):
        """Test both connections"""
        self.log_manager.log(LogLevel.INFO, "GUI", "Testing all connections from GUI")
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
        self.log_manager.log(LogLevel.INFO, "GUI", "Refreshing migration status from GUI")
        
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
                    self.log_manager.log(LogLevel.ERROR, "GUI", f"Failed to refresh status: {data}")
            
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
    
    # Child window management methods
    def open_configuration(self):
        """Open configuration window with proper management"""
        # Close existing config window if open
        if 'config' in self.child_windows:
            try:
                self.child_windows['config'].close_window()
            except:
                pass
            del self.child_windows['config']
        
        # Create new config window
        self.child_windows['config'] = ConfigurationWindow(
            self.root, 'config.toml', self.on_config_saved
        )
    
    def on_config_saved(self):
        """Handle configuration save"""
        if 'config' in self.child_windows:
            del self.child_windows['config']
        
        self.log_manager.log(LogLevel.INFO, "Config", "Configuration updated via GUI")
        # Test connections after config save
        self.root.after(1000, self.test_all_connections)
    
    def open_log_viewer(self):
        """Open log viewer window with proper management"""
        # Close existing log viewer if open
        if 'log_viewer' in self.child_windows:
            try:
                self.child_windows['log_viewer'].close_window()
            except:
                pass
            del self.child_windows['log_viewer']
        
        # Create new log viewer
        try:
            self.child_windows['log_viewer'] = LogViewerWindow(self.root, self.log_manager)
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "GUI", f"Failed to open log viewer: {e}")
            messagebox.showerror("Error", f"Failed to open log viewer: {e}")
    
    def open_log_stats(self):
        """Open log statistics window with proper management"""
        # Close existing stats window if open
        if 'log_stats' in self.child_windows:
            try:
                self.child_windows['log_stats'].close_window()
            except:
                pass
            del self.child_windows['log_stats']
        
        # Create new stats window
        try:
            self.child_windows['log_stats'] = LogStatsWindow(self.root, self.log_manager)
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "GUI", f"Failed to open log stats: {e}")
            messagebox.showerror("Error", f"Failed to open log statistics: {e}")
    
    def run_diagnostics(self):
        """Run system diagnostics"""
        self.log_manager.log(LogLevel.INFO, "Diagnostics", "Starting system diagnostics from GUI")
        
        def run_diag():
            results = {
                'timestamp': datetime.now().isoformat(),
                'config_file_exists': Path('config.toml').exists(),
                'script_file_exists': Path('filemaker_extract_refactored.py').exists(),
                'logs_dir_exists': Path('logs').exists()
            }
            
            self.root.after(0, lambda: self.show_diagnostic_results(results))
        
        threading.Thread(target=run_diag, daemon=True).start()
    
    def show_diagnostic_results(self, results):
        """Show diagnostic results in a popup window"""
        diag_window = tk.Toplevel(self.root)
        diag_window.title("System Diagnostics")
        diag_window.geometry("500x400")
        diag_window.transient(self.root)
        diag_window.grab_set()
        
        # Create scrollable text widget
        from tkinter import scrolledtext
        text_widget = scrolledtext.ScrolledText(diag_window, wrap=tk.WORD)
        text_widget.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Format results
        report = f"""System Diagnostic Report
========================
Timestamp: {results['timestamp']}

File Checks:
✓ config.toml exists: {results['config_file_exists']}
✓ filemaker_extract_refactored.py exists: {results['script_file_exists']}
✓ logs directory exists: {results['logs_dir_exists']}

Connection Status:
• FileMaker: {self.connection_tester.connection_status['filemaker']['message']}
• Target: {self.connection_tester.connection_status['target']['message']}

For detailed logs, check Tools → View Activity Logs
"""
        
        text_widget.insert('1.0', report)
        text_widget.configure(state='disabled')
        
        # Button frame
        button_frame = ttk.Frame(diag_window)
        button_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        ttk.Button(button_frame, text="Close", command=diag_window.destroy).pack(side='right')
    
    def open_export_folder(self):
        """Open export folder in file explorer"""
        export_path = Path('./exports')
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
        self.update_status_indicator()
        self.root.after(30000, self.auto_refresh)
    
    def on_closing(self):
        """Handle application close"""
        if self.operation_manager.is_operation_running:
            if messagebox.askokcancel("Quit", 
                                    "An operation is running. Do you want to stop it and quit?"):
                self.cleanup_and_exit()
        else:
            self.cleanup_and_exit()
    
    def cleanup_and_exit(self):
        """Clean up resources and exit"""
        self.log_manager.log(LogLevel.INFO, "Application", "Application closing")
        
        # Close all child windows
        for window_name, window_obj in list(self.child_windows.items()):
            try:
                window_obj.close_window()
            except:
                pass
        
        self.child_windows.clear()
        
        # Destroy main window
        self.root.destroy()


def main():
    """Main entry point"""
    # Create root window
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
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("Application interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
    finally:
        try:
            root.destroy()
        except:
            pass


if __name__ == "__main__":
    main()