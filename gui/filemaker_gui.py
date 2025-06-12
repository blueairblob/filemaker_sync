#!/usr/bin/env python3
# FILE: gui/filemaker_gui.py
"""
Enhanced FileMaker Sync GUI - COMPACT VERSION
Compact layout with larger section fonts and no redundant titles
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
from gui_widgets import StatusCard, MigrationOverview, QuickActions, StatusBar
from gui_operations import OperationManager, ConnectionTester, StatusManager
from gui_logviewer import LogViewerWindow, LogStatsWindow

class ConfigurationWindow:
    """Configuration management window for TOML config"""
    
    def __init__(self, parent, config_file: str = 'config.toml', on_save_callback=None):
        self.parent = parent
        self.config_file = Path(config_file)
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
        
        # Info label
        info_label = ttk.Label(main_frame, 
                              text="Edit config.toml directly for advanced settings. This dialog shows key connection settings.",
                              font=('Arial', 9), foreground='gray')
        info_label.pack(pady=(0, 10))
        
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
        ttk.Button(button_frame, text="Save to TOML", command=self.save_config).pack(side='right')
        ttk.Button(button_frame, text="Open config.toml", command=self.open_config_file).pack(side='left')
    
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
                self.export_path_var.set(export.get('path', ''))
                self.export_prefix_var.set(export.get('prefix', 'rat'))
                
                # Image formats
                formats = export.get('image_formats_supported', ['jpg'])
                self.jpg_var.set('jpg' in formats)
                self.webp_var.set('webp' in formats)
                
        except Exception as e:
            messagebox.showwarning("Config Load Error", f"Could not load config.toml: {e}")
    
    def browse_export_path(self):
        """Browse for export path"""
        path = filedialog.askdirectory(title="Select Export Directory")
        if path:
            self.export_path_var.set(path)
    
    def save_config(self):
        """Save configuration changes to TOML file"""
        try:
            # Read existing config
            import tomli
            config = {}
            if self.config_file.exists():
                with open(self.config_file, 'rb') as f:
                    config = tomli.load(f)
            
            # Update with new values
            if 'database' not in config:
                config['database'] = {}
            if 'source' not in config['database']:
                config['database']['source'] = {}
            if 'target' not in config['database']:
                config['database']['target'] = {}
            if 'export' not in config:
                config['export'] = {}
            
            # Update source
            config['database']['source'].update({
                'dsn': self.fm_dsn_var.get(),
                'user': self.fm_user_var.get(),
                'pwd': self.fm_pwd_var.get()
            })
            
            # Update target
            db_type = config['database']['target'].get('db', 'supabase')
            if db_type not in config['database']['target']:
                config['database']['target'][db_type] = {}
            
            config['database']['target'].update({
                'host': self.sb_host_var.get(),
                'dsn': self.sb_db_var.get()
            })
            config['database']['target'][db_type].update({
                'user': self.sb_user_var.get(),
                'pwd': self.sb_pwd_var.get(),
                'port': self.sb_port_var.get()
            })
            
            # Update export
            formats = []
            if self.jpg_var.get():
                formats.append('jpg')
            if self.webp_var.get():
                formats.append('webp')
            
            config['export'].update({
                'path': self.export_path_var.get(),
                'prefix': self.export_prefix_var.get(),
                'image_formats_supported': formats
            })
            
            # Write back to TOML file
            try:
                import tomli_w
                with open(self.config_file, 'wb') as f:
                    tomli_w.dump(config, f)
            except ImportError:
                # Fallback: write as text (less robust but works)
                with open(self.config_file, 'w') as f:
                    f.write("# Configuration updated via GUI\n")
                    f.write(f"# Updated: {datetime.now().isoformat()}\n\n")
                    # Write basic structure - user should edit manually for complex configs
                    f.write("[database.source]\n")
                    f.write(f'dsn = "{self.fm_dsn_var.get()}"\n')
                    f.write(f'user = "{self.fm_user_var.get()}"\n')
                    f.write(f'pwd = "{self.fm_pwd_var.get()}"\n\n')
                    f.write("[database.target]\n")
                    f.write(f'host = "{self.sb_host_var.get()}"\n')
                    f.write(f'dsn = "{self.sb_db_var.get()}"\n\n')
                    f.write(f"[database.target.supabase]\n")
                    f.write(f'user = "{self.sb_user_var.get()}"\n')
                    f.write(f'pwd = "{self.sb_pwd_var.get()}"\n')
                    f.write(f'port = "{self.sb_port_var.get()}"\n\n')
            
            if self.on_save_callback:
                self.on_save_callback()
            
            messagebox.showinfo("Success", "Configuration saved to config.toml!")
            self.window.destroy()
            
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
    """Enhanced FileMaker Sync GUI - COMPACT VERSION"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("FileMaker Sync Dashboard")
        self.root.geometry("1000x650")  # Slightly smaller due to compact layout
        self.root.minsize(800, 550)
        
        # Initialize core systems
        self.log_manager = LogManager()
        self.operation_manager = OperationManager(self.log_manager)
        self.connection_tester = ConnectionTester(self.operation_manager)
        self.status_manager = StatusManager(self.operation_manager)
        
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
        """Create the main dashboard layout - COMPACT VERSION"""
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
        
        subtitle_label = ttk.Label(header_frame, 
                                  text="Monitor and manage your FileMaker to Supabase migration",
                                  font=('Arial', 9))
        subtitle_label.pack(side='left', padx=(20, 0))
        
        # Activity log button (clock icon)
        activity_button = ttk.Button(header_frame, text="🕒", width=3, 
                                    command=self.open_log_viewer)
        activity_button.pack(side='right', padx=(0, 10))
        
        # Add tooltip-like label for the button
        ttk.Label(header_frame, text="Activity Log", font=('Arial', 8)).pack(side='right')
    
    def create_connection_status(self, parent):
        """Create connection status section - COMPACT"""
        conn_frame = ttk.Frame(parent)
        conn_frame.pack(fill='x', pady=(0, 15))
        
        # LARGER SECTION FONTS - Use LabelFrame with bigger font
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
        """Create main content area - SUPER COMPACT VERSION"""
        content_frame = ttk.Frame(parent)
        content_frame.pack(fill='both', expand=True)
        
        # Configure style for larger section fonts
        style = ttk.Style()
        style.configure('Large.TLabelframe.Label', font=('Arial', 12, 'bold'))
        
        # Migration Overview - MINIMAL padding
        migration_frame = ttk.LabelFrame(content_frame, text="Migration Overview", 
                                        style='Large.TLabelframe', padding=5)
        migration_frame.pack(fill='both', expand=True, pady=(0, 8))
        
        self.migration_overview = MigrationOverview(migration_frame)
        self.migration_overview.pack(fill='both', expand=True)
        
        # Quick Actions - MINIMAL padding
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
        # Update status if it's an error
        if log_entry.level in ['ERROR', 'CRITICAL']:
            self.root.after(0, self.update_status_indicator)
    
    def on_operation_status(self, status, operation, result=None):
        """Handle operation status updates"""
        def update_ui():
            if status == 'start':
                self.quick_actions.show_progress(operation.replace('_', ' ').title())
            elif status == 'complete':
                self.quick_actions.hide_progress()
                # Refresh status after operation
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
            self.log_manager.log(LogLevel.INFO, "GUI", f"Connection test complete: {connection_type} = {status}")
            
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
    
    # Menu actions
    def open_configuration(self):
        """Open configuration window"""
        if self.config_window is None:
            self.config_window = ConfigurationWindow(self.root, 'config.toml', self.on_config_saved)
        else:
            self.config_window.window.lift()
    
    def on_config_saved(self):
        """Handle configuration save"""
        self.config_window = None
        self.log_manager.log(LogLevel.INFO, "Config", "Configuration updated via GUI")
        # Test connections after config save
        self.root.after(1000, self.test_all_connections)
    
    def open_log_viewer(self):
        """Open log viewer window"""
        if self.log_viewer_window is None:
            self.log_viewer_window = LogViewerWindow(self.root, self.log_manager)
            # FIXED: Clean up reference when window is closed
            def on_close():
                self.log_viewer_window = None
            # Store the original close method
            original_close = self.log_viewer_window.close_window
            def enhanced_close():
                original_close()
                on_close()
            self.log_viewer_window.close_window = enhanced_close
        else:
            try:
                self.log_viewer_window.window.lift()
                self.log_viewer_window.window.focus_force()
            except:
                # Window was destroyed, create new one
                self.log_viewer_window = None
                self.open_log_viewer()
    
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
            # Simple diagnostic check
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