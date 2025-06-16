#!/usr/bin/env python3
# FILE: gui/filemaker_gui.py
"""
FileMaker Sync GUI Application
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
import queue
import time

# Import our modules
from gui_logging import LogManager, LogLevel, PerformanceLogger
from gui_widgets import StatusCard, MigrationOverview, QuickActions, StatusBar
from gui_operations import OperationManager, ConnectionTester, StatusManager
from gui_logviewer import LogViewerWindow, LogStatsWindow

class FileMakerSyncGUI:
    """FileMaker Sync GUI that prevents hanging"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("FileMaker Sync Dashboard")
        self.root.geometry("1000x650")
        self.root.minsize(800, 550)
        
        # Thread safety infrastructure
        self._gui_lock = threading.RLock()
        self._update_queue = queue.Queue(maxsize=100)
        self._shutdown_requested = threading.Event()
        
        # Load configuration first
        self.config = self.load_configuration()
        
        # Initialize core systems with thread-safe versions
        self.log_manager = LogManager(config=self.config)
        self.operation_manager = OperationManager(self.log_manager)
        self.connection_tester = ConnectionTester(self.operation_manager)
        self.status_manager = StatusManager(self.operation_manager)
        
        # Child window management
        self._child_windows_lock = threading.Lock()
        self.child_windows = {}
        
        # Auto-refresh settings
        self._auto_refresh_timer = None
        self._auto_refresh_interval = 30000  # 30 seconds
        
        # Initialize GUI
        self.create_widgets()
        self.setup_bindings()
        self.setup_callbacks()
        
        # Start GUI update processing
        self.start_gui_update_processor()
        
        # Start auto-refresh with delay
        self.root.after(2000, self.start_auto_refresh)
        
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
    
    def start_gui_update_processor(self):
        """Start the GUI update processor"""
        def process_gui_updates():
            """Process GUI updates from the queue"""
            try:
                while not self._update_queue.empty():
                    try:
                        update_func = self._update_queue.get_nowait()
                        if callable(update_func):
                            update_func()
                    except queue.Empty:
                        break
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "GUI", f"Error processing GUI update: {e}")
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error in GUI update processor: {e}")
            finally:
                # Schedule next processing
                if not self._shutdown_requested.is_set():
                    self.root.after(100, process_gui_updates)
        
        # Start the processor
        self.root.after(100, process_gui_updates)
    
    def schedule_gui_update(self, update_func):
        """Thread-safely schedule a GUI update"""
        try:
            self._update_queue.put_nowait(update_func)
        except queue.Full:
            # Queue is full, skip this update
            self.log_manager.log(LogLevel.WARNING, "GUI", "GUI update queue full, skipping update")
    
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
            #subtitle_text += f" | Log Level: {log_level} | Thread-Safe Mode"
        
        subtitle_label = ttk.Label(header_frame, text=subtitle_text, font=('Arial', 9))
        subtitle_label.pack(side='left', padx=(20, 0))
        
        # Activity log button
        activity_button = ttk.Button(header_frame, text="🕒", width=3, 
                                    command=self.safe_open_log_viewer)
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
        file_menu.add_command(label="Configuration...", command=self.safe_open_configuration)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.safe_exit)
        
        # Tools menu
        tools_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="View Activity Logs", command=self.safe_open_log_viewer)
        tools_menu.add_command(label="Log Statistics", command=self.safe_open_log_stats)
        tools_menu.add_separator()
        tools_menu.add_command(label="Run Diagnostics", command=self.safe_run_diagnostics)
        tools_menu.add_separator()
        tools_menu.add_command(label="Open Export Folder", command=self.safe_open_export_folder)
        tools_menu.add_command(label="Open Log Folder", command=self.safe_open_log_folder)
        
        # Help menu
        help_menu = tk.Menu(self.menubar, tearoff=0)
        self.menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.safe_show_about)
    
    def setup_bindings(self):
        """Set up event bindings with thread safety"""
        # Connection test buttons
        self.fm_status_card.test_button.configure(command=self.safe_test_filemaker_connection)
        self.target_status_card.test_button.configure(command=self.safe_test_target_connection)
        
        # Refresh button
        self.migration_overview.refresh_button.configure(command=self.safe_refresh_migration_status)
        
        # Quick action buttons
        actions = self.quick_actions.action_buttons
        actions['Full Sync'].configure(command=lambda: self.safe_run_operation('full_sync'))
        actions['Incremental Sync'].configure(command=lambda: self.safe_run_operation('incremental_sync'))
        actions['Export to Files'].configure(command=lambda: self.safe_run_operation('export_files'))
        actions['Export Images'].configure(command=lambda: self.safe_run_operation('export_images'))
        actions['Test Connections'].configure(command=self.safe_test_all_connections)
        actions['View Logs'].configure(command=self.safe_open_log_viewer)
    
    def setup_callbacks(self):
        """Set up callbacks for real-time updates with thread safety"""
        # Log manager callbacks
        self.log_manager.add_callback(self.on_new_log_entry_safe)
        
        # Operation manager callbacks
        self.operation_manager.add_operation_callback(self.on_operation_status_safe)
    
    def on_new_log_entry_safe(self, log_entry):
        """Thread-safe handler for new log entries"""
        if log_entry.level in ['ERROR', 'CRITICAL']:
            self.schedule_gui_update(self.update_status_indicator)
    
    def on_operation_status_safe(self, status, operation, result=None):
        """Thread-safe handler for operation status updates"""
        def update_operation_ui():
            try:
                if status == 'start':
                    self.quick_actions.show_progress(operation.replace('_', ' ').title())
                elif status == 'complete':
                    self.quick_actions.hide_progress()
                    # Schedule refresh after operation completes
                    self.root.after(2000, self.safe_refresh_migration_status)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error updating operation UI: {e}")
        
        self.schedule_gui_update(update_operation_ui)
    
    # Thread-safe wrapper methods for all operations
    def safe_test_filemaker_connection(self):
        """Thread-safe FileMaker connection test"""
        def test_operation():
            try:
                self.log_manager.log(LogLevel.INFO, "GUI", "Testing FileMaker connection from GUI")
                self.connection_tester.test_filemaker_connection(self.on_connection_test_complete_safe)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error in FileMaker connection test: {e}")
        
        threading.Thread(target=test_operation, daemon=True, name="FM-Test-Trigger").start()
    
    def safe_test_target_connection(self):
        """Thread-safe target connection test"""
        def test_operation():
            try:
                self.log_manager.log(LogLevel.INFO, "GUI", "Testing target connection from GUI")
                self.connection_tester.test_target_connection(self.on_connection_test_complete_safe)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error in target connection test: {e}")
        
        threading.Thread(target=test_operation, daemon=True, name="Target-Test-Trigger").start()
    
    def safe_test_all_connections(self):
        """Thread-safe test all connections"""
        def test_operation():
            try:
                self.log_manager.log(LogLevel.INFO, "GUI", "Testing all connections from GUI")
                self.connection_tester.test_all_connections(self.on_connection_test_complete_safe)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error in connection tests: {e}")
        
        threading.Thread(target=test_operation, daemon=True, name="All-Tests-Trigger").start()
    
    def on_connection_test_complete_safe(self, connection_type, status):
        """Thread-safe connection test completion handler"""
        def update_connection_ui():
            try:
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
                
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error updating connection UI: {e}")
        
        self.schedule_gui_update(update_connection_ui)
    
    def safe_run_operation(self, operation: str):
        """Thread-safe operation runner"""
        def run_operation():
            try:
                if self.operation_manager.is_operation_running:
                    def show_warning():
                        messagebox.showwarning("Operation Running", 
                                             "Another operation is already running. Please wait.")
                    self.schedule_gui_update(show_warning)
                    return
                
                # Confirm operation
                def confirm_and_run():
                    if messagebox.askyesno("Confirm Operation", 
                                          f"Are you sure you want to run {operation.replace('_', ' ')}?"):
                        self.operation_manager.run_operation_async(operation)
                
                self.schedule_gui_update(confirm_and_run)
                
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error running operation {operation}: {e}")
        
        threading.Thread(target=run_operation, daemon=True, name=f"Operation-{operation}").start()
    
    def safe_refresh_migration_status(self):
        """Thread-safe migration status refresh"""
        def refresh_operation():
            try:
                self.log_manager.log(LogLevel.INFO, "GUI", "Refreshing migration status from GUI")
                self.status_manager.refresh_migration_status(self.on_status_complete_safe)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error refreshing status: {e}")
        
        threading.Thread(target=refresh_operation, daemon=True, name="Status-Refresh-Trigger").start()
    
    def on_status_complete_safe(self, success, data):
        """Thread-safe status refresh completion handler"""
        def update_status_ui():
            try:
                if success:
                    self.migration_overview.update_overview(data)
                    # Update connection status from the data
                    conn_status = data.get('connection_status', {})
                    if conn_status:
                        # Update internal connection status
                        for conn_type in ['filemaker', 'target']:
                            if conn_type in conn_status:
                                status_info = conn_status[conn_type]
                                self.connection_tester._update_connection_status(
                                    conn_type, 
                                    status_info.get('connected', False),
                                    status_info.get('message', 'Unknown')
                                )
                    
                    self.update_connection_displays()
                else:
                    self.log_manager.log(LogLevel.ERROR, "GUI", f"Failed to refresh status: {data}")
                    
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error updating status UI: {e}")
        
        self.schedule_gui_update(update_status_ui)
    
    def update_connection_displays(self):
        """Update connection status displays"""
        try:
            connection_status = self.connection_tester.connection_status
            fm_status = connection_status['filemaker']
            target_status = connection_status['target']
            
            self.fm_status_card.update_status(fm_status['connected'], fm_status['message'])
            self.target_status_card.update_status(target_status['connected'], target_status['message'])
            
            self.quick_actions.update_button_states(
                fm_status['connected'], 
                target_status['connected']
            )
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "GUI", f"Error updating connection displays: {e}")
    
    def update_status_indicator(self):
        """Update the overall status indicator"""
        try:
            # Count recent errors
            recent_logs = self.log_manager.get_recent_logs(limit=100)
            error_count = len([log for log in recent_logs if log.level in ['ERROR', 'CRITICAL']])
            
            self.status_bar.update_health(error_count)
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "GUI", f"Error updating status indicator: {e}")
    
    # Thread-safe child window management
    def safe_open_configuration(self):
        """Thread-safe configuration window opener"""
        def open_config():
            try:
                with self._child_windows_lock:
                    # Close existing config window if open
                    if 'config' in self.child_windows:
                        try:
                            self.child_windows['config'].close_window()
                        except:
                            pass
                        del self.child_windows['config']
                    
                    # Import here to avoid circular imports
                    from gui.filemaker_gui import ConfigurationWindow
                    self.child_windows['config'] = ConfigurationWindow(
                        self.root, 'config.toml', self.on_config_saved_safe
                    )
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error opening configuration: {e}")
        
        self.schedule_gui_update(open_config)
    
    def on_config_saved_safe(self):
        """Thread-safe configuration save handler"""
        def handle_config_save():
            try:
                with self._child_windows_lock:
                    if 'config' in self.child_windows:
                        del self.child_windows['config']
                
                self.log_manager.log(LogLevel.INFO, "Config", "Configuration updated via GUI")
                # Test connections after config save
                self.root.after(1000, self.safe_test_all_connections)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error handling config save: {e}")
        
        self.schedule_gui_update(handle_config_save)
    
    def safe_open_log_viewer(self):
        """Thread-safe log viewer opener"""
        def open_log_viewer():
            try:
                with self._child_windows_lock:
                    # Close existing log viewer if open
                    if 'log_viewer' in self.child_windows:
                        try:
                            self.child_windows['log_viewer'].close_window()
                        except:
                            pass
                        del self.child_windows['log_viewer']
                    
                    # Create new log viewer
                    self.child_windows['log_viewer'] = LogViewerWindow(self.root, self.log_manager)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Failed to open log viewer: {e}")
                def show_error():
                    messagebox.showerror("Error", f"Failed to open log viewer: {e}")
                self.schedule_gui_update(show_error)
        
        self.schedule_gui_update(open_log_viewer)
    
    def safe_open_log_stats(self):
        """Thread-safe log statistics opener"""
        def open_log_stats():
            try:
                with self._child_windows_lock:
                    # Close existing stats window if open
                    if 'log_stats' in self.child_windows:
                        try:
                            self.child_windows['log_stats'].close_window()
                        except:
                            pass
                        del self.child_windows['log_stats']
                    
                    # Create new stats window
                    self.child_windows['log_stats'] = LogStatsWindow(self.root, self.log_manager)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Failed to open log stats: {e}")
                def show_error():
                    messagebox.showerror("Error", f"Failed to open log statistics: {e}")
                self.schedule_gui_update(show_error)
        
        self.schedule_gui_update(open_log_stats)
    
    def safe_run_diagnostics(self):
        """Thread-safe diagnostics runner"""
        def run_diagnostics():
            try:
                self.log_manager.log(LogLevel.INFO, "Diagnostics", "Starting system diagnostics")
                
                results = {
                    'timestamp': datetime.now().isoformat(),
                    'config_file_exists': Path('config.toml').exists(),
                    'script_file_exists': Path('filemaker_extract_refactored.py').exists(),
                    'logs_dir_exists': Path('logs').exists(),
                    'thread_safe_mode': True
                }
                
                def show_results():
                    self.show_diagnostic_results(results)
                
                self.schedule_gui_update(show_results)
                
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error running diagnostics: {e}")
        
        threading.Thread(target=run_diagnostics, daemon=True, name="Diagnostics").start()
    
    def show_diagnostic_results(self, results):
        """Show diagnostic results in a popup window"""
        try:
            diag_window = tk.Toplevel(self.root)
            diag_window.title("System Diagnostics")
            diag_window.geometry("600x500")
            diag_window.transient(self.root)
            diag_window.grab_set()
            
            # Create scrollable text widget
            from tkinter import scrolledtext
            text_widget = scrolledtext.ScrolledText(diag_window, wrap=tk.WORD)
            text_widget.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Format results
            connection_status = self.connection_tester.connection_status
            fm_status = connection_status['filemaker']
            target_status = connection_status['target']
            
            report = f"""System Diagnostic Report
========================
Timestamp: {results['timestamp']}

File Checks:
✓ config.toml exists: {results['config_file_exists']}
✓ filemaker_extract_refactored.py exists: {results['script_file_exists']}
✓ logs directory exists: {results['logs_dir_exists']}

Connection Status:
• FileMaker: {'Connected' if fm_status['connected'] else 'Disconnected'}
  Message: {fm_status['message']}
• Target: {'Connected' if target_status['connected'] else 'Disconnected'}
  Message: {target_status['message']}

Operation Manager:
• Current State: {'Running' if self.operation_manager.is_operation_running else 'Idle'}

Log Manager:
• Session ID: {self.log_manager.session_id}
• Log Level: {self.log_manager.log_level}
• Debug Mode: {self.log_manager.debug_mode}
• Total Logs: {self.log_manager.get_log_count():,}

For detailed logs, check Tools → View Activity Logs
"""
            
            text_widget.insert('1.0', report)
            text_widget.configure(state='disabled')
            
            # Button frame
            button_frame = ttk.Frame(diag_window)
            button_frame.pack(fill='x', padx=10, pady=(0, 10))
            
            ttk.Button(button_frame, text="Close", command=diag_window.destroy).pack(side='right')
            
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "GUI", f"Error showing diagnostic results: {e}")
    
    def safe_open_export_folder(self):
        """Export folder opener"""
        def open_folder():
            try:
                export_path = Path('./exports')
                if export_path.exists():
                    if sys.platform == 'win32':
                        os.startfile(export_path)
                    elif sys.platform == 'darwin':
                        subprocess.run(['open', export_path])
                    else:
                        subprocess.run(['xdg-open', export_path])
                else:
                    def show_not_found():
                        messagebox.showinfo("Not Found", f"Export directory not found: {export_path}")
                    self.schedule_gui_update(show_not_found)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error opening export folder: {e}")
        
        threading.Thread(target=open_folder, daemon=True, name="Open-Export").start()
    
    def safe_open_log_folder(self):
        """log folder opener"""
        def open_folder():
            try:
                log_path = self.log_manager.log_dir
                if log_path.exists():
                    if sys.platform == 'win32':
                        os.startfile(log_path)
                    elif sys.platform == 'darwin':
                        subprocess.run(['open', log_path])
                    else:
                        subprocess.run(['xdg-open', log_path])
                else:
                    def show_not_found():
                        messagebox.showinfo("Not Found", f"Log directory not found: {log_path}")
                    self.schedule_gui_update(show_not_found)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error opening log folder: {e}")
        
        threading.Thread(target=open_folder, daemon=True, name="Open-Logs").start()
    
    def safe_show_about(self):
        """About dialog"""
        def show_about():
            try:
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
• Thread-safe operation for stability

Built with Python and tkinter.
Enhanced with comprehensive thread safety.
"""
                messagebox.showinfo("About FileMaker Sync", about_text)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error showing about dialog: {e}")
        
        self.schedule_gui_update(show_about)
    
    def safe_exit(self):
        """Application exit"""
        def check_and_exit():
            try:
                if self.operation_manager.is_operation_running:
                    if messagebox.askokcancel("Quit", 
                                            "An operation is running. Do you want to stop it and quit?"):
                        self.cleanup_and_exit()
                else:
                    self.cleanup_and_exit()
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error during exit check: {e}")
                self.cleanup_and_exit()
        
        self.schedule_gui_update(check_and_exit)
    
    def start_auto_refresh(self):
        """Start auto-refresh"""
        def auto_refresh():
            try:
                if not self._shutdown_requested.is_set():
                    # Only refresh if not currently running an operation
                    if not self.operation_manager.is_operation_running:
                        self.update_status_indicator()
                    
                    # Schedule next refresh
                    self._auto_refresh_timer = self.root.after(self._auto_refresh_interval, auto_refresh)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "GUI", f"Error in auto-refresh: {e}")
        
        auto_refresh()
    
    def stop_auto_refresh(self):
        """Stop auto-refresh"""
        if self._auto_refresh_timer:
            self.root.after_cancel(self._auto_refresh_timer)
            self._auto_refresh_timer = None
    
    def on_closing(self):
        """Handle application close"""
        def handle_close():
            try:
                if self.operation_manager.is_operation_running:
                    if messagebox.askokcancel("Quit", 
                                            "An operation is running. Do you want to stop it and quit?"):
                        self.cleanup_and_exit()
                else:
                    self.cleanup_and_exit()
            except Exception as e:
                print(f"Error during close: {e}")
                self.cleanup_and_exit()
        
        self.schedule_gui_update(handle_close)
    
    def cleanup_and_exit(self):
        """Clean up resources and exit"""
        try:
            self.log_manager.log(LogLevel.INFO, "Application", "Application shutting down")
            
            # Set shutdown flag
            self._shutdown_requested.set()
            
            # Stop auto-refresh
            self.stop_auto_refresh()
            
            # Cancel any running operations
            self.operation_manager.cancel_current_operation()
            
            # Shutdown operation manager
            self.operation_manager.shutdown()
            
            # Close all child windows
            with self._child_windows_lock:
                for window_name, window_obj in list(self.child_windows.items()):
                    try:
                        window_obj.close_window()
                    except:
                        pass
                self.child_windows.clear()
            
            # Remove callbacks
            try:
                self.log_manager.remove_callback(self.on_new_log_entry_safe)
            except:
                pass
            
            # Destroy main window
            self.root.destroy()
            
        except Exception as e:
            print(f"Error during cleanup: {e}")
            try:
                self.root.destroy()
            except:
                pass


def main():
    """Main entry point"""
    # Create root window
    root = tk.Tk()
    
    # Set window icon if available
    try:
        root.iconbitmap('icon.ico')
    except:
        pass
    
    # Create the thread-safe application
    app = FileMakerSyncGUI(root)
    
    # Handle window close
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    
    # Center window on screen
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    # Start the GUI with error handling
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("Application interrupted by user")
        try:
            app.cleanup_and_exit()
        except:
            pass
    except Exception as e:
        print(f"Application error: {e}")
        try:
            app.cleanup_and_exit()
        except:
            pass
    finally:
        try:
            root.destroy()
        except:
            pass


if __name__ == "__main__":
    main()