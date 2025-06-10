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

# Import our modules
from gui_logging import LogManager, LogLevel
from gui_widgets import StatusCard, MigrationOverview, QuickActions, RecentActivity, StatusBar
from gui_operations import OperationManager, ConnectionTester, StatusManager
from gui_logviewer import LogViewerWindow, LogStatsWindow

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
        
        # Configuration
        self.config_file = Path("sync_config.json")
        self.config = self.load_config()
        
        # Windows
        self.log_viewer_window = None
        self.log_stats_window = None
        
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
        
        self.operation_manager.run_operation_async