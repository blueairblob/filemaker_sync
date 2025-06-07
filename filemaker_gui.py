#!/usr/bin/env python3
"""
FileMaker Sync GUI - Standalone Desktop Application
Wraps the existing filemaker_extract.py script with a user-friendly interface
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import subprocess
import sys
import os
import json
from pathlib import Path
import queue
import time
from datetime import datetime

class FileMakerSyncGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("FileMaker to Supabase Sync")
        self.root.geometry("800x700")
        self.root.resizable(True, True)
        
        # Initialize variables
        self.config_file = Path("sync_config.json")
        self.sync_process = None
        self.log_queue = queue.Queue()
        self.is_syncing = False
        
        # Load saved configuration
        self.config = self.load_config()
        
        self.create_widgets()
        self.load_saved_settings()
        
        # Start log monitoring
        self.check_log_queue()
    
    def create_widgets(self):
        # Create main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Create notebook for main tabs (takes most of the space)
        notebook = ttk.Notebook(main_container)
        notebook.pack(fill='both', expand=True)
        
        # Sync Tab (first tab)
        sync_frame = ttk.Frame(notebook)
        notebook.add(sync_frame, text="Sync Operations")
        self.create_sync_tab(sync_frame)
        
        # Configuration Tab (last tab) - full tab display
        config_frame = ttk.Frame(notebook)
        notebook.add(config_frame, text="Configuration")
        self.create_config_tab(config_frame)
        
        # Create logs section at bottom (fixed height, doesn't expand)
        log_section = ttk.LabelFrame(main_container, text="Activity Log", padding=5)
        log_section.pack(fill='x', pady=(10, 0))  # Changed from fill='both', expand=True
        self.create_log_section(log_section)
    
    def create_config_tab(self, parent):
        # Main frame with scrollbar
        canvas = tk.Canvas(parent)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Database Settings
        db_group = ttk.LabelFrame(scrollable_frame, text="Database Settings", padding=10)
        db_group.pack(fill='x', padx=5, pady=5)
        
    def browse_export_dir(self):
        directory = filedialog.askdirectory(title="Select Export Directory")
        if directory:
            self.export_dir_var.set(directory)
    
    def create_config_tab(self, parent):
        # Main frame with scrollbar
        canvas = tk.Canvas(parent)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Database Settings
        db_group = ttk.LabelFrame(scrollable_frame, text="FileMaker Database Connection", padding=10)
        db_group.pack(fill='x', padx=5, pady=5)
        
        # FileMaker DSN with dropdown and help
        dsn_frame = ttk.Frame(db_group)
        dsn_frame.grid(row=0, column=0, columnspan=3, sticky='ew', pady=5)
        dsn_frame.columnconfigure(1, weight=1)
        
        ttk.Label(dsn_frame, text="FileMaker DSN:").grid(row=0, column=0, sticky='w', padx=(0,5))
        
        # Create combobox for DSN selection
        self.dsn_var = tk.StringVar()
        self.dsn_combo = ttk.Combobox(dsn_frame, textvariable=self.dsn_var, width=40)
        self.dsn_combo.grid(row=0, column=1, sticky='ew', padx=5)
        self.dsn_combo.bind('<<ComboboxSelected>>', self.on_dsn_selection)
        
        # Refresh button
        ttk.Button(dsn_frame, text="↻", width=3, command=self.refresh_dsn_list).grid(row=0, column=2, padx=2)
        
        # Help text for DSN
        help_text = ("💡 DSN = ODBC Data Source Name. If empty, click 'Setup ODBC' to create one.\n"
                    "   Example DSN names: FileMaker_DB, RAT_Database, MyFileMaker")
        help_label = ttk.Label(dsn_frame, text=help_text, foreground='gray', font=('Arial', 8))
        help_label.grid(row=1, column=0, columnspan=3, sticky='w', pady=(2,0))
        
        # ODBC setup buttons
        odbc_buttons = ttk.Frame(db_group)
        odbc_buttons.grid(row=1, column=0, columnspan=3, sticky='ew', pady=5)
        
        ttk.Button(odbc_buttons, text="Setup ODBC", command=self.open_odbc_admin).pack(side='left', padx=5)
        ttk.Button(odbc_buttons, text="Test Connection", command=self.test_connection).pack(side='left', padx=5)
        ttk.Button(odbc_buttons, text="Refresh DSN List", command=self.refresh_dsn_list).pack(side='left', padx=5)
        
        # Export Directory
        ttk.Label(db_group, text="Export Directory:").grid(row=2, column=0, sticky='w', pady=(10,2))
        self.export_dir_var = tk.StringVar()
        dir_frame = ttk.Frame(db_group)
        dir_frame.grid(row=2, column=1, columnspan=2, sticky='ew', padx=5, pady=(10,2))
        dir_frame.columnconfigure(0, weight=1)
        ttk.Entry(dir_frame, textvariable=self.export_dir_var).grid(row=0, column=0, sticky='ew')
        ttk.Button(dir_frame, text="Browse", command=self.browse_export_dir).grid(row=0, column=1, padx=(5,0))
        
        # Configure grid weights
        db_group.columnconfigure(1, weight=1)
        
        # Sync Options
        sync_group = ttk.LabelFrame(scrollable_frame, text="Sync Options", padding=10)
        sync_group.pack(fill='x', padx=5, pady=5)
        
        # Database type
        ttk.Label(sync_group, text="Target Database:").grid(row=0, column=0, sticky='w', pady=2)
        self.db_type_var = tk.StringVar(value="supabase")
        db_combo = ttk.Combobox(sync_group, textvariable=self.db_type_var, 
                               values=["supabase", "mysql"], state="readonly", width=20)
        db_combo.grid(row=0, column=1, sticky='w', padx=5, pady=2)
        
        # Max rows
        ttk.Label(sync_group, text="Max Rows (or 'all'):").grid(row=1, column=0, sticky='w', pady=2)
        self.max_rows_var = tk.StringVar(value="all")
        ttk.Entry(sync_group, textvariable=self.max_rows_var, width=20).grid(row=1, column=1, sticky='w', padx=5, pady=2)
        
        # Start from
        ttk.Label(sync_group, text="Start from Image ID:").grid(row=2, column=0, sticky='w', pady=2)
        self.start_from_var = tk.StringVar()
        ttk.Entry(sync_group, textvariable=self.start_from_var, width=20).grid(row=2, column=1, sticky='w', padx=5, pady=2)
        
        # Tables to export
        ttk.Label(sync_group, text="Tables (comma-separated or 'all'):").grid(row=3, column=0, sticky='w', pady=2)
        self.tables_var = tk.StringVar(value="all")
        ttk.Entry(sync_group, textvariable=self.tables_var, width=50).grid(row=3, column=1, padx=5, pady=2)
        
        # Checkboxes for operations
        ops_group = ttk.LabelFrame(scrollable_frame, text="Operations", padding=10)
        ops_group.pack(fill='x', padx=5, pady=5)
        
        self.ddl_var = tk.BooleanVar(value=True)
        self.dml_var = tk.BooleanVar(value=True)
        self.del_data_var = tk.BooleanVar()
        self.del_db_var = tk.BooleanVar()
        self.debug_var = tk.BooleanVar()
        
        ttk.Checkbutton(ops_group, text="Export DDL (Table Structure)", variable=self.ddl_var).pack(anchor='w')
        ttk.Checkbutton(ops_group, text="Export DML (Data)", variable=self.dml_var).pack(anchor='w')
        ttk.Checkbutton(ops_group, text="Delete existing data", variable=self.del_data_var).pack(anchor='w')
        ttk.Checkbutton(ops_group, text="Delete database objects", variable=self.del_db_var).pack(anchor='w')
        ttk.Checkbutton(ops_group, text="Debug mode", variable=self.debug_var).pack(anchor='w')
        
        # Save/Load buttons
        button_frame = ttk.Frame(scrollable_frame)
        button_frame.pack(fill='x', padx=5, pady=10)
        
        ttk.Button(button_frame, text="Save Configuration", command=self.save_config).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Load Configuration", command=self.load_saved_settings).pack(side='left', padx=5)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Load DSN list on startup
        self.root.after(100, self.refresh_dsn_list)
    
    def create_widgets(self):
        # Create main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Create notebook for main tabs
        notebook = ttk.Notebook(main_container)
        notebook.pack(fill='both', expand=True)
        
        # Sync Tab (first tab) - with resizable log section
        sync_frame = ttk.Frame(notebook)
        notebook.add(sync_frame, text="Sync Operations")
        self.create_sync_tab(sync_frame)
        
        # Configuration Tab (last tab) - with fixed bottom log
        config_frame = ttk.Frame(notebook)
        notebook.add(config_frame, text="Configuration")
        self.create_config_tab_with_log(config_frame)
    
    def create_sync_tab(self, parent):
        # Create main paned window for resizable sections
        paned_window = ttk.PanedWindow(parent, orient='vertical')
        paned_window.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Top section for sync controls
        top_frame = ttk.Frame(paned_window)
        paned_window.add(top_frame, weight=3)  # Takes 3/4 of space initially
        
        # Status display
        status_frame = ttk.LabelFrame(top_frame, text="Sync Status", padding=10)
        status_frame.pack(fill='x', padx=5, pady=5)
        
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(status_frame, textvariable=self.status_var, font=('Arial', 12, 'bold')).pack()
        
        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(status_frame, variable=self.progress_var, 
                                          maximum=100, length=400)
        self.progress_bar.pack(pady=10)
        
        # Quick actions
        actions_frame = ttk.LabelFrame(top_frame, text="Quick Actions", padding=10)
        actions_frame.pack(fill='x', padx=5, pady=5)
        
        # Create a grid of buttons
        button_configs = [
            ("Full Sync to Database", self.full_sync_db, 0, 0),
            ("Export to Files Only", self.export_files, 0, 1),
            ("Export Images Only", self.export_images, 1, 0),
            ("Schema Info Only", self.schema_info, 1, 1),
        ]
        
        for text, command, row, col in button_configs:
            btn = ttk.Button(actions_frame, text=text, command=command, width=20)
            btn.grid(row=row, column=col, padx=5, pady=5)
        
        # Control buttons
        control_frame = ttk.Frame(top_frame)
        control_frame.pack(fill='x', padx=5, pady=10)
        
        self.start_btn = ttk.Button(control_frame, text="Start Custom Sync", 
                                   command=self.start_custom_sync, style='Accent.TButton')
        self.start_btn.pack(side='left', padx=5)
        
        self.stop_btn = ttk.Button(control_frame, text="Stop Sync", 
                                  command=self.stop_sync, state='disabled')
        self.stop_btn.pack(side='left', padx=5)
        
        ttk.Button(control_frame, text="Open Export Folder", 
                  command=self.open_export_folder).pack(side='right', padx=5)
        
        # Bottom section for resizable logs
        log_frame = ttk.LabelFrame(paned_window, text="Activity Log - Real-time Output", padding=5)
        paned_window.add(log_frame, weight=1)  # Takes 1/4 of space initially
        
        # Log display (resizable)
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=8)
        self.log_text.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Log controls
        log_controls = ttk.Frame(log_frame)
        log_controls.pack(fill='x', padx=5, pady=(0, 5))
        
        ttk.Button(log_controls, text="Clear Logs", command=self.clear_logs).pack(side='left', padx=5)
        ttk.Button(log_controls, text="Save Logs", command=self.save_logs).pack(side='left', padx=5)
        ttk.Button(log_controls, text="Test Connections", command=self.test_all_connections).pack(side='left', padx=10)
        
        self.autoscroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(log_controls, text="Auto-scroll", variable=self.autoscroll_var).pack(side='right', padx=5)
        
        # Add initial welcome message
        self.log_message("Welcome to FileMaker Sync! Test your connections, then start syncing.")
    
    def create_config_tab_with_log(self, parent):
        # Create main container
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill='both', expand=True)
        
        # Configuration content (takes most space)
        config_content = ttk.Frame(main_frame)
        config_content.pack(fill='both', expand=True)
        self.create_config_tab(config_content)
        
        # Small log section at bottom (fixed height)
        log_section = ttk.LabelFrame(main_frame, text="Activity Log", padding=5)
        log_section.pack(fill='x', pady=(10, 0))
        
        # Smaller log display for config tab
        config_log_text = scrolledtext.ScrolledText(log_section, wrap=tk.WORD, height=4)
        config_log_text.pack(fill='x', padx=5, pady=5)
        
        # Share the same log content
        self.config_log_text = config_log_text
        
        # Log controls for config tab
        config_log_controls = ttk.Frame(log_section)
        config_log_controls.pack(fill='x', padx=5, pady=(0, 5))
        
        ttk.Button(config_log_controls, text="Test Connections", command=self.test_all_connections).pack(side='left', padx=5)
        ttk.Button(config_log_controls, text="Clear", command=self.clear_logs).pack(side='left', padx=5)
    
    def create_log_section(self, parent):
        """Legacy method - no longer used since logs are integrated into tabs"""
        pass
    
    def create_log_tab(self, parent):
        """Legacy method - no longer used since logs are at bottom"""
        pass
    
    def get_available_dsns(self):
        """Get list of available ODBC DSNs"""
        system_dsns = []
        user_dsns = []
        
        try:
            import winreg
            
            # Get System DSNs
            try:
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                                   r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources")
                i = 0
                while True:
                    try:
                        name, value, type = winreg.EnumValue(key, i)
                        if 'filemaker' in value.lower() or 'fm' in value.lower():
                            system_dsns.append(f"{name} (System - {value})")
                        else:
                            system_dsns.append(f"{name} (System)")
                        i += 1
                    except WindowsError:
                        break
                winreg.CloseKey(key)
            except Exception:
                pass
            
            # Get User DSNs
            try:
                key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, 
                                   r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources")
                i = 0
                while True:
                    try:
                        name, value, type = winreg.EnumValue(key, i)
                        if 'filemaker' in value.lower() or 'fm' in value.lower():
                            user_dsns.append(f"{name} (User - {value})")
                        else:
                            user_dsns.append(f"{name} (User)")
                        i += 1
                    except WindowsError:
                        break
                winreg.CloseKey(key)
            except Exception:
                pass
                
        except ImportError:
            # winreg not available (non-Windows)
            pass
        except Exception as e:
            self.log_message(f"Error reading DSNs: {e}")
        
        # Combine and sort
        all_dsns = system_dsns + user_dsns
        return sorted(all_dsns) if all_dsns else ["No DSNs found"]
    
    def refresh_dsn_list(self):
        """Refresh the DSN dropdown list"""
        dsns = self.get_available_dsns()
        self.dsn_combo['values'] = dsns
        if dsns and dsns[0] != "No DSNs found":
            # If current value not in list, clear it
            current_val = self.dsn_var.get()
            if current_val and not any(current_val in dsn for dsn in dsns):
                pass  # Keep current value even if not in list (user might have typed it)
    
    def on_dsn_selection(self, event):
        """Handle DSN selection from dropdown"""
        selected = self.dsn_combo.get()
        if selected and "(" in selected:
            # Extract just the DSN name (before the parentheses)
            dsn_name = selected.split(" (")[0]
            self.dsn_var.set(dsn_name)
    
    def open_odbc_admin(self):
        """Open ODBC Data Source Administrator"""
        try:
            import subprocess
            subprocess.Popen(['odbcad32.exe'])
            self.log_message("Opened ODBC Data Source Administrator")
        except Exception as e:
            self.log_message(f"Could not open ODBC Admin: {e}")
            messagebox.showwarning("Warning", 
                                 "Could not open ODBC Administrator.\n"
                                 "Please open it manually: Start Menu → ODBC Data Sources")
        directory = filedialog.askdirectory(title="Select Export Directory")
        if directory:
            self.export_dir_var.set(directory)
    
    def save_config(self):
        config = {
            'dsn': self.dsn_var.get(),
            'export_dir': self.export_dir_var.get(),
            'db_type': self.db_type_var.get(),
            'max_rows': self.max_rows_var.get(),
            'start_from': self.start_from_var.get(),
            'tables': self.tables_var.get(),
            'ddl': self.ddl_var.get(),
            'dml': self.dml_var.get(),
            'del_data': self.del_data_var.get(),
            'del_db': self.del_db_var.get(),
            'debug': self.debug_var.get()
        }
        
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=4)
            self.log_message("Configuration saved successfully")
            messagebox.showinfo("Success", "Configuration saved!")
        except Exception as e:
            self.log_message(f"Error saving configuration: {e}")
            messagebox.showerror("Error", f"Failed to save configuration: {e}")
    
    def load_config(self):
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.log_message(f"Error loading configuration: {e}")
        return {}
    
    def load_saved_settings(self):
        if self.config:
            self.dsn_var.set(self.config.get('dsn', ''))
            self.export_dir_var.set(self.config.get('export_dir', ''))
            self.db_type_var.set(self.config.get('db_type', 'supabase'))
            self.max_rows_var.set(self.config.get('max_rows', 'all'))
            self.start_from_var.set(self.config.get('start_from', ''))
            self.tables_var.set(self.config.get('tables', 'all'))
            self.ddl_var.set(self.config.get('ddl', True))
            self.dml_var.set(self.config.get('dml', True))
            self.del_data_var.set(self.config.get('del_data', False))
            self.del_db_var.set(self.config.get('del_db', False))
            self.debug_var.set(self.config.get('debug', False))
    
    def test_all_connections(self):
        """Test both FileMaker and Supabase connections"""
        dsn_input = self.dsn_var.get().strip()
        if not dsn_input:
            messagebox.showerror("Error", "Please enter or select a FileMaker DSN first")
            return
            
        # Extract DSN name if it was selected from dropdown
        if "(" in dsn_input:
            dsn_name = dsn_input.split(" (")[0]
        else:
            dsn_name = dsn_input
            
        self.log_message("=" * 50)
        self.log_message("🔍 Testing Connections...")
        self.log_message(f"📊 FileMaker DSN: {dsn_name}")
        self.log_message(f"🚀 Target Database: {self.db_type_var.get()}")
        self.log_message("=" * 50)
        
        # Create test command that will test both connections
        cmd = self.build_command(['--info-only', '--max-rows', '1', '--debug'])
        
        def run_connection_test():
            try:
                # Start the process with real-time output
                self.sync_process = subprocess.Popen(
                    cmd, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                # Read output line by line and display in real-time
                output_lines = []
                for line in iter(self.sync_process.stdout.readline, ''):
                    if line:
                        line = line.strip()
                        output_lines.append(line)
                        self.log_queue.put(line)
                
                # Wait for process to complete
                self.sync_process.wait()
                
                # Analyze the output for connection results
                output_text = '\n'.join(output_lines)
                
                filemaker_success = False
                supabase_success = False
                tables_found = []
                
                # Check for FileMaker success indicators
                if "base tables found:" in output_text:
                    filemaker_success = True
                    # Extract table names
                    import re
                    match = re.search(r"base tables found: \[(.*?)\]", output_text)
                    if match:
                        tables_text = match.group(1)
                        tables_found = [t.strip().strip("'") for t in tables_text.split(',')]
                
                # Check for Supabase success
                if "Connected to PostgreSQL version:" in output_text:
                    supabase_success = True
                elif "Error connecting:" in output_text and "supabase" in output_text.lower():
                    supabase_success = False
                
                # Check for errors
                connection_errors = []
                if "Data source name not found" in output_text:
                    connection_errors.append("FileMaker DSN not found")
                if "FATAL:" in output_text or "connection to server" in output_text:
                    connection_errors.append("Supabase connection failed")
                
                # Display results
                self.log_message("=" * 50)
                self.log_message("📋 CONNECTION TEST RESULTS:")
                self.log_message("=" * 50)
                
                if filemaker_success:
                    self.log_message(f"✅ FileMaker: Connected successfully to DSN '{dsn_name}'")
                    self.log_message(f"📊 Found {len(tables_found)} tables: {', '.join(tables_found)}")
                else:
                    self.log_message(f"❌ FileMaker: Connection failed to DSN '{dsn_name}'")
                
                if supabase_success:
                    self.log_message(f"✅ Supabase: Connected successfully")
                else:
                    self.log_message(f"❌ Supabase: Connection failed")
                
                if filemaker_success and supabase_success:
                    self.log_message("🎉 All connections successful! Ready to sync.")
                    messagebox.showinfo("Success", 
                                       f"✅ All connections successful!\n\n"
                                       f"FileMaker: {len(tables_found)} tables found\n"
                                       f"Supabase: Connected\n\n"
                                       f"Ready to sync!")
                else:
                    error_msg = "Connection issues found:\n\n"
                    if not filemaker_success:
                        error_msg += "❌ FileMaker connection failed\n"
                        error_msg += "   • Check DSN name and spelling\n"
                        error_msg += "   • Ensure FileMaker Pro is running\n"
                        error_msg += "   • Verify ODBC sharing is enabled\n\n"
                    if not supabase_success:
                        error_msg += "❌ Supabase connection failed\n"
                        error_msg += "   • Check config.toml settings\n"
                        error_msg += "   • Verify database URL and credentials\n"
                        error_msg += "   • Check network connectivity\n\n"
                    
                    messagebox.showerror("Connection Test Failed", error_msg)
                
                self.log_message("=" * 50)
                
            except subprocess.TimeoutExpired:
                self.log_message("❌ Connection test timed out")
                messagebox.showerror("Timeout", "Connection test timed out. Check if services are running.")
            except Exception as e:
                self.log_message(f"❌ Connection test error: {e}")
                messagebox.showerror("Error", f"Test failed: {e}")
            finally:
                self.sync_process = None
        
        threading.Thread(target=run_connection_test, daemon=True).start()
    
    def test_connection(self):
        """Legacy single connection test - now calls the comprehensive test"""
        self.test_all_connections()
    
    def build_command(self, extra_args=None):
        """Build the command line arguments for filemaker_extract.py"""
        # Get the path to filemaker_extract.py
        script_path = Path(__file__).parent / "filemaker_extract.py"
        
        cmd = [sys.executable, str(script_path)]
        
        # Add operation type
        if extra_args and '--info-only' in extra_args:
            cmd.extend(['--info-only'])
        elif self.ddl_var.get() and self.dml_var.get():
            cmd.extend(['--db-exp', '--ddl', '--dml'])
        elif self.ddl_var.get():
            cmd.extend(['--db-exp', '--ddl'])
        elif self.dml_var.get():
            cmd.extend(['--db-exp', '--dml'])
        else:
            cmd.extend(['--db-exp'])
        
        # Add other options
        if self.export_dir_var.get():
            cmd.extend(['--export-dir', self.export_dir_var.get()])
        
        if self.max_rows_var.get() and self.max_rows_var.get() != 'all':
            cmd.extend(['--max-rows', self.max_rows_var.get()])
        
        if self.start_from_var.get():
            cmd.extend(['--start-from', self.start_from_var.get()])
        
        if self.tables_var.get() and self.tables_var.get() != 'all':
            cmd.extend(['--tables-to-export', self.tables_var.get()])
        
        if self.db_type_var.get():
            cmd.extend(['--db-type', self.db_type_var.get()])
        
        if self.del_data_var.get():
            cmd.append('--del-data')
        
        if self.del_db_var.get():
            cmd.append('--del-db')
        
        if self.debug_var.get():
            cmd.append('--debug')
        
        # Add any extra arguments
        if extra_args:
            cmd.extend(extra_args)
        
        return cmd
    
    def run_sync(self, cmd):
        """Run the sync process in a separate thread"""
        def sync_worker():
            try:
                self.is_syncing = True
                self.status_var.set("Syncing...")
                self.start_btn.config(state='disabled')
                self.stop_btn.config(state='normal')
                
                # Start the process
                self.sync_process = subprocess.Popen(
                    cmd, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                # Read output line by line
                for line in iter(self.sync_process.stdout.readline, ''):
                    if line:
                        self.log_queue.put(line.strip())
                
                # Wait for process to complete
                self.sync_process.wait()
                
                if self.sync_process.returncode == 0:
                    self.log_queue.put("✓ Sync completed successfully!")
                    self.status_var.set("Completed Successfully")
                else:
                    self.log_queue.put(f"✗ Sync failed with code {self.sync_process.returncode}")
                    self.status_var.set("Sync Failed")
                
            except Exception as e:
                self.log_queue.put(f"✗ Sync error: {e}")
                self.status_var.set("Sync Error")
            finally:
                self.is_syncing = False
                self.start_btn.config(state='normal')
                self.stop_btn.config(state='disabled')
                self.sync_process = None
        
        threading.Thread(target=sync_worker, daemon=True).start()
    
    def full_sync_db(self):
        """Full sync to database"""
        self.ddl_var.set(True)
        self.dml_var.set(True)
        cmd = self.build_command()
        self.run_sync(cmd)
    
    def export_files(self):
        """Export to files only"""
        cmd = self.build_command(['--fn-exp'])
        self.run_sync(cmd)
    
    def export_images(self):
        """Export images only"""
        cmd = self.build_command(['--get-images'])
        self.run_sync(cmd)
    
    def schema_info(self):
        """Get schema information only"""
        cmd = self.build_command(['--info-only', '--get-schema'])
        self.run_sync(cmd)
    
    def start_custom_sync(self):
        """Start sync with current settings"""
        # Validate DSN
        dsn_input = self.dsn_var.get().strip()
        if not dsn_input:
            messagebox.showerror("Error", "Please enter or select a FileMaker DSN")
            return
        
        # Extract clean DSN name
        if "(" in dsn_input:
            clean_dsn = dsn_input.split(" (")[0]
            self.dsn_var.set(clean_dsn)  # Update with clean name
        
        if not self.ddl_var.get() and not self.dml_var.get():
            messagebox.showerror("Error", "Please select at least DDL or DML")
            return
        
        cmd = self.build_command()
        self.run_sync(cmd)
    
    def toggle_autoscroll(self):
        """Toggle auto-scroll for logs"""
        self.autoscroll_var.set(not self.autoscroll_var.get())
    
    def stop_sync(self):
        """Stop the current sync process"""
        if self.sync_process:
            try:
                self.sync_process.terminate()
                self.log_message("Sync process terminated by user")
                self.status_var.set("Stopped by User")
            except Exception as e:
                self.log_message(f"Error stopping sync: {e}")
    
    def open_export_folder(self):
        """Open the export folder in file explorer"""
        export_dir = self.export_dir_var.get()
        if export_dir and Path(export_dir).exists():
            os.startfile(export_dir)  # Windows specific
        else:
            messagebox.showwarning("Warning", "Export directory not set or doesn't exist")
    
    def log_message(self, message):
        """Add a message to the log queue"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.log_queue.put(formatted_message)
    
    def check_log_queue(self):
        """Check for new log messages and display them"""
        try:
            while True:
                message = self.log_queue.get_nowait()
                
                # Add to main log (Sync Operations tab)
                self.log_text.insert(tk.END, message + '\n')
                
                # Add to config log if it exists
                if hasattr(self, 'config_log_text'):
                    self.config_log_text.insert(tk.END, message + '\n')
                    # Keep config log shorter
                    if self.config_log_text.index('end-1c').split('.')[0] > '50':
                        self.config_log_text.delete('1.0', '10.0')
                
                # Auto-scroll if enabled
                if self.autoscroll_var.get():
                    self.log_text.see(tk.END)
                    if hasattr(self, 'config_log_text'):
                        self.config_log_text.see(tk.END)
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self.check_log_queue)
    
    def clear_logs(self):
        """Clear both log displays"""
        self.log_text.delete(1.0, tk.END)
        if hasattr(self, 'config_log_text'):
            self.config_log_text.delete(1.0, tk.END)
    
    def save_logs(self):
        """Save logs to a file"""
        filename = filedialog.asksaveasfilename(
            title="Save Logs",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'w') as f:
                    f.write(self.log_text.get(1.0, tk.END))
                messagebox.showinfo("Success", "Logs saved successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save logs: {e}")
    
    def toggle_autoscroll(self):
        """Toggle auto-scroll for logs"""
        self.autoscroll_var.set(not self.autoscroll_var.get())


def main():
    # Set up the main window
    root = tk.Tk()
    
    # Set window icon (if available)
    try:
        root.iconbitmap('icon.ico')  # Add your icon file
    except:
        pass
    
    # Create the application
    app = FileMakerSyncGUI(root)
    
    # Handle window close
    def on_closing():
        if app.is_syncing:
            if messagebox.askokcancel("Quit", "Sync is running. Do you want to stop it and quit?"):
                app.stop_sync()
                root.destroy()
        else:
            root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    # Start the GUI
    root.mainloop()


if __name__ == "__main__":
    main()