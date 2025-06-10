#!/usr/bin/env python3
"""
Enhanced FileMaker Sync GUI - Dashboard Style
Provides a comprehensive overview with real-time status monitoring
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import subprocess
import json
import sys
import os
from pathlib import Path
import queue
import time
from datetime import datetime
from typing import Dict, Any, Optional

class StatusCard(ttk.Frame):
    """Custom widget for displaying connection status"""
    
    def __init__(self, parent, title: str, **kwargs):
        super().__init__(parent, **kwargs)
        self.title = title
        self.create_widgets()
    
    def create_widgets(self):
        # Header frame
        header_frame = ttk.Frame(self)
        header_frame.pack(fill='x', padx=5, pady=5)
        
        # Status indicator (colored circle)
        self.status_label = ttk.Label(header_frame, text="●", font=('Arial', 16))
        self.status_label.pack(side='left', padx=(0, 10))
        
        # Title and message
        info_frame = ttk.Frame(header_frame)
        info_frame.pack(side='left', fill='x', expand=True)
        
        self.title_label = ttk.Label(info_frame, text=self.title, font=('Arial', 12, 'bold'))
        self.title_label.pack(anchor='w')
        
        self.message_label = ttk.Label(info_frame, text="Not tested", font=('Arial', 9))
        self.message_label.pack(anchor='w')
        
        # Test button
        self.test_button = ttk.Button(header_frame, text="Test", width=8)
        self.test_button.pack(side='right', padx=(10, 0))
    
    def update_status(self, connected: bool, message: str):
        """Update the status display"""
        if connected:
            self.status_label.configure(foreground='green')
            self.message_label.configure(foreground='dark green')
        else:
            self.status_label.configure(foreground='red')
            self.message_label.configure(foreground='dark red')
        
        self.message_label.configure(text=message)


class MigrationOverview(ttk.Frame):
    """Widget showing migration progress overview"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets()
    
    def create_widgets(self):
        # Header
        header_frame = ttk.Frame(self)
        header_frame.pack(fill='x', padx=5, pady=5)
        
        ttk.Label(header_frame, text="Migration Overview", font=('Arial', 14, 'bold')).pack(side='left')
        self.refresh_button = ttk.Button(header_frame, text="↻ Refresh", width=12)
        self.refresh_button.pack(side='right')
        
        # Summary stats
        stats_frame = ttk.Frame(self)
        stats_frame.pack(fill='x', padx=5, pady=10)
        
        # Create stat boxes
        self.stat_boxes = {}
        stat_configs = [
            ('source_rows', 'Source Rows', 'lightblue'),
            ('target_rows', 'Target Rows', 'lightgreen'),
            ('tables_done', 'Tables Done', 'plum'),
            ('completion', 'Complete', 'lightyellow')
        ]
        
        for i, (key, label, color) in enumerate(stat_configs):
            stat_frame = ttk.LabelFrame(stats_frame, text=label, padding=10)
            stat_frame.grid(row=0, column=i, padx=5, sticky='ew')
            
            value_label = ttk.Label(stat_frame, text="0", font=('Arial', 16, 'bold'))
            value_label.pack()
            
            self.stat_boxes[key] = value_label
        
        # Configure grid weights
        for i in range(4):
            stats_frame.columnconfigure(i, weight=1)
        
        # Table progress list
        self.table_frame = ttk.Frame(self)
        self.table_frame.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create treeview for table details
        columns = ('Table', 'Source', 'Target', 'Status', 'Progress')
        self.table_tree = ttk.Treeview(self.table_frame, columns=columns, show='headings', height=8)
        
        for col in columns:
            self.table_tree.heading(col, text=col)
            self.table_tree.column(col, width=100)
        
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(self.table_frame, orient='vertical', command=self.table_tree.yview)
        self.table_tree.configure(yscrollcommand=scrollbar.set)
        
        self.table_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
    
    def update_overview(self, data: Dict[str, Any]):
        """Update the overview with new data"""
        # Update summary stats
        self.stat_boxes['source_rows'].configure(text=f"{data.get('source_total_rows', 0):,}")
        self.stat_boxes['target_rows'].configure(text=f"{data.get('target_total_rows', 0):,}")
        self.stat_boxes['tables_done'].configure(text=f"{data.get('tables_migrated', 0)}/{data.get('total_tables', 0)}")
        
        completion = 0
        if data.get('source_total_rows', 0) > 0:
            completion = round((data.get('target_total_rows', 0) / data.get('source_total_rows', 1)) * 100)
        self.stat_boxes['completion'].configure(text=f"{completion}%")
        
        # Update table list
        for item in self.table_tree.get_children():
            self.table_tree.delete(item)
        
        tables_data = data.get('tables', {})
        for table_name, table_info in tables_data.items():
            source_rows = table_info.get('source_rows', 0)
            target_rows = table_info.get('target_rows', 0)
            status = table_info.get('status', 'unknown')
            percentage = table_info.get('migration_percentage', 0)
            
            # Format status for display
            status_display = {
                'fully_migrated': '✓ Complete',
                'partially_migrated': '⚠ Partial',
                'not_migrated': '✗ Not Done',
                'source_error': '❌ Src Error',
                'target_error': '❌ Tgt Error'
            }.get(status, status)
            
            self.table_tree.insert('', 'end', values=(
                table_name,
                f"{source_rows:,}" if source_rows >= 0 else "N/A",
                f"{target_rows:,}" if target_rows >= 0 else "N/A",
                status_display,
                f"{percentage:.1f}%"
            ))


class QuickActions(ttk.Frame):
    """Widget for quick action buttons"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets()
    
    def create_widgets(self):
        # Header
        ttk.Label(self, text="Quick Actions", font=('Arial', 14, 'bold')).pack(pady=(5, 10))
        
        # Action buttons
        button_configs = [
            ('Full Sync', 'green', 'both_required'),
            ('Incremental Sync', 'blue', 'both_required'),
            ('Export to Files', 'purple', 'source_only'),
            ('Export Images', 'orange', 'source_only'),
            ('Test Connections', 'gray', 'none'),
            ('View Logs', 'brown', 'none')
        ]
        
        self.action_buttons = {}
        for i, (text, color, requirement) in enumerate(button_configs):
            row = i // 2
            col = i % 2
            
            button = ttk.Button(self, text=text, width=18)
            button.grid(row=row, column=col, padx=5, pady=3, sticky='ew')
            self.action_buttons[text] = button
        
        # Configure grid
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=1)
        
        # Progress indicator
        self.progress_frame = ttk.Frame(self)
        self.progress_frame.grid(row=3, column=0, columnspan=2, sticky='ew', pady=(10, 0))
        
        self.progress_label = ttk.Label(self.progress_frame, text="")
        self.progress_label.pack()
        
        self.progress_bar = ttk.Progressbar(self.progress_frame, mode='indeterminate')
        self.progress_bar.pack(fill='x', pady=5)
        
        self.progress_frame.grid_remove()  # Hide initially
    
    def show_progress(self, operation: str):
        """Show progress for an operation"""
        self.progress_label.configure(text=f"Running: {operation}")
        self.progress_bar.start()
        self.progress_frame.grid()
    
    def hide_progress(self):
        """Hide progress indicator"""
        self.progress_bar.stop()
        self.progress_frame.grid_remove()
    
    def update_button_states(self, fm_connected: bool, target_connected: bool):
        """Update button states based on connections"""
        states = {
            'Full Sync': 'normal' if fm_connected and target_connected else 'disabled',
            'Incremental Sync': 'normal' if fm_connected and target_connected else 'disabled',
            'Export to Files': 'normal' if fm_connected else 'disabled',
            'Export Images': 'normal' if fm_connected else 'disabled',
            'Test Connections': 'normal',
            'View Logs': 'normal'
        }
        
        for button_text, state in states.items():
            if button_text in self.action_buttons:
                self.action_buttons[button_text].configure(state=state)


class RecentActivity(ttk.Frame):
    """Widget showing recent activity log"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.activities = []
        self.create_widgets()
    
    def create_widgets(self):
        # Header
        ttk.Label(self, text="Recent Activity", font=('Arial', 14, 'bold')).pack(pady=(5, 10))
        
        # Activity list
        self.activity_listbox = tk.Listbox(self, height=8, font=('Arial', 9))
        self.activity_listbox.pack(fill='both', expand=True)
        
        # Add some sample activities
        self.add_activity("Application started")
    
    def add_activity(self, message: str):
        """Add a new activity to the log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        activity = f"[{timestamp}] {message}"
        
        self.activities.append(activity)
        self.activity_listbox.insert(0, activity)  # Insert at top
        
        # Keep only last 20 activities
        if len(self.activities) > 20:
            self.activities = self.activities[:20]
            self.activity_listbox.delete(20, tk.END)


class EnhancedFileMakerGUI:
    """Enhanced dashboard-style GUI for FileMaker sync"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("FileMaker Sync Dashboard")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        
        # Data storage
        self.connection_status = {
            'filemaker': {'connected': False, 'message': 'Not tested'},
            'target': {'connected': False, 'message': 'Not tested'}
        }
        self.migration_data = None
        self.log_queue = queue.Queue()
        self.is_operation_running = False
        
        # Configuration
        self.config_file = Path("sync_config.json")
        self.config = self.load_config()
        
        self.create_widgets()
        self.setup_bindings()
        
        # Start background refresh
        self.auto_refresh()
        
        # Check log queue
        self.check_log_queue()
    
    def create_widgets(self):
        """Create the main dashboard layout"""
        # Configure root
        self.root.configure(bg='#f0f0f0')
        
        # Main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Header
        header_frame = ttk.Frame(main_container)
        header_frame.pack(fill='x', pady=(0, 20))
        
        title_label = ttk.Label(header_frame, text="FileMaker Sync Dashboard", 
                               font=('Arial', 20, 'bold'))
        title_label.pack(side='left')
        
        subtitle_label = ttk.Label(header_frame, 
                                  text="Monitor and manage your FileMaker to Supabase migration",
                                  font=('Arial', 10))
        subtitle_label.pack(side='left', padx=(20, 0))
        
        # Connection status row
        conn_frame = ttk.Frame(main_container)
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
        
        # Main content area
        content_frame = ttk.Frame(main_container)
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
        actions['View Logs'].configure(command=self.view_logs)
    
    def load_config(self) -> dict:
        """Load configuration from file"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading config: {e}")
        return {}
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def run_python_command(self, cmd_args: list, description: str) -> dict:
        """Run a Python command and return JSON result"""
        try:
            result = subprocess.run(
                [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                try:
                    return {'success': True, 'data': json.loads(result.stdout)}
                except json.JSONDecodeError:
                    return {'success': True, 'data': None, 'message': result.stdout}
            else:
                return {'success': False, 'error': result.stderr or result.stdout}
                
        except subprocess.TimeoutExpired:
            return {'success': False, 'error': 'Operation timed out'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def test_filemaker_connection(self):
        """Test FileMaker connection"""
        self.recent_activity.add_activity("Testing FileMaker connection...")
        
        def test_connection():
            result = self.run_python_command(['--src-cnt', '--json', '--max-rows', '1'], 
                                           "FileMaker connection test")
            
            if result['success'] and result.get('data'):
                data = result['data']
                if data.get('summary', {}).get('connection_error'):
                    self.connection_status['filemaker'] = {
                        'connected': False,
                        'message': data.get('error_detail', 'Connection failed')
                    }
                else:
                    self.connection_status['filemaker'] = {
                        'connected': True,
                        'message': f"Connected via DSN: {data.get('dsn', 'unknown')}"
                    }
            else:
                self.connection_status['filemaker'] = {
                    'connected': False,
                    'message': result.get('error', 'Connection test failed')
                }
            
            # Update UI in main thread
            self.root.after(0, self.update_connection_status)
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_target_connection(self):
        """Test target database connection"""
        self.recent_activity.add_activity("Testing target database connection...")
        
        def test_connection():
            result = self.run_python_command(['--tgt-cnt', '--json', '--max-rows', '1'], 
                                           "Target connection test")
            
            if result['success'] and result.get('data'):
                data = result['data']
                if data.get('summary', {}).get('connection_error'):
                    self.connection_status['target'] = {
                        'connected': False,
                        'message': data.get('error_detail', 'Connection failed')
                    }
                else:
                    self.connection_status['target'] = {
                        'connected': True,
                        'message': f"Connected to {data.get('database', 'target')}"
                    }
            else:
                self.connection_status['target'] = {
                    'connected': False,
                    'message': result.get('error', 'Connection test failed')
                }
            
            # Update UI in main thread
            self.root.after(0, self.update_connection_status)
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_all_connections(self):
        """Test both connections"""
        self.recent_activity.add_activity("Testing all connections...")
        self.test_filemaker_connection()
        self.root.after(1000, self.test_target_connection)  # Stagger the tests
    
    def update_connection_status(self):
        """Update connection status display"""
        # Update FileMaker status
        fm_status = self.connection_status['filemaker']
        self.fm_status_card.update_status(fm_status['connected'], fm_status['message'])
        
        # Update target status
        target_status = self.connection_status['target']
        self.target_status_card.update_status(target_status['connected'], target_status['message'])
        
        # Update button states
        self.quick_actions.update_button_states(
            fm_status['connected'], 
            target_status['connected']
        )
    
    def refresh_migration_status(self):
        """Refresh migration status data"""
        self.recent_activity.add_activity("Refreshing migration status...")
        
        def get_status():
            result = self.run_python_command(['--migration-status', '--json'], 
                                           "Migration status refresh")
            
            if result['success'] and result.get('data'):
                self.migration_data = result['data']
                
                # Update connection status from the data
                conn_status = self.migration_data.get('connection_status', {})
                if 'filemaker' in conn_status:
                    self.connection_status['filemaker'] = conn_status['filemaker']
                if 'target' in conn_status:
                    self.connection_status['target'] = conn_status['target']
                
                # Update UI in main thread
                self.root.after(0, self.update_migration_display)
            else:
                self.root.after(0, lambda: self.recent_activity.add_activity(
                    f"Failed to refresh status: {result.get('error', 'Unknown error')}"
                ))
        
        threading.Thread(target=get_status, daemon=True).start()
    
    def update_migration_display(self):
        """Update the migration overview display"""
        if self.migration_data:
            self.migration_overview.update_overview(self.migration_data)
            self.update_connection_status()
            self.recent_activity.add_activity("Migration status updated")
    
    def run_operation(self, operation: str):
        """Run a migration operation"""
        if self.is_operation_running:
            messagebox.showwarning("Operation Running", 
                                 "Another operation is already running. Please wait.")
            return
        
        operation_commands = {
            'full_sync': ['--db-exp', '--ddl', '--dml'],
            'incremental_sync': ['--db-exp', '--dml'],
            'export_files': ['--fn-exp', '--ddl', '--dml'],
            'export_images': ['--get-images']
        }
        
        if operation not in operation_commands:
            messagebox.showerror("Error", f"Unknown operation: {operation}")
            return
        
        # Confirm operation
        if not messagebox.askyesno("Confirm Operation", 
                                  f"Are you sure you want to run {operation.replace('_', ' ')}?"):
            return
        
        self.is_operation_running = True
        self.quick_actions.show_progress(operation.replace('_', ' ').title())
        self.recent_activity.add_activity(f"Started {operation.replace('_', ' ')}")
        
        def run_op():
            try:
                cmd = operation_commands[operation]
                
                # Run with real-time output capture
                process = subprocess.Popen(
                    [sys.executable, 'filemaker_extract_refactored.py'] + cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )
                
                # Read output line by line
                for line in iter(process.stdout.readline, ''):
                    if line:
                        self.log_queue.put(f"[{operation}] {line.strip()}")
                
                process.wait()
                
                if process.returncode == 0:
                    self.log_queue.put(f"✓ {operation.replace('_', ' ').title()} completed successfully")
                else:
                    self.log_queue.put(f"✗ {operation.replace('_', ' ').title()} failed")
                
            except Exception as e:
                self.log_queue.put(f"✗ {operation.replace('_', ' ').title()} error: {e}")
            finally:
                self.is_operation_running = False
                self.root.after(0, self.quick_actions.hide_progress)
                # Refresh status after operation
                self.root.after(1000, self.refresh_migration_status)
        
        threading.Thread(target=run_op, daemon=True).start()
    
    def view_logs(self):
        """Open log file directory"""
        log_dir = Path("./logs")
        if log_dir.exists():
            os.startfile(log_dir)  # Windows
        else:
            messagebox.showinfo("Logs", "No log directory found")
    
    def check_log_queue(self):
        """Check for new log messages"""
        try:
            while True:
                message = self.log_queue.get_nowait()
                self.recent_activity.add_activity(message)
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self.check_log_queue)
    
    def auto_refresh(self):
        """Automatically refresh status every 30 seconds"""
        self.refresh_migration_status()
        self.root.after(30000, self.auto_refresh)  # 30 seconds


def main():
    """Main entry point"""
    root = tk.Tk()
    
    # Set window icon if available
    try:
        root.iconbitmap('icon.ico')
    except:
        pass
    
    # Create the application
    app = EnhancedFileMakerGUI(root)
    
    # Handle window close
    def on_closing():
        if app.is_operation_running:
            if messagebox.askokcancel("Quit", 
                                    "An operation is running. Do you want to stop it and quit?"):
                root.destroy()
        else:
            root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    # Start the GUI
    root.mainloop()


if __name__ == "__main__":
    main()