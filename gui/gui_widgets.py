#!/usr/bin/env python3
"""
GUI Widgets Module
Custom widgets for the FileMaker Sync Dashboard
Fixed layout manager conflicts
"""

import tkinter as tk
from tkinter import ttk
from datetime import datetime
from typing import Dict, Any, Callable

class StatusCard(ttk.Frame):
    """SIMPLIFIED status card for connection display"""
    
    def __init__(self, parent, title: str, **kwargs):
        super().__init__(parent, **kwargs)
        self.title = title
        self.create_widgets()
    
    def create_widgets(self):
        # Simple status display
        self.status_var = tk.StringVar(value="Not tested")
        self.status_label = ttk.Label(self, textvariable=self.status_var)
        self.status_label.pack(fill='x')
    
    def update_status(self, connected: bool, message: str):
        """Update the status display"""
        status_text = "✓ Connected" if connected else "✗ Failed"
        self.status_var.set(status_text)

class MigrationOverview(ttk.Frame):
    """IMPROVED migration overview with proper table display"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets()
    
    def create_widgets(self):
        # Header
        header_frame = ttk.Frame(self)
        header_frame.pack(fill='x', padx=5, pady=5)
        
        ttk.Label(header_frame, text="Migration Status", font=('Arial', 14, 'bold')).pack(side='left')
        self.refresh_button = ttk.Button(header_frame, text="↻ Refresh", width=12)
        self.refresh_button.pack(side='right')
        
        # REMOVED: Source/Target row counts (as requested)
        # Summary stats - only show relevant info
        stats_frame = ttk.Frame(self)
        stats_frame.pack(fill='x', padx=5, pady=10)
        
        # Create stat boxes
        self.stat_boxes = {}
        stat_configs = [
            ('tables_done', 'Tables Migrated'),
            ('completion', 'Completion %')
        ]
        
        for i, (key, label) in enumerate(stat_configs):
            stat_frame = ttk.LabelFrame(stats_frame, text=label, padding=5)
            stat_frame.grid(row=0, column=i, padx=5, sticky='ew')
            
            value_label = ttk.Label(stat_frame, text="0", font=('Arial', 14, 'bold'))
            value_label.pack()
            
            self.stat_boxes[key] = value_label
        
        # Configure grid weights
        for i in range(len(stat_configs)):
            stats_frame.columnconfigure(i, weight=1)
        
        # FIXED: Improved table with proper grid and dynamic height
        self.table_frame = ttk.Frame(self)
        self.table_frame.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create treeview for table details with improved columns
        columns = ('Table', 'Status', 'Progress')
        self.table_tree = ttk.Treeview(self.table_frame, columns=columns, show='headings')
        
        # Configure column headings and widths
        self.table_tree.heading('Table', text='Table Name')
        self.table_tree.heading('Status', text='Migration Status')
        self.table_tree.heading('Progress', text='Progress %')
        
        self.table_tree.column('Table', width=150, anchor='w')
        self.table_tree.column('Status', width=120, anchor='center')
        self.table_tree.column('Progress', width=100, anchor='center')
        
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(self.table_frame, orient='vertical', command=self.table_tree.yview)
        self.table_tree.configure(yscrollcommand=scrollbar.set)
        
        self.table_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        # Configure tag colors for different statuses
        self.table_tree.tag_configure('complete', foreground='green')
        self.table_tree.tag_configure('partial', foreground='orange')
        self.table_tree.tag_configure('not_done', foreground='red')
        self.table_tree.tag_configure('error', foreground='dark red', background='light pink')
    
    def update_overview(self, data: Dict[str, Any]):
        """Update the overview with new data"""
        # Get summary info from data
        summary = data.get('summary', {})
        
        # Update summary stats (simplified)
        total_tables = summary.get('total_tables', 0)
        tables_migrated = summary.get('tables_migrated', 0)
        
        self.stat_boxes['tables_done'].configure(text=f"{tables_migrated}/{total_tables}")
        
        # Calculate overall completion percentage
        completion = 0
        if total_tables > 0:
            completion = round((tables_migrated / total_tables) * 100)
        self.stat_boxes['completion'].configure(text=f"{completion}%")
        
        # Clear and update table list
        for item in self.table_tree.get_children():
            self.table_tree.delete(item)
        
        tables_data = data.get('tables', {})
        
        # FIXED: Dynamic height based on number of tables
        num_tables = len(tables_data)
        table_height = min(max(num_tables, 3), 15)  # Between 3-15 rows
        self.table_tree.configure(height=table_height)
        
        for table_name, table_info in tables_data.items():
            status = table_info.get('status', 'unknown')
            percentage = table_info.get('migration_percentage', 0)
            
            # Format status for display
            status_display = {
                'fully_migrated': 'Complete',
                'partially_migrated': 'Partial',
                'not_migrated': 'Not Started',
                'source_error': 'Source Error',
                'target_error': 'Target Error',
                'both_error': 'Connection Error'
            }.get(status, status.title())
            
            # Determine tag for color coding
            tag = {
                'fully_migrated': 'complete',
                'partially_migrated': 'partial',
                'not_migrated': 'not_done',
                'source_error': 'error',
                'target_error': 'error',
                'both_error': 'error'
            }.get(status, '')
            
            # Insert row with proper formatting
            self.table_tree.insert('', 'end', values=(
                table_name.title(),
                status_display,
                f"{percentage:.1f}%"
            ), tags=(tag,) if tag else ())

class QuickActions(ttk.Frame):
    """FIXED Quick Actions widget with improved layout"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets()
    
    def create_widgets(self):
        # Main button container using grid for better control
        button_container = ttk.Frame(self)
        button_container.pack(fill='x', pady=5)
        
        # Action buttons in a proper grid
        self.action_buttons = {}
        button_configs = [
            ('Full Sync', 'DDL + DML migration (destructive)', 0, 0),
            ('Incremental Sync', 'DML only migration', 0, 1),
            ('Export to Files', 'Export SQL files', 1, 0),
            ('Export Images', 'Extract container images', 1, 1),
            ('Test Connections', 'Test database connections', 2, 0),
            ('View Logs', 'Open log viewer', 2, 1)
        ]
        
        # Create buttons in grid layout
        for text, tooltip, row, col in button_configs:
            button = ttk.Button(button_container, text=text, width=18)
            button.grid(row=row, column=col, padx=5, pady=3, sticky='ew')
            self.action_buttons[text] = button
            
            # Add tooltip (simple implementation)
            self._add_tooltip(button, tooltip)
        
        # Configure grid weights for responsive layout
        button_container.columnconfigure(0, weight=1)
        button_container.columnconfigure(1, weight=1)
        
        # Progress indicator
        self.progress_frame = ttk.Frame(self)
        self.progress_frame.pack(fill='x', pady=(10, 0))
        
        self.progress_label = ttk.Label(self.progress_frame, text="")
        self.progress_label.pack()
        
        self.progress_bar = ttk.Progressbar(self.progress_frame, mode='indeterminate')
        self.progress_bar.pack(fill='x', pady=5)
        
        # Hide progress initially
        self.progress_frame.pack_forget()
    
    def _add_tooltip(self, widget, text):
        """Simple tooltip implementation"""
        def on_enter(event):
            # Could implement proper tooltip here
            pass
        
        def on_leave(event):
            pass
        
        widget.bind('<Enter>', on_enter)
        widget.bind('<Leave>', on_leave)
    
    def show_progress(self, operation: str):
        """Show progress for an operation"""
        self.progress_label.configure(text=f"Running: {operation}")
        self.progress_bar.start()
        self.progress_frame.pack(fill='x', pady=(10, 0))
        
        # Disable all action buttons during operation
        for button in self.action_buttons.values():
            button.configure(state='disabled')
    
    def hide_progress(self):
        """Hide progress indicator"""
        self.progress_bar.stop()
        self.progress_frame.pack_forget()
        
        # Re-enable buttons (will be further controlled by connection status)
        for button in self.action_buttons.values():
            button.configure(state='normal')
    
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
    """IMPROVED Recent Activity widget with better formatting"""
    
    def __init__(self, parent, height=10, **kwargs):
        super().__init__(parent, **kwargs)
        self.max_activities = 50
        self.create_widgets(height)
    
    def create_widgets(self, height):
        # Activity list with better formatting
        # Use Text widget instead of Listbox for better control
        self.activity_text = tk.Text(self, height=height, font=('Arial', 9), 
                                   wrap=tk.WORD, state='disabled')
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(self, orient='vertical', command=self.activity_text.yview)
        self.activity_text.configure(yscrollcommand=scrollbar.set)
        
        self.activity_text.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        # Configure text tags for different message types
        self.activity_text.tag_configure('info', foreground='black')
        self.activity_text.tag_configure('success', foreground='green')
        self.activity_text.tag_configure('warning', foreground='orange')
        self.activity_text.tag_configure('error', foreground='red')
        self.activity_text.tag_configure('timestamp', foreground='gray', font=('Arial', 8))
        
        # Track number of lines
        self.line_count = 0
        
        # Add initial message
        self.add_activity("Application started", "info")
    
    def add_activity(self, message: str, level: str = "info"):
        """Add a new activity to the log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Enable editing temporarily
        self.activity_text.configure(state='normal')
        
        # Add new line (except for first entry)
        if self.line_count > 0:
            self.activity_text.insert('end', '\n')
        
        # Insert timestamp
        self.activity_text.insert('end', f"[{timestamp}] ", 'timestamp')
        
        # Insert message with appropriate tag
        self.activity_text.insert('end', message, level)
        
        # Disable editing
        self.activity_text.configure(state='disabled')
        
        # Auto-scroll to bottom
        self.activity_text.see('end')
        
        self.line_count += 1
        
        # Keep only recent activities
        if self.line_count > self.max_activities:
            self.activity_text.configure(state='normal')
            self.activity_text.delete('1.0', '2.0')  # Delete first line
            self.activity_text.configure(state='disabled')
            self.line_count -= 1

class StatusBar(ttk.Frame):
    """Enhanced status bar widget"""
    
    def __init__(self, parent, session_id: str, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets(session_id)
    
    def create_widgets(self, session_id: str):
        # Create a frame for the status bar with relief
        status_frame = ttk.Frame(self, relief='sunken')
        status_frame.pack(fill='x', expand=True)
        
        # System health indicator
        self.health_label = ttk.Label(status_frame, text="● System Healthy", foreground='green')
        self.health_label.pack(side='left', padx=5)
        
        # Error count indicator
        self.error_count_label = ttk.Label(status_frame, text="0 errors")
        self.error_count_label.pack(side='left', padx=5)
        
        # Session info
        self.session_label = ttk.Label(status_frame, text=f"Session: {session_id}")
        self.session_label.pack(side='left', padx=10)
        
        # Last update time
        self.last_update_label = ttk.Label(status_frame, text="")
        self.last_update_label.pack(side='right', padx=5)
        
        # Memory usage (optional)
        self.memory_label = ttk.Label(status_frame, text="")
        self.memory_label.pack(side='right', padx=5)
        
        # Update memory usage
        self.update_memory_usage()
    
    def update_health(self, error_count: int):
        """Update health indicator based on error count"""
        self.error_count_label.configure(text=f"{error_count} errors")
        
        if error_count == 0:
            self.health_label.configure(text="● System Healthy", foreground='green')
        elif error_count < 5:
            self.health_label.configure(text="⚠ Minor Issues", foreground='orange')
        else:
            self.health_label.configure(text="✗ System Issues", foreground='red')
        
        # Update timestamp
        self.last_update_label.configure(text=f"Updated: {datetime.now().strftime('%H:%M:%S')}")
    
    def update_memory_usage(self):
        """Update memory usage display"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self.memory_label.configure(text=f"Memory: {memory_mb:.1f}MB")
        except ImportError:
            # psutil not available, skip memory display
            self.memory_label.configure(text="")
        except Exception:
            self.memory_label.configure(text="Memory: N/A")
        
        # Schedule next update in 10 seconds
        self.after(10000, self.update_memory_usage)

class ConnectionDetailWindow:
    """NEW: Dedicated window for connection details"""
    
    def __init__(self, parent, connection_type: str, status_data: dict):
        self.parent = parent
        self.connection_type = connection_type
        self.status_data = status_data
        
        self.window = tk.Toplevel(parent)
        self.window.title(f"{connection_type.title()} Connection Details")
        self.window.geometry("500x400")
        self.window.transient(parent)
        self.window.protocol("WM_DELETE_WINDOW", self.window.destroy)
        
        self.create_widgets()
    
    def create_widgets(self):
        # Main container
        main_frame = ttk.Frame(self.window)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Title
        title_label = ttk.Label(main_frame, 
                               text=f"{self.connection_type.title()} Connection Details",
                               font=('Arial', 14, 'bold'))
        title_label.pack(pady=(0, 10))
        
        # Connection status
        status_frame = ttk.LabelFrame(main_frame, text="Status", padding=10)
        status_frame.pack(fill='x', pady=(0, 10))
        
        connected = self.status_data.get('connected', False)
        status_text = "✓ Connected" if connected else "✗ Disconnected"
        status_color = 'green' if connected else 'red'
        
        status_label = ttk.Label(status_frame, text=status_text, foreground=status_color,
                                font=('Arial', 12, 'bold'))
        status_label.pack()
        
        message_label = ttk.Label(status_frame, text=self.status_data.get('message', 'No details'),
                                 wraplength=400)
        message_label.pack(pady=(5, 0))
        
        # Configuration details (if available)
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding=10)
        config_frame.pack(fill='both', expand=True, pady=(0, 10))
        
        # Create text widget for config details
        from tkinter import scrolledtext
        config_text = scrolledtext.ScrolledText(config_frame, height=10, wrap=tk.WORD)
        config_text.pack(fill='both', expand=True)
        
        # Add configuration information
        config_info = self._get_config_info()
        config_text.insert('1.0', config_info)
        config_text.configure(state='disabled')
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x')
        
        ttk.Button(button_frame, text="Test Connection", 
                  command=self._test_connection).pack(side='left')
        ttk.Button(button_frame, text="Close", 
                  command=self.window.destroy).pack(side='right')
    
    def _get_config_info(self) -> str:
        """Get configuration information to display"""
        info = f"{self.connection_type.title()} Configuration Details\n"
        info += "=" * 40 + "\n\n"
        
        if self.connection_type.lower() == 'filemaker':
            info += "Connection Type: ODBC\n"
            info += "Driver: FileMaker Pro ODBC\n"
            info += "DSN: [From configuration]\n"
            info += "Authentication: Username/Password\n\n"
            info += "Requirements:\n"
            info += "• FileMaker Pro must be running\n"
            info += "• ODBC/JDBC sharing enabled\n"
            info += "• Valid DSN configured\n"
            info += "• Network connectivity to FileMaker server\n"
        
        elif self.connection_type.lower() == 'target':
            info += "Connection Type: PostgreSQL\n"
            info += "Platform: Supabase\n"
            info += "Protocol: TCP/IP\n"
            info += "Port: 5432 (default)\n\n"
            info += "Requirements:\n"
            info += "• Valid Supabase credentials\n"
            info += "• Network connectivity to Supabase\n"
            info += "• Proper database permissions\n"
            info += "• SSL/TLS support\n"
        
        # Add last test results
        info += f"\nLast Test Result:\n"
        info += f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        info += f"Status: {'Success' if self.status_data.get('connected') else 'Failed'}\n"
        info += f"Message: {self.status_data.get('message', 'No details')}\n"
        
        return info
    
    def _test_connection(self):
        """Test the connection (placeholder)"""
        # This would integrate with the main application's connection testing
        from tkinter import messagebox
        messagebox.showinfo("Test Connection", 
                          f"Connection test for {self.connection_type} would be performed here.\n"
                          f"This should integrate with the main application's connection testing.")

# Factory function for creating connection detail windows
def show_connection_details(parent, connection_type: str, status_data: dict):
    """Show connection details window"""
    return ConnectionDetailWindow(parent, connection_type, status_data)
