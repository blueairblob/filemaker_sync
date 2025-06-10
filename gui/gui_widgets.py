#!/usr/bin/env python3
"""
GUI Widgets Module
Custom widgets for the FileMaker Sync Dashboard
"""

import tkinter as tk
from tkinter import ttk
from datetime import datetime
from typing import Dict, Any, Callable

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
            ('source_rows', 'Source Rows'),
            ('target_rows', 'Target Rows'),
            ('tables_done', 'Tables Done'),
            ('completion', 'Complete')
        ]
        
        for i, (key, label) in enumerate(stat_configs):
            stat_frame = ttk.LabelFrame(stats_frame, text=label, padding=5)
            stat_frame.grid(row=0, column=i, padx=5, sticky='ew')
            
            value_label = ttk.Label(stat_frame, text="0", font=('Arial', 14, 'bold'))
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
        self.action_buttons = {}
        button_configs = [
            ('Full Sync', 'both_required'),
            ('Incremental Sync', 'both_required'),
            ('Export to Files', 'source_only'),
            ('Export Images', 'source_only'),
            ('Test Connections', 'none'),
            ('View Logs', 'none')
        ]
        
        for i, (text, requirement) in enumerate(button_configs):
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
        self.create_widgets()
    
    def create_widgets(self):
        # Header
        ttk.Label(self, text="Recent Activity", font=('Arial', 14, 'bold')).pack(pady=(5, 10))
        
        # Activity list
        self.activity_listbox = tk.Listbox(self, height=8, font=('Arial', 9))
        self.activity_listbox.pack(fill='both', expand=True)
        
        # Add sample activity
        self.add_activity("Application started")
    
    def add_activity(self, message: str):
        """Add a new activity to the log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        activity = f"[{timestamp}] {message}"
        
        self.activity_listbox.insert(0, activity)  # Insert at top
        
        # Keep only last 50 activities
        if self.activity_listbox.size() > 50:
            self.activity_listbox.delete(50, tk.END)

class StatusBar(ttk.Frame):
    """Status bar widget for the bottom of the application"""
    
    def __init__(self, parent, session_id: str, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets(session_id)
    
    def create_widgets(self, session_id: str):
        # System health indicator
        self.health_label = ttk.Label(self, text="● System Healthy", foreground='green')
        self.health_label.pack(side='left', padx=5)
        
        # Error count indicator
        self.error_count_label = ttk.Label(self, text="0 errors")
        self.error_count_label.pack(side='left', padx=5)
        
        # Session info
        self.session_label = ttk.Label(self, text=f"Session: {session_id}")
        self.session_label.pack(side='left', padx=10)
        
        # Last update time
        self.last_update_label = ttk.Label(self, text="")
        self.last_update_label.pack(side='right', padx=5)
    
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