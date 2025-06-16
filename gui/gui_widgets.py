#!/usr/bin/env python3
# FILE: gui/gui_widgets.py
"""
GUI Widgets Module - Updated with Stop Action and Dashboard Update
Custom widgets for the FileMaker Sync Dashboard
"""

import tkinter as tk
from tkinter import ttk
from datetime import datetime
from typing import Dict, Any, Callable

class StatusCard(ttk.Frame):
    """Custom widget for displaying connection status with test button and details"""
    
    def __init__(self, parent, title: str, **kwargs):
        super().__init__(parent, **kwargs)
        self.title = title
        self.full_message = "Not tested"  # Store full message for details
        self.create_widgets()
    
    def create_widgets(self):
        # Header frame
        header_frame = ttk.Frame(self)
        header_frame.pack(fill='x', padx=5, pady=5)
        
        # Status indicator (colored circle)
        self.status_label = ttk.Label(header_frame, text="●", font=('Arial', 16))
        self.status_label.pack(side='left', padx=(0, 10))
        
        # Message frame (NO TITLE - section label is sufficient)
        message_frame = ttk.Frame(header_frame)
        message_frame.pack(side='left', fill='x', expand=True)
        
        self.message_label = ttk.Label(message_frame, text="Not tested", font=('Arial', 10))
        self.message_label.pack(side='left')
        
        # Details button (initially hidden)
        self.details_button = ttk.Button(message_frame, text="Details", width=8, 
                                        command=self.show_details)
        
        # Button frame for test button
        button_frame = ttk.Frame(header_frame)
        button_frame.pack(side='right', padx=(10, 0))
        
        self.test_button = ttk.Button(button_frame, text="Test", width=8)
        self.test_button.pack()
    
    def update_status(self, connected: bool, message: str):
        """Update the status display with compact message and details button"""
        self.full_message = message  # Store full message
        
        # Create compact message
        if connected:
            if "DSN:" in message:
                dsn_part = message.split("DSN:")[-1].strip()
                compact_message = f"Connected via {dsn_part.split()[0]}"
            elif "Connected to" in message:
                compact_message = "Connected successfully"
            else:
                compact_message = "Connected"
            
            self.status_label.configure(foreground='green')
            self.message_label.configure(foreground='dark green')
            
            # Hide details button for successful connections
            self.details_button.pack_forget()
            
        else:
            # For errors, show compact message with details button
            if "Connection failed" in message:
                compact_message = "Connection failed"
            elif "not found" in message.lower():
                compact_message = "Service not found"
            elif "timeout" in message.lower():
                compact_message = "Connection timeout"
            elif "authentication" in message.lower():
                compact_message = "Authentication failed"
            else:
                # Truncate long error messages
                compact_message = message[:30] + "..." if len(message) > 30 else message
            
            self.status_label.configure(foreground='red')
            self.message_label.configure(foreground='dark red')
            
            # Show details button for errors
            self.details_button.pack(side='left', padx=(5, 0))
        
        self.message_label.configure(text=compact_message)
    
    def show_details(self):
        """Show detailed message in a modal dialog"""
        detail_window = tk.Toplevel(self)
        detail_window.title(f"{self.title} - Connection Details")
        detail_window.geometry("500x300")
        detail_window.transient(self.winfo_toplevel())
        detail_window.grab_set()
        
        # Center the window
        detail_window.update_idletasks()
        x = (detail_window.winfo_screenwidth() // 2) - (detail_window.winfo_width() // 2)
        y = (detail_window.winfo_screenheight() // 2) - (detail_window.winfo_height() // 2)
        detail_window.geometry(f"+{x}+{y}")
        
        # Main frame
        main_frame = ttk.Frame(detail_window)
        main_frame.pack(fill='both', expand=True, padx=15, pady=15)
        
        # Title
        title_label = ttk.Label(main_frame, text=f"{self.title} Connection Details", 
                               font=('Arial', 12, 'bold'))
        title_label.pack(pady=(0, 10))
        
        # Message text with scrollbar
        from tkinter import scrolledtext
        text_widget = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD, height=8)
        text_widget.pack(fill='both', expand=True, pady=(0, 10))
        
        # Insert full message
        text_widget.insert('1.0', self.full_message)
        text_widget.configure(state='disabled')
        
        # Close button
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x')
        
        ttk.Button(button_frame, text="Close", command=detail_window.destroy).pack(side='right')

class MigrationOverview(ttk.Frame):
    """Widget showing migration progress overview - COMPACT VERSION WITHOUT REFRESH BUTTON"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets()
    
    def create_widgets(self):
        # Summary stats - COMPACT spacing (no header with refresh button)
        stats_frame = ttk.Frame(self)
        stats_frame.pack(fill='x', padx=2, pady=2)
        
        # Create stat boxes - only Tables Done and Completion percentage
        self.stat_boxes = {}
        stat_configs = [
            ('tables_done', 'Tables Migrated'),
            ('completion', 'Completion %')
        ]
        
        for i, (key, label) in enumerate(stat_configs):
            stat_frame = ttk.LabelFrame(stats_frame, text=label, padding=3)
            stat_frame.grid(row=0, column=i, padx=3, sticky='ew')
            
            value_label = ttk.Label(stat_frame, text="0", font=('Arial', 16, 'bold'))
            value_label.pack()
            
            self.stat_boxes[key] = value_label
        
        # Configure grid weights for equal distribution
        for i in range(2):
            stats_frame.columnconfigure(i, weight=1)
        
        # Table progress list - COMPACT with auto-sizing
        self.table_frame = ttk.Frame(self)
        self.table_frame.pack(fill='both', expand=True, padx=2, pady=2)
        
        # Create treeview for table details - START WITH MINIMAL HEIGHT
        columns = ('Table', 'Source', 'Target', 'Status', 'Progress')
        self.table_tree = ttk.Treeview(self.table_frame, columns=columns, show='headings', height=3)
        
        # Set column widths more efficiently
        self.table_tree.column('#0', width=0, stretch=False)  # Hide tree column
        self.table_tree.column('Table', width=120, minwidth=100)
        self.table_tree.column('Source', width=80, minwidth=60)
        self.table_tree.column('Target', width=80, minwidth=60)
        self.table_tree.column('Status', width=100, minwidth=80)
        self.table_tree.column('Progress', width=80, minwidth=60)
        
        for col in columns:
            self.table_tree.heading(col, text=col)
        
        # Scrollbar for treeview (only show when needed)
        scrollbar = ttk.Scrollbar(self.table_frame, orient='vertical', command=self.table_tree.yview)
        self.table_tree.configure(yscrollcommand=scrollbar.set)
        
        self.table_tree.pack(side='left', fill='both', expand=True)
        # Don't pack scrollbar initially - will show only when needed
    
    def update_overview(self, data: Dict[str, Any]):
        """Update the overview with new data - AUTO-SIZING VERSION"""
        try:
            # Extract summary data safely
            summary = data.get('summary', {})
            
            # Update summary stats - ONLY essential ones
            tables_migrated = summary.get('tables_migrated', 0)
            total_tables = summary.get('total_tables', 0)
            source_total = summary.get('source_total_rows', 0)
            target_total = summary.get('target_total_rows', 0)
            
            self.stat_boxes['tables_done'].configure(text=f"{tables_migrated}/{total_tables}")
            
            completion = 0
            if source_total > 0:
                completion = round((target_total / source_total) * 100)
            self.stat_boxes['completion'].configure(text=f"{completion}%")
            
            # Update table list
            for item in self.table_tree.get_children():
                self.table_tree.delete(item)
            
            tables_data = data.get('tables', {})
            table_count = len(tables_data)
            
            # AUTO-SIZE the table height based on data
            if table_count > 0:
                # Set height to fit data (minimum 3, maximum 12 to prevent huge tables)
                optimal_height = min(max(table_count, 3), 12)
                self.table_tree.configure(height=optimal_height)
                
                # Show scrollbar only if needed
                if table_count > 12:
                    scrollbar = self.table_frame.winfo_children()[-1]  # Get scrollbar
                    if hasattr(scrollbar, 'pack'):
                        scrollbar.pack(side='right', fill='y')
            
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
                
        except Exception as e:
            print(f"Error updating overview: {e}")

class QuickActions(ttk.Frame):
    """Widget for quick action buttons - UPDATED WITH STOP ACTION AND UPDATE DASHBOARD"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets()
    
    def create_widgets(self):
        # NO header/title - just buttons directly
        
        # Button container
        button_container = ttk.Frame(self)
        button_container.pack(fill='x', pady=5)
        
        # Action buttons - UPDATED LIST
        self.action_buttons = {}
        button_configs = [
            ('Full Sync', 'both_required'),
            ('Incremental Sync', 'both_required'),
            ('Export to Files', 'source_only'),
            ('Export Images', 'source_only'),
            ('Test Connections', 'none'),
            ('View Logs', 'none'),
            ('Update Dashboard', 'none'),  # NEW: Moved from refresh button
            ('Stop Action', 'none')        # NEW: Stop current action
        ]
        
        # Create buttons in rows
        for i in range(0, len(button_configs), 2):
            row_frame = ttk.Frame(button_container)
            row_frame.pack(fill='x', pady=2)
            
            # First button in row
            if i < len(button_configs):
                text, requirement = button_configs[i]
                button1 = ttk.Button(row_frame, text=text, width=18)
                button1.pack(side='left', padx=(0, 5), fill='x', expand=True)
                self.action_buttons[text] = button1
                
                # Special styling for Stop Action button
                if text == 'Stop Action':
                    button1.configure(state='disabled')  # Initially disabled
            
            # Second button in row (if exists)
            if i + 1 < len(button_configs):
                text, requirement = button_configs[i + 1]
                button2 = ttk.Button(row_frame, text=text, width=18)
                button2.pack(side='right', padx=(5, 0), fill='x', expand=True)
                self.action_buttons[text] = button2
                
                # Special styling for Stop Action button
                if text == 'Stop Action':
                    button2.configure(state='disabled')  # Initially disabled
        
        # Progress indicator
        self.progress_frame = ttk.Frame(self)
        self.progress_frame.pack(fill='x', pady=(10, 0))
        
        self.progress_label = ttk.Label(self.progress_frame, text="")
        self.progress_label.pack()
        
        self.progress_bar = ttk.Progressbar(self.progress_frame, mode='indeterminate')
        self.progress_bar.pack(fill='x', pady=5)
        
        # Hide progress initially
        self.progress_frame.pack_forget()
    
    def show_progress(self, operation: str):
        """Show progress for an operation and enable Stop Action button"""
        self.progress_label.configure(text=f"Running: {operation}")
        self.progress_bar.start()
        self.progress_frame.pack(fill='x', pady=(10, 0))
        
        # Enable Stop Action button when operation is running
        if 'Stop Action' in self.action_buttons:
            self.action_buttons['Stop Action'].configure(state='normal')
    
    def hide_progress(self):
        """Hide progress indicator and disable Stop Action button"""
        self.progress_bar.stop()
        self.progress_frame.pack_forget()
        
        # Disable Stop Action button when no operation is running
        if 'Stop Action' in self.action_buttons:
            self.action_buttons['Stop Action'].configure(state='disabled')
    
    def update_button_states(self, fm_connected: bool, target_connected: bool):
        """Update button states based on connections"""
        states = {
            'Full Sync': 'normal' if fm_connected and target_connected else 'disabled',
            'Incremental Sync': 'normal' if fm_connected and target_connected else 'disabled',
            'Export to Files': 'normal' if fm_connected else 'disabled',
            'Export Images': 'normal' if fm_connected else 'disabled',
            'Test Connections': 'normal',
            'View Logs': 'normal',
            'Update Dashboard': 'normal',
            # Stop Action state is managed by show_progress/hide_progress
        }
        
        for button_text, state in states.items():
            if button_text in self.action_buttons:
                # Don't override Stop Action state management
                if button_text != 'Stop Action':
                    self.action_buttons[button_text].configure(state=state)

class StatusBar(ttk.Frame):
    """Status bar widget for the bottom of the application - ALWAYS VISIBLE"""
    
    def __init__(self, parent, session_id: str, **kwargs):
        super().__init__(parent, **kwargs)
        self.create_widgets(session_id)
    
    def create_widgets(self, session_id: str):
        # Make status bar have a distinct background to ensure visibility
        self.configure(relief='sunken', borderwidth=1)
        
        # System health indicator
        self.health_label = ttk.Label(self, text="● System Healthy", foreground='green')
        self.health_label.pack(side='left', padx=5, pady=2)
        
        # Error count indicator
        self.error_count_label = ttk.Label(self, text="0 errors")
        self.error_count_label.pack(side='left', padx=5, pady=2)
        
        # Session info
        self.session_label = ttk.Label(self, text=f"Session: {session_id}")
        self.session_label.pack(side='left', padx=10, pady=2)
        
        # Last update time
        self.last_update_label = ttk.Label(self, text="")
        self.last_update_label.pack(side='right', padx=5, pady=2)
    
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