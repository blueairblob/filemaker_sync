#!/usr/bin/env python3
"""
GUI Log Viewer Module
Dedicated window for viewing and analyzing logs
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
from datetime import datetime
from typing import List
from dataclasses import asdict

from gui_logging import LogManager, LogEntry

class LogViewerWindow:
    """Dedicated window for viewing and analyzing logs"""
    
    def __init__(self, parent, log_manager: LogManager):
        self.parent = parent
        self.log_manager = log_manager
        
        self.window = tk.Toplevel(parent)
        self.window.title("Log Viewer")
        self.window.geometry("900x600")
        self.window.transient(parent)
        
        self.create_widgets()
        self.refresh_logs()
        
        # Auto-refresh every 5 seconds
        self.auto_refresh()
    
    def create_widgets(self):
        """Create the log viewer interface"""
        # Main container
        main_container = ttk.Frame(self.window)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Control panel
        control_frame = ttk.Frame(main_container)
        control_frame.pack(fill='x', pady=(0, 10))
        
        # Filter controls
        filter_frame = ttk.LabelFrame(control_frame, text="Filters", padding=5)
        filter_frame.pack(side='left', fill='x', expand=True, padx=(0, 10))
        
        # Log level filter
        ttk.Label(filter_frame, text="Level:").grid(row=0, column=0, padx=(0, 5))
        self.level_var = tk.StringVar(value="ALL")
        level_combo = ttk.Combobox(filter_frame, textvariable=self.level_var, 
                                  values=["ALL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                                  width=10, state="readonly")
        level_combo.grid(row=0, column=1, padx=(0, 10))
        level_combo.bind('<<ComboboxSelected>>', self.on_filter_change)
        
        # Component filter
        ttk.Label(filter_frame, text="Component:").grid(row=0, column=2, padx=(0, 5))
        self.component_var = tk.StringVar(value="ALL")
        self.component_combo = ttk.Combobox(filter_frame, textvariable=self.component_var, 
                                           width=15, state="readonly")
        self.component_combo.grid(row=0, column=3, padx=(0, 10))
        self.component_combo.bind('<<ComboboxSelected>>', self.on_filter_change)
        
        # Search
        ttk.Label(filter_frame, text="Search:").grid(row=0, column=4, padx=(0, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(filter_frame, textvariable=self.search_var, width=20)
        search_entry.grid(row=0, column=5)
        search_entry.bind('<KeyRelease>', self.on_search_change)
        
        # Action buttons
        action_frame = ttk.Frame(control_frame)
        action_frame.pack(side='right')
        
        ttk.Button(action_frame, text="Refresh", command=self.refresh_logs).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Clear Filters", command=self.clear_filters).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Export Logs", command=self.export_logs).pack(side='left', padx=2)
        
        # Log display
        log_frame = ttk.Frame(main_container)
        log_frame.pack(fill='both', expand=True)
        
        self.log_tree = ttk.Treeview(log_frame, columns=('Time', 'Level', 'Component', 'Message'), 
                                    show='headings', height=20)
        
        # Configure columns
        self.log_tree.heading('Time', text='Time')
        self.log_tree.heading('Level', text='Level')
        self.log_tree.heading('Component', text='Component')
        self.log_tree.heading('Message', text='Message')
        
        self.log_tree.column('Time', width=150)
        self.log_tree.column('Level', width=80)
        self.log_tree.column('Component', width=120)
        self.log_tree.column('Message', width=500)
        
        # Scrollbar for log tree
        log_scroll = ttk.Scrollbar(log_frame, orient='vertical', command=self.log_tree.yview)
        self.log_tree.configure(yscrollcommand=log_scroll.set)
        
        self.log_tree.pack(side='left', fill='both', expand=True)
        log_scroll.pack(side='right', fill='y')
        
        # Double-click to view details
        self.log_tree.bind('<Double-1>', self.show_log_details)
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_container, textvariable=self.status_var, relief='sunken')
        status_bar.pack(fill='x', pady=(5, 0))
    
    def refresh_logs(self):
        """Refresh the log display"""
        # Clear existing items
        for item in self.log_tree.get_children():
            self.log_tree.delete(item)
        
        # Get filtered logs
        logs = self.get_filtered_logs()
        
        # Update component filter options
        components = sorted(set(log.component for log in self.log_manager.memory_logs))
        self.component_combo['values'] = ["ALL"] + components
        
        # Add logs to tree
        for log in logs:
            # Format timestamp
            try:
                dt = datetime.fromisoformat(log.timestamp)
                time_str = dt.strftime("%H:%M:%S")
            except:
                time_str = log.timestamp
            
            # Color code by level
            tags = []
            if log.level == "ERROR":
                tags = ['error']
            elif log.level == "WARNING":
                tags = ['warning']
            elif log.level == "CRITICAL":
                tags = ['critical']
            
            self.log_tree.insert('', 'end', values=(
                time_str, log.level, log.component, log.message
            ), tags=tags)
        
        # Configure tag colors
        self.log_tree.tag_configure('error', foreground='red')
        self.log_tree.tag_configure('warning', foreground='orange')
        self.log_tree.tag_configure('critical', foreground='dark red', background='light pink')
        
        self.status_var.set(f"Showing {len(logs)} log entries")
    
    def get_filtered_logs(self) -> List[LogEntry]:
        """Get logs with current filters applied"""
        logs = self.log_manager.get_recent_logs(limit=500)
        
        # Level filter
        level_filter = self.level_var.get()
        if level_filter != "ALL":
            logs = [log for log in logs if log.level == level_filter]
        
        # Component filter
        component_filter = self.component_var.get()
        if component_filter != "ALL":
            logs = [log for log in logs if log.component == component_filter]
        
        # Search filter
        search_term = self.search_var.get().lower()
        if search_term:
            logs = [log for log in logs if search_term in log.message.lower()]
        
        return logs
    
    def on_filter_change(self, event=None):
        """Handle filter changes"""
        self.refresh_logs()
    
    def on_search_change(self, event=None):
        """Handle search changes with debounce"""
        if hasattr(self, '_search_timer'):
            self.window.after_cancel(self._search_timer)
        self._search_timer = self.window.after(500, self.refresh_logs)
    
    def clear_filters(self):
        """Clear all filters"""
        self.level_var.set("ALL")
        self.component_var.set("ALL")
        self.search_var.set("")
        self.refresh_logs()
    
    def show_log_details(self, event):
        """Show detailed log information in a popup"""
        selection = self.log_tree.selection()
        if not selection:
            return
        
        # Get the selected log entry
        item_values = self.log_tree.item(selection[0])['values']
        if not item_values:
            return
        
        # Find the corresponding log entry
        logs = self.get_filtered_logs()
        selected_index = self.log_tree.index(selection[0])
        
        if selected_index < len(logs):
            log_entry = logs[selected_index]
            self.show_log_detail_window(log_entry)
    
    def show_log_detail_window(self, log_entry: LogEntry):
        """Show detailed log entry in a popup window"""
        detail_window = tk.Toplevel(self.window)
        detail_window.title("Log Entry Details")
        detail_window.geometry("600x400")
        detail_window.transient(self.window)
        
        # Create text widget with details
        from tkinter import scrolledtext
        text_widget = scrolledtext.ScrolledText(detail_window, wrap=tk.WORD)
        text_widget.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Format log details
        details_text = f"""Log Entry Details
================

Timestamp: {log_entry.timestamp}
Level: {log_entry.level}
Component: {log_entry.component}
Session ID: {log_entry.session_id}

Message:
{log_entry.message}
"""
        
        if log_entry.details:
            import json
            details_text += f"\n\nAdditional Details:\n{json.dumps(log_entry.details, indent=2)}"
        
        text_widget.insert('1.0', details_text)
        text_widget.configure(state='disabled')
        
        # Close button
        button_frame = ttk.Frame(detail_window)
        button_frame.pack(fill='x', padx=10, pady=(0, 10))
        ttk.Button(button_frame, text="Close", command=detail_window.destroy).pack(side='right')
    
    def export_logs(self):
        """Export logs to file"""
        filename = filedialog.asksaveasfilename(
            title="Export Logs",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                filepath = Path(filename)
                logs = self.get_filtered_logs()
                
                self.log_manager.export_logs(filepath, logs)
                messagebox.showinfo("Export Complete", f"Logs exported to {filename}")
                self.status_var.set(f"Exported {len(logs)} logs to {filename}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export logs: {e}")
    
    def auto_refresh(self):
        """Auto-refresh logs every 5 seconds"""
        if self.window.winfo_exists():
            self.refresh_logs()
            self.window.after(5000, self.auto_refresh)

class LogStatsWindow:
    """Window showing log statistics and analysis"""
    
    def __init__(self, parent, log_manager: LogManager):
        self.parent = parent
        self.log_manager = log_manager
        
        self.window = tk.Toplevel(parent)
        self.window.title("Log Statistics")
        self.window.geometry("600x500")
        self.window.transient(parent)
        
        self.create_widgets()
        self.update_stats()
    
    def create_widgets(self):
        """Create the statistics interface"""
        main_container = ttk.Frame(self.window)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Header
        ttk.Label(main_container, text="Log Statistics & Analysis", 
                 font=('Arial', 16, 'bold')).pack(pady=(0, 20))
        
        # Create notebook for different stats
        notebook = ttk.Notebook(main_container)
        notebook.pack(fill='both', expand=True)
        
        # Summary tab
        summary_frame = ttk.Frame(notebook)
        notebook.add(summary_frame, text="Summary")
        self.create_summary_tab(summary_frame)
        
        # By Level tab
        level_frame = ttk.Frame(notebook)
        notebook.add(level_frame, text="By Level")
        self.create_level_tab(level_frame)
        
        # By Component tab
        component_frame = ttk.Frame(notebook)
        notebook.add(component_frame, text="By Component")
        self.create_component_tab(component_frame)
        
        # Refresh button
        ttk.Button(main_container, text="Refresh", command=self.update_stats).pack(pady=(10, 0))
    
    def create_summary_tab(self, parent):
        """Create summary statistics tab"""
        self.summary_text = tk.Text(parent, wrap=tk.WORD, height=20)
        scrollbar = ttk.Scrollbar(parent, orient='vertical', command=self.summary_text.yview)
        self.summary_text.configure(yscrollcommand=scrollbar.set)
        
        self.summary_text.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
    
    def create_level_tab(self, parent):
        """Create level statistics tab"""
        self.level_tree = ttk.Treeview(parent, columns=('Level', 'Count', 'Percentage'), 
                                      show='headings', height=15)
        
        self.level_tree.heading('Level', text='Level')
        self.level_tree.heading('Count', text='Count')
        self.level_tree.heading('Percentage', text='Percentage')
        
        self.level_tree.pack(fill='both', expand=True)
    
    def create_component_tab(self, parent):
        """Create component statistics tab"""
        self.component_tree = ttk.Treeview(parent, columns=('Component', 'Count', 'Last Entry'), 
                                          show='headings', height=15)
        
        self.component_tree.heading('Component', text='Component')
        self.component_tree.heading('Count', text='Count')
        self.component_tree.heading('Last Entry', text='Last Entry')
        
        self.component_tree.pack(fill='both', expand=True)
    
    def update_stats(self):
        """Update all statistics"""
        logs = self.log_manager.memory_logs
        
        if not logs:
            return
        
        # Update summary
        total_logs = len(logs)
        levels = {}
        components = {}
        
        for log in logs:
            # Count by level
            levels[log.level] = levels.get(log.level, 0) + 1
            
            # Count by component
            if log.component not in components:
                components[log.component] = {'count': 0, 'last': log.timestamp}
            components[log.component]['count'] += 1
            if log.timestamp > components[log.component]['last']:
                components[log.component]['last'] = log.timestamp
        
        # Update summary text
        self.summary_text.delete('1.0', tk.END)
        summary = f"""Log Statistics Summary
=====================

Total Log Entries: {total_logs:,}
Session ID: {self.log_manager.session_id}
Time Range: {logs[0].timestamp} to {logs[-1].timestamp}

Log Levels:
"""
        for level, count in sorted(levels.items()):
            percentage = (count / total_logs) * 100
            summary += f"  {level}: {count:,} ({percentage:.1f}%)\n"
        
        summary += f"\nComponents: {len(components)} unique components\n"
        summary += f"Most Active Component: {max(components.keys(), key=lambda k: components[k]['count'])}\n"
        
        # Recent activity
        recent_errors = [log for log in logs[-100:] if log.level in ['ERROR', 'CRITICAL']]
        if recent_errors:
            summary += f"\nRecent Errors: {len(recent_errors)} in last 100 entries\n"
        
        self.summary_text.insert('1.0', summary)
        
        # Update level tree
        for item in self.level_tree.get_children():
            self.level_tree.delete(item)
        
        for level, count in sorted(levels.items()):
            percentage = (count / total_logs) * 100
            self.level_tree.insert('', 'end', values=(level, f"{count:,}", f"{percentage:.1f}%"))
        
        # Update component tree
        for item in self.component_tree.get_children():
            self.component_tree.delete(item)
        
        for component, info in sorted(components.items(), key=lambda x: x[1]['count'], reverse=True):
            try:
                last_dt = datetime.fromisoformat(info['last'])
                last_str = last_dt.strftime("%H:%M:%S")
            except:
                last_str = info['last']
            
            self.component_tree.insert('', 'end', values=(component, f"{info['count']:,}", last_str))