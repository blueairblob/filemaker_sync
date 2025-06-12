#!/usr/bin/env python3
# FILE: gui/gui_logviewer.py
"""
GUI Log Viewer Module - FIXED VERSION
Enhanced log viewer with proper column sizing and message handling
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
from datetime import datetime
from typing import List
from dataclasses import asdict
import re

from gui_logging import LogManager, LogEntry

class LogViewerWindow:
    """FIXED log viewer with proper column sizing and message handling"""
    
    def __init__(self, parent, log_manager: LogManager):
        self.parent = parent
        self.log_manager = log_manager
        
        self.window = tk.Toplevel(parent)
        self.window.title("Log Viewer")
        self.window.geometry("1200x700")
        # REMOVED: self.window.transient(parent) - this was causing the focus issues
        
        # FIXED: Proper window management
        self.window.protocol("WM_DELETE_WINDOW", self.close_window)
        self.window.focus_set()  # Set focus initially
        
        # Auto-scroll setting
        self.auto_scroll_var = tk.BooleanVar(value=True)
        
        self.create_widgets()
        self.refresh_logs()
        
        # Auto-refresh every 3 seconds
        self.auto_refresh()
    
    def close_window(self):
        """Properly close the window"""
        try:
            # Cancel any pending timers
            if hasattr(self, '_search_timer'):
                self.window.after_cancel(self._search_timer)
            if hasattr(self, '_refresh_timer'):
                self.window.after_cancel(self._refresh_timer)
            
            # Destroy the window
            self.window.quit()  # Stop the window's event loop
            self.window.destroy()
        except Exception as e:
            print(f"Error closing log viewer: {e}")
            # Force destroy if normal close fails
            try:
                self.window.destroy()
            except:
                pass
    
    def create_widgets(self):
        """Create the enhanced log viewer interface"""
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
        
        # Auto-scroll checkbox
        ttk.Checkbutton(filter_frame, text="Auto-scroll", variable=self.auto_scroll_var).grid(row=0, column=6, padx=(10, 0))
        
        # Action buttons
        action_frame = ttk.Frame(control_frame)
        action_frame.pack(side='right')
        
        ttk.Button(action_frame, text="Refresh", command=self.refresh_logs).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Clear Filters", command=self.clear_filters).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Export Logs", command=self.export_logs).pack(side='left', padx=2)
        
        # FIXED: Enhanced log display with proper TEXT widget instead of Treeview
        log_frame = ttk.Frame(main_container)
        log_frame.pack(fill='both', expand=True)
        
        # Use Text widget with proper formatting instead of Treeview
        from tkinter import scrolledtext
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            wrap=tk.NONE,  # No word wrapping for proper column alignment
            font=('Consolas', 9),  # Monospace font for alignment
            height=25,
            state='disabled'
        )
        self.log_text.pack(fill='both', expand=True)
        
        # Configure text tags for coloring
        self.log_text.tag_configure('header', font=('Consolas', 9, 'bold'), background='lightgray')
        self.log_text.tag_configure('error', foreground='red')
        self.log_text.tag_configure('warning', foreground='orange')
        self.log_text.tag_configure('critical', foreground='dark red', background='light pink')
        self.log_text.tag_configure('debug', foreground='gray')
        self.log_text.tag_configure('info', foreground='black')
        
        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(main_container, textvariable=self.status_var, relief='sunken')
        status_bar.pack(fill='x', pady=(5, 0))
    
    def refresh_logs(self):
        """FIXED log refresh using Text widget for proper rendering"""
        # Clear existing content
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', tk.END)
        
        # Get filtered logs
        logs = self.get_filtered_logs()
        
        # Update component filter options
        components = sorted(set(self.extract_component_from_log(log) for log in self.log_manager.memory_logs))
        components = [c for c in components if c]  # Remove empty components
        self.component_combo['values'] = ["ALL"] + components
        
        # Add header
        header = f"{'Date & Time':<20} {'Level':<8} {'Source':<10} {'Component':<12} {'Message'}\n"
        header += "-" * 120 + "\n"
        self.log_text.insert(tk.END, header, 'header')
        
        # Add logs with proper formatting
        for log in logs:
            # Parse log entry
            datetime_str, level, source, component, message = self.parse_log_entry(log)
            
            # Format datetime to be shorter
            try:
                dt = datetime.fromisoformat(log.timestamp)
                short_datetime = dt.strftime("%m-%d %H:%M:%S")
            except:
                short_datetime = datetime_str[-8:]  # Last 8 chars
            
            # Format line with fixed widths
            log_line = f"{short_datetime:<20} {level:<8} {source:<10} {component:<12} {message}\n"
            
            # Determine tag for coloring
            tag = level.lower() if level.lower() in ['error', 'warning', 'critical', 'debug'] else 'info'
            
            # Insert with color tag
            self.log_text.insert(tk.END, log_line, tag)
        
        # Disable editing
        self.log_text.configure(state='disabled')
        
        # Auto-scroll to bottom if enabled
        if self.auto_scroll_var.get() and logs:
            self.log_text.see(tk.END)
        
        self.status_var.set(f"Showing {len(logs)} log entries")
    
    def truncate_message(self, message: str, max_length: int) -> str:
        """Truncate message for display while preserving readability"""
        if len(message) <= max_length:
            return message
        
        # Try to break at a word boundary
        truncated = message[:max_length]
        last_space = truncated.rfind(' ')
        
        if last_space > max_length * 0.7:  # If we find a space in the last 30%
            return truncated[:last_space] + "..."
        else:
            return truncated + "..."
    
    def parse_log_entry(self, log: LogEntry) -> tuple:
        """ENHANCED log entry parsing to extract all components"""
        # Format full datetime
        try:
            dt = datetime.fromisoformat(log.timestamp)
            datetime_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            datetime_str = log.timestamp
        
        level = log.level
        component = log.component
        message = log.message
        
        # FIXED: Extract source application from message if available
        # Look for patterns like [FileMakerSync:94] at the start of the message
        source = "GUI"  # Default for GUI-generated logs
        
        # Try to extract source from detailed log format
        source_pattern = r'\[([^:\]]+):\d+\]'
        match = re.search(source_pattern, message)
        if match:
            source = match.group(1)
            # Clean up the message by removing the source pattern
            message = re.sub(r'\[[^:\]]+:\d+\]\s*', '', message)
        
        # Additional parsing for specific components
        if component == "Command":
            source = "CLI"
        elif component in ["Application", "Config"]:
            source = "GUI"
        elif component in ["Connection", "Operation"]:
            source = "Backend"
        
        return datetime_str, level, source, component, message
    
    def extract_component_from_log(self, log: LogEntry) -> str:
        """Extract component name for filtering"""
        return log.component
    
    def get_filtered_logs(self) -> List[LogEntry]:
        """Get logs with current filters applied"""
        logs = self.log_manager.get_recent_logs(limit=1000)
        
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
            logs = [log for log in logs if search_term in log.message.lower() or 
                   search_term in log.component.lower()]
        
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
        """Show detailed log information in a popup - UPDATED for Text widget"""
        # Get current cursor position
        cursor_pos = self.log_text.index(tk.INSERT)
        line_num = int(cursor_pos.split('.')[0]) - 3  # Subtract header lines
        
        if line_num < 0:
            return
        
        # Get the corresponding log entry
        logs = self.get_filtered_logs()
        if line_num < len(logs):
            log_entry = logs[line_num]
            self.show_log_detail_window(log_entry)
    
    def show_log_detail_window(self, log_entry: LogEntry):
        """ENHANCED log detail window"""
        detail_window = tk.Toplevel(self.window)
        detail_window.title("Log Entry Details")
        detail_window.geometry("800x600")  # Larger for better viewing
        detail_window.transient(self.window)
        detail_window.protocol("WM_DELETE_WINDOW", detail_window.destroy)
        
        # Create text widget with details
        from tkinter import scrolledtext
        text_widget = scrolledtext.ScrolledText(detail_window, wrap=tk.WORD, font=('Consolas', 10))
        text_widget.pack(fill='both', expand=True, padx=10, pady=10)
        
        # ENHANCED format log details
        datetime_str, level, source, component, message = self.parse_log_entry(log_entry)
        
        details_text = f"""Log Entry Details
================

Full Timestamp: {log_entry.timestamp}
Formatted Time: {datetime_str}
Level: {level}
Source Application: {source}
Component: {component}
Session ID: {log_entry.session_id}

Full Message:
{'-' * 50}
{message}
"""
        
        if log_entry.details:
            import json
            details_text += f"\n\nAdditional Details:\n{'-' * 30}\n{json.dumps(log_entry.details, indent=2)}"
        
        # Add raw log data for debugging
        details_text += f"\n\nRaw Log Data:\n{'-' * 20}\n"
        details_text += f"Original Message: {log_entry.message}\n"
        details_text += f"Component: {log_entry.component}\n"
        details_text += f"Level: {log_entry.level}\n"
        
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
            filetypes=[("JSON files", "*.json"), ("Text files", "*.txt"), ("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                filepath = Path(filename)
                logs = self.get_filtered_logs()
                
                if filepath.suffix.lower() == '.csv':
                    self.export_logs_csv(filepath, logs)
                elif filepath.suffix.lower() == '.json':
                    self.log_manager.export_logs(filepath, logs)
                else:
                    self.export_logs_text(filepath, logs)
                
                messagebox.showinfo("Export Complete", f"Logs exported to {filename}")
                self.status_var.set(f"Exported {len(logs)} logs to {filename}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export logs: {e}")
    
    def export_logs_csv(self, filepath: Path, logs: List[LogEntry]):
        """Export logs in CSV format"""
        import csv
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['DateTime', 'Level', 'Source', 'Component', 'Message', 'Session_ID'])
            
            # Write data
            for log in logs:
                datetime_str, level, source, component, message = self.parse_log_entry(log)
                writer.writerow([datetime_str, level, source, component, message, log.session_id])
    
    def export_logs_text(self, filepath: Path, logs: List[LogEntry]):
        """Export logs in enhanced text format"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("FileMaker Sync Log Export\n")
            f.write("=" * 50 + "\n")
            f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Entries: {len(logs)}\n")
            f.write("=" * 50 + "\n\n")
            
            for log in logs:
                datetime_str, level, source, component, message = self.parse_log_entry(log)
                f.write(f"[{datetime_str}] {level} - {source} - {component}\n")
                f.write(f"  {message}\n")
                if log.details:
                    import json
                    f.write(f"  Details: {json.dumps(log.details)}\n")
                f.write("\n")
    
    def auto_refresh(self):
        """Auto-refresh logs every 3 seconds"""
        try:
            if self.window.winfo_exists():
                self.refresh_logs()
                self._refresh_timer = self.window.after(3000, self.auto_refresh)
        except (tk.TclError, AttributeError):
            # Window was destroyed, stop refreshing
            pass

class LogStatsWindow:
    """ENHANCED log statistics window"""
    
    def __init__(self, parent, log_manager: LogManager):
        self.parent = parent
        self.log_manager = log_manager
        
        self.window = tk.Toplevel(parent)
        self.window.title("Log Statistics")
        self.window.geometry("700x600")
        self.window.transient(parent)
        self.window.protocol("WM_DELETE_WINDOW", self.window.destroy)
        
        self.create_widgets()
        self.update_stats()
    
    def create_widgets(self):
        """Create the enhanced statistics interface"""
        main_container = ttk.Frame(self.window)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Header
        header_frame = ttk.Frame(main_container)
        header_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Label(header_frame, text="Log Statistics & Analysis", 
                 font=('Arial', 16, 'bold')).pack(side='left')
        
        ttk.Button(header_frame, text="Refresh", command=self.update_stats).pack(side='right')
        ttk.Button(header_frame, text="Export Stats", command=self.export_stats).pack(side='right', padx=(0, 5))
        
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
        
        # By Source tab
        source_frame = ttk.Frame(notebook)
        notebook.add(source_frame, text="By Source")
        self.create_source_tab(source_frame)
        
        # Timeline tab
        timeline_frame = ttk.Frame(notebook)
        notebook.add(timeline_frame, text="Timeline")
        self.create_timeline_tab(timeline_frame)
    
    def create_summary_tab(self, parent):
        """Create summary statistics tab"""
        self.summary_text = tk.Text(parent, wrap=tk.WORD, height=20, font=('Consolas', 10))
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
        
        self.level_tree.column('Level', width=100)
        self.level_tree.column('Count', width=100)
        self.level_tree.column('Percentage', width=100)
        
        self.level_tree.pack(fill='both', expand=True)
    
    def create_component_tab(self, parent):
        """Create component statistics tab"""
        self.component_tree = ttk.Treeview(parent, columns=('Component', 'Count', 'Last Entry'), 
                                          show='headings', height=15)
        
        self.component_tree.heading('Component', text='Component')
        self.component_tree.heading('Count', text='Count')
        self.component_tree.heading('Last Entry', text='Last Entry')
        
        self.component_tree.column('Component', width=150)
        self.component_tree.column('Count', width=100)
        self.component_tree.column('Last Entry', width=150)
        
        self.component_tree.pack(fill='both', expand=True)
    
    def create_source_tab(self, parent):
        """Create source application statistics tab"""
        self.source_tree = ttk.Treeview(parent, columns=('Source', 'Count', 'Percentage'), 
                                       show='headings', height=15)
        
        self.source_tree.heading('Source', text='Source Application')
        self.source_tree.heading('Count', text='Count')
        self.source_tree.heading('Percentage', text='Percentage')
        
        self.source_tree.column('Source', width=150)
        self.source_tree.column('Count', width=100)
        self.source_tree.column('Percentage', width=100)
        
        self.source_tree.pack(fill='both', expand=True)
    
    def create_timeline_tab(self, parent):
        """Create timeline statistics tab"""
        timeline_frame = ttk.Frame(parent)
        timeline_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Controls
        control_frame = ttk.Frame(timeline_frame)
        control_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Label(control_frame, text="Time Period:").pack(side='left')
        self.timeline_var = tk.StringVar(value="Last Hour")
        timeline_combo = ttk.Combobox(control_frame, textvariable=self.timeline_var,
                                     values=["Last Hour", "Last 4 Hours", "Last Day", "All Time"],
                                     state="readonly")
        timeline_combo.pack(side='left', padx=(5, 0))
        timeline_combo.bind('<<ComboboxSelected>>', self.update_timeline)
        
        # Timeline display
        self.timeline_text = tk.Text(timeline_frame, wrap=tk.WORD, height=15, font=('Consolas', 10))
        timeline_scroll = ttk.Scrollbar(timeline_frame, orient='vertical', command=self.timeline_text.yview)
        self.timeline_text.configure(yscrollcommand=timeline_scroll.set)
        
        self.timeline_text.pack(side='left', fill='both', expand=True)
        timeline_scroll.pack(side='right', fill='y')
    
    def update_stats(self):
        """Update all statistics"""
        logs = self.log_manager.memory_logs
        
        if not logs:
            return
        
        # Parse logs to extract source information
        parsed_logs = []
        for log in logs:
            viewer = LogViewerWindow.__new__(LogViewerWindow)  # Create instance without __init__
            viewer.log_manager = self.log_manager
            datetime_str, level, source, component, message = viewer.parse_log_entry(log)
            parsed_logs.append({
                'log': log,
                'datetime_str': datetime_str,
                'level': level,
                'source': source,
                'component': component,
                'message': message
            })
        
        # Update summary
        total_logs = len(logs)
        levels = {}
        components = {}
        sources = {}
        
        for parsed in parsed_logs:
            log = parsed['log']
            # Count by level
            levels[log.level] = levels.get(log.level, 0) + 1
            
            # Count by component
            if log.component not in components:
                components[log.component] = {'count': 0, 'last': log.timestamp}
            components[log.component]['count'] += 1
            if log.timestamp > components[log.component]['last']:
                components[log.component]['last'] = log.timestamp
            
            # Count by source
            source = parsed['source']
            sources[source] = sources.get(source, 0) + 1
        
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
        if components:
            most_active = max(components.keys(), key=lambda k: components[k]['count'])
            summary += f"Most Active Component: {most_active}\n"
        
        summary += f"\nSource Applications: {len(sources)} different sources\n"
        if sources:
            most_active_source = max(sources.keys(), key=lambda k: sources[k])
            summary += f"Most Active Source: {most_active_source}\n"
        
        # Recent activity
        recent_errors = [log for log in logs[-100:] if log.level in ['ERROR', 'CRITICAL']]
        if recent_errors:
            summary += f"\nRecent Errors: {len(recent_errors)} in last 100 entries\n"
        
        # Memory usage stats
        import sys
        memory_usage = sys.getsizeof(logs) / 1024  # KB
        summary += f"\nMemory Usage: {memory_usage:.1f} KB for log storage\n"
        
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
        
        # Update source tree
        for item in self.source_tree.get_children():
            self.source_tree.delete(item)
        
        for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_logs) * 100
            self.source_tree.insert('', 'end', values=(source, f"{count:,}", f"{percentage:.1f}%"))
        
        # Update timeline
        self.update_timeline()
    
    def update_timeline(self, event=None):
        """Update timeline display"""
        period = self.timeline_var.get()
        logs = self.log_manager.memory_logs
        
        # Filter logs by time period
        now = datetime.now()
        if period == "Last Hour":
            cutoff = now.replace(minute=0, second=0, microsecond=0)
        elif period == "Last 4 Hours":
            cutoff = now.replace(hour=now.hour-4, minute=0, second=0, microsecond=0)
        elif period == "Last Day":
            cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
        else:  # All Time
            cutoff = None
        
        if cutoff:
            filtered_logs = [log for log in logs 
                           if datetime.fromisoformat(log.timestamp) >= cutoff]
        else:
            filtered_logs = logs
        
        # Group by hour
        hourly_stats = {}
        for log in filtered_logs:
            try:
                dt = datetime.fromisoformat(log.timestamp)
                hour_key = dt.strftime("%Y-%m-%d %H:00")
                
                if hour_key not in hourly_stats:
                    hourly_stats[hour_key] = {'total': 0, 'by_level': {}}
                
                hourly_stats[hour_key]['total'] += 1
                level = log.level
                hourly_stats[hour_key]['by_level'][level] = hourly_stats[hour_key]['by_level'].get(level, 0) + 1
            except:
                continue
        
        # Display timeline
        self.timeline_text.delete('1.0', tk.END)
        timeline_text = f"Activity Timeline - {period}\n"
        timeline_text += "=" * 40 + "\n\n"
        
        for hour, stats in sorted(hourly_stats.items()):
            timeline_text += f"{hour}: {stats['total']} entries\n"
            for level, count in sorted(stats['by_level'].items()):
                timeline_text += f"  {level}: {count}\n"
            timeline_text += "\n"
        
        if not hourly_stats:
            timeline_text += "No log entries in selected time period.\n"
        
        self.timeline_text.insert('1.0', timeline_text)
    
    def export_stats(self):
        """Export statistics to file"""
        filename = filedialog.asksaveasfilename(
            title="Export Statistics",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                # Get current statistics text
                stats_content = self.summary_text.get('1.0', tk.END)
                
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write("FileMaker Sync - Log Statistics Export\n")
                    f.write("=" * 50 + "\n")
                    f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 50 + "\n\n")
                    f.write(stats_content)
                
                messagebox.showinfo("Export Complete", f"Statistics exported to {filename}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export statistics: {e}")