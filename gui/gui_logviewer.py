#!/usr/bin/env python3
# FILE: gui/gui_logviewer.py
"""
GUI Log Viewer Module - FIXED VERSION
Properly handles window lifecycle and prevents common tkinter issues
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from dataclasses import asdict
import re
import threading

from gui_logging import LogManager, LogEntry

class LogViewerWindow:
    """Fixed log viewer with proper window management"""
    
    def __init__(self, parent, log_manager: LogManager):
        self.parent = parent
        self.log_manager = log_manager
        self.window = None
        self._refresh_timer = None
        self._search_timer = None
        self._destroyed = False
        
        # Auto-scroll setting
        self.auto_scroll_var = tk.BooleanVar(value=True)
        
        self.create_window()
        self.create_widgets()
        self.refresh_logs()
        
        # Start auto-refresh
        self.start_auto_refresh()
    
    def create_window(self):
        """Create the window with proper settings"""
        self.window = tk.Toplevel(self.parent)
        self.window.title("FileMaker Sync - Activity Log")
        self.window.geometry("1200x700")
        
        # Set window icon if available
        try:
            # Try to inherit icon from parent
            self.window.iconbitmap(default="")
        except:
            pass
        
        # Make window resizable
        self.window.minsize(800, 500)
        
        # Set proper window management
        self.window.protocol("WM_DELETE_WINDOW", self.close_window)
        
        # Center window on parent
        self.center_on_parent()
        
        # Bring window to front
        self.window.lift()
        self.window.focus_force()
    
    def center_on_parent(self):
        """Center the window on the parent window"""
        try:
            self.window.update_idletasks()
            
            # Get parent position and size
            parent_x = self.parent.winfo_rootx()
            parent_y = self.parent.winfo_rooty()
            parent_width = self.parent.winfo_width()
            parent_height = self.parent.winfo_height()
            
            # Get window size
            window_width = self.window.winfo_width()
            window_height = self.window.winfo_height()
            
            # Calculate center position
            x = parent_x + (parent_width - window_width) // 2
            y = parent_y + (parent_height - window_height) // 2
            
            # Ensure window stays on screen
            x = max(0, min(x, self.window.winfo_screenwidth() - window_width))
            y = max(0, min(y, self.window.winfo_screenheight() - window_height))
            
            self.window.geometry(f"+{x}+{y}")
        except:
            # Fallback to screen center
            self.window.update_idletasks()
            x = (self.window.winfo_screenwidth() // 2) - (self.window.winfo_width() // 2)
            y = (self.window.winfo_screenheight() // 2) - (self.window.winfo_height() // 2)
            self.window.geometry(f"+{x}+{y}")
    
    def close_window(self):
        """Properly close the window and clean up resources"""
        if self._destroyed:
            return
        
        self._destroyed = True
        
        try:
            # Cancel any pending timers
            if self._refresh_timer:
                self.window.after_cancel(self._refresh_timer)
                self._refresh_timer = None
            
            if self._search_timer:
                self.window.after_cancel(self._search_timer)
                self._search_timer = None
            
            # Destroy the window
            if self.window and self.window.winfo_exists():
                self.window.destroy()
                
        except Exception as e:
            print(f"Error closing log viewer: {e}")
    
    def create_widgets(self):
        """Create the log viewer interface"""
        if self._destroyed:
            return
        
        # Main container
        main_container = ttk.Frame(self.window)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Control panel
        self.create_control_panel(main_container)
        
        # Log display
        self.create_log_display(main_container)
        
        # Status bar
        self.create_status_bar(main_container)
    
    def create_control_panel(self, parent):
        """Create the control panel with filters and buttons"""
        control_frame = ttk.Frame(parent)
        control_frame.pack(fill='x', pady=(0, 10))
        
        # Filter controls
        filter_frame = ttk.LabelFrame(control_frame, text="Filters", padding=5)
        filter_frame.pack(side='left', fill='x', expand=True, padx=(0, 10))
        
        # Log level filter
        ttk.Label(filter_frame, text="Level:").grid(row=0, column=0, padx=(0, 5), sticky='w')
        self.level_var = tk.StringVar(value="ALL")
        level_combo = ttk.Combobox(filter_frame, textvariable=self.level_var, 
                                  values=["ALL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                                  width=10, state="readonly")
        level_combo.grid(row=0, column=1, padx=(0, 10), sticky='w')
        level_combo.bind('<<ComboboxSelected>>', self.on_filter_change)
        
        # Component filter
        ttk.Label(filter_frame, text="Component:").grid(row=0, column=2, padx=(0, 5), sticky='w')
        self.component_var = tk.StringVar(value="ALL")
        self.component_combo = ttk.Combobox(filter_frame, textvariable=self.component_var, 
                                           width=15, state="readonly")
        self.component_combo.grid(row=0, column=3, padx=(0, 10), sticky='w')
        self.component_combo.bind('<<ComboboxSelected>>', self.on_filter_change)
        
        # Search
        ttk.Label(filter_frame, text="Search:").grid(row=0, column=4, padx=(0, 5), sticky='w')
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(filter_frame, textvariable=self.search_var, width=20)
        search_entry.grid(row=0, column=5, sticky='w')
        search_entry.bind('<KeyRelease>', self.on_search_change)
        
        # Auto-scroll checkbox
        ttk.Checkbutton(filter_frame, text="Auto-scroll", 
                       variable=self.auto_scroll_var).grid(row=0, column=6, padx=(10, 0), sticky='w')
        
        # Action buttons
        action_frame = ttk.Frame(control_frame)
        action_frame.pack(side='right')
        
        ttk.Button(action_frame, text="Refresh", command=self.refresh_logs).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Clear Filters", command=self.clear_filters).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Export", command=self.export_logs).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Close", command=self.close_window).pack(side='left', padx=2)
    
    def create_log_display(self, parent):
        """Create the log display area"""
        log_frame = ttk.Frame(parent)
        log_frame.pack(fill='both', expand=True)
        
        # Create Treeview for log display
        columns = ('Time', 'Level', 'Component', 'Message')
        self.log_tree = ttk.Treeview(log_frame, columns=columns, show='headings', height=20)
        
        # Configure columns
        self.log_tree.heading('Time', text='Time')
        self.log_tree.heading('Level', text='Level')
        self.log_tree.heading('Component', text='Component')
        self.log_tree.heading('Message', text='Message')
        
        # Set column widths
        self.log_tree.column('Time', width=120, minwidth=100)
        self.log_tree.column('Level', width=80, minwidth=60)
        self.log_tree.column('Component', width=100, minwidth=80)
        self.log_tree.column('Message', width=600, minwidth=300)
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(log_frame, orient='vertical', command=self.log_tree.yview)
        h_scrollbar = ttk.Scrollbar(log_frame, orient='horizontal', command=self.log_tree.xview)
        
        self.log_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # Pack everything
        self.log_tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        
        # Configure grid weights
        log_frame.grid_rowconfigure(0, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)
        
        # Bind double-click to show details
        self.log_tree.bind('<Double-1>', self.show_log_details)
        
        # Configure row colors based on log level
        self.log_tree.tag_configure('ERROR', background='#ffcccc')
        self.log_tree.tag_configure('CRITICAL', background='#ff9999')
        self.log_tree.tag_configure('WARNING', background='#ffffcc')
        self.log_tree.tag_configure('DEBUG', foreground='gray')
    
    def create_status_bar(self, parent):
        """Create the status bar"""
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(parent, textvariable=self.status_var, relief='sunken')
        status_bar.pack(fill='x', pady=(5, 0))
    
    def refresh_logs(self):
        """Refresh the log display"""
        if self._destroyed or not self.window or not self.window.winfo_exists():
            return
        
        try:
            # Clear existing items
            for item in self.log_tree.get_children():
                self.log_tree.delete(item)
            
            # Get filtered logs
            logs = self.get_filtered_logs()
            
            # Update component filter options
            self.update_component_filter()
            
            # Add logs to tree
            for log in logs:
                time_str = self.format_time(log.timestamp)
                message = self.truncate_message(log.message, 100)
                
                # Determine row tag for coloring
                tag = log.level if log.level in ['ERROR', 'CRITICAL', 'WARNING', 'DEBUG'] else ''
                
                item_id = self.log_tree.insert('', 'end', 
                                              values=(time_str, log.level, log.component, message),
                                              tags=(tag,))
                
                # Store full log entry for details view
                self.log_tree.set(item_id, 'log_entry', log)
            
            # Auto-scroll to bottom if enabled
            if self.auto_scroll_var.get() and logs:
                children = self.log_tree.get_children()
                if children:
                    self.log_tree.see(children[-1])
            
            # Update status
            self.status_var.set(f"Showing {len(logs)} log entries")
            
        except Exception as e:
            print(f"Error refreshing logs: {e}")
    
    def update_component_filter(self):
        """Update the component filter dropdown"""
        try:
            components = sorted(set(log.component for log in self.log_manager.memory_logs if log.component))
            self.component_combo['values'] = ["ALL"] + components
        except:
            pass
    
    def format_time(self, timestamp: str) -> str:
        """Format timestamp for display"""
        try:
            dt = datetime.fromisoformat(timestamp)
            return dt.strftime("%H:%M:%S")
        except:
            return timestamp[-8:] if len(timestamp) >= 8 else timestamp
    
    def truncate_message(self, message: str, max_length: int) -> str:
        """Truncate message for display"""
        if len(message) <= max_length:
            return message
        
        # Try to break at a word boundary
        truncated = message[:max_length]
        last_space = truncated.rfind(' ')
        
        if last_space > max_length * 0.7:
            return truncated[:last_space] + "..."
        else:
            return truncated + "..."
    
    def get_filtered_logs(self) -> List[LogEntry]:
        """Get logs with current filters applied"""
        try:
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
        except:
            return []
    
    def on_filter_change(self, event=None):
        """Handle filter changes"""
        if not self._destroyed:
            self.refresh_logs()
    
    def on_search_change(self, event=None):
        """Handle search changes with debounce"""
        if self._destroyed:
            return
        
        if self._search_timer:
            self.window.after_cancel(self._search_timer)
        
        self._search_timer = self.window.after(500, self.refresh_logs)
    
    def clear_filters(self):
        """Clear all filters"""
        if self._destroyed:
            return
        
        self.level_var.set("ALL")
        self.component_var.set("ALL")
        self.search_var.set("")
        self.refresh_logs()
    
    def show_log_details(self, event):
        """Show detailed log information"""
        if self._destroyed:
            return
        
        selection = self.log_tree.selection()
        if not selection:
            return
        
        item = selection[0]
        
        # Get log entry from tree item
        try:
            # Since we can't store objects directly, we'll find the log by timestamp and message
            values = self.log_tree.item(item)['values']
            if len(values) >= 4:
                time_str, level, component, message = values[:4]
                
                # Find matching log entry
                for log in self.get_filtered_logs():
                    if (log.level == level and 
                        log.component == component and 
                        message in log.message):
                        self.show_log_detail_window(log)
                        break
        except Exception as e:
            print(f"Error showing log details: {e}")
    
    def show_log_detail_window(self, log_entry: LogEntry):
        """Show detailed log information in a popup"""
        try:
            detail_window = tk.Toplevel(self.window)
            detail_window.title("Log Entry Details")
            detail_window.geometry("800x600")
            detail_window.transient(self.window)
            detail_window.grab_set()
            
            # Create scrolled text widget
            from tkinter import scrolledtext
            text_widget = scrolledtext.ScrolledText(detail_window, wrap=tk.WORD, font=('Consolas', 10))
            text_widget.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Format log details
            details_text = f"""Log Entry Details
================

Timestamp: {log_entry.timestamp}
Level: {log_entry.level}
Component: {log_entry.component}
Session ID: {log_entry.session_id or 'N/A'}

Message:
{'-' * 50}
{log_entry.message}
"""
            
            if log_entry.details:
                import json
                details_text += f"\n\nAdditional Details:\n{'-' * 30}\n{json.dumps(log_entry.details, indent=2)}"
            
            text_widget.insert('1.0', details_text)
            text_widget.configure(state='disabled')
            
            # Button frame
            button_frame = ttk.Frame(detail_window)
            button_frame.pack(fill='x', padx=10, pady=(0, 10))
            
            ttk.Button(button_frame, text="Close", command=detail_window.destroy).pack(side='right')
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to show log details: {e}")
    
    def export_logs(self):
        """Export logs to file"""
        if self._destroyed:
            return
        
        try:
            filename = filedialog.asksaveasfilename(
                parent=self.window,
                title="Export Logs",
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("JSON files", "*.json"), ("CSV files", "*.csv")]
            )
            
            if filename:
                logs = self.get_filtered_logs()
                filepath = Path(filename)
                
                if filepath.suffix.lower() == '.json':
                    self.export_logs_json(filepath, logs)
                elif filepath.suffix.lower() == '.csv':
                    self.export_logs_csv(filepath, logs)
                else:
                    self.export_logs_text(filepath, logs)
                
                messagebox.showinfo("Export Complete", f"Exported {len(logs)} logs to {filename}")
                
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export logs: {e}")
    
    def export_logs_json(self, filepath: Path, logs: List[LogEntry]):
        """Export logs in JSON format"""
        import json
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump([asdict(log) for log in logs], f, indent=2)
    
    def export_logs_csv(self, filepath: Path, logs: List[LogEntry]):
        """Export logs in CSV format"""
        import csv
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Level', 'Component', 'Message', 'Session_ID'])
            
            for log in logs:
                writer.writerow([log.timestamp, log.level, log.component, log.message, log.session_id])
    
    def export_logs_text(self, filepath: Path, logs: List[LogEntry]):
        """Export logs in text format"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("FileMaker Sync Log Export\n")
            f.write("=" * 50 + "\n")
            f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Entries: {len(logs)}\n\n")
            
            for log in logs:
                f.write(f"[{log.timestamp}] {log.level} - {log.component}\n")
                f.write(f"  {log.message}\n")
                if log.details:
                    import json
                    f.write(f"  Details: {json.dumps(log.details)}\n")
                f.write("\n")
    
    def start_auto_refresh(self):
        """Start auto-refresh timer"""
        if not self._destroyed and self.window and self.window.winfo_exists():
            try:
                self.refresh_logs()
                self._refresh_timer = self.window.after(3000, self.start_auto_refresh)
            except:
                pass


class LogStatsWindow:
    """Log statistics window with proper window management"""
    
    def __init__(self, parent, log_manager: LogManager):
        self.parent = parent
        self.log_manager = log_manager
        self.window = None
        self._destroyed = False
        
        self.create_window()
        self.create_widgets()
        self.update_stats()
    
    def create_window(self):
        """Create the statistics window"""
        self.window = tk.Toplevel(self.parent)
        self.window.title("Log Statistics")
        self.window.geometry("700x600")
        self.window.minsize(600, 500)
        
        # Set window management
        self.window.protocol("WM_DELETE_WINDOW", self.close_window)
        
        # Center on parent
        self.center_on_parent()
        
        # Bring to front
        self.window.lift()
        self.window.focus_force()
    
    def center_on_parent(self):
        """Center window on parent"""
        try:
            self.window.update_idletasks()
            
            parent_x = self.parent.winfo_rootx()
            parent_y = self.parent.winfo_rooty()
            parent_width = self.parent.winfo_width()
            parent_height = self.parent.winfo_height()
            
            window_width = self.window.winfo_width()
            window_height = self.window.winfo_height()
            
            x = parent_x + (parent_width - window_width) // 2
            y = parent_y + (parent_height - window_height) // 2
            
            self.window.geometry(f"+{x}+{y}")
        except:
            pass
    
    def close_window(self):
        """Close the window properly"""
        if self._destroyed:
            return
        
        self._destroyed = True
        
        try:
            if self.window and self.window.winfo_exists():
                self.window.destroy()
        except:
            pass
    
    def create_widgets(self):
        """Create the statistics interface"""
        if self._destroyed:
            return
        
        main_container = ttk.Frame(self.window)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Header
        header_frame = ttk.Frame(main_container)
        header_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Label(header_frame, text="Log Statistics", 
                 font=('Arial', 16, 'bold')).pack(side='left')
        
        ttk.Button(header_frame, text="Refresh", command=self.update_stats).pack(side='right')
        ttk.Button(header_frame, text="Close", command=self.close_window).pack(side='right', padx=(0, 5))
        
        # Statistics display
        self.stats_text = tk.Text(main_container, wrap=tk.WORD, font=('Consolas', 10))
        scrollbar = ttk.Scrollbar(main_container, orient='vertical', command=self.stats_text.yview)
        self.stats_text.configure(yscrollcommand=scrollbar.set)
        
        self.stats_text.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
    
    def update_stats(self):
        """Update statistics display"""
        if self._destroyed:
            return
        
        try:
            logs = self.log_manager.memory_logs
            
            if not logs:
                self.stats_text.delete('1.0', tk.END)
                self.stats_text.insert('1.0', "No log entries available.")
                return
            
            # Calculate statistics
            total_logs = len(logs)
            levels = {}
            components = {}
            
            for log in logs:
                levels[log.level] = levels.get(log.level, 0) + 1
                components[log.component] = components.get(log.component, 0) + 1
            
            # Generate statistics text
            stats_text = f"""Log Statistics Summary
=====================

Total Entries: {total_logs:,}
Session ID: {self.log_manager.session_id}
Time Range: {logs[0].timestamp} to {logs[-1].timestamp}

By Level:
{'-' * 20}
"""
            
            for level, count in sorted(levels.items()):
                percentage = (count / total_logs) * 100
                stats_text += f"{level:10}: {count:6,} ({percentage:5.1f}%)\n"
            
            stats_text += f"\nBy Component:\n{'-' * 20}\n"
            
            for component, count in sorted(components.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_logs) * 100
                stats_text += f"{component:15}: {count:6,} ({percentage:5.1f}%)\n"
            
            # Recent activity
            recent_errors = [log for log in logs[-100:] if log.level in ['ERROR', 'CRITICAL']]
            stats_text += f"\nRecent Activity:\n{'-' * 20}\n"
            stats_text += f"Recent Errors: {len(recent_errors)} in last 100 entries\n"
            
            # Update display
            self.stats_text.delete('1.0', tk.END)
            self.stats_text.insert('1.0', stats_text)
            
        except Exception as e:
            self.stats_text.delete('1.0', tk.END)
            self.stats_text.insert('1.0', f"Error generating statistics: {e}")