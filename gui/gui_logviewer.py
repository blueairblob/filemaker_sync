#!/usr/bin/env python3
# FILE: gui/gui_logviewer.py
"""
GUI Log Viewer Module
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from dataclasses import asdict
import re
import threading
import queue
import time

from gui_logging import LogManager, LogEntry, LogLevel  # Import LogLevel separately

class LogViewerWindow:
    """Thread-safe log viewer that prevents hanging"""
    
    def __init__(self, parent, log_manager: LogManager):
        self.parent = parent
        self.log_manager = log_manager
        self.window = None
        
        # Thread safety infrastructure
        self._destroyed = False
        self._update_lock = threading.RLock()
        self._gui_update_queue = queue.Queue(maxsize=50)
        
        # Timers and refresh control
        self._refresh_timer = None
        self._search_timer = None
        self._gui_processor_timer = None
        
        # Auto-scroll and refresh settings
        self.auto_scroll_var = tk.BooleanVar(value=True)
        self.auto_refresh_var = tk.BooleanVar(value=True)
        self.refresh_interval = 3000  # 3 seconds (slower to prevent hanging)
        
        # Current log count to detect new logs
        self.last_log_count = 0
        
        # Initialize window and widgets
        self.create_window()
        self.create_widgets()
        
        # Register callback for new log entries (thread-safe)
        self.log_manager.add_callback(self.on_new_log_entry)
        
        # Start thread-safe GUI update processor
        self.start_gui_update_processor()
        
        # Initial load and start auto-refresh
        self.schedule_gui_update(self.refresh_logs)
        self.start_auto_refresh()
        
        # Add debug features if in debug mode
        if self.log_manager.debug_mode:
            self.add_debug_features()
    
    def create_window(self):
        """Create the window with proper thread-safe settings"""
        self.window = tk.Toplevel(self.parent)
        self.window.title(f"FileMaker Sync - Activity Log (Session: {self.log_manager.session_id})")
        self.window.geometry("1200x700")
        
        # Make window resizable
        self.window.minsize(800, 500)
        
        # Set proper window management
        self.window.protocol("WM_DELETE_WINDOW", self.close_window_safe)
        
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
        except Exception as e:
            print(f"Error centering window: {e}")
    
    def close_window_safe(self):
        """Thread-safe window close"""
        if self._destroyed:
            return
        
        self._destroyed = True
        
        try:
            # Stop all timers
            self.stop_auto_refresh()
            
            if self._search_timer:
                self.window.after_cancel(self._search_timer)
                self._search_timer = None
            
            if self._gui_processor_timer:
                self.window.after_cancel(self._gui_processor_timer)
                self._gui_processor_timer = None
            
            # Remove callback from log manager
            try:
                self.log_manager.remove_callback(self.on_new_log_entry)
            except:
                pass
            
            # Destroy the window
            if self.window and self.window.winfo_exists():
                self.window.destroy()
                
        except Exception as e:
            print(f"Error closing log viewer: {e}")
    
    def close_window(self):
        """Public interface for closing window"""
        self.close_window_safe()
    
    def start_gui_update_processor(self):
        """Start the thread-safe GUI update processor"""
        def process_gui_updates():
            """Process queued GUI updates"""
            if self._destroyed:
                return
            
            try:
                # Process up to 10 updates per cycle to prevent blocking
                updates_processed = 0
                while not self._gui_update_queue.empty() and updates_processed < 10:
                    try:
                        update_func = self._gui_update_queue.get_nowait()
                        if callable(update_func):
                            update_func()
                        updates_processed += 1
                    except queue.Empty:
                        break
                    except Exception as e:
                        print(f"Error processing GUI update: {e}")
                
            except Exception as e:
                print(f"Error in GUI update processor: {e}")
            
            finally:
                # Schedule next processing
                if not self._destroyed and self.window and self.window.winfo_exists():
                    self._gui_processor_timer = self.window.after(100, process_gui_updates)
        
        # Start the processor
        if not self._destroyed:
            self._gui_processor_timer = self.window.after(100, process_gui_updates)
    
    def schedule_gui_update(self, update_func):
        """Thread-safely schedule a GUI update"""
        if self._destroyed:
            return
        
        try:
            self._gui_update_queue.put_nowait(update_func)
        except queue.Full:
            # Queue is full, skip this update
            print("GUI update queue full, skipping update")
    
    def create_widgets(self):
        """Create the log viewer interface"""
        if self._destroyed:
            return
        
        # Main container
        main_container = ttk.Frame(self.window)
        main_container.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Header with title and stats
        self.create_header(main_container)
        
        # Control panel
        self.create_control_panel(main_container)
        
        # Log display
        self.create_log_display(main_container)
        
        # Status bar
        self.create_status_bar(main_container)
    
    def create_header(self, parent):
        """Create header with statistics"""
        header_frame = ttk.Frame(parent)
        header_frame.pack(fill='x', pady=(0, 10))
        
        # Title
        title_label = ttk.Label(header_frame, text="Activity Log Viewer", 
                               font=('Arial', 14, 'bold'))
        title_label.pack(side='left')
        
        # Stats frame
        stats_frame = ttk.Frame(header_frame)
        stats_frame.pack(side='right')
        
        self.stats_label = ttk.Label(stats_frame, text="Loading...", font=('Arial', 9))
        self.stats_label.pack(side='right')
        
        # Live indicator
        self.live_indicator = ttk.Label(stats_frame, text="● LIVE", 
                                       foreground='green', font=('Arial', 9, 'bold'))
        self.live_indicator.pack(side='right', padx=(0, 10))
    
    def create_control_panel(self, parent):
        """Create the control panel with filters and buttons"""
        control_frame = ttk.Frame(parent)
        control_frame.pack(fill='x', pady=(0, 10))
        
        # Filter controls
        filter_frame = ttk.LabelFrame(control_frame, text="Filters", padding=5)
        filter_frame.pack(side='left', fill='x', expand=True, padx=(0, 10))
        
        # Row 1: Level and Component filters
        row1_frame = ttk.Frame(filter_frame)
        row1_frame.pack(fill='x', pady=2)
        
        # Log level filter
        ttk.Label(row1_frame, text="Level:").pack(side='left', padx=(0, 5))
        self.level_var = tk.StringVar(value="ALL")
        level_combo = ttk.Combobox(row1_frame, textvariable=self.level_var, 
                                  values=["ALL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                                  width=10, state="readonly")
        level_combo.pack(side='left', padx=(0, 10))
        level_combo.bind('<<ComboboxSelected>>', self.on_filter_change_safe)
        
        # Component filter
        ttk.Label(row1_frame, text="Component:").pack(side='left', padx=(0, 5))
        self.component_var = tk.StringVar(value="ALL")
        self.component_combo = ttk.Combobox(row1_frame, textvariable=self.component_var, 
                                           width=15, state="readonly")
        self.component_combo.pack(side='left', padx=(0, 10))
        self.component_combo.bind('<<ComboboxSelected>>', self.on_filter_change_safe)
        
        # Row 2: Search and options
        row2_frame = ttk.Frame(filter_frame)
        row2_frame.pack(fill='x', pady=2)
        
        # Search
        ttk.Label(row2_frame, text="Search:").pack(side='left', padx=(0, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(row2_frame, textvariable=self.search_var, width=20)
        search_entry.pack(side='left', padx=(0, 10))
        search_entry.bind('<KeyRelease>', self.on_search_change_safe)
        
        # Options
        ttk.Checkbutton(row2_frame, text="Auto-scroll", 
                       variable=self.auto_scroll_var).pack(side='left', padx=(10, 0))
        
        ttk.Checkbutton(row2_frame, text="Auto-refresh", 
                       variable=self.auto_refresh_var,
                       command=self.toggle_auto_refresh_safe).pack(side='left', padx=(10, 0))
        
        # Action buttons
        action_frame = ttk.Frame(control_frame)
        action_frame.pack(side='right')
        
        ttk.Button(action_frame, text="Refresh Now", command=self.refresh_logs_safe).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Clear Filters", command=self.clear_filters_safe).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Export", command=self.export_logs_safe).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Clear Logs", command=self.clear_logs_safe).pack(side='left', padx=2)
        ttk.Button(action_frame, text="Close", command=self.close_window_safe).pack(side='left', padx=2)
    
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
        self.log_tree.column('Time', width=100, minwidth=80)
        self.log_tree.column('Level', width=80, minwidth=60)
        self.log_tree.column('Component', width=120, minwidth=80)
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
        self.log_tree.bind('<Double-1>', self.show_log_details_safe)
        
        # Configure row colors based on log level
        self.log_tree.tag_configure('ERROR', background='#ffcccc')
        self.log_tree.tag_configure('CRITICAL', background='#ff9999')
        self.log_tree.tag_configure('WARNING', background='#ffffcc')
        self.log_tree.tag_configure('DEBUG', foreground='gray')
        self.log_tree.tag_configure('INFO', foreground='black')
    
    def create_status_bar(self, parent):
        """Create the status bar"""
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill='x', pady=(5, 0))
        
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(status_frame, textvariable=self.status_var, relief='sunken')
        status_bar.pack(side='left', fill='x', expand=True)
        
        # Connection status
        self.connection_status_label = ttk.Label(status_frame, text="Mode", font=('Arial', 8))
        self.connection_status_label.pack(side='right', padx=(5, 0))
    
    def add_debug_features(self):
        """Add debug features for testing logging"""
        # Add a debug frame to the control panel
        debug_frame = ttk.LabelFrame(self.window, text="Debug Controls", padding=5)
        debug_frame.pack(side='bottom', fill='x', padx=10, pady=(5, 10))
        
        ttk.Button(debug_frame, text="Generate Test Logs", 
                  command=self.generate_test_logs_safe).pack(side='left', padx=2)
        ttk.Button(debug_frame, text="Test All Levels", 
                  command=self.test_all_log_levels_safe).pack(side='left', padx=2)
        ttk.Button(debug_frame, text="Log Statistics", 
                  command=self.show_log_statistics_safe).pack(side='left', padx=2)
    
    # Thread-safe event handlers
    def on_new_log_entry(self, log_entry: LogEntry):
        """Thread-safe handler for new log entries"""
        if self._destroyed:
            return
        
        def process_new_entry():
            try:
                if self.auto_refresh_var.get() and self.entry_matches_filters(log_entry):
                    self.add_log_entry_to_tree(log_entry)
                    
                    # Auto-scroll if enabled
                    if self.auto_scroll_var.get():
                        children = self.log_tree.get_children()
                        if children:
                            self.log_tree.see(children[-1])
                    
                    # Update status display
                    self.update_status_display()
            except Exception as e:
                print(f"Error processing new log entry: {e}")
        
        self.schedule_gui_update(process_new_entry)
    
    def on_filter_change_safe(self, event=None):
        """Thread-safe filter change handler"""
        def refresh_with_delay():
            self.schedule_gui_update(self.refresh_logs)
        
        # Small delay to prevent rapid refreshes
        if not self._destroyed:
            self.window.after(200, refresh_with_delay)
    
    def on_search_change_safe(self, event=None):
        """Thread-safe search change handler with debounce"""
        if self._destroyed:
            return
        
        if self._search_timer:
            self.window.after_cancel(self._search_timer)
        
        def delayed_refresh():
            self.schedule_gui_update(self.refresh_logs)
        
        self._search_timer = self.window.after(800, delayed_refresh)  # Longer delay to prevent hanging
    
    def refresh_logs_safe(self):
        """Thread-safe refresh logs wrapper"""
        self.schedule_gui_update(self.refresh_logs)
    
    def clear_filters_safe(self):
        """Thread-safe clear filters"""
        def clear_filters():
            self.level_var.set("ALL")
            self.component_var.set("ALL")
            self.search_var.set("")
            self.refresh_logs()
        
        self.schedule_gui_update(clear_filters)
    
    def clear_logs_safe(self):
        """Thread-safe clear logs"""
        def confirm_and_clear():
            if messagebox.askyesno("Clear Logs", 
                                  "Are you sure you want to clear all logs from memory?\n\n"
                                  "This will remove all log entries from the current session."):
                self.log_manager.clear_logs()
                self.refresh_logs()
        
        self.schedule_gui_update(confirm_and_clear)
    
    def export_logs_safe(self):
        """Thread-safe export logs"""
        def export_logs():
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
                    
                    # Use the log manager's export functionality
                    self.log_manager.export_logs(filepath, logs)
                    
                    messagebox.showinfo("Export Complete", f"Exported {len(logs)} logs to {filename}")
                    
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export logs: {e}")
        
        self.schedule_gui_update(export_logs)
    
    def show_log_details_safe(self, event):
        """Thread-safe log details display"""
        def show_details():
            self.show_log_details(event)
        
        self.schedule_gui_update(show_details)
    
    def toggle_auto_refresh_safe(self):
        """Thread-safe auto-refresh toggle"""
        if self.auto_refresh_var.get():
            self.start_auto_refresh()
            self.live_indicator.configure(text="● LIVE", foreground='green')
        else:
            self.stop_auto_refresh()
            self.live_indicator.configure(text="⏸ PAUSED", foreground='orange')
    
    # Core functionality methods (called from GUI thread only)
    def refresh_logs(self):
        """Refresh the log display (GUI thread only)"""
        if self._destroyed or not self.window or not self.window.winfo_exists():
            return
        
        try:
            # Clear existing items
            for item in self.log_tree.get_children():
                self.log_tree.delete(item)
            
            # Get filtered logs (limit to prevent hanging)
            logs = self.get_filtered_logs()
            
            # Update component filter options
            self.update_component_filter()
            
            # Add logs to tree (limit to prevent UI freeze)
            max_display_logs = 1000  # Prevent UI freeze with too many logs
            display_logs = logs[:max_display_logs]
            
            for log in display_logs:
                self.add_log_entry_to_tree(log)
            
            # Auto-scroll to bottom if enabled
            if self.auto_scroll_var.get() and display_logs:
                children = self.log_tree.get_children()
                if children:
                    self.log_tree.see(children[-1])
            
            # Update status and statistics
            self.update_status_display()
            self.update_statistics_display()
            
            # Update last log count
            self.last_log_count = self.log_manager.get_log_count()
            
            if len(logs) > max_display_logs:
                self.status_var.set(f"Showing {max_display_logs} of {len(logs)} filtered logs")
            
        except Exception as e:
            print(f"Error refreshing logs: {e}")
            self.status_var.set(f"Error refreshing logs: {e}")
    
    def entry_matches_filters(self, entry: LogEntry) -> bool:
        """Check if a log entry matches current filters"""
        try:
            # Level filter
            level_filter = self.level_var.get()
            if level_filter != "ALL" and entry.level != level_filter:
                return False
            
            # Component filter
            component_filter = self.component_var.get()
            if component_filter != "ALL" and entry.component != component_filter:
                return False
            
            # Search filter
            search_term = self.search_var.get().lower()
            if search_term:
                if (search_term not in entry.message.lower() and 
                    search_term not in entry.component.lower()):
                    return False
            
            return True
        except Exception:
            return True  # Default to showing entry if filter check fails
    
    def add_log_entry_to_tree(self, entry: LogEntry):
        """Add a single log entry to the tree"""
        try:
            time_str = self.format_time(entry.timestamp)
            message = self.truncate_message(entry.message, 100)
            
            # Determine row tag for coloring
            tag = entry.level if entry.level in ['ERROR', 'CRITICAL', 'WARNING', 'DEBUG', 'INFO'] else ''
            
            self.log_tree.insert('', 'end', 
                                values=(time_str, entry.level, entry.component, message),
                                tags=(tag,))
        except Exception as e:
            print(f"Error adding log entry to tree: {e}")
    
    def update_component_filter(self):
        """Update the component filter dropdown"""
        try:
            # Get all components from recent logs (limit to prevent hanging)
            recent_logs = self.log_manager.get_recent_logs(limit=500)
            components = sorted(set(log.component for log in recent_logs if log.component))
            
            current_value = self.component_var.get()
            self.component_combo['values'] = ["ALL"] + components
            
            # Restore selection if it's still valid
            if current_value not in self.component_combo['values']:
                self.component_var.set("ALL")
        except Exception as e:
            print(f"Error updating component filter: {e}")
    
    def update_status_display(self):
        """Update the status bar with current information"""
        try:
            # Get current log count and filter info
            total_logs = self.log_manager.get_log_count()
            displayed_logs = len(self.log_tree.get_children())
            
            # Create status message
            status_parts = [f"Total: {total_logs:,}", f"Displayed: {displayed_logs:,}"]
            
            # Add filter info if active
            active_filters = []
            if self.level_var.get() != "ALL":
                active_filters.append(f"Level: {self.level_var.get()}")
            if self.component_var.get() != "ALL":
                active_filters.append(f"Component: {self.component_var.get()}")
            if self.search_var.get():
                active_filters.append(f"Search: '{self.search_var.get()}'")
            
            if active_filters:
                status_parts.append(f"Filters: {', '.join(active_filters)}")
            
            self.status_var.set(" | ".join(status_parts))
            
        except Exception as e:
            self.status_var.set(f"Status update error: {e}")
    
    def update_statistics_display(self):
        """Update the header statistics display"""
        try:
            stats = self.log_manager.get_log_statistics()
            
            # Format statistics
            total = stats['total_logs']
            by_level = stats.get('by_level', {})
            
            errors = by_level.get('ERROR', 0) + by_level.get('CRITICAL', 0)
            warnings = by_level.get('WARNING', 0)
            
            stats_text = f"Total: {total:,}"
            if errors > 0:
                stats_text += f" | Errors: {errors}"
            if warnings > 0:
                stats_text += f" | Warnings: {warnings}"
            
            # Add session info
            stats_text += f" | Session: {stats.get('session_id', 'Unknown')}"
            
            self.stats_label.configure(text=stats_text)
            
            # Update live indicator color based on recent activity
            current_count = self.log_manager.get_log_count()
            if current_count > self.last_log_count:
                self.live_indicator.configure(foreground='green')
            else:
                self.live_indicator.configure(foreground='gray')
                
        except Exception as e:
            self.stats_label.configure(text=f"Stats error: {e}")
    
    def format_time(self, timestamp: str) -> str:
        """Format timestamp for display"""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
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
            # Get recent logs with filters (limit to prevent hanging)
            level_filter = self.level_var.get() if self.level_var.get() != "ALL" else None
            component_filter = self.component_var.get() if self.component_var.get() != "ALL" else None
            
            logs = self.log_manager.get_recent_logs(
                limit=2000,  # Increased but still limited
                level_filter=level_filter,
                component_filter=component_filter
            )
            
            # Apply search filter
            search_term = self.search_var.get().lower()
            if search_term:
                logs = [log for log in logs if search_term in log.message.lower() or 
                       search_term in log.component.lower()]
            
            return logs
        except Exception as e:
            print(f"Error getting filtered logs: {e}")
            return []
    
    def show_log_details(self, event):
        """Show detailed log information"""
        if self._destroyed:
            return
        
        selection = self.log_tree.selection()
        if not selection:
            return
        
        item = selection[0]
        
        try:
            # Get log entry details from tree values
            values = self.log_tree.item(item)['values']
            if len(values) >= 4:
                time_str, level, component, message = values[:4]
                
                # Find matching log entry in recent logs
                logs = self.get_filtered_logs()
                for log in logs:
                    if (log.level == level and 
                        log.component == component and 
                        message.replace("...", "") in log.message):
                        self.show_log_detail_window(log)
                        break
        except Exception as e:
            messagebox.showerror("Error", f"Failed to show log details: {e}")
    
    def show_log_detail_window(self, log_entry: LogEntry):
        """Show detailed log information in a popup"""
        try:
            detail_window = tk.Toplevel(self.window)
            detail_window.title("Log Entry Details")
            detail_window.geometry("800x600")
            detail_window.transient(self.window)
            detail_window.grab_set()
            
            # Main frame
            main_frame = ttk.Frame(detail_window)
            main_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Create scrolled text widget
            from tkinter import scrolledtext
            text_widget = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD, font=('Consolas', 10))
            text_widget.pack(fill='both', expand=True, pady=(0, 10))
            
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
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill='x')
            
            ttk.Button(button_frame, text="Close", command=detail_window.destroy).pack(side='right')
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to show log details: {e}")
    
    def start_auto_refresh(self):
        """Start auto-refresh timer with thread safety"""
        def auto_refresh():
            if self._destroyed or not self.window or not self.window.winfo_exists():
                return
            
            try:
                # Only refresh if there are new logs and auto-refresh is enabled
                current_count = self.log_manager.get_log_count()
                if (current_count != self.last_log_count and 
                    self.auto_refresh_var.get() and 
                    not self._destroyed):
                    
                    self.schedule_gui_update(self.refresh_logs)
                
                # Schedule next refresh
                if self.auto_refresh_var.get() and not self._destroyed:
                    self._refresh_timer = self.window.after(self.refresh_interval, auto_refresh)
                    
            except Exception as e:
                print(f"Error in auto-refresh: {e}")
        
        if not self._destroyed:
            auto_refresh()
    
    def stop_auto_refresh(self):
        """Stop auto-refresh timer"""
        if self._refresh_timer:
            self.window.after_cancel(self._refresh_timer)
            self._refresh_timer = None
    
    # Thread-safe debug methods
    def generate_test_logs_safe(self):
        """Thread-safe test log generation"""
        def generate_logs():
            try:
                self.log_manager.test_logging()
            except Exception as e:
                print(f"Error generating test logs: {e}")
        
        # Run in background thread to prevent GUI blocking
        threading.Thread(target=generate_logs, daemon=True, name="TestLogGeneration").start()
    
    def test_all_log_levels_safe(self):
        """Thread-safe test all log levels - FIXED VERSION"""
        def test_levels():
            try:
                components = ["Test", "GUI", "Database", "Connection", "Operation", "Export"]
                levels = [
                    LogLevel.DEBUG,     # Fixed: Use imported LogLevel, not self.log_manager.LogLevel
                    LogLevel.INFO, 
                    LogLevel.WARNING,
                    LogLevel.ERROR,
                    LogLevel.CRITICAL
                ]
                
                for i, component in enumerate(components):
                    for j, level in enumerate(levels):
                        message = f"Test {level.value} message from {component} component"
                        details = {"test_number": i * len(levels) + j, "component": component, "level": level.value}
                        self.log_manager.log(level, component, message, details)
                        
                        # Small delay to prevent overwhelming the system
                        time.sleep(0.01)
                        
            except Exception as e:
                print(f"Error testing log levels: {e}")
                # Log the error using the correct LogLevel import
                self.log_manager.log(LogLevel.ERROR, "TestLogLevels", f"Error in test: {e}")
        
        # Run in background thread
        threading.Thread(target=test_levels, daemon=True, name="TestLogLevels").start()
    
    def show_log_statistics_safe(self):
        """Thread-safe log statistics display"""
        def show_stats():
            try:
                stats = self.log_manager.get_log_statistics()
                
                # Create statistics window
                stats_window = tk.Toplevel(self.window)
                stats_window.title("Log Statistics")
                stats_window.geometry("500x400")
                stats_window.transient(self.window)
                
                # Create text widget
                from tkinter import scrolledtext
                text_widget = scrolledtext.ScrolledText(stats_window, wrap=tk.WORD, font=('Consolas', 10))
                text_widget.pack(fill='both', expand=True, padx=10, pady=10)
                
                # Format statistics
                stats_text = f"""Log Statistics (Thread-Safe Mode)
=====================================

Total Entries: {stats['total_logs']:,}
Session ID: {stats.get('session_id', 'Unknown')}
Current Level: {stats['current_level']}
Console Enabled: {stats['console_enabled']}
Debug Mode: {stats['debug_mode']}

Time Range:
  Oldest: {stats.get('oldest_entry', 'N/A')}
  Newest: {stats.get('newest_entry', 'N/A')}

By Level:
{'-' * 20}
"""
                
                for level, count in sorted(stats.get('by_level', {}).items()):
                    percentage = (count / stats['total_logs']) * 100 if stats['total_logs'] > 0 else 0
                    stats_text += f"{level:10}: {count:6,} ({percentage:5.1f}%)\n"
                
                stats_text += f"\nBy Component:\n{'-' * 20}\n"
                
                for component, count in sorted(stats.get('by_component', {}).items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / stats['total_logs']) * 100 if stats['total_logs'] > 0 else 0
                    stats_text += f"{component:15}: {count:6,} ({percentage:5.1f}%)\n"
                
                text_widget.insert('1.0', stats_text)
                text_widget.configure(state='disabled')
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to show statistics: {e}")
        
        self.schedule_gui_update(show_stats)


# Backward compatibility class that uses the thread-safe version
class LogViewerWindow(LogViewerWindow):
    """Backward compatibility wrapper for the thread-safe log viewer"""
    pass


class LogStatsWindow:
    """Thread-safe log statistics window"""
    
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
        self.window.title("Log Statistics & Analysis (Thread-Safe)")
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
        
        ttk.Label(header_frame, text="Log Statistics & Analysis (Thread-Safe)", 
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
        """Update statistics display with comprehensive analysis"""
        if self._destroyed:
            return
        
        def update_in_background():
            """Update stats in background thread to prevent hanging"""
            try:
                stats = self.log_manager.get_log_statistics()
                logs = self.log_manager.get_recent_logs(limit=1000)  # Limit to prevent hanging
                
                if not logs:
                    stats_text = "No log entries available."
                else:
                    stats_text = self.generate_comprehensive_stats(stats, logs)
                
                # Schedule GUI update
                def update_display():
                    if not self._destroyed:
                        self.stats_text.delete('1.0', tk.END)
                        self.stats_text.insert('1.0', stats_text)
                
                if not self._destroyed and self.window and self.window.winfo_exists():
                    self.window.after(0, update_display)
                
            except Exception as e:
                error_text = f"Error generating statistics: {e}"
                def show_error():
                    if not self._destroyed:
                        self.stats_text.delete('1.0', tk.END)
                        self.stats_text.insert('1.0', error_text)
                
                if not self._destroyed and self.window and self.window.winfo_exists():
                    self.window.after(0, show_error)
        
        # Run stats generation in background to prevent GUI blocking
        threading.Thread(target=update_in_background, daemon=True, name="StatsUpdate").start()
    
    def generate_comprehensive_stats(self, stats: dict, logs: List[LogEntry]) -> str:
        """Generate comprehensive statistics report"""
        total_logs = len(logs)
        
        # Time analysis
        if logs:
            oldest = logs[-1].timestamp
            newest = logs[0].timestamp
            try:
                oldest_dt = datetime.fromisoformat(oldest.replace('Z', '+00:00'))
                newest_dt = datetime.fromisoformat(newest.replace('Z', '+00:00'))
                duration = newest_dt - oldest_dt
                duration_str = str(duration).split('.')[0]  # Remove microseconds
            except:
                duration_str = "Unknown"
        else:
            oldest = newest = duration_str = "N/A"
        
        # Level analysis
        by_level = stats.get('by_level', {})
        by_component = stats.get('by_component', {})
        
        # Error analysis
        error_logs = [log for log in logs if log.level in ['ERROR', 'CRITICAL']]
        warning_logs = [log for log in logs if log.level == 'WARNING']
        
        # Recent activity (last 50 entries)
        recent_activity = logs[:50]
        recent_errors = [log for log in recent_activity if log.level in ['ERROR', 'CRITICAL']]
        
        # Generate report
        report = f"""Log Statistics & Analysis Report (Thread-Safe Mode)
==================================================

Session Information:
  Session ID: {stats.get('session_id', 'Unknown')}
  Current Log Level: {stats['current_level']}
  Debug Mode: {'Enabled' if stats['debug_mode'] else 'Disabled'}
  Console Logging: {'Enabled' if stats['console_enabled'] else 'Disabled'}
  Thread-Safe Mode: Enabled

Data Summary:
  Total Entries: {total_logs:,} (limited to recent 1000 for performance)
  Time Span: {duration_str}
  Oldest Entry: {oldest}
  Newest Entry: {newest}

Level Distribution:
{'-' * 30}
"""
        
        for level, count in sorted(by_level.items()):
            percentage = (count / total_logs) * 100 if total_logs > 0 else 0
            report += f"  {level:10}: {count:6,} ({percentage:5.1f}%)\n"
        
        report += f"\nComponent Activity:\n{'-' * 30}\n"
        
        # Limit component display to prevent huge output
        sorted_components = sorted(by_component.items(), key=lambda x: x[1], reverse=True)[:20]
        for component, count in sorted_components:
            percentage = (count / total_logs) * 100 if total_logs > 0 else 0
            report += f"  {component:15}: {count:6,} ({percentage:5.1f}%)\n"
        
        if len(by_component) > 20:
            report += f"  ... and {len(by_component) - 20} more components\n"
        
        report += f"\nError Analysis:\n{'-' * 30}\n"
        report += f"  Total Errors: {len(error_logs):,}\n"
        report += f"  Total Warnings: {len(warning_logs):,}\n"
        report += f"  Error Rate: {(len(error_logs) / total_logs * 100):.2f}%\n"
        
        if recent_errors:
            report += f"\nRecent Errors (Last 50 entries):\n{'-' * 30}\n"
            for error in recent_errors[:5]:  # Show last 5 errors
                time_str = error.timestamp.split('T')[1][:8] if 'T' in error.timestamp else error.timestamp[-8:]
                report += f"  [{time_str}] {error.level} - {error.component}: {error.message[:60]}...\n"
        
        # Health assessment
        error_rate = len(error_logs) / total_logs * 100 if total_logs > 0 else 0
        warning_rate = len(warning_logs) / total_logs * 100 if total_logs > 0 else 0
        
        if error_rate == 0 and warning_rate < 5:
            health = "Excellent"
        elif error_rate < 1 and warning_rate < 10:
            health = "Good"
        elif error_rate < 5 and warning_rate < 20:
            health = "Fair"
        else:
            health = "Poor"
        
        report += f"\nSystem Health Assessment:\n{'-' * 30}\n"
        report += f"  Overall Health: {health}\n"
        report += f"  Error Rate: {error_rate:.2f}%\n"
        report += f"  Warning Rate: {warning_rate:.2f}%\n"
        report += f"  Thread Safety: Enabled\n"
        
        return report