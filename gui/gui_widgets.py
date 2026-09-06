#!/usr/bin/env python3
# FILE: gui/gui_widgets.py
"""
GUI Widgets Module - Updated with Stop Action and Dashboard Update
Custom widgets for the FileMaker Sync Dashboard
"""

import tkinter as tk
from tkinter import ttk, scrolledtext
from datetime import datetime
from typing import Dict, Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from gui_logging import LogManager, LogEntry

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
        self.stat_frames = {}  # kept so show_delta_result()/update_overview() can relabel them
        stat_configs = [
            ('tables_done', 'Tables Migrated'),
            ('completion', 'Staging Match %')
        ]

        for i, (key, label) in enumerate(stat_configs):
            stat_frame = ttk.LabelFrame(stats_frame, text=label, padding=3)
            stat_frame.grid(row=0, column=i, padx=3, sticky='ew')
            self.stat_frames[key] = stat_frame

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
        
        # 'Target' is the internal column id (kept as-is -- it's referenced
        # positionally elsewhere, e.g. update_overview()'s values= tuple);
        # only the displayed heading text changes, to be honest about what
        # this actually shows (rat_migration staging counts, not the final
        # rat.* schema -- see the caption below the table).
        column_headings = {'Target': 'Staging'}
        for col in columns:
            self.table_tree.heading(col, text=column_headings.get(col, col))
        
        # Scrollbar for treeview (only show when needed)
        scrollbar = ttk.Scrollbar(self.table_frame, orient='vertical', command=self.table_tree.yview)
        self.table_tree.configure(yscrollcommand=scrollbar.set)
        
        self.table_tree.pack(side='left', fill='both', expand=True)
        # Don't pack scrollbar initially - will show only when needed

        ttk.Label(
            self, text="Reflects the last extract's staging snapshot -- Delta Sync intentionally narrows "
                       "this to just the changed rows. See the summary above for what actually happened.",
            font=('Arial', 8), foreground='gray', wraplength=460, justify='left',
        ).pack(fill='x', padx=4, pady=(2, 0))

    def update_overview(self, data: Dict[str, Any]):
        """Update the overview with new data - AUTO-SIZING VERSION"""
        try:
            # Restore the normal labels in case show_delta_result() last relabeled them.
            self.stat_frames['tables_done'].configure(text='Tables Migrated')
            self.stat_frames['completion'].configure(text='Staging Match %')

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

    def show_delta_result(self, data: Dict[str, Any]):
        """Populate with a Delta Sync-specific breakdown instead of the
        source-vs-staging percentage view (which doesn't apply -- Delta Sync
        deliberately doesn't leave staging as a full mirror). Built entirely
        from run_incremental_sync.py's own already-parsed JSON summary, no
        fresh query needed: Delta Sync always touches a known, fixed set of
        tables -- ratcatalogue gets the actual delta, ratbuilders/ratroutes/
        ratcollections/prompts get a full refresh every run regardless, and
        ratcopyright/ratlabels aren't touched at all."""
        try:
            total_changed = data.get('new', 0) + data.get('changed', 0)
            verified = data.get('verified', 0)
            touched = 5 if total_changed or 'manifest_advanced' in data else 0

            self.stat_frames['tables_done'].configure(text='Tables Touched')
            self.stat_boxes['tables_done'].configure(text=f"{touched}/7")
            self.stat_frames['completion'].configure(text='Rows Changed')
            self.stat_boxes['completion'].configure(text=str(total_changed))

            for item in self.table_tree.get_children():
                self.table_tree.delete(item)
            self.table_tree.configure(height=7)

            cat_status = f"{total_changed} update(s)" if total_changed else "No changes"
            cat_progress = "✓ Success" if verified == total_changed else "⚠ Check log"
            self.table_tree.insert('', 'end', values=(
                'ratcatalogue', '—', str(verified), cat_status, cat_progress))
            for t in ('ratbuilders', 'ratroutes', 'ratcollections', 'prompts'):
                self.table_tree.insert('', 'end', values=(t, '—', '—', 'Refreshed (full)', '✓ Success'))
            for t in ('ratcopyright', 'ratlabels'):
                self.table_tree.insert('', 'end', values=(t, '—', '—', 'Not touched by Delta Sync', '—'))
        except Exception as e:
            print(f"Error showing delta result: {e}")

class Tooltip:
    """Minimal hover tooltip -- tkinter has no built-in widget for this.
    Shows a small borderless window near the cursor on <Enter>, destroys it
    on <Leave>. Standard pattern, no external dependency."""

    def __init__(self, widget, text: str):
        self.widget = widget
        self.text = text
        self.tip_window = None
        widget.bind('<Enter>', self.show)
        widget.bind('<Leave>', self.hide)

    def show(self, _event=None):
        if self.tip_window or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 5
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw, text=self.text, background='#ffffe0', relief='solid', borderwidth=1,
            font=('Arial', 9), padx=6, pady=3, wraplength=280, justify='left',
        )
        label.pack()

    def hide(self, _event=None):
        if self.tip_window:
            self.tip_window.destroy()
            self.tip_window = None


class QuickActions(ttk.Frame):
    """Widget for quick action buttons - UPDATED WITH STOP ACTION AND UPDATE DASHBOARD"""

    BUTTON_TOOLTIPS = {
        'Full Sync': "Full extract + load of every table. Clears staging first (--del-data), so it "
                     "always ends up mirroring FileMaker exactly. Can take several minutes for the full "
                     "catalogue.",
        'Incremental Sync': "A plain re-extract of every table -- doesn't clear staging first or "
                             "regenerate DDL. Despite the name, this is NOT delta-driven -- see Delta "
                             "Sync for that.",
        'Delta Sync': "Scans FileMaker for records new or changed since the last sync, extracts and "
                      "loads only those rows, verifies them against the target, and advances the sync "
                      "manifest. The recommended way to pick up recent edits.",
        'Load to Target': "Runs the loader against whatever's currently in the staging tables -- doesn't "
                           "touch FileMaker. Useful if a load step failed partway after an extract.",
        'Export to Files': "Extracts FileMaker data to local DML/DDL files on disk, without touching the "
                            "target database.",
        'Export Images': "Extracts photo images from FileMaker to local files.",
        'Upload Images': "Uploads locally-exported images (run Export Images first) to the target's "
                          "Supabase Storage bucket. Needs the target connection, not FileMaker.",
        'Test Connections': "Checks that both the FileMaker source and the target database are reachable "
                             "right now.",
        'View Logs': "Opens the full activity log viewer -- search, filter, and sort every log entry "
                     "from this session.",
        'Update Dashboard': "Refreshes the Migration Overview and connection status above.",
        'Stop Action': "Cancels the currently running operation, if any. Only works for Full/Incremental/"
                        "Delta Sync, Load to Target, and Export operations -- not Test Connections or "
                        "Update Dashboard.",
    }

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
            ('Delta Sync', 'both_required'),
            ('Load to Target', 'target_only'),
            ('Export to Files', 'source_only'),
            ('Export Images', 'source_only'),
            ('Upload Images', 'target_only'),
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
                Tooltip(button1, self.BUTTON_TOOLTIPS.get(text, ''))

                # Special styling for Stop Action button
                if text == 'Stop Action':
                    button1.configure(state='disabled')  # Initially disabled
            
            # Second button in row (if exists)
            if i + 1 < len(button_configs):
                text, requirement = button_configs[i + 1]
                button2 = ttk.Button(row_frame, text=text, width=18)
                button2.pack(side='right', padx=(5, 0), fill='x', expand=True)
                self.action_buttons[text] = button2
                Tooltip(button2, self.BUTTON_TOOLTIPS.get(text, ''))

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
            'Delta Sync': 'normal' if fm_connected and target_connected else 'disabled',
            'Load to Target': 'normal' if target_connected else 'disabled',
            'Export to Files': 'normal' if fm_connected else 'disabled',
            'Export Images': 'normal' if fm_connected else 'disabled',
            'Upload Images': 'normal' if target_connected else 'disabled',
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


class LiveStatusPanel(ttk.Frame):
    """Simple, always-live scrolling log view for the Status tab -- deliberately
    no search/filter/sort (that stays in LogViewerWindow's separate popup, which
    this doesn't replace), just a level filter (to cut volume) and a completed-
    operation duration summary. Just: what's happening right now, auto-scrolled."""

    MAX_LINES = 500
    LEVEL_ORDER = {'DEBUG': 10, 'INFO': 20, 'WARNING': 30, 'ERROR': 40, 'CRITICAL': 50}
    FILTER_OPTIONS = {'All': 0, 'Info+': 20, 'Warning+': 30, 'Errors only': 40}

    def __init__(self, parent, log_manager: 'LogManager', **kwargs):
        super().__init__(parent, **kwargs)
        self.log_manager = log_manager
        self._line_count = 0
        self._filter_threshold = self.FILTER_OPTIONS['Info+']  # default: hide DEBUG chatter
        self.create_widgets()
        self._load_history()
        self.log_manager.add_callback(self._on_new_entry)

    def create_widgets(self):
        header = ttk.Frame(self)
        header.pack(fill='x', pady=(0, 4))

        ttk.Label(header, text="Show:").pack(side='left')
        self.filter_var = tk.StringVar(value='Info+')
        filter_combo = ttk.Combobox(
            header, textvariable=self.filter_var, values=list(self.FILTER_OPTIONS.keys()),
            state='readonly', width=12,
        )
        filter_combo.pack(side='left', padx=(4, 0))
        filter_combo.bind('<<ComboboxSelected>>', self._on_filter_changed)

        self.summary_label = ttk.Label(header, text="", foreground='gray')
        self.summary_label.pack(side='right')

        self.text = scrolledtext.ScrolledText(
            self, wrap=tk.WORD, state='disabled', font=('Consolas', 9),
        )
        self.text.pack(fill='both', expand=True)

        # Same palette as LogViewerWindow's log_tree tag_configure(), for visual
        # consistency between the popup and this panel.
        self.text.tag_configure('ERROR', background='#ffcccc')
        self.text.tag_configure('CRITICAL', background='#ff9999')
        self.text.tag_configure('WARNING', background='#ffffcc')
        self.text.tag_configure('DEBUG', foreground='gray')
        self.text.tag_configure('INFO', foreground='black')

    def _on_filter_changed(self, _event=None):
        self._filter_threshold = self.FILTER_OPTIONS[self.filter_var.get()]
        self.text.configure(state='normal')
        self.text.delete('1.0', 'end')
        self.text.configure(state='disabled')
        self._line_count = 0
        self._load_history()

    def _load_history(self):
        # get_recent_logs() returns newest-first; reverse for chronological append.
        for entry in reversed(self.log_manager.get_recent_logs(limit=self.MAX_LINES)):
            self._append(entry)

    def _on_new_entry(self, entry: 'LogEntry'):
        """Called from whatever thread produced the log entry -- marshal the
        actual Text-widget mutation onto the Tk main loop, same principle
        LogViewerWindow uses for its own callback. Catches broadly, not just
        tk.TclError: this runs as a LogManager callback, and any exception
        that escapes here propagates into LogManager's own error handling --
        which must never happen (see gui_logging.py's _add_log_entry/
        _notify_callbacks for why that path is deadlock-sensitive)."""
        try:
            self.after(0, lambda: self._append(entry))
        except Exception:
            pass  # widget destroyed, or a Tk threading edge case -- never let
                   # this propagate back into LogManager's callback chain

    def _append(self, entry: 'LogEntry'):
        if self.LEVEL_ORDER.get(entry.level, 20) < self._filter_threshold:
            return
        try:
            self.text.configure(state='normal')
            timestamp = entry.timestamp.split('T')[-1].split('.')[0]  # HH:MM:SS
            self.text.insert('end', f"{timestamp} [{entry.level}] {entry.message}\n", entry.level)
            self._line_count += 1
            if self._line_count > self.MAX_LINES:
                self.text.delete('1.0', '2.0')
                self._line_count -= 1
            self.text.see('end')
            self.text.configure(state='disabled')
        except tk.TclError:
            pass  # widget destroyed (window closing) -- nothing to update

    @staticmethod
    def _format_duration(duration: float) -> str:
        if duration >= 60:
            return f"{int(duration // 60)}m {duration % 60:.0f}s"
        return f"{duration:.1f}s"

    def set_running(self, operation: str):
        """Called when an operation starts -- see FileMakerSyncGUI.on_operation_status_safe()."""
        try:
            self.summary_label.configure(
                text=f"Running: {operation.replace('_', ' ').title()}...", foreground='#1a6fbd')
        except tk.TclError:
            pass

    def update_summary(self, operation: str, result: str, duration: float, data: Optional[Dict] = None):
        """Called when an operation completes -- see FileMakerSyncGUI.on_operation_status_safe().

        data: the operation's parsed JSON result, if any (e.g. run_incremental_sync.py's
        summary for delta_sync). Only used when it's shaped like that summary
        (has 'manifest_advanced') -- a cheap, specific-enough check that won't
        misfire on Test Connections/Migration Status's differently-shaped JSON.
        Every other operation falls back to the plain duration-only text.
        """
        label = operation.replace('_', ' ').title()
        duration_text = self._format_duration(duration)
        detail = ""
        if data and 'manifest_advanced' in data:
            if data.get('delta', 0) == 0:
                detail = " — nothing to do"
            else:
                detail = f" — {data.get('real_changes', 0)} changed, {data.get('verified', 0)} verified"
        try:
            if result == 'success':
                self.summary_label.configure(
                    text=f"✓ {label} completed in {duration_text}{detail}", foreground='#1a7f37')
            else:
                self.summary_label.configure(
                    text=f"✗ {label} failed after {duration_text}{detail}", foreground='#c53030')
        except tk.TclError:
            pass