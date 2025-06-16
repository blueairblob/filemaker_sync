#!/usr/bin/env python3
# FILE: gui/gui_operations.py
"""
GUI Operations Module
"""

import subprocess
import threading
import json
import sys
import time
import queue
from typing import Dict, Any, List, Callable, Optional
from pathlib import Path
from enum import Enum
import logging

from gui_logging import LogManager, LogLevel, PerformanceLogger

class OperationState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    FAILED = "failed"

class OperationManager:
    """Operation manager"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
        
        # State management
        self._state_lock = threading.RLock()  # Use RLock to prevent deadlocks
        self._operation_state = OperationState.IDLE
        self._current_operation = None
        self._current_process = None
        self._operation_thread = None
        
        # Callback management
        self._callback_lock = threading.Lock()
        self._operation_callbacks = []
        
        # Result queue for communication
        self._result_queue = queue.Queue(maxsize=10)
        
        # Shutdown flag
        self._shutdown_requested = threading.Event()
        
        # Timeout settings
        self.command_timeout = 120  # 2 minutes max for any command
        self.connection_timeout = 30  # 30 seconds for connection tests
        
        self.log_manager.log(LogLevel.INFO, "OperationManager", "Operation manager initialized")
    
    @property
    def is_operation_running(self) -> bool:
        """Check for running operations"""
        with self._state_lock:
            return self._operation_state == OperationState.RUNNING
    
    def add_operation_callback(self, callback: Callable):
        """Operation callback"""
        with self._callback_lock:
            self._operation_callbacks.append(callback)
        self.log_manager.log(LogLevel.DEBUG, "OperationManager", f"Added callback (total: {len(self._operation_callbacks)})")
    
    def remove_operation_callback(self, callback: Callable):
        """Remove operation callback"""
        with self._callback_lock:
            if callback in self._operation_callbacks:
                self._operation_callbacks.remove(callback)
    
    def _notify_callbacks_safe(self, status: str, operation: str, result: Any = None):
        """Notify callbacks"""
        # Get callbacks under lock
        with self._callback_lock:
            callbacks_to_call = self._operation_callbacks.copy()
        
        # Call callbacks outside the lock to prevent deadlocks
        for callback in callbacks_to_call:
            try:
                # Use queue to defer callback to main thread if needed
                callback(status, operation, result)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "OperationManager", f"Error in callback: {e}")
    
    def run_python_command(self, cmd_args: List[str], description: str, timeout: Optional[int] = None) -> Dict[str, Any]:
        """Run Python command with proper timeout and thread safety"""
        if timeout is None:
            timeout = self.connection_timeout if 'info-only' in cmd_args else self.command_timeout
        
        self.log_manager.log(LogLevel.INFO, "Command", f"Starting: {description} (timeout: {timeout}s)")
        self.log_manager.log(LogLevel.DEBUG, "Command", f"Args: {' '.join(cmd_args)}")
        
        try:
            # Check if shutdown requested
            if self._shutdown_requested.is_set():
                return {'success': False, 'error': 'Shutdown requested'}
            
            # Check if script exists
            script_path = Path('filemaker_extract_refactored.py')
            if not script_path.exists():
                error_msg = 'filemaker_extract_refactored.py not found'
                self.log_manager.log(LogLevel.ERROR, "Command", error_msg)
                return {'success': False, 'error': error_msg}
            
            # Build command
            full_command = [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args
            self.log_manager.log(LogLevel.DEBUG, "Command", f"Executing: {' '.join(full_command)}")
            
            with PerformanceLogger(self.log_manager, "Command", description):
                # Use shorter timeout for connection tests
                actual_timeout = min(timeout, 60) if 'info-only' in cmd_args or 'migration-status' in cmd_args else timeout
                
                result = subprocess.run(
                    full_command,
                    capture_output=True,
                    text=True,
                    timeout=actual_timeout,
                    cwd=Path.cwd()
                )
                
                return self._process_command_result(result, description)
        
        except subprocess.TimeoutExpired:
            error_msg = f"Command timed out after {timeout}s: {description}"
            self.log_manager.log(LogLevel.ERROR, "Command", error_msg)
            return {'success': False, 'error': error_msg}
        except Exception as e:
            error_msg = f"Command exception: {description} - {str(e)}"
            self.log_manager.log(LogLevel.ERROR, "Command", error_msg)
            return {'success': False, 'error': str(e)}
    
    def _process_command_result(self, result: subprocess.CompletedProcess, description: str) -> Dict[str, Any]:
        """Process command result with enhanced error handling"""
        self.log_manager.log(LogLevel.DEBUG, "Command", f"Return code: {result.returncode}")
        
        # Log output (with length limits to prevent memory issues)
        if result.stdout:
            stdout_lines = result.stdout.split('\n')[:50]  # Limit to 50 lines
            for line in stdout_lines:
                if line.strip():
                    self.log_manager.log_subprocess_output("Command-Output", line)
        
        if result.stderr:
            stderr_lines = result.stderr.split('\n')[:20]  # Limit to 20 lines
            for line in stderr_lines:
                if line.strip():
                    self.log_manager.log(LogLevel.ERROR, "Command-Error", line)
        
        if result.returncode == 0:
            self.log_manager.log(LogLevel.INFO, "Command", f"✓ Completed: {description}")
            
            try:
                output = result.stdout.strip()
                if not output:
                    return {'success': True, 'data': None, 'message': 'Command completed successfully'}
                
                # Try to parse JSON
                json_data = self._extract_json_from_output(output)
                if json_data:
                    self.log_manager.log(LogLevel.DEBUG, "Command", f"Parsed JSON response")
                    return {'success': True, 'data': json_data}
                else:
                    return {'success': True, 'data': None, 'message': output[:500]}  # Limit message length
                    
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Command", f"Error processing output: {e}")
                return {'success': True, 'data': None, 'message': result.stdout[:500]}
        else:
            error_msg = (result.stderr or result.stdout or "Unknown error")[:500]
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command failed: {description} - {error_msg}")
            return {'success': False, 'error': error_msg}
    
    def _extract_json_from_output(self, output: str) -> Optional[Dict]:
        """Extract JSON from output with timeout protection"""
        try:
            lines = output.split('\n')
            
            # Limit processing to prevent hanging
            max_lines = min(len(lines), 1000)
            
            for i in range(max_lines):
                line = lines[i].strip()
                if line.startswith('{'):
                    json_lines = [line]
                    brace_count = line.count('{') - line.count('}')
                    
                    # Limit JSON collection to prevent infinite loops
                    for j in range(i + 1, min(i + 100, max_lines)):
                        next_line = lines[j].strip()
                        if next_line:
                            json_lines.append(next_line)
                            brace_count += next_line.count('{') - next_line.count('}')
                            if brace_count == 0:
                                break
                    
                    json_content = '\n'.join(json_lines)
                    
                    # Limit JSON size to prevent memory issues
                    if len(json_content) > 100000:  # 100KB limit
                        self.log_manager.log(LogLevel.WARNING, "Command", "JSON response too large, truncating")
                        continue
                    
                    try:
                        return json.loads(json_content)
                    except json.JSONDecodeError:
                        continue
            
            return None
            
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Error extracting JSON: {e}")
            return None
    
    def run_operation_async(self, operation: str, on_complete: Optional[Callable] = None) -> bool:
        """Run operation asynchronously"""
        # Check if already running
        with self._state_lock:
            if self._operation_state == OperationState.RUNNING:
                self.log_manager.log(LogLevel.WARNING, "Operation", f"Cannot start {operation}: Another operation is running")
                return False
            
            self._operation_state = OperationState.RUNNING
            self._current_operation = operation
        
        # Operation commands
        operation_commands = {
            'full_sync': ['--db-exp', '--ddl', '--dml'],
            'incremental_sync': ['--db-exp', '--dml'],
            'export_files': ['--fn-exp', '--ddl', '--dml'],
            'export_images': ['--get-images'],
            'test_connections': ['--info-only'],
            'migration_status': ['--migration-status', '--json']
        }
        
        if operation not in operation_commands:
            with self._state_lock:
                self._operation_state = OperationState.IDLE
                self._current_operation = None
            self.log_manager.log(LogLevel.ERROR, "Operation", f"Unknown operation: {operation}")
            return False
        
        self.log_manager.log(LogLevel.INFO, "Operation", f"🚀 Starting: {operation}")
        
        # Notify start (safely)
        try:
            self._notify_callbacks_safe('start', operation)
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in start notification: {e}")
        
        def run_operation_thread():
            """Thread function for running operations"""
            result = 'error'
            error_msg = None
            
            try:
                cmd = operation_commands[operation]
                self.log_manager.log(LogLevel.INFO, "Operation", f"Executing: {operation}")
                
                # Choose timeout based on operation type
                if operation in ['full_sync', 'export_files']:
                    timeout = 300  # 5 minutes for long operations
                elif operation == 'export_images':
                    timeout = 600  # 10 minutes for image export
                else:
                    timeout = 60   # 1 minute for quick operations
                
                command_result = self.run_python_command(cmd, f"{operation.replace('_', ' ').title()}", timeout)
                
                if command_result['success']:
                    self.log_manager.log(LogLevel.INFO, "Operation", f"✓ {operation} completed successfully")
                    result = 'success'
                else:
                    error_msg = command_result.get('error', 'Unknown error')
                    self.log_manager.log(LogLevel.ERROR, "Operation", f"✗ {operation} failed: {error_msg}")
                    result = 'failure'
                
            except Exception as e:
                error_msg = str(e)
                self.log_manager.log(LogLevel.ERROR, "Operation", f"Exception in {operation}: {e}")
                result = 'error'
            
            finally:
                # Clean up state
                with self._state_lock:
                    self._operation_state = OperationState.COMPLETED if result == 'success' else OperationState.FAILED
                    self._current_operation = None
                    self._operation_thread = None
                
                self.log_manager.log(LogLevel.INFO, "Operation", f"🏁 {operation} finished: {result}")
                
                # Notify completion (safely)
                try:
                    self._notify_callbacks_safe('complete', operation, {'result': result, 'error': error_msg})
                except Exception as e:
                    self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in completion notification: {e}")
                
                # Call completion callback if provided
                if on_complete:
                    try:
                        on_complete(result)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in completion callback: {e}")
                
                # Reset state to idle after a brief delay
                def reset_to_idle():
                    with self._state_lock:
                        if self._operation_state in [OperationState.COMPLETED, OperationState.FAILED]:
                            self._operation_state = OperationState.IDLE
                
                # Schedule state reset (this will be called from the thread)
                threading.Timer(2.0, reset_to_idle).start()
        
        # Start operation thread
        self._operation_thread = threading.Thread(target=run_operation_thread, daemon=True, name=f"Operation-{operation}")
        self._operation_thread.start()
        
        return True
    
    def cancel_current_operation(self) -> bool:
        """Cancel the currently running operation"""
        with self._state_lock:
            if self._operation_state != OperationState.RUNNING:
                return False
            
            self._operation_state = OperationState.STOPPING
            operation = self._current_operation
        
        self.log_manager.log(LogLevel.WARNING, "Operation", f"Cancelling operation: {operation}")
        
        # Try to terminate current process
        if self._current_process:
            try:
                self._current_process.terminate()
                self._current_process.wait(timeout=5)
            except:
                try:
                    self._current_process.kill()
                except:
                    pass
        
        # Wait for thread to finish (with timeout)
        if self._operation_thread and self._operation_thread.is_alive():
            self._operation_thread.join(timeout=10)
        
        # Force state reset
        with self._state_lock:
            self._operation_state = OperationState.IDLE
            self._current_operation = None
            self._operation_thread = None
        
        self.log_manager.log(LogLevel.INFO, "Operation", "Operation cancelled")
        return True
    
    def shutdown(self):
        """Shutdown the operation manager"""
        self.log_manager.log(LogLevel.INFO, "OperationManager", "Shutting down operation manager")
        
        # Set shutdown flag
        self._shutdown_requested.set()
        
        # Cancel any running operation
        self.cancel_current_operation()
        
        # Clear callbacks
        with self._callback_lock:
            self._operation_callbacks.clear()


class ConnectionTester:
    """Connection tester that prevents hanging"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        
        # Connection status
        self._status_lock = threading.Lock()
        self._connection_status = {
            'filemaker': {'connected': False, 'message': 'Not tested', 'last_test': None},
            'target': {'connected': False, 'message': 'Not tested', 'last_test': None}
        }
        
        # Prevent concurrent tests
        self._test_locks = {
            'filemaker': threading.Lock(),
            'target': threading.Lock()
        }
        
        self.log_manager.log(LogLevel.INFO, "ConnectionTester", "Connection tester initialized")
    
    @property
    def connection_status(self) -> Dict[str, Dict[str, Any]]:
        """Get copy of connection status"""
        with self._status_lock:
            return {
                'filemaker': self._connection_status['filemaker'].copy(),
                'target': self._connection_status['target'].copy()
            }
    
    def _update_connection_status(self, connection_type: str, connected: bool, message: str):
        """Update connection status"""
        with self._status_lock:
            self._connection_status[connection_type].update({
                'connected': connected,
                'message': message,
                'last_test': time.time()
            })
    
    def test_filemaker_connection(self, callback: Optional[Callable] = None):
        """Test FileMaker connection"""
        # Prevent concurrent tests
        if not self._test_locks['filemaker'].acquire(blocking=False):
            self.log_manager.log(LogLevel.WARNING, "Connection", "FileMaker test already in progress")
            return
        
        try:
            self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing FileMaker connection...")
            
            def test_connection_thread():
                try:
                    result = self.operation_manager.run_python_command(
                        ['--info-only', '--json'], 
                        "FileMaker connection test",
                        timeout=30  # 30 second timeout
                    )
                    
                    self._process_connection_result(result, 'filemaker', callback)
                    
                except Exception as e:
                    error_msg = f"Exception during FileMaker test: {e}"
                    self._update_connection_status('filemaker', False, error_msg)
                    self.log_manager.log(LogLevel.ERROR, "Connection", error_msg)
                    
                    if callback:
                        try:
                            callback('filemaker', self.connection_status['filemaker'])
                        except Exception as cb_e:
                            self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in callback: {cb_e}")
                finally:
                    self._test_locks['filemaker'].release()
            
            # Start test thread
            test_thread = threading.Thread(target=test_connection_thread, daemon=True, name="FM-Connection-Test")
            test_thread.start()
            
        except Exception as e:
            self._test_locks['filemaker'].release()
            raise
    
    def test_target_connection(self, callback: Optional[Callable] = None):
        """Test target connection"""
        # Prevent concurrent tests
        if not self._test_locks['target'].acquire(blocking=False):
            self.log_manager.log(LogLevel.WARNING, "Connection", "Target test already in progress")
            return
        
        try:
            self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing target connection...")
            
            def test_connection_thread():
                try:
                    result = self.operation_manager.run_python_command(
                        ['--info-only', '--json'], 
                        "Target connection test",
                        timeout=30  # 30 second timeout
                    )
                    
                    self._process_connection_result(result, 'target', callback)
                    
                except Exception as e:
                    error_msg = f"Exception during target test: {e}"
                    self._update_connection_status('target', False, error_msg)
                    self.log_manager.log(LogLevel.ERROR, "Connection", error_msg)
                    
                    if callback:
                        try:
                            callback('target', self.connection_status['target'])
                        except Exception as cb_e:
                            self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in callback: {cb_e}")
                finally:
                    self._test_locks['target'].release()
            
            # Start test thread
            test_thread = threading.Thread(target=test_connection_thread, daemon=True, name="Target-Connection-Test")
            test_thread.start()
            
        except Exception as e:
            self._test_locks['target'].release()
            raise
    
    def _process_connection_result(self, result: Dict[str, Any], connection_type: str, callback: Optional[Callable] = None):
        """Process connection test result"""
        try:
            if result['success']:
                data = result.get('data')
                if data:
                    conn_status = data.get('connection_status', {})
                    status_info = conn_status.get(connection_type, {})
                    
                    if status_info.get('connected', False):
                        if connection_type == 'filemaker':
                            message = f"Connected via DSN: {data.get('source_dsn', 'unknown')}"
                        else:
                            message = f"Connected to {data.get('target_database', 'target')}"
                        
                        self._update_connection_status(connection_type, True, message)
                        self.log_manager.log(LogLevel.INFO, "Connection", f"✓ {connection_type.title()} connection successful")
                    else:
                        error_msg = status_info.get('message', 'Connection failed')
                        self._update_connection_status(connection_type, False, error_msg)
                        self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ {connection_type.title()} connection failed: {error_msg}")
                else:
                    error_msg = result.get('message', result.get('error', 'No response data'))
                    self._update_connection_status(connection_type, False, error_msg)
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ {connection_type.title()} test failed: {error_msg}")
            else:
                error_msg = result.get('error', 'Connection test failed')
                self._update_connection_status(connection_type, False, error_msg)
                self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ {connection_type.title()} test failed: {error_msg}")
            
            # Call callback with current status
            if callback:
                current_status = self.connection_status[connection_type]
                callback(connection_type, current_status)
                
        except Exception as e:
            error_msg = f"Error processing {connection_type} result: {e}"
            self._update_connection_status(connection_type, False, error_msg)
            self.log_manager.log(LogLevel.ERROR, "Connection", error_msg)
            
            if callback:
                try:
                    callback(connection_type, self.connection_status[connection_type])
                except Exception as cb_e:
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in callback: {cb_e}")
    
    def test_all_connections(self, callback: Optional[Callable] = None):
        """Test both connections with proper sequencing"""
        self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing all connections...")
        
        # Counter to track completion
        completion_counter = {'count': 0}
        completion_lock = threading.Lock()
        
        def on_test_complete(connection_type, status):
            with completion_lock:
                completion_counter['count'] += 1
                
                # Call callback for this connection
                if callback:
                    try:
                        callback(connection_type, status)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in callback: {e}")
                
                # If both tests completed, log summary
                if completion_counter['count'] >= 2:
                    self.log_manager.log(LogLevel.INFO, "Connection", "✓ All connection tests completed")
        
        # Start both tests concurrently (they have their own locking)
        self.test_filemaker_connection(on_test_complete)
        self.test_target_connection(on_test_complete)


class StatusManager:
    """Thread-safe status manager"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        
        # Data storage
        self._data_lock = threading.Lock()
        self._migration_data = None
        self._last_refresh = None
        
        # Prevent concurrent refreshes
        self._refresh_lock = threading.Lock()
        
        self.log_manager.log(LogLevel.INFO, "StatusManager", "Status manager initialized")
    
    @property
    def migration_data(self) -> Optional[Dict[str, Any]]:
        """Get copy of migration data"""
        with self._data_lock:
            return self._migration_data.copy() if self._migration_data else None
    
    def refresh_migration_status(self, callback: Optional[Callable] = None):
        """Refresh migration status"""
        # Prevent concurrent refreshes
        if not self._refresh_lock.acquire(blocking=False):
            self.log_manager.log(LogLevel.WARNING, "Status", "Status refresh already in progress")
            return
        
        try:
            self.log_manager.log(LogLevel.INFO, "Status", "🔄 Refreshing migration status...")
            
            def refresh_thread():
                try:
                    result = self.operation_manager.run_python_command(
                        ['--migration-status', '--json'], 
                        "Migration status refresh",
                        timeout=45  # 45 second timeout
                    )
                    
                    if result['success'] and result.get('data'):
                        with self._data_lock:
                            self._migration_data = result['data']
                            self._last_refresh = time.time()
                        
                        # Log summary
                        summary = result['data'].get('summary', {})
                        total_tables = summary.get('total_tables', 0)
                        migrated_tables = summary.get('tables_migrated', 0)
                        source_rows = summary.get('source_total_rows', 0)
                        target_rows = summary.get('target_total_rows', 0)
                        
                        completion_pct = (target_rows / source_rows * 100) if source_rows > 0 else 0
                        
                        self.log_manager.log(LogLevel.INFO, "Status", 
                                           f"✓ Status updated - Tables: {migrated_tables}/{total_tables}, "
                                           f"Rows: {target_rows:,}/{source_rows:,} ({completion_pct:.1f}%)")
                        
                        if callback:
                            callback(True, result['data'])
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        self.log_manager.log(LogLevel.ERROR, "Status", f"✗ Status refresh failed: {error_msg}")
                        
                        if callback:
                            callback(False, error_msg)
                
                except Exception as e:
                    error_msg = f"Exception during status refresh: {e}"
                    self.log_manager.log(LogLevel.ERROR, "Status", error_msg)
                    
                    if callback:
                        callback(False, error_msg)
                
                finally:
                    self._refresh_lock.release()
            
            # Start refresh thread
            refresh_thread_obj = threading.Thread(target=refresh_thread, daemon=True, name="Status-Refresh")
            refresh_thread_obj.start()
            
        except Exception as e:
            self._refresh_lock.release()
            raise