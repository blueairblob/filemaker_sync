#!/usr/bin/env python3
"""
GUI Operations Module - FIXED VERSION
Enhanced operation execution with debug support, cancellation, and multi-threading
"""

import subprocess
import threading
import json
import sys
import time
import signal
import os
from typing import Dict, Any, List, Callable, Optional
from gui_logging import LogManager, LogLevel

class OperationManager:
    """ENHANCED operation manager with cancellation and debug support"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
        self.is_operation_running = False
        self.current_operation = None
        self.current_process = None
        self.operation_callbacks: List[Callable] = []
        
        # Threading support
        self.operation_thread = None
        self.cancel_requested = False
    
    def add_operation_callback(self, callback: Callable):
        """Add callback for operation status updates"""
        self.operation_callbacks.append(callback)
    
    def run_python_command(self, cmd_args: List[str], description: str, timeout: int = 300) -> Dict[str, Any]:
        """ENHANCED command execution with timeout and cancellation support"""
        self.log_manager.log(LogLevel.INFO, "Command", f"Running: {description}", {"args": cmd_args})
        
        try:
            # Store the process for potential cancellation
            self.current_process = subprocess.Popen(
                [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Read output in real-time
            output_lines = []
            error_lines = []
            
            # Use threading to read output and check for cancellation
            start_time = time.time()
            while self.current_process.poll() is None:
                if self.cancel_requested:
                    self.log_manager.log(LogLevel.WARNING, "Command", f"Cancelling operation: {description}")
                    self._terminate_process()
                    return {'success': False, 'error': 'Operation cancelled by user', 'cancelled': True}
                
                # Check for timeout
                if time.time() - start_time > timeout:
                    self.log_manager.log(LogLevel.ERROR, "Command", f"Operation timed out: {description}")
                    self._terminate_process()
                    return {'success': False, 'error': f'Operation timed out after {timeout} seconds', 'timeout': True}
                
                # Read available output
                try:
                    line = self.current_process.stdout.readline()
                    if line:
                        output_lines.append(line.strip())
                        # Log real-time output
                        self.log_manager.log(LogLevel.INFO, "Operation", line.strip())
                except:
                    pass
                
                time.sleep(0.1)  # Small delay to prevent busy waiting
            
            # Get any remaining output
            remaining_output, remaining_error = self.current_process.communicate()
            if remaining_output:
                output_lines.extend(remaining_output.strip().split('\n'))
            if remaining_error:
                error_lines.extend(remaining_error.strip().split('\n'))
            
            # Process results
            return_code = self.current_process.returncode
            full_output = '\n'.join(output_lines)
            full_error = '\n'.join(error_lines)
            
            if return_code == 0:
                try:
                    # Try to parse JSON from output
                    json_data = None
                    for line in output_lines:
                        if line.strip().startswith('{'):
                            try:
                                json_data = json.loads(line)
                                break
                            except json.JSONDecodeError:
                                continue
                    
                    self.log_manager.log(LogLevel.INFO, "Command", f"Command succeeded: {description}")
                    return {'success': True, 'data': json_data, 'output': full_output}
                except Exception as e:
                    return {'success': True, 'data': None, 'output': full_output}
            else:
                error_msg = full_error or full_output or f"Process exited with code {return_code}"
                self.log_manager.log(LogLevel.ERROR, "Command", f"Command failed: {description}", {"error": error_msg})
                return {'success': False, 'error': error_msg, 'return_code': return_code}
                
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command exception: {description}", {"exception": str(e)})
            return {'success': False, 'error': str(e)}
        finally:
            self.current_process = None
    
    def _terminate_process(self):
        """Safely terminate the current process"""
        if self.current_process:
            try:
                if sys.platform == 'win32':
                    # On Windows, use terminate
                    self.current_process.terminate()
                else:
                    # On Unix-like systems, try SIGTERM first
                    self.current_process.send_signal(signal.SIGTERM)
                
                # Wait a bit for graceful shutdown
                try:
                    self.current_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if necessary
                    self.current_process.kill()
                    self.current_process.wait()
                    
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Command", f"Error terminating process: {e}")
    
    def run_operation_async(self, operation: str, debug_args: List[str] = None, on_complete: Callable = None):
        """ENHANCED async operation with debug support and cancellation"""
        if self.is_operation_running:
            self.log_manager.log(LogLevel.WARNING, "Operation", "Operation already running")
            return False
        
        # Define operation commands with enhanced options
        operation_commands = {
            'full_sync': ['--db-exp', '--ddl', '--dml'],
            'incremental_sync': ['--db-exp', '--dml'],
            'export_files': ['--fn-exp', '--ddl', '--dml'],
            'export_images': ['--get-images']
        }
        
        if operation not in operation_commands:
            self.log_manager.log(LogLevel.ERROR, "Operation", f"Unknown operation: {operation}")
            return False
        
        self.is_operation_running = True
        self.current_operation = operation
        self.cancel_requested = False
        
        # Add debug arguments if provided
        cmd_args = operation_commands[operation].copy()
        if debug_args:
            cmd_args.extend(debug_args)
        
        # Notify callbacks of operation start
        for callback in self.operation_callbacks:
            try:
                callback('start', operation)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", f"Callback error: {e}")
        
        def run_op():
            try:
                self.log_manager.log(LogLevel.INFO, "Operation", 
                                   f"Started {operation.replace('_', ' ')} with args: {cmd_args}")
                
                # Determine timeout based on operation type
                timeout = {
                    'full_sync': 3600,  # 1 hour for full sync
                    'incremental_sync': 1800,  # 30 minutes for incremental
                    'export_files': 900,  # 15 minutes for file export
                    'export_images': 1800  # 30 minutes for image export
                }.get(operation, 600)  # Default 10 minutes
                
                # Run the operation
                result_data = self.run_python_command(cmd_args, operation.replace('_', ' '), timeout)
                
                if result_data['success']:
                    if result_data.get('cancelled'):
                        self.log_manager.log(LogLevel.WARNING, "Operation", 
                                           f"{operation.replace('_', ' ').title()} was cancelled")
                        result = 'cancelled'
                    else:
                        self.log_manager.log(LogLevel.INFO, "Operation", 
                                           f"{operation.replace('_', ' ').title()} completed successfully")
                        result = 'success'
                else:
                    if result_data.get('timeout'):
                        self.log_manager.log(LogLevel.ERROR, "Operation", 
                                           f"{operation.replace('_', ' ').title()} timed out")
                        result = 'timeout'
                    elif result_data.get('cancelled'):
                        result = 'cancelled'
                    else:
                        self.log_manager.log(LogLevel.ERROR, "Operation", 
                                           f"{operation.replace('_', ' ').title()} failed: {result_data.get('error', 'Unknown error')}")
                        result = 'failure'
                
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", 
                                   f"{operation.replace('_', ' ').title()} error: {e}")
                result = 'error'
            finally:
                self.is_operation_running = False
                self.current_operation = None
                self.cancel_requested = False
                
                # Notify callbacks of operation completion
                for callback in self.operation_callbacks:
                    try:
                        callback('complete', operation, result)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Operation", f"Callback error: {e}")
                
                if on_complete:
                    try:
                        on_complete(result)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Operation", f"Completion callback error: {e}")
        
        # Start operation in separate thread
        self.operation_thread = threading.Thread(target=run_op, daemon=True, name=f"Operation-{operation}")
        self.operation_thread.start()
        return True
    
    def cancel_current_operation(self) -> bool:
        """Cancel the currently running operation"""
        if not self.is_operation_running:
            return False
        
        self.log_manager.log(LogLevel.WARNING, "Operation", f"Cancellation requested for: {self.current_operation}")
        self.cancel_requested = True
        
        # Try to terminate the process
        if self.current_process:
            self._terminate_process()
        
        return True
    
    def get_operation_status(self) -> Dict[str, Any]:
        """Get current operation status"""
        return {
            'is_running': self.is_operation_running,
            'current_operation': self.current_operation,
            'cancel_requested': self.cancel_requested,
            'thread_alive': self.operation_thread.is_alive() if self.operation_thread else False
        }

class ConnectionTester:
    """ENHANCED connection tester with timeout and better error handling"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        
        self.connection_status = {
            'filemaker': {'connected': False, 'message': 'Not tested', 'last_test': None},
            'target': {'connected': False, 'message': 'Not tested', 'last_test': None}
        }
    
    def test_filemaker_connection(self, callback: Callable = None):
        """ENHANCED FileMaker connection test"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing FileMaker connection")
        
        def test_connection():
            start_time = time.time()
            result = self.operation_manager.run_python_command(
                ['--src-cnt', '--json', '--max-rows', '1'], 
                "FileMaker connection test",
                timeout=60  # 1 minute timeout for connection test
            )
            test_duration = time.time() - start_time
            
            if result['success'] and result.get('data'):
                data = result['data']
                if data.get('summary', {}).get('connection_error'):
                    self.connection_status['filemaker'] = {
                        'connected': False,
                        'message': data.get('error_detail', 'Connection failed'),
                        'last_test': time.time(),
                        'test_duration': test_duration
                    }
                    self.log_manager.log(LogLevel.ERROR, "Connection", "FileMaker connection failed", 
                                       {"error": data.get('error_detail'), "duration": test_duration})
                else:
                    table_count = data.get('summary', {}).get('total_tables', 0)
                    self.connection_status['filemaker'] = {
                        'connected': True,
                        'message': f"Connected via DSN: {data.get('dsn', 'unknown')} ({table_count} tables)",
                        'last_test': time.time(),
                        'test_duration': test_duration
                    }
                    self.log_manager.log(LogLevel.INFO, "Connection", "FileMaker connection successful",
                                       {"dsn": data.get('dsn'), "tables": table_count, "duration": test_duration})
            else:
                error_msg = result.get('error', 'Connection test failed')
                self.connection_status['filemaker'] = {
                    'connected': False,
                    'message': error_msg,
                    'last_test': time.time(),
                    'test_duration': test_duration
                }
                self.log_manager.log(LogLevel.ERROR, "Connection", f"FileMaker test failed: {error_msg}")
            
            if callback:
                callback('filemaker', self.connection_status['filemaker'])
        
        threading.Thread(target=test_connection, daemon=True, name="FM-Connection-Test").start()
    
    def test_target_connection(self, callback: Callable = None):
        """ENHANCED target database connection test"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing target database connection")
        
        def test_connection():
            start_time = time.time()
            result = self.operation_manager.run_python_command(
                ['--tgt-cnt', '--json', '--max-rows', '1'], 
                "Target connection test",
                timeout=60  # 1 minute timeout
            )
            test_duration = time.time() - start_time
            
            if result['success'] and result.get('data'):
                data = result['data']
                if data.get('summary', {}).get('connection_error'):
                    self.connection_status['target'] = {
                        'connected': False,
                        'message': data.get('error_detail', 'Connection failed'),
                        'last_test': time.time(),
                        'test_duration': test_duration
                    }
                    self.log_manager.log(LogLevel.ERROR, "Connection", "Target connection failed",
                                       {"error": data.get('error_detail'), "duration": test_duration})
                else:
                    schema = data.get('schema', 'unknown')
                    host = data.get('host', 'unknown')
                    self.connection_status['target'] = {
                        'connected': True,
                        'message': f"Connected to {data.get('database', 'target')} (Schema: {schema})",
                        'last_test': time.time(),
                        'test_duration': test_duration,
                        'host': host
                    }
                    self.log_manager.log(LogLevel.INFO, "Connection", "Target connection successful",
                                       {"database": data.get('database'), "schema": schema, "duration": test_duration})
            else:
                error_msg = result.get('error', 'Connection test failed')
                self.connection_status['target'] = {
                    'connected': False,
                    'message': error_msg,
                    'last_test': time.time(),
                    'test_duration': test_duration
                }
                self.log_manager.log(LogLevel.ERROR, "Connection", f"Target test failed: {error_msg}")
            
            if callback:
                callback('target', self.connection_status['target'])
        
        threading.Thread(target=test_connection, daemon=True, name="Target-Connection-Test").start()
    
    def test_all_connections(self, callback: Callable = None):
        """Test both connections with improved coordination"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing all connections")
        
        # Track completion of both tests
        completed_tests = {'filemaker': False, 'target': False}
        
        def on_test_complete(connection_type, status):
            completed_tests[connection_type] = True
            if callback:
                callback(connection_type, status)
            
            # Start target test after FileMaker test completes (to avoid resource conflicts)
            if connection_type == 'filemaker' and not completed_tests['target']:
                self.test_target_connection(on_test_complete)
        
        # Start with FileMaker test
        self.test_filemaker_connection(on_test_complete)
    
    def get_connection_details(self, connection_type: str) -> Dict[str, Any]:
        """Get detailed connection information"""
        if connection_type not in self.connection_status:
            return {}
        
        status = self.connection_status[connection_type].copy()
        
        # Add formatted last test time
        if status.get('last_test'):
            last_test_dt = time.ctime(status['last_test'])
            status['last_test_formatted'] = last_test_dt
        
        # Add connection health score
        if status['connected']:
            duration = status.get('test_duration', 0)
            if duration < 2:
                status['health'] = 'excellent'
            elif duration < 5:
                status['health'] = 'good'
            elif duration < 10:
                status['health'] = 'fair'
            else:
                status['health'] = 'poor'
        else:
            status['health'] = 'failed'
        
        return status

class StatusManager:
    """ENHANCED status manager with caching and performance monitoring"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        self.migration_data = None
        self.last_refresh = None
        self.refresh_in_progress = False
    
    def refresh_migration_status(self, callback: Callable = None, force_refresh: bool = False):
        """ENHANCED migration status refresh with caching"""
        # Avoid multiple simultaneous refreshes
        if self.refresh_in_progress and not force_refresh:
            if callback:
                callback(False, "Refresh already in progress")
            return
        
        # Check if we need to refresh (cache for 5 seconds unless forced)
        if not force_refresh and self.last_refresh and self.migration_data:
            time_since_refresh = time.time() - self.last_refresh
            if time_since_refresh < 5:  # 5 second cache
                if callback:
                    callback(True, self.migration_data)
                return
        
        self.log_manager.log(LogLevel.INFO, "Status", "Refreshing migration status")
        self.refresh_in_progress = True
        
        def get_status():
            try:
                start_time = time.time()
                result = self.operation_manager.run_python_command(
                    ['--migration-status', '--json'], 
                    "Migration status refresh",
                    timeout=120  # 2 minutes timeout
                )
                refresh_duration = time.time() - start_time
                
                if result['success'] and result.get('data'):
                    self.migration_data = result['data']
                    self.last_refresh = time.time()
                    
                    # Add performance metrics
                    self.migration_data['refresh_duration'] = refresh_duration
                    self.migration_data['refresh_timestamp'] = self.last_refresh
                    
                    self.log_manager.log(LogLevel.INFO, "Status", 
                                       f"Migration status updated (took {refresh_duration:.1f}s)")
                    
                    if callback:
                        callback(True, self.migration_data)
                else:
                    error_msg = result.get('error', 'Unknown error')
                    self.log_manager.log(LogLevel.ERROR, "Status", f"Failed to refresh status: {error_msg}")
                    
                    if callback:
                        callback(False, error_msg)
                        
            except Exception as e:
                error_msg = f"Status refresh exception: {e}"
                self.log_manager.log(LogLevel.ERROR, "Status", error_msg)
                if callback:
                    callback(False, error_msg)
            finally:
                self.refresh_in_progress = False
        
        threading.Thread(target=get_status, daemon=True, name="Status-Refresh").start()
    
    def get_cached_status(self) -> Optional[Dict[str, Any]]:
        """Get cached migration status without refresh"""
        return self.migration_data
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get a summary of the current migration status"""
        if not self.migration_data:
            return {
                'status': 'unknown',
                'message': 'No status data available',
                'last_refresh': None
            }
        
        summary = self.migration_data.get('summary', {})
        total_tables = summary.get('total_tables', 0)
        migrated_tables = summary.get('tables_migrated', 0)
        
        # Calculate overall status
        if total_tables == 0:
            status = 'no_data'
            message = 'No tables found'
        elif migrated_tables == 0:
            status = 'not_started'
            message = 'Migration not started'
        elif migrated_tables == total_tables:
            status = 'complete'
            message = 'Migration complete'
        else:
            status = 'in_progress'
            message = f'{migrated_tables}/{total_tables} tables migrated'
        
        return {
            'status': status,
            'message': message,
            'total_tables': total_tables,
            'migrated_tables': migrated_tables,
            'completion_percentage': (migrated_tables / total_tables * 100) if total_tables > 0 else 0,
            'last_refresh': self.last_refresh,
            'refresh_duration': self.migration_data.get('refresh_duration', 0)
        }

class DebugManager:
    """NEW: Debug manager for handling debug operations"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
        self.debug_enabled = False
    
    def set_debug_mode(self, enabled: bool):
        """Enable or disable debug mode"""
        self.debug_enabled = enabled
        level = "enabled" if enabled else "disabled"
        self.log_manager.log(LogLevel.INFO, "Debug", f"Debug mode {level}")
    
    def get_debug_args(self) -> List[str]:
        """Get debug arguments for operations"""
        return ['--debug'] if self.debug_enabled else []
    
    def run_diagnostic_command(self, operation_manager: OperationManager, callback: Callable = None):
        """Run diagnostic commands for troubleshooting"""
        self.log_manager.log(LogLevel.INFO, "Debug", "Running diagnostic commands")
        
        def run_diagnostics():
            diagnostics = {}
            
            # Test basic info
            result = operation_manager.run_python_command(['--info-only', '--json'], "System info", timeout=30)
            diagnostics['system_info'] = result
            
            # Test source connection
            result = operation_manager.run_python_command(['--src-cnt', '--json', '--max-rows', '1'], "Source test", timeout=60)
            diagnostics['source_test'] = result
            
            # Test target connection
            result = operation_manager.run_python_command(['--tgt-cnt', '--json', '--max-rows', '1'], "Target test", timeout=60)
            diagnostics['target_test'] = result
            
            self.log_manager.log(LogLevel.INFO, "Debug", "Diagnostic commands completed")
            
            if callback:
                callback(diagnostics)
        
        threading.Thread(target=run_diagnostics, daemon=True, name="Debug-Diagnostics").start()

class PerformanceMonitor:
    """NEW: Performance monitoring for operations"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
        self.operation_history = []
        self.max_history = 100
    
    def record_operation(self, operation: str, duration: float, result: str):
        """Record operation performance"""
        record = {
            'operation': operation,
            'duration': duration,
            'result': result,
            'timestamp': time.time()
        }
        
        self.operation_history.append(record)
        
        # Keep only recent history
        if len(self.operation_history) > self.max_history:
            self.operation_history.pop(0)
        
        self.log_manager.log(LogLevel.INFO, "Performance", 
                           f"Operation {operation} took {duration:.1f}s - {result}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        if not self.operation_history:
            return {'message': 'No performance data available'}
        
        # Calculate statistics
        total_ops = len(self.operation_history)
        successful_ops = len([op for op in self.operation_history if op['result'] == 'success'])
        failed_ops = total_ops - successful_ops
        
        durations = [op['duration'] for op in self.operation_history]
        avg_duration = sum(durations) / len(durations)
        max_duration = max(durations)
        min_duration = min(durations)
        
        return {
            'total_operations': total_ops,
            'successful_operations': successful_ops,
            'failed_operations': failed_ops,
            'success_rate': (successful_ops / total_ops * 100) if total_ops > 0 else 0,
            'average_duration': avg_duration,
            'max_duration': max_duration,
            'min_duration': min_duration,
            'recent_operations': self.operation_history[-10:]  # Last 10 operations
        }