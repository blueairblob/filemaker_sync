#!/usr/bin/env python3
# FILE: gui/gui_operations.py
"""

"""

import subprocess
import threading
import json
import sys
import time
from typing import Dict, Any, List, Callable
from pathlib import Path
from gui_logging import LogManager, LogLevel, PerformanceLogger

class OperationManager:
    """Manages operation execution with comprehensive logging integration"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
        self.is_operation_running = False
        self.current_operation = None
        self.operation_callbacks: List[Callable] = []
        self.current_process = None
        
        # Log that operation manager is initialized
        self.log_manager.log(LogLevel.INFO, "OperationManager", "Operation manager initialized")
    
    def add_operation_callback(self, callback: Callable):
        """Add callback for operation status updates"""
        self.operation_callbacks.append(callback)
        self.log_manager.log(LogLevel.DEBUG, "OperationManager", f"Added operation callback (total: {len(self.operation_callbacks)})")
    
    def run_python_command(self, cmd_args: List[str], description: str, capture_output: bool = True) -> Dict[str, Any]:
        """Run a Python command with comprehensive logging and output capture"""
        self.log_manager.log(LogLevel.INFO, "Command", f"Starting: {description}")
        self.log_manager.log(LogLevel.DEBUG, "Command", f"Args: {' '.join(cmd_args)}")
        
        try:
            # Check if the script exists
            script_path = Path('filemaker_extract_refactored.py')
            if not script_path.exists():
                error_msg = 'filemaker_extract_refactored.py not found'
                self.log_manager.log(LogLevel.ERROR, "Command", error_msg)
                return {'success': False, 'error': error_msg}
            
            # Build full command
            full_command = [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args
            self.log_manager.log(LogLevel.DEBUG, "Command", f"Executing: {' '.join(full_command)}")
            
            with PerformanceLogger(self.log_manager, "Command", description):
                if capture_output:
                    # For commands that return JSON data
                    result = subprocess.run(
                        full_command,
                        capture_output=True,
                        text=True,
                        timeout=60,
                        cwd=Path.cwd()
                    )
                    
                    return self._process_command_result(result, description)
                else:
                    # For long-running operations with streaming output
                    return self._run_streaming_command(full_command, description)
        
        except subprocess.TimeoutExpired:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command timed out: {description}")
            return {'success': False, 'error': 'Operation timed out'}
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command exception: {description} - {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _process_command_result(self, result: subprocess.CompletedProcess, description: str) -> Dict[str, Any]:
        """Process the result of a completed subprocess command"""
        self.log_manager.log(LogLevel.DEBUG, "Command", 
                           f"Command completed with return code: {result.returncode}")
        
        # Log all output for debugging
        if result.stdout:
            # Split stdout into lines and log each
            for line in result.stdout.split('\n'):
                if line.strip():
                    self.log_manager.log_subprocess_output("Command-Output", line)
        
        if result.stderr:
            # Log stderr as errors
            for line in result.stderr.split('\n'):
                if line.strip():
                    self.log_manager.log(LogLevel.ERROR, "Command-Error", line)
        
        if result.returncode == 0:
            self.log_manager.log(LogLevel.INFO, "Command", f"✓ Successfully completed: {description}")
            
            try:
                # Look for JSON in the output
                output = result.stdout.strip()
                if not output:
                    self.log_manager.log(LogLevel.INFO, "Command", "Command completed with no output")
                    return {'success': True, 'data': None, 'message': 'Command completed successfully'}
                
                # Try to parse JSON from output
                json_data = self._extract_json_from_output(output)
                if json_data:
                    self.log_manager.log(LogLevel.DEBUG, "Command", f"Parsed JSON response for: {description}")
                    return {'success': True, 'data': json_data}
                else:
                    # No valid JSON found, return raw output
                    self.log_manager.log(LogLevel.INFO, "Command", f"Command completed (text output): {description}")
                    return {'success': True, 'data': None, 'message': output}
                    
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Command", f"Error processing output: {e}")
                return {'success': True, 'data': None, 'message': result.stdout}
        else:
            error_msg = result.stderr or result.stdout or "Unknown error"
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command failed: {description} - {error_msg}")
            return {'success': False, 'error': error_msg}
    
    def _extract_json_from_output(self, output: str) -> Dict:
        """Extract JSON data from command output"""
        try:
            lines = output.split('\n')
            
            # Look for lines that start with { or contain JSON
            for i, line in enumerate(lines):
                line = line.strip()
                if line.startswith('{'):
                    # Found start of JSON, collect until we have complete JSON
                    json_lines = [line]
                    brace_count = line.count('{') - line.count('}')
                    
                    for j in range(i + 1, len(lines)):
                        next_line = lines[j].strip()
                        if next_line:
                            json_lines.append(next_line)
                            brace_count += next_line.count('{') - next_line.count('}')
                            if brace_count == 0:
                                break
                    
                    json_content = '\n'.join(json_lines)
                    try:
                        return json.loads(json_content)
                    except json.JSONDecodeError:
                        continue
            
            return None
            
        except Exception:
            return None
    
    def _run_streaming_command(self, full_command: List[str], description: str) -> Dict[str, Any]:
        """Run command with real-time output streaming"""
        try:
            self.current_process = subprocess.Popen(
                full_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Read output line by line and log it
            output_lines = []
            while True:
                line = self.current_process.stdout.readline()
                if not line and self.current_process.poll() is not None:
                    break
                if line:
                    line = line.strip()
                    output_lines.append(line)
                    # Log each line of output with proper classification
                    self.log_manager.log_subprocess_output(f"Operation-{self.current_operation or 'Unknown'}", line)
            
            # Wait for process to complete
            self.current_process.wait()
            
            # Return result based on exit code
            if self.current_process.returncode == 0:
                self.log_manager.log(LogLevel.INFO, "Command", f"✓ Streaming command completed: {description}")
                return {'success': True, 'message': f'{description} completed successfully'}
            else:
                self.log_manager.log(LogLevel.ERROR, "Command", f"✗ Streaming command failed: {description}")
                return {'success': False, 'error': f'{description} failed with exit code {self.current_process.returncode}'}
        
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Exception in streaming command: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            self.current_process = None
    
    def cancel_current_operation(self):
        """Cancel the currently running operation"""
        if self.current_process:
            try:
                self.current_process.terminate()
                self.log_manager.log(LogLevel.WARNING, "Operation", "Current operation cancelled by user")
            except:
                pass
    
    def run_operation_async(self, operation: str, on_complete: Callable = None):
        """Run an operation asynchronously with comprehensive logging"""
        if self.is_operation_running:
            self.log_manager.log(LogLevel.WARNING, "Operation", f"Cannot start {operation}: Another operation is already running")
            return False
        
        operation_commands = {
            'full_sync': ['--db-exp', '--ddl', '--dml'],
            'incremental_sync': ['--db-exp', '--dml'],
            'export_files': ['--fn-exp', '--ddl', '--dml'],
            'export_images': ['--get-images'],
            'test_connections': ['--info-only'],
            'migration_status': ['--migration-status', '--json']
        }
        
        if operation not in operation_commands:
            self.log_manager.log(LogLevel.ERROR, "Operation", f"Unknown operation: {operation}")
            return False
        
        self.is_operation_running = True
        self.current_operation = operation
        
        self.log_manager.log(LogLevel.INFO, "Operation", f"🚀 Starting operation: {operation}")
        
        # Notify callbacks of operation start
        for callback in self.operation_callbacks:
            try:
                callback('start', operation)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in start callback: {e}")
        
        def run_op():
            result = 'error'
            try:
                cmd = operation_commands[operation]
                self.log_manager.log(LogLevel.INFO, "Operation", f"Executing: {operation} with args: {cmd}")
                
                # Use streaming output for long operations
                streaming_operations = ['full_sync', 'incremental_sync', 'export_files', 'export_images']
                use_streaming = operation in streaming_operations
                
                if use_streaming:
                    # Use streaming for operations that take time
                    command_result = self.run_python_command(cmd, f"{operation.replace('_', ' ').title()}", capture_output=False)
                else:
                    # Use regular capture for quick operations
                    command_result = self.run_python_command(cmd, f"{operation.replace('_', ' ').title()}", capture_output=True)
                
                if command_result['success']:
                    self.log_manager.log(LogLevel.INFO, "Operation", 
                                       f"✓ {operation.replace('_', ' ').title()} completed successfully")
                    result = 'success'
                else:
                    error_msg = command_result.get('error', 'Unknown error')
                    self.log_manager.log(LogLevel.ERROR, "Operation", 
                                       f"✗ {operation.replace('_', ' ').title()} failed: {error_msg}")
                    result = 'failure'
                
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", 
                                   f"Exception in {operation}: {e}")
                result = 'error'
            finally:
                self.is_operation_running = False
                self.current_operation = None
                
                self.log_manager.log(LogLevel.INFO, "Operation", f"🏁 Operation {operation} finished with result: {result}")
                
                # Notify callbacks of operation completion
                for callback in self.operation_callbacks:
                    try:
                        callback('complete', operation, result)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in completion callback: {e}")
                
                if on_complete:
                    try:
                        on_complete(result)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in completion callback: {e}")
        
        threading.Thread(target=run_op, daemon=True).start()
        return True


class ConnectionTester:
    """Enhanced connection testing with better logging integration"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        
        self.connection_status = {
            'filemaker': {'connected': False, 'message': 'Not tested'},
            'target': {'connected': False, 'message': 'Not tested'}
        }
        
        self.log_manager.log(LogLevel.INFO, "ConnectionTester", "Connection tester initialized")
    
    def test_filemaker_connection(self, callback: Callable = None):
        """Test FileMaker connection with enhanced logging"""
        self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing FileMaker Pro connection...")
        
        def test_connection():
            try:
                with PerformanceLogger(self.log_manager, "Connection", "FileMaker connection test"):
                    result = self.operation_manager.run_python_command(
                        ['--info-only', '--json'], 
                        "FileMaker connection test"
                    )
                    
                    self._process_connection_result(result, 'filemaker', callback)
                    
            except Exception as e:
                error_msg = f"Exception during FileMaker test: {e}"
                self.connection_status['filemaker'] = {
                    'connected': False,
                    'message': error_msg
                }
                self.log_manager.log(LogLevel.ERROR, "Connection", error_msg)
                
                if callback:
                    try:
                        callback('filemaker', self.connection_status['filemaker'])
                    except Exception as cb_e:
                        self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in FileMaker callback: {cb_e}")
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_target_connection(self, callback: Callable = None):
        """Test target database connection with enhanced logging"""
        self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing Supabase target connection...")
        
        def test_connection():
            try:
                with PerformanceLogger(self.log_manager, "Connection", "Target connection test"):
                    result = self.operation_manager.run_python_command(
                        ['--info-only', '--json'], 
                        "Target connection test"
                    )
                    
                    self._process_connection_result(result, 'target', callback)
                    
            except Exception as e:
                error_msg = f"Exception during target test: {e}"
                self.connection_status['target'] = {
                    'connected': False,
                    'message': error_msg
                }
                self.log_manager.log(LogLevel.ERROR, "Connection", error_msg)
                
                if callback:
                    try:
                        callback('target', self.connection_status['target'])
                    except Exception as cb_e:
                        self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in target callback: {cb_e}")
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def _process_connection_result(self, result: Dict[str, Any], connection_type: str, callback: Callable = None):
        """Process connection test result and update status"""
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
                    
                    self.connection_status[connection_type] = {
                        'connected': True,
                        'message': message
                    }
                    self.log_manager.log(LogLevel.INFO, "Connection", 
                                       f"✓ {connection_type.title()} connection successful - {message}")
                else:
                    error_msg = status_info.get('message', 'Connection failed')
                    self.connection_status[connection_type] = {
                        'connected': False,
                        'message': error_msg
                    }
                    self.log_manager.log(LogLevel.ERROR, "Connection", 
                                       f"✗ {connection_type.title()} connection failed: {error_msg}")
            else:
                error_msg = result.get('message', result.get('error', 'Connection test failed'))
                self.connection_status[connection_type] = {
                    'connected': False,
                    'message': error_msg
                }
                self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ {connection_type.title()} test failed: {error_msg}")
        else:
            error_msg = result.get('error', 'Connection test failed')
            self.connection_status[connection_type] = {
                'connected': False,
                'message': error_msg
            }
            self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ {connection_type.title()} test failed: {error_msg}")
        
        if callback:
            try:
                callback(connection_type, self.connection_status[connection_type])
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in {connection_type} callback: {e}")
    
    def test_all_connections(self, callback: Callable = None):
        """Test both connections with proper sequencing"""
        self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing all database connections...")
        
        def on_filemaker_complete(connection_type, status):
            if callback:
                try:
                    callback(connection_type, status)
                except Exception as e:
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in callback: {e}")
            # Start target test after FM test completes
            self.test_target_connection(callback)
        
        self.test_filemaker_connection(on_filemaker_complete)


class StatusManager:
    """Enhanced status management with better logging"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        self.migration_data = None
        
        self.log_manager.log(LogLevel.INFO, "StatusManager", "Status manager initialized")
    
    def refresh_migration_status(self, callback: Callable = None):
        """Refresh migration status with comprehensive logging"""
        self.log_manager.log(LogLevel.INFO, "Status", "🔄 Refreshing migration status...")
        
        def get_status():
            try:
                with PerformanceLogger(self.log_manager, "Status", "Migration status refresh"):
                    result = self.operation_manager.run_python_command(
                        ['--migration-status', '--json'], 
                        "Migration status refresh"
                    )
                    
                    if result['success'] and result.get('data'):
                        self.migration_data = result['data']
                        
                        # Log detailed summary information
                        summary = self.migration_data.get('summary', {})
                        total_tables = summary.get('total_tables', 0)
                        migrated_tables = summary.get('tables_migrated', 0)
                        source_rows = summary.get('source_total_rows', 0)
                        target_rows = summary.get('target_total_rows', 0)
                        
                        completion_percentage = (target_rows / source_rows * 100) if source_rows > 0 else 0
                        
                        self.log_manager.log(LogLevel.INFO, "Status", 
                                           f"✓ Migration status updated - Tables: {migrated_tables}/{total_tables} ({migrated_tables/total_tables*100:.1f}%), Rows: {target_rows:,}/{source_rows:,} ({completion_percentage:.1f}%)")
                        
                        # Log connection status from the data
                        conn_status = self.migration_data.get('connection_status', {})
                        if conn_status:
                            fm_status = conn_status.get('filemaker', {})
                            target_status = conn_status.get('target', {})
                            
                            self.log_manager.log(LogLevel.DEBUG, "Status", 
                                               f"Connection status - FM: {fm_status.get('connected', False)}, Target: {target_status.get('connected', False)}")
                        
                        if callback:
                            try:
                                callback(True, self.migration_data)
                            except Exception as e:
                                self.log_manager.log(LogLevel.ERROR, "Status", f"Error in status callback: {e}")
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        self.log_manager.log(LogLevel.ERROR, "Status", f"✗ Failed to refresh migration status: {error_msg}")
                        
                        if callback:
                            try:
                                callback(False, error_msg)
                            except Exception as e:
                                self.log_manager.log(LogLevel.ERROR, "Status", f"Error in error callback: {e}")
            
            except Exception as e:
                error_msg = f"Exception during status refresh: {e}"
                self.log_manager.log(LogLevel.ERROR, "Status", error_msg)
                
                if callback:
                    try:
                        callback(False, error_msg)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Status", f"Error in exception callback: {e}")
        
        threading.Thread(target=get_status, daemon=True).start()


# Enhanced utility function for testing logging system
def test_logging_system(log_manager: LogManager):
    """Comprehensive test of the logging system"""
    log_manager.log(LogLevel.INFO, "LogTest", "Starting logging system test...")
    
    # Test all log levels
    log_manager.log(LogLevel.DEBUG, "LogTest", "This is a DEBUG test message")
    log_manager.log(LogLevel.INFO, "LogTest", "This is an INFO test message")
    log_manager.log(LogLevel.WARNING, "LogTest", "This is a WARNING test message")
    log_manager.log(LogLevel.ERROR, "LogTest", "This is an ERROR test message")
    log_manager.log(LogLevel.CRITICAL, "LogTest", "This is a CRITICAL test message")
    
    # Test with details
    log_manager.log(LogLevel.INFO, "LogTest", "Message with details", 
                   {"test_key": "test_value", "number": 42, "timestamp": "2025-06-16T14:54:06"})
    
    # Test different components
    components = ["Database", "GUI", "Operation", "Connection", "Export", "Import"]
    for component in components:
        log_manager.log(LogLevel.INFO, component, f"Test message from {component} component")
    
    # Test subprocess-style logging
    log_manager.log_subprocess_output("TestProcess", "✓ Process started successfully")
    log_manager.log_subprocess_output("TestProcess", "Processing data...")
    log_manager.log_subprocess_output("TestProcess", "Warning: Low memory")
    log_manager.log_subprocess_output("TestProcess", "Error: Connection timeout")
    log_manager.log_subprocess_output("TestProcess", "✓ Process completed")
    
    log_manager.log(LogLevel.INFO, "LogTest", "Logging system test completed")
    
    # Print statistics
    stats = log_manager.get_log_statistics()
    log_manager.log(LogLevel.INFO, "LogTest", f"Test generated {stats['total_logs']} total log entries")