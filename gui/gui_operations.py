#!/usr/bin/env python3
# FILE: gui/gui_operations.py
"""
GUI Operations Module - FIXED VERSION
Handles operation execution with proper logging integration
"""

import subprocess
import threading
import json
import sys
from typing import Dict, Any, List, Callable
from pathlib import Path
from gui_logging import LogManager, LogLevel
import time

class OperationManager:
    """Manages operation execution and status with proper logging"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
        self.is_operation_running = False
        self.current_operation = None
        self.operation_callbacks: List[Callable] = []
        
        # Log that operation manager is initialized
        self.log_manager.log(LogLevel.INFO, "OperationManager", "Operation manager initialized")
    
    def add_operation_callback(self, callback: Callable):
        """Add callback for operation status updates"""
        self.operation_callbacks.append(callback)
        self.log_manager.log(LogLevel.DEBUG, "OperationManager", "Added operation callback")
    
    def run_python_command(self, cmd_args: List[str], description: str) -> Dict[str, Any]:
        """Run a Python command and return JSON result with proper logging"""
        self.log_manager.log(LogLevel.INFO, "Command", f"Starting: {description}")
        self.log_manager.log(LogLevel.DEBUG, "Command", f"Args: {' '.join(cmd_args)}")
        
        try:
            # Check if the script exists
            script_path = Path('filemaker_extract_refactored.py')
            if not script_path.exists():
                error_msg = 'filemaker_extract_refactored.py not found'
                self.log_manager.log(LogLevel.ERROR, "Command", error_msg)
                return {'success': False, 'error': error_msg}
            
            # Log that we're executing the command
            full_command = [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args
            self.log_manager.log(LogLevel.DEBUG, "Command", f"Executing: {' '.join(full_command)}")
            
            start_time = time.time()
            
            result = subprocess.run(
                full_command,
                capture_output=True,
                text=True,
                timeout=60,
                cwd=Path.cwd()
            )
            
            duration = time.time() - start_time
            
            self.log_manager.log(LogLevel.DEBUG, "Command", 
                               f"Command completed in {duration:.2f}s with return code: {result.returncode}")
            
            # Always log the output for debugging
            if result.stdout:
                self.log_manager.log(LogLevel.DEBUG, "Command", f"STDOUT: {result.stdout[:1000]}...")
            if result.stderr:
                self.log_manager.log(LogLevel.DEBUG, "Command", f"STDERR: {result.stderr[:1000]}...")
            
            if result.returncode == 0:
                self.log_manager.log(LogLevel.INFO, "Command", f"Successfully completed: {description}")
                
                try:
                    # Look for JSON in the output
                    output = result.stdout.strip()
                    if not output:
                        self.log_manager.log(LogLevel.INFO, "Command", "Command completed with no output")
                        return {'success': True, 'data': None, 'message': 'Command completed successfully'}
                    
                    # Try to find JSON content
                    lines = output.split('\n')
                    json_content = None
                    
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
                            break
                    
                    if json_content:
                        try:
                            data = json.loads(json_content)
                            self.log_manager.log(LogLevel.INFO, "Command", f"Parsed JSON response for: {description}")
                            return {'success': True, 'data': data}
                        except json.JSONDecodeError as e:
                            self.log_manager.log(LogLevel.WARNING, "Command", f"JSON decode error: {e}")
                    
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
                
        except subprocess.TimeoutExpired:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command timed out: {description}")
            return {'success': False, 'error': 'Operation timed out'}
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command exception: {description} - {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def run_operation_async(self, operation: str, on_complete: Callable = None):
        """Run an operation asynchronously with detailed logging"""
        if self.is_operation_running:
            self.log_manager.log(LogLevel.WARNING, "Operation", f"Cannot start {operation}: Another operation is already running")
            return False
        
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
        
        self.log_manager.log(LogLevel.INFO, "Operation", f"Starting operation: {operation}")
        
        # Notify callbacks of operation start
        for callback in self.operation_callbacks:
            try:
                callback('start', operation)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in start callback: {e}")
        
        def run_op():
            try:
                cmd = operation_commands[operation]
                self.log_manager.log(LogLevel.INFO, "Operation", f"Executing: {operation} with args: {cmd}")
                
                # Run with real-time output capture
                full_command = [sys.executable, 'filemaker_extract_refactored.py'] + cmd
                self.log_manager.log(LogLevel.DEBUG, "Operation", f"Full command: {' '.join(full_command)}")
                
                process = subprocess.Popen(
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
                    line = process.stdout.readline()
                    if not line and process.poll() is not None:
                        break
                    if line:
                        line = line.strip()
                        output_lines.append(line)
                        # Log each line of output
                        self.log_manager.log(LogLevel.INFO, f"Operation-{operation}", line)
                
                # Wait for process to complete
                process.wait()
                
                # Log the final result
                if process.returncode == 0:
                    self.log_manager.log(LogLevel.INFO, "Operation", 
                                       f"✓ {operation.replace('_', ' ').title()} completed successfully")
                    result = 'success'
                else:
                    self.log_manager.log(LogLevel.ERROR, "Operation", 
                                       f"✗ {operation.replace('_', ' ').title()} failed with return code {process.returncode}")
                    result = 'failure'
                
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", 
                                   f"Exception in {operation}: {e}")
                result = 'error'
            finally:
                self.is_operation_running = False
                self.current_operation = None
                
                self.log_manager.log(LogLevel.INFO, "Operation", f"Operation {operation} finished with result: {result}")
                
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
    """Handles connection testing operations with detailed logging"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        
        self.connection_status = {
            'filemaker': {'connected': False, 'message': 'Not tested'},
            'target': {'connected': False, 'message': 'Not tested'}
        }
        
        self.log_manager.log(LogLevel.INFO, "ConnectionTester", "Connection tester initialized")
    
    def test_filemaker_connection(self, callback: Callable = None):
        """Test FileMaker connection with detailed logging"""
        self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing FileMaker Pro connection...")
        
        def test_connection():
            try:
                # Use info-only command to test FileMaker
                result = self.operation_manager.run_python_command(
                    ['--info-only', '--json'], 
                    "FileMaker connection test"
                )
                
                if result['success']:
                    data = result.get('data')
                    if data:
                        conn_status = data.get('connection_status', {})
                        fm_status = conn_status.get('filemaker', {})
                        
                        if fm_status.get('connected', False):
                            self.connection_status['filemaker'] = {
                                'connected': True,
                                'message': f"Connected via DSN: {data.get('source_dsn', 'unknown')}"
                            }
                            self.log_manager.log(LogLevel.INFO, "Connection", 
                                               f"✓ FileMaker connection successful - DSN: {data.get('source_dsn')}")
                        else:
                            self.connection_status['filemaker'] = {
                                'connected': False,
                                'message': fm_status.get('message', 'Connection failed')
                            }
                            self.log_manager.log(LogLevel.ERROR, "Connection", 
                                               f"✗ FileMaker connection failed: {fm_status.get('message')}")
                    else:
                        error_msg = result.get('message', result.get('error', 'Connection test failed'))
                        self.connection_status['filemaker'] = {
                            'connected': False,
                            'message': error_msg
                        }
                        self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ FileMaker test failed: {error_msg}")
                else:
                    error_msg = result.get('error', 'Connection test failed')
                    self.connection_status['filemaker'] = {
                        'connected': False,
                        'message': error_msg
                    }
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ FileMaker test failed: {error_msg}")
                
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
                except Exception as e:
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in FileMaker callback: {e}")
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_target_connection(self, callback: Callable = None):
        """Test target database connection with detailed logging"""
        self.log_manager.log(LogLevel.INFO, "Connection", "🔍 Testing Supabase target connection...")
        
        def test_connection():
            try:
                # Use info-only command to test target
                result = self.operation_manager.run_python_command(
                    ['--info-only', '--json'], 
                    "Target connection test"
                )
                
                if result['success']:
                    data = result.get('data')
                    if data:
                        conn_status = data.get('connection_status', {})
                        target_status = conn_status.get('target', {})
                        
                        if target_status.get('connected', False):
                            self.connection_status['target'] = {
                                'connected': True,
                                'message': f"Connected to {data.get('target_database', 'target')}"
                            }
                            self.log_manager.log(LogLevel.INFO, "Connection", 
                                               f"✓ Target connection successful - {data.get('target_database')}")
                        else:
                            self.connection_status['target'] = {
                                'connected': False,
                                'message': target_status.get('message', 'Connection failed')
                            }
                            self.log_manager.log(LogLevel.ERROR, "Connection", 
                                               f"✗ Target connection failed: {target_status.get('message')}")
                    else:
                        error_msg = result.get('message', result.get('error', 'Connection test failed'))
                        self.connection_status['target'] = {
                            'connected': False,
                            'message': error_msg
                        }
                        self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ Target test failed: {error_msg}")
                else:
                    error_msg = result.get('error', 'Connection test failed')
                    self.connection_status['target'] = {
                        'connected': False,
                        'message': error_msg
                    }
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"✗ Target test failed: {error_msg}")
                
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
                except Exception as e:
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in target callback: {e}")
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_all_connections(self, callback: Callable = None):
        """Test both connections with detailed logging"""
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
    """Manages migration status and data with detailed logging"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        self.migration_data = None
        
        self.log_manager.log(LogLevel.INFO, "StatusManager", "Status manager initialized")
    
    def refresh_migration_status(self, callback: Callable = None):
        """Refresh migration status data with detailed logging"""
        self.log_manager.log(LogLevel.INFO, "Status", "🔄 Refreshing migration status...")
        
        def get_status():
            try:
                result = self.operation_manager.run_python_command(
                    ['--migration-status', '--json'], 
                    "Migration status refresh"
                )
                
                if result['success'] and result.get('data'):
                    self.migration_data = result['data']
                    
                    # Log summary information
                    summary = self.migration_data.get('summary', {})
                    total_tables = summary.get('total_tables', 0)
                    migrated_tables = summary.get('tables_migrated', 0)
                    source_rows = summary.get('source_total_rows', 0)
                    target_rows = summary.get('target_total_rows', 0)
                    
                    self.log_manager.log(LogLevel.INFO, "Status", 
                                       f"✓ Migration status updated - Tables: {migrated_tables}/{total_tables}, Rows: {target_rows:,}/{source_rows:,}")
                    
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

# Test function to generate sample log entries
def test_logging(log_manager: LogManager):
    """Generate test log entries for debugging"""
    log_manager.log(LogLevel.INFO, "Test", "This is a test info message")
    log_manager.log(LogLevel.WARNING, "Test", "This is a test warning message")
    log_manager.log(LogLevel.ERROR, "Test", "This is a test error message")
    log_manager.log(LogLevel.DEBUG, "Test", "This is a test debug message")
    log_manager.log(LogLevel.CRITICAL, "Test", "This is a test critical message")