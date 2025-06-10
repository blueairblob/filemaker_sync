#!/usr/bin/env python3
# FILE: gui/gui_operations.py
"""
GUI Operations Module
Handles operation execution and command interface with proper TOML config support
"""

import subprocess
import threading
import json
import sys
from typing import Dict, Any, List, Callable
from pathlib import Path
from gui_logging import LogManager, LogLevel

class OperationManager:
    """Manages operation execution and status"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
        self.is_operation_running = False
        self.current_operation = None
        self.operation_callbacks: List[Callable] = []
    
    def add_operation_callback(self, callback: Callable):
        """Add callback for operation status updates"""
        self.operation_callbacks.append(callback)
    
    def run_python_command(self, cmd_args: List[str], description: str) -> Dict[str, Any]:
        """Run a Python command and return JSON result"""
        self.log_manager.log(LogLevel.INFO, "Command", f"Running: {description}", {"args": cmd_args})
        
        try:
            # Check if the script exists
            script_path = Path('filemaker_extract_refactored.py')
            if not script_path.exists():
                return {'success': False, 'error': 'filemaker_extract_refactored.py not found'}
            
            result = subprocess.run(
                [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args,
                capture_output=True,
                text=True,
                timeout=60,
                cwd=Path.cwd()  # Ensure we're in the right directory
            )
            
            self.log_manager.log(LogLevel.DEBUG, "Command", f"Return code: {result.returncode}")
            self.log_manager.log(LogLevel.DEBUG, "Command", f"STDOUT: {result.stdout[:500]}...")
            if result.stderr:
                self.log_manager.log(LogLevel.DEBUG, "Command", f"STDERR: {result.stderr[:500]}...")
            
            if result.returncode == 0:
                try:
                    # Look for JSON in the output - handle multi-line JSON
                    output = result.stdout.strip()
                    if not output:
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
                            self.log_manager.log(LogLevel.INFO, "Command", f"Command succeeded with JSON: {description}")
                            return {'success': True, 'data': data}
                        except json.JSONDecodeError as e:
                            self.log_manager.log(LogLevel.WARNING, "Command", f"JSON decode error: {e}")
                            # Fall through to return raw output
                    
                    # No valid JSON found, return raw output
                    self.log_manager.log(LogLevel.INFO, "Command", f"Command succeeded (no JSON): {description}")
                    return {'success': True, 'data': None, 'message': output}
                        
                except Exception as e:
                    self.log_manager.log(LogLevel.ERROR, "Command", f"Error processing output: {e}")
                    return {'success': True, 'data': None, 'message': result.stdout}
            else:
                error_msg = result.stderr or result.stdout or "Unknown error"
                self.log_manager.log(LogLevel.ERROR, "Command", f"Command failed: {description}", {"error": error_msg})
                return {'success': False, 'error': error_msg}
                
        except subprocess.TimeoutExpired:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command timed out: {description}")
            return {'success': False, 'error': 'Operation timed out'}
        except Exception as e:
            self.log_manager.log(LogLevel.ERROR, "Command", f"Command exception: {description}", {"exception": str(e)})
            return {'success': False, 'error': str(e)}
    
    def run_operation_async(self, operation: str, on_complete: Callable = None):
        """Run an operation asynchronously"""
        if self.is_operation_running:
            self.log_manager.log(LogLevel.WARNING, "Operation", "Operation already running")
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
        
        # Notify callbacks of operation start
        for callback in self.operation_callbacks:
            try:
                callback('start', operation)
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in callback: {e}")
        
        def run_op():
            try:
                cmd = operation_commands[operation]
                self.log_manager.log(LogLevel.INFO, "Operation", f"Started {operation.replace('_', ' ')}")
                
                # Run with real-time output capture
                process = subprocess.Popen(
                    [sys.executable, 'filemaker_extract_refactored.py'] + cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )
                
                # Read output line by line
                for line in iter(process.stdout.readline, ''):
                    if line:
                        self.log_manager.log(LogLevel.INFO, "Operation", line.strip())
                
                process.wait()
                
                if process.returncode == 0:
                    self.log_manager.log(LogLevel.INFO, "Operation", 
                                       f"{operation.replace('_', ' ').title()} completed successfully")
                    result = 'success'
                else:
                    self.log_manager.log(LogLevel.ERROR, "Operation", 
                                       f"{operation.replace('_', ' ').title()} failed with code {process.returncode}")
                    result = 'failure'
                
            except Exception as e:
                self.log_manager.log(LogLevel.ERROR, "Operation", 
                                   f"{operation.replace('_', ' ').title()} error: {e}")
                result = 'error'
            finally:
                self.is_operation_running = False
                self.current_operation = None
                
                # Notify callbacks of operation completion
                for callback in self.operation_callbacks:
                    try:
                        callback('complete', operation, result)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in callback: {e}")
                
                if on_complete:
                    try:
                        on_complete(result)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Operation", f"Error in completion callback: {e}")
        
        threading.Thread(target=run_op, daemon=True).start()
        return True

class ConnectionTester:
    """Handles connection testing operations"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        
        self.connection_status = {
            'filemaker': {'connected': False, 'message': 'Not tested'},
            'target': {'connected': False, 'message': 'Not tested'}
        }
    
    def test_filemaker_connection(self, callback: Callable = None):
        """Test FileMaker connection"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing FileMaker connection")
        
        def test_connection():
            # Use info-only command to test FileMaker without requiring target
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
                        self.log_manager.log(LogLevel.INFO, "Connection", "FileMaker connection successful",
                                           {"dsn": data.get('source_dsn'), "tables": data.get('table_count', 0)})
                    else:
                        self.connection_status['filemaker'] = {
                            'connected': False,
                            'message': fm_status.get('message', 'Connection failed')
                        }
                        self.log_manager.log(LogLevel.ERROR, "Connection", "FileMaker connection failed", 
                                           {"error": fm_status.get('message')})
                else:
                    # Try to parse error from message
                    error_msg = result.get('message', result.get('error', 'Connection test failed'))
                    self.connection_status['filemaker'] = {
                        'connected': False,
                        'message': error_msg
                    }
            else:
                self.connection_status['filemaker'] = {
                    'connected': False,
                    'message': result.get('error', 'Connection test failed')
                }
                self.log_manager.log(LogLevel.ERROR, "Connection", 
                                   f"FileMaker connection test failed: {result.get('error')}")
            
            if callback:
                try:
                    callback('filemaker', self.connection_status['filemaker'])
                except Exception as e:
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in FM callback: {e}")
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_target_connection(self, callback: Callable = None):
        """Test target database connection"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing target database connection")
        
        def test_connection():
            # Use info-only command to test target without requiring FileMaker
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
                        self.log_manager.log(LogLevel.INFO, "Connection", "Target connection successful",
                                           {"database": data.get('target_database'), "host": data.get('target_host')})
                    else:
                        self.connection_status['target'] = {
                            'connected': False,
                            'message': target_status.get('message', 'Connection failed')
                        }
                        self.log_manager.log(LogLevel.ERROR, "Connection", "Target connection failed",
                                           {"error": target_status.get('message')})
                else:
                    error_msg = result.get('message', result.get('error', 'Connection test failed'))
                    self.connection_status['target'] = {
                        'connected': False,
                        'message': error_msg
                    }
            else:
                self.connection_status['target'] = {
                    'connected': False,
                    'message': result.get('error', 'Connection test failed')
                }
                self.log_manager.log(LogLevel.ERROR, "Connection", 
                                   f"Target connection test failed: {result.get('error')}")
            
            if callback:
                try:
                    callback('target', self.connection_status['target'])
                except Exception as e:
                    self.log_manager.log(LogLevel.ERROR, "Connection", f"Error in target callback: {e}")
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_all_connections(self, callback: Callable = None):
        """Test both connections"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing all connections")
        
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
    """Manages migration status and data"""
    
    def __init__(self, operation_manager: OperationManager):
        self.operation_manager = operation_manager
        self.log_manager = operation_manager.log_manager
        self.migration_data = None
    
    def refresh_migration_status(self, callback: Callable = None):
        """Refresh migration status data"""
        self.log_manager.log(LogLevel.INFO, "Status", "Refreshing migration status")
        
        def get_status():
            result = self.operation_manager.run_python_command(
                ['--migration-status', '--json'], 
                "Migration status refresh"
            )
            
            if result['success'] and result.get('data'):
                self.migration_data = result['data']
                self.log_manager.log(LogLevel.INFO, "Status", "Migration status updated")
                
                if callback:
                    try:
                        callback(True, self.migration_data)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Status", f"Error in status callback: {e}")
            else:
                error_msg = result.get('error', 'Unknown error')
                self.log_manager.log(LogLevel.ERROR, "Status", f"Failed to refresh status: {error_msg}")
                
                if callback:
                    try:
                        callback(False, error_msg)
                    except Exception as e:
                        self.log_manager.log(LogLevel.ERROR, "Status", f"Error in error callback: {e}")
        
        threading.Thread(target=get_status, daemon=True).start()
