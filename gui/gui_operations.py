#!/usr/bin/env python3
"""
GUI Operations Module
Handles operation execution and command interface
"""

import subprocess
import threading
import json
import sys
from typing import Dict, Any, List, Callable
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
            result = subprocess.run(
                [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout) if result.stdout.strip() else None
                    self.log_manager.log(LogLevel.INFO, "Command", f"Command succeeded: {description}")
                    return {'success': True, 'data': data}
                except json.JSONDecodeError:
                    return {'success': True, 'data': None, 'message': result.stdout}
            else:
                error_msg = result.stderr or result.stdout
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
            callback('start', operation)
        
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
                    callback('complete', operation, result)
                
                if on_complete:
                    on_complete(result)
        
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
            result = self.operation_manager.run_python_command(
                ['--src-cnt', '--json', '--max-rows', '1'], 
                "FileMaker connection test"
            )
            
            if result['success'] and result.get('data'):
                data = result['data']
                if data.get('summary', {}).get('connection_error'):
                    self.connection_status['filemaker'] = {
                        'connected': False,
                        'message': data.get('error_detail', 'Connection failed')
                    }
                    self.log_manager.log(LogLevel.ERROR, "Connection", "FileMaker connection failed", 
                                       {"error": data.get('error_detail')})
                else:
                    self.connection_status['filemaker'] = {
                        'connected': True,
                        'message': f"Connected via DSN: {data.get('dsn', 'unknown')}"
                    }
                    self.log_manager.log(LogLevel.INFO, "Connection", "FileMaker connection successful",
                                       {"dsn": data.get('dsn'), "tables": data.get('summary', {}).get('total_tables')})
            else:
                self.connection_status['filemaker'] = {
                    'connected': False,
                    'message': result.get('error', 'Connection test failed')
                }
            
            if callback:
                callback('filemaker', self.connection_status['filemaker'])
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_target_connection(self, callback: Callable = None):
        """Test target database connection"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing target database connection")
        
        def test_connection():
            result = self.operation_manager.run_python_command(
                ['--tgt-cnt', '--json', '--max-rows', '1'], 
                "Target connection test"
            )
            
            if result['success'] and result.get('data'):
                data = result['data']
                if data.get('summary', {}).get('connection_error'):
                    self.connection_status['target'] = {
                        'connected': False,
                        'message': data.get('error_detail', 'Connection failed')
                    }
                    self.log_manager.log(LogLevel.ERROR, "Connection", "Target connection failed",
                                       {"error": data.get('error_detail')})
                else:
                    self.connection_status['target'] = {
                        'connected': True,
                        'message': f"Connected to {data.get('database', 'target')}"
                    }
                    self.log_manager.log(LogLevel.INFO, "Connection", "Target connection successful",
                                       {"database": data.get('database'), "schema": data.get('schema')})
            else:
                self.connection_status['target'] = {
                    'connected': False,
                    'message': result.get('error', 'Connection test failed')
                }
            
            if callback:
                callback('target', self.connection_status['target'])
        
        threading.Thread(target=test_connection, daemon=True).start()
    
    def test_all_connections(self, callback: Callable = None):
        """Test both connections"""
        self.log_manager.log(LogLevel.INFO, "Connection", "Testing all connections")
        
        def on_filemaker_complete(connection_type, status):
            if callback:
                callback(connection_type, status)
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
                    callback(True, self.migration_data)
            else:
                error_msg = result.get('error', 'Unknown error')
                self.log_manager.log(LogLevel.ERROR, "Status", f"Failed to refresh status: {error_msg}")
                
                if callback:
                    callback(False, error_msg)
        
        threading.Thread(target=get_status, daemon=True).start()