#!/usr/bin/env python3
# FILE: gui/gui_logging.py
"""
GUI Logging Module
"""

import logging
import json
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import threading
import queue

class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

@dataclass
class LogEntry:
    timestamp: str
    level: str
    component: str
    message: str
    details: Optional[Dict] = None
    session_id: Optional[str] = None

class LogCaptureHandler(logging.Handler):
    """Custom logging handler that captures logs for the GUI"""
    
    def __init__(self, log_manager):
        super().__init__()
        self.log_manager = log_manager
        
    def emit(self, record):
        try:
            # Convert logging record to our LogEntry format
            entry = LogEntry(
                timestamp=datetime.fromtimestamp(record.created).isoformat(),
                level=record.levelname,
                component=record.name,
                message=record.getMessage(),
                details=getattr(record, 'details', None),
                session_id=self.log_manager.session_id
            )
            
            # Add to memory storage directly
            self.log_manager._add_log_entry(entry)
            
        except Exception:
            # Avoid recursion - don't log errors from the log handler
            pass

class LogManager:
    """Enhanced logging manager with proper GUI integration"""
    
    def __init__(self, log_dir: Path = None, config: Dict = None):
        self.log_dir = log_dir or Path("./logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # Load configuration or use defaults
        self.config = config or {}
        debug_config = self.config.get('debug', {})
        
        self.log_level = debug_config.get('log_level', 'INFO')
        self.console_logging = debug_config.get('console_logging', False)
        self.max_memory_logs = debug_config.get('max_log_entries', 1000)
        self.debug_mode = debug_config.get('debug_mode', False)
        
        # Memory storage for recent logs with thread safety
        self.memory_logs: List[LogEntry] = []
        self._log_lock = threading.Lock()
        
        # Session ID for tracking
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Event callbacks with thread safety
        self.log_callbacks: List[Callable[[LogEntry], None]] = []
        self._callback_lock = threading.Lock()
        
        # Set up comprehensive logging
        self.setup_logging_system()
        
        # Log startup info
        self.log(LogLevel.INFO, "LogManager", f"Logging initialized - Level: {self.log_level}, Console: {self.console_logging}, Debug: {self.debug_mode}")
    
    def setup_logging_system(self):
        """Set up comprehensive logging system that captures all logs"""
        # Create log file
        log_file = self.log_dir / f"filemaker_sync_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Get numeric log level
        numeric_level = getattr(logging, self.log_level.upper(), logging.INFO)
        
        # Configure root logger to capture EVERYTHING
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # Always capture debug at root level
        
        # Clear existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create formatters
        file_formatter = logging.Formatter(
            '%(asctime)s,%(msecs)03d %(levelname)-8s [%(name)s:%(lineno)d] %(message)s',
            '%Y-%m-%d:%H:%M:%S'
        )
        
        console_formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s [%(name)s] %(message)s',
            '%H:%M:%S'
        )
        
        # File handler - always enabled
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Capture everything to file
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
        
        # Console handler - conditional
        if self.console_logging:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(numeric_level)
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)
        
        # GUI capture handler - captures for memory storage
        gui_handler = LogCaptureHandler(self)
        gui_handler.setLevel(logging.DEBUG)  # Capture everything for GUI
        root_logger.addHandler(gui_handler)
        
        # Set specific logger levels based on configuration
        app_logger = logging.getLogger('FileMakerSync')
        app_logger.setLevel(numeric_level)
        
        # Configure third-party loggers
        if self.debug_mode:
            if self.config.get('debug', {}).get('verbose_sql', False):
                logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
            else:
                logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
                
            if self.config.get('debug', {}).get('debug_connections', False):
                logging.getLogger('urllib3.connectionpool').setLevel(logging.DEBUG)
            else:
                logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
        else:
            # Suppress noisy loggers in non-debug mode
            logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
            logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
            logging.getLogger('PIL').setLevel(logging.WARNING)
        
        self.logger = app_logger
    
    def _add_log_entry(self, entry: LogEntry):
        """Thread-safe method to add log entry to memory storage"""
        with self._log_lock:
            # Check if we should include this log based on our filtering
            if self.should_log_level(entry.level):
                self.memory_logs.append(entry)
                
                # Trim if too many logs
                if len(self.memory_logs) > self.max_memory_logs:
                    self.memory_logs.pop(0)
                
                # Notify callbacks in a thread-safe way
                self._notify_callbacks(entry)
    
    def _notify_callbacks(self, entry: LogEntry):
        """Thread-safe callback notification"""
        with self._callback_lock:
            callbacks_to_call = self.log_callbacks.copy()
        
        # Call callbacks outside the lock to avoid deadlocks
        for callback in callbacks_to_call:
            try:
                callback(entry)
            except Exception as e:
                # Use standard logging to avoid recursion
                self.logger.error(f"Error in log callback: {e}")
    
    def should_log_level(self, level: str) -> bool:
        """Check if a log level should be recorded based on current configuration"""
        level_hierarchy = {
            'DEBUG': 10,
            'INFO': 20,
            'WARNING': 30,
            'ERROR': 40,
            'CRITICAL': 50
        }
        
        current_level_value = level_hierarchy.get(self.log_level, 20)
        message_level_value = level_hierarchy.get(level, 20)
        
        return message_level_value >= current_level_value
    
    def should_log(self, level: LogLevel) -> bool:
        """Check if a log level should be recorded"""
        return self.should_log_level(level.value)
    
    def add_callback(self, callback: Callable[[LogEntry], None]):
        """Add callback for real-time log updates"""
        with self._callback_lock:
            self.log_callbacks.append(callback)
        self.log(LogLevel.DEBUG, "LogManager", f"Added log callback (total: {len(self.log_callbacks)})")
    
    def remove_callback(self, callback: Callable[[LogEntry], None]):
        """Remove log callback"""
        with self._callback_lock:
            if callback in self.log_callbacks:
                self.log_callbacks.remove(callback)
    
    def log(self, level: LogLevel, component: str, message: str, details: Dict = None):
        """Add a log entry through the standard logging system"""
        # Use the standard logging system which will be captured by our handler
        log_method = getattr(self.logger, level.value.lower())
        
        # Create a log record with extra details
        extra = {'details': details} if details else {}
        
        # Format the message with component
        formatted_message = f"[{component}] {message}"
        
        # Log through standard logging (will be captured by our handler)
        log_method(formatted_message, extra=extra)
    
    def log_subprocess_output(self, component: str, line: str):
        """Special method for logging subprocess output with proper parsing"""
        line = line.strip()
        if not line:
            return
        
        # Try to detect log level from subprocess output
        level = LogLevel.INFO  # Default
        
        if any(indicator in line.lower() for indicator in ['error', 'failed', 'exception']):
            level = LogLevel.ERROR
        elif any(indicator in line.lower() for indicator in ['warning', 'warn']):
            level = LogLevel.WARNING
        elif 'debug' in line.lower():
            level = LogLevel.DEBUG
        elif any(indicator in line.lower() for indicator in ['✓', 'success', 'complete']):
            level = LogLevel.INFO
        
        # Clean up the message
        clean_message = line
        
        # Remove common prefixes that might confuse the display
        prefixes_to_remove = [
            'INFO:FileMakerSync:',
            'DEBUG:FileMakerSync:',
            'WARNING:FileMakerSync:',
            'ERROR:FileMakerSync:',
        ]
        
        for prefix in prefixes_to_remove:
            if clean_message.startswith(prefix):
                clean_message = clean_message[len(prefix):].strip()
                break
        
        self.log(level, component, clean_message)
    
    def get_recent_logs(self, limit: int = 100, level_filter: str = None, component_filter: str = None) -> List[LogEntry]:
        """Get recent logs with optional filtering (thread-safe)"""
        with self._log_lock:
            logs = self.memory_logs.copy()
        
        # Apply filters
        if level_filter and level_filter != "ALL":
            logs = [log for log in logs if log.level == level_filter]
        
        if component_filter and component_filter != "ALL":
            logs = [log for log in logs if log.component == component_filter]
        
        # Return most recent first, limited
        return list(reversed(logs[-limit:]))
    
    def get_log_count(self) -> int:
        """Get total log count"""
        with self._log_lock:
            return len(self.memory_logs)
    
    def clear_logs(self):
        """Clear all memory logs"""
        with self._log_lock:
            self.memory_logs.clear()
        self.log(LogLevel.INFO, "LogManager", "Memory logs cleared")
    
    def update_log_level(self, new_level: str):
        """Update the logging level dynamically"""
        old_level = self.log_level
        self.log_level = new_level.upper()
        
        # Update logger levels
        numeric_level = getattr(logging, self.log_level, logging.INFO)
        self.logger.setLevel(numeric_level)
        
        # Update console handler if it exists
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                handler.setLevel(numeric_level)
        
        self.log(LogLevel.INFO, "LogManager", f"Log level changed from {old_level} to {self.log_level}")
    
    def toggle_console_logging(self, enable: bool):
        """Toggle console logging on/off"""
        self.console_logging = enable
        
        root_logger = logging.getLogger()
        
        # Remove existing console handlers
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                root_logger.removeHandler(handler)
        
        # Add console handler if enabled
        if enable:
            numeric_level = getattr(logging, self.log_level, logging.INFO)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(numeric_level)
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)-8s [%(name)s] %(message)s',
                '%H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
        
        self.log(LogLevel.INFO, "LogManager", f"Console logging {'enabled' if enable else 'disabled'}")
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """Get statistics about current logs (thread-safe)"""
        with self._log_lock:
            logs = self.memory_logs.copy()
        
        if not logs:
            return {
                'total_logs': 0,
                'by_level': {},
                'by_component': {},
                'oldest_entry': None,
                'newest_entry': None,
                'current_level': self.log_level,
                'console_enabled': self.console_logging,
                'debug_mode': self.debug_mode
            }
        
        # Count by level and component
        by_level = {}
        by_component = {}
        
        for log in logs:
            by_level[log.level] = by_level.get(log.level, 0) + 1
            by_component[log.component] = by_component.get(log.component, 0) + 1
        
        return {
            'total_logs': len(logs),
            'by_level': by_level,
            'by_component': by_component,
            'oldest_entry': logs[0].timestamp if logs else None,
            'newest_entry': logs[-1].timestamp if logs else None,
            'current_level': self.log_level,
            'console_enabled': self.console_logging,
            'debug_mode': self.debug_mode,
            'session_id': self.session_id
        }
    
    def export_logs(self, filepath: Path, logs: List[LogEntry] = None):
        """Export logs to file"""
        if logs is None:
            with self._log_lock:
                logs = self.memory_logs.copy()
        
        if filepath.suffix.lower() == '.json':
            # Export as JSON
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump([asdict(log) for log in logs], f, indent=2)
        else:
            # Export as text
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("FileMaker Sync Log Export\n")
                f.write("=" * 50 + "\n")
                f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Session ID: {self.session_id}\n")
                f.write(f"Log Level: {self.log_level}\n")
                f.write(f"Debug Mode: {self.debug_mode}\n")
                f.write(f"Total Entries: {len(logs)}\n\n")
                
                for log in logs:
                    f.write(f"[{log.timestamp}] {log.level} - {log.component}\n")
                    f.write(f"  {log.message}\n")
                    if log.details and self.debug_mode:
                        f.write(f"  Details: {json.dumps(log.details)}\n")
                    f.write("\n")
    
    def test_logging(self):
        """Generate test log entries for debugging the logging system"""
        self.log(LogLevel.DEBUG, "Test", "This is a DEBUG test message")
        self.log(LogLevel.INFO, "Test", "This is an INFO test message")
        self.log(LogLevel.WARNING, "Test", "This is a WARNING test message") 
        self.log(LogLevel.ERROR, "Test", "This is an ERROR test message")
        self.log(LogLevel.CRITICAL, "Test", "This is a CRITICAL test message")
        
        # Test with details
        self.log(LogLevel.INFO, "Test", "Message with details", {"test_key": "test_value", "number": 42})
        
        # Test different components
        self.log(LogLevel.INFO, "Database", "Database connection test")
        self.log(LogLevel.INFO, "GUI", "GUI component test")
        self.log(LogLevel.INFO, "Operation", "Operation test")


# Enhanced error tracking with better integration
class ErrorTracker:
    """Track and analyze error patterns with configurable sensitivity"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
    
    def get_error_summary(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get summary of recent errors with enhanced analysis"""
        recent_logs = self.log_manager.get_recent_logs(limit=1000)
        error_logs = [log for log in recent_logs if log.level in ['ERROR', 'CRITICAL']]
        warning_logs = [log for log in recent_logs if log.level == 'WARNING']
        
        # Group by component
        by_component = {}
        for log in error_logs:
            component = log.component
            if component not in by_component:
                by_component[component] = []
            by_component[component].append(log)
        
        # Analyze error patterns
        error_patterns = {}
        for log in error_logs:
            message_lower = log.message.lower()
            if 'connection' in message_lower or 'connect' in message_lower:
                pattern = 'Connection Issues'
            elif 'permission' in message_lower or 'access' in message_lower or 'denied' in message_lower:
                pattern = 'Permission Issues'
            elif 'timeout' in message_lower:
                pattern = 'Timeout Issues'
            elif 'not found' in message_lower or 'missing' in message_lower:
                pattern = 'Resource Not Found'
            elif 'sql' in message_lower or 'database' in message_lower:
                pattern = 'Database Issues'
            elif 'file' in message_lower or 'directory' in message_lower:
                pattern = 'File System Issues'
            else:
                pattern = 'Other Errors'
            
            error_patterns[pattern] = error_patterns.get(pattern, 0) + 1
        
        return {
            'total_errors': len(error_logs),
            'total_warnings': len(warning_logs),
            'by_component': {k: len(v) for k, v in by_component.items()},
            'error_patterns': error_patterns,
            'recent_errors': error_logs[:10],  # Last 10 errors
            'severity_assessment': self._assess_severity(error_logs, warning_logs),
            'session_id': self.log_manager.session_id
        }
    
    def _assess_severity(self, errors: List, warnings: List) -> str:
        """Assess overall system health based on recent logs"""
        error_count = len(errors)
        warning_count = len(warnings)
        
        if error_count == 0 and warning_count == 0:
            return 'Healthy'
        elif error_count == 0 and warning_count < 5:
            return 'Good'
        elif error_count < 3 and warning_count < 10:
            return 'Fair'
        elif error_count < 10:
            return 'Poor'
        else:
            return 'Critical'


# Context manager for performance logging
class PerformanceLogger:
    """Context manager for logging operation performance"""
    
    def __init__(self, logger: LogManager, component: str, operation: str):
        self.logger = logger
        self.component = component
        self.operation = operation
        self.start_time = None
    
    def __enter__(self):
        if self.logger.debug_mode:
            import time
            self.start_time = time.time()
            self.logger.log(LogLevel.DEBUG, self.component, f"Starting: {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.logger.debug_mode and self.start_time:
            import time
            duration = time.time() - self.start_time
            
            if exc_type:
                self.logger.log(LogLevel.ERROR, self.component, 
                              f"Failed: {self.operation} (after {duration:.2f}s): {exc_val}")
            else:
                # Choose log level based on duration
                if duration > 10:
                    level = LogLevel.WARNING
                    message = f"SLOW: {self.operation} took {duration:.2f}s"
                elif duration > 5:
                    level = LogLevel.INFO
                    message = f"Performance: {self.operation} took {duration:.2f}s"
                else:
                    level = LogLevel.DEBUG
                    message = f"Performance: {self.operation} took {duration:.2f}s"
                
                self.logger.log(level, self.component, message, {'duration_seconds': duration})


# Utility functions
def create_debug_logger(component_name: str, config: Dict = None) -> LogManager:
    """Create a logger with debug configuration"""
    return LogManager(config=config)

def log_function_call(logger: LogManager, component: str, function_name: str, args: tuple = None, kwargs: dict = None):
    """Log function calls for debugging (only in debug mode)"""
    if logger.debug_mode:
        details = {}
        if args:
            details['args'] = str(args)[:100]  # Limit length
        if kwargs:
            details['kwargs'] = {k: str(v)[:50] for k, v in kwargs.items()}  # Limit length
        
        logger.log(LogLevel.DEBUG, component, f"Function call: {function_name}", details)