#!/usr/bin/env python3
# FILE: gui/gui_logging.py
"""
Enhanced GUI Logging Module with Configurable Debug Levels
"""

import logging
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, asdict
from enum import Enum

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

class LogManager:
    """Enhanced logging manager with configurable debug levels"""
    
    def __init__(self, log_dir: Path = None, config: Dict = None):
        self.log_dir = log_dir or Path("./logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # Load configuration or use defaults
        self.config = config or {}
        self.log_level = self.config.get('debug', {}).get('log_level', 'INFO')
        self.console_logging = self.config.get('debug', {}).get('console_logging', False)
        self.max_memory_logs = self.config.get('debug', {}).get('max_log_entries', 1000)
        self.debug_mode = self.config.get('debug', {}).get('debug_mode', False)
        
        # Memory storage for recent logs
        self.memory_logs: List[LogEntry] = []
        
        # Session ID for tracking
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Set up file logging
        self.setup_file_logging()
        
        # Event callbacks
        self.log_callbacks: List[Callable[[LogEntry], None]] = []
        
        # Log startup info
        self.log(LogLevel.INFO, "LogManager", f"Logging initialized - Level: {self.log_level}, Console: {self.console_logging}, Debug: {self.debug_mode}")
    
    def setup_file_logging(self):
        """Set up file-based logging with configurable level"""
        log_file = self.log_dir / f"filemaker_sync_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Convert string log level to logging level
        numeric_level = getattr(logging, self.log_level.upper(), logging.INFO)
        
        # Clear any existing handlers
        logging.getLogger().handlers.clear()
        
        # Create handlers list
        handlers = [logging.FileHandler(log_file, encoding='utf-8')]
        
        # Add console handler if enabled
        if self.console_logging:
            console_handler = logging.StreamHandler(sys.stdout)
            handlers.append(console_handler)
        
        # Configure logging
        logging.basicConfig(
            level=numeric_level,
            format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(name)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d:%H:%M:%S',
            handlers=handlers,
            force=True  # Override existing configuration
        )
        
        self.logger = logging.getLogger('FileMakerSync')
        
        # Set specific loggers to appropriate levels
        if self.debug_mode:
            # Enable debug for specific components
            logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO if self.config.get('debug', {}).get('verbose_sql', False) else logging.WARNING)
            logging.getLogger('urllib3.connectionpool').setLevel(logging.DEBUG if self.config.get('debug', {}).get('debug_connections', False) else logging.WARNING)
        else:
            # Suppress noisy loggers in non-debug mode
            logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
            logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
            logging.getLogger('PIL').setLevel(logging.WARNING)
    
    def should_log(self, level: LogLevel) -> bool:
        """Check if a log level should be recorded based on current configuration"""
        level_hierarchy = {
            'DEBUG': 10,
            'INFO': 20,
            'WARNING': 30,
            'ERROR': 40,
            'CRITICAL': 50
        }
        
        current_level_value = level_hierarchy.get(self.log_level, 20)  # Default to INFO
        message_level_value = level_hierarchy.get(level.value, 20)
        
        return message_level_value >= current_level_value
    
    def add_callback(self, callback: Callable[[LogEntry], None]):
        """Add callback for real-time log updates"""
        self.log_callbacks.append(callback)
    
    def log(self, level: LogLevel, component: str, message: str, details: Dict = None):
        """Add a log entry with level filtering"""
        # Check if we should log this level
        if not self.should_log(level):
            return
        
        entry = LogEntry(
            timestamp=datetime.now().isoformat(),
            level=level.value,
            component=component,
            message=message,
            details=details,
            session_id=self.session_id
        )
        
        # Add to memory storage
        self.memory_logs.append(entry)
        if len(self.memory_logs) > self.max_memory_logs:
            self.memory_logs.pop(0)
        
        # Write to file/console using standard logging
        log_method = getattr(self.logger, level.value.lower())
        if details:
            detail_str = f" | Details: {json.dumps(details)}" if self.debug_mode else ""
            log_method(f"[{component}] {message}{detail_str}")
        else:
            log_method(f"[{component}] {message}")
        
        # Notify callbacks
        for callback in self.log_callbacks:
            try:
                callback(entry)
            except Exception as e:
                # Use standard logging to avoid recursion
                self.logger.error(f"Error in log callback: {e}")
    
    def get_recent_logs(self, limit: int = 100, level_filter: str = None) -> List[LogEntry]:
        """Get recent logs with optional filtering"""
        logs = self.memory_logs[-limit:]
        
        if level_filter and level_filter != "ALL":
            logs = [log for log in logs if log.level == level_filter]
        
        return list(reversed(logs))  # Most recent first
    
    def update_log_level(self, new_level: str):
        """Update the logging level dynamically"""
        old_level = self.log_level
        self.log_level = new_level.upper()
        
        # Reconfigure logging
        numeric_level = getattr(logging, self.log_level, logging.INFO)
        self.logger.setLevel(numeric_level)
        
        # Update all handlers
        for handler in self.logger.handlers:
            handler.setLevel(numeric_level)
        
        self.log(LogLevel.INFO, "LogManager", f"Log level changed from {old_level} to {self.log_level}")
    
    def toggle_console_logging(self, enable: bool):
        """Toggle console logging on/off"""
        self.console_logging = enable
        
        # Remove existing console handlers
        for handler in self.logger.handlers[:]:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                self.logger.removeHandler(handler)
        
        # Add console handler if enabled
        if enable:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, self.log_level, logging.INFO))
            formatter = logging.Formatter(
                '%(asctime)s,%(msecs)03d %(levelname)-8s [%(name)s:%(lineno)d] %(message)s',
                '%Y-%m-%d:%H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        self.log(LogLevel.INFO, "LogManager", f"Console logging {'enabled' if enable else 'disabled'}")
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """Get statistics about current logs"""
        if not self.memory_logs:
            return {
                'total_logs': 0,
                'by_level': {},
                'by_component': {},
                'oldest_entry': None,
                'newest_entry': None
            }
        
        # Count by level
        by_level = {}
        by_component = {}
        
        for log in self.memory_logs:
            by_level[log.level] = by_level.get(log.level, 0) + 1
            by_component[log.component] = by_component.get(log.component, 0) + 1
        
        return {
            'total_logs': len(self.memory_logs),
            'by_level': by_level,
            'by_component': by_component,
            'oldest_entry': self.memory_logs[0].timestamp if self.memory_logs else None,
            'newest_entry': self.memory_logs[-1].timestamp if self.memory_logs else None,
        return {
            'total_logs': len(self.memory_logs),
            'by_level': by_level,
            'by_component': by_component,
            'oldest_entry': self.memory_logs[0].timestamp if self.memory_logs else None,
            'newest_entry': self.memory_logs[-1].timestamp if self.memory_logs else None,
            'current_level': self.log_level,
            'console_enabled': self.console_logging,
            'debug_mode': self.debug_mode
        }
    
    def export_logs(self, filepath: Path, logs: List[LogEntry] = None):
        """Export logs to file"""
        if logs is None:
            logs = self.memory_logs
        
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
                f.write(f"Log Level: {self.log_level}\n")
                f.write(f"Debug Mode: {self.debug_mode}\n")
                f.write(f"Total Entries: {len(logs)}\n\n")
                
                for log in logs:
                    f.write(f"[{log.timestamp}] {log.level} - {log.component}\n")
                    f.write(f"  {log.message}\n")
                    if log.details and self.debug_mode:
                        f.write(f"  Details: {json.dumps(log.details)}\n")
                    f.write("\n")

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
            # Simple pattern matching - look for common error types
            message_lower = log.message.lower()
            if 'connection' in message_lower:
                pattern = 'Connection Issues'
            elif 'permission' in message_lower or 'access' in message_lower:
                pattern = 'Permission Issues'
            elif 'timeout' in message_lower:
                pattern = 'Timeout Issues'
            elif 'not found' in message_lower:
                pattern = 'Resource Not Found'
            else:
                pattern = 'Other Errors'
            
            error_patterns[pattern] = error_patterns.get(pattern, 0) + 1
        
        return {
            'total_errors': len(error_logs),
            'total_warnings': len(warning_logs),
            'by_component': {k: len(v) for k, v in by_component.items()},
            'error_patterns': error_patterns,
            'recent_errors': error_logs[:10],  # Last 10 errors
            'severity_assessment': self._assess_severity(error_logs, warning_logs)
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

# Enhanced logging helper functions
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

def log_performance(logger: LogManager, component: str, operation: str, duration: float, details: Dict = None):
    """Log performance metrics (only in debug mode with performance profiling enabled)"""
    if logger.debug_mode and logger.config.get('debug', {}).get('profile_performance', False):
        perf_details = {'duration_seconds': duration}
        if details:
            perf_details.update(details)
        
        # Choose log level based on duration
        if duration > 10:
            level = LogLevel.WARNING
            message = f"SLOW: {operation} took {duration:.2f}s"
        elif duration > 5:
            level = LogLevel.INFO
            message = f"Performance: {operation} took {duration:.2f}s"
        else:
            level = LogLevel.DEBUG
            message = f"Performance: {operation} took {duration:.2f}s"
        
        logger.log(level, component, message, perf_details)

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
                log_performance(self.logger, self.component, self.operation, duration)