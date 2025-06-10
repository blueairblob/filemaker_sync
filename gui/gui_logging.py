#!/usr/bin/env python3
"""
GUI Logging Module
Handles all logging functionality for the FileMaker Sync GUI
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
    """Enhanced logging manager with file and memory storage"""
    
    def __init__(self, log_dir: Path = None):
        self.log_dir = log_dir or Path("./logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # Memory storage for recent logs
        self.memory_logs: List[LogEntry] = []
        self.max_memory_logs = 1000
        
        # Session ID for tracking
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Set up file logging
        self.setup_file_logging()
        
        # Event callbacks
        self.log_callbacks: List[Callable[[LogEntry], None]] = []
    
    def setup_file_logging(self):
        """Set up file-based logging"""
        log_file = self.log_dir / f"filemaker_sync_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(name)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d:%H:%M:%S',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger('FileMakerSync')
    
    def add_callback(self, callback: Callable[[LogEntry], None]):
        """Add callback for real-time log updates"""
        self.log_callbacks.append(callback)
    
    def log(self, level: LogLevel, component: str, message: str, details: Dict = None):
        """Add a log entry"""
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
        
        # Write to file
        log_method = getattr(self.logger, level.value.lower())
        if details:
            log_method(f"[{component}] {message} | Details: {json.dumps(details)}")
        else:
            log_method(f"[{component}] {message}")
        
        # Notify callbacks
        for callback in self.log_callbacks:
            try:
                callback(entry)
            except Exception as e:
                self.logger.error(f"Error in log callback: {e}")
    
    def get_recent_logs(self, limit: int = 100, level_filter: str = None) -> List[LogEntry]:
        """Get recent logs with optional filtering"""
        logs = self.memory_logs[-limit:]
        
        if level_filter and level_filter != "ALL":
            logs = [log for log in logs if log.level == level_filter]
        
        return list(reversed(logs))  # Most recent first
    
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
                f.write("=" * 50 + "\n\n")
                for log in logs:
                    f.write(f"[{log.timestamp}] {log.level} - {log.component}\n")
                    f.write(f"  {log.message}\n")
                    if log.details:
                        f.write(f"  Details: {json.dumps(log.details)}\n")
                    f.write("\n")

class ErrorTracker:
    """Track and analyze error patterns"""
    
    def __init__(self, log_manager: LogManager):
        self.log_manager = log_manager
    
    def get_error_summary(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get summary of recent errors"""
        recent_logs = self.log_manager.get_recent_logs(limit=1000)
        error_logs = [log for log in recent_logs if log.level in ['ERROR', 'CRITICAL']]
        
        # Group by component
        by_component = {}
        for log in error_logs:
            component = log.component
            if component not in by_component:
                by_component[component] = []
            by_component[component].append(log)
        
        return {
            'total_errors': len(error_logs),
            'by_component': {k: len(v) for k, v in by_component.items()},
            'recent_errors': error_logs[:10]  # Last 10 errors
        }