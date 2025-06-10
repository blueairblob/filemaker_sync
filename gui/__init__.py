#!/usr/bin/env python3
"""
FileMaker Sync GUI Package
Enhanced modular GUI components for the FileMaker to Supabase migration tool
"""

from .gui_logging import LogManager, LogLevel, LogEntry
from .gui_widgets import StatusCard, MigrationOverview, QuickActions, RecentActivity, StatusBar
from .gui_operations import OperationManager, ConnectionTester, StatusManager
from .gui_logviewer import LogViewerWindow, LogStatsWindow

__version__ = "2.0.0"
__author__ = "FileMaker Sync Team"

__all__ = [
    'LogManager',
    'LogLevel', 
    'LogEntry',
    'StatusCard',
    'MigrationOverview',
    'QuickActions',
    'RecentActivity',
    'StatusBar',
    'OperationManager',
    'ConnectionTester',
    'StatusManager',
    'LogViewerWindow',
    'LogStatsWindow'
]