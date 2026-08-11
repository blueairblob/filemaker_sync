#!/usr/bin/env python3
"""
FileMaker Extract - Enhanced Version with Graceful Connection Handling
Handles cases where source or target databases are unavailable for info-only operations
"""

import sys
import argparse
import logging
import pandas as pd
import warnings
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from tqdm import tqdm

# Import our new modules
from config_manager import ConfigManager
from database_connections import DatabaseManager
from data_exporter import DataExporter, ExportOptions


class FileMakerMigrationManager:
    """Main orchestrator for FileMaker migration operations"""
    
    def __init__(self, args):
        self.args = args
        self.logger = self._setup_logging()
        
        # Initialize components
        self.config_manager = ConfigManager(getattr(args, 'config_file', 'config.toml'))
        self.config = self.config_manager.load_config()
        
        # Override DSN if provided in args
        if hasattr(args, 'dsn') and args.dsn:
            self.config_manager.update_dsn(args.dsn)
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
        
        # Initialize export options from args
        self.export_options = ExportOptions(
            export_to_files=getattr(args, 'fn_exp', False),
            export_to_database=getattr(args, 'db_exp', True),
            include_ddl=getattr(args, 'ddl', False),
            include_dml=getattr(args, 'dml', False),
            reset_data=getattr(args, 'del_data', False),
            reset_database=getattr(args, 'del_db', False),
            file_format=getattr(args, 'fn_fmt', 'multi'),
            max_rows=getattr(args, 'max_rows', 'all'),
            start_from=getattr(args, 'start_from', None),
            debug=getattr(args, 'debug', False)
        )
        
        # Initialize data exporter
        self.exporter = DataExporter(self.config, self.export_options)
        
        # Connection status tracking
        self.connection_status = {
            'filemaker': {'connected': False, 'message': 'Not tested'},
            'target': {'connected': False, 'message': 'Not tested'}
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration"""
        # Create logs directory
        log_dir = Path("./logs")
        log_dir.mkdir(exist_ok=True)
        
        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d")
        log_file = log_dir / f'filemaker_extract_{timestamp}.log'
        
        logger = logging.getLogger('filemaker_migration')
        logger.setLevel(logging.DEBUG if getattr(self.args, 'debug', False) else logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        formatter = logging.Formatter(
            '%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
            '%Y-%m-%d:%H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Console handler for debug mode
        if getattr(self.args, 'debug', False):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        logger.info(f"Logging initialized - {log_file}")
        return logger
    
    def test_connections_selectively(self, require_filemaker: bool = True, require_target: bool = True) -> Dict[str, Tuple[bool, str]]:
        """Test database connections with selective requirements"""
        self.logger.info("Testing database connections...")
        
        results = {}
        
        # Test FileMaker if required or always test for info
        try:
            if require_filemaker:
                fm_success, fm_message = self.db_manager.filemaker.test_connection()
                results['filemaker'] = (fm_success, fm_message)
                self.connection_status['filemaker'] = {
                    'connected': fm_success, 
                    'message': fm_message
                }
                
                if fm_success:
                    self.logger.info(f"✓ FileMaker: {fm_message}")
                else:
                    self.logger.error(f"✗ FileMaker: {fm_message}")
            else:
                # Still try to test but don't fail if it doesn't work
                try:
                    fm_success, fm_message = self.db_manager.filemaker.test_connection()
                    results['filemaker'] = (fm_success, fm_message)
                    self.connection_status['filemaker'] = {
                        'connected': fm_success, 
                        'message': fm_message
                    }
                    self.logger.info(f"✓ FileMaker: {fm_message}")
                except Exception as e:
                    fm_message = str(e)
                    results['filemaker'] = (False, fm_message)
                    self.connection_status['filemaker'] = {
                        'connected': False, 
                        'message': fm_message
                    }
                    self.logger.warning(f"⚠ FileMaker: {fm_message}")
        except Exception as e:
            results['filemaker'] = (False, str(e))
            self.connection_status['filemaker'] = {
                'connected': False, 
                'message': str(e)
            }
            if require_filemaker:
                self.logger.error(f"✗ FileMaker: {str(e)}")
            else:
                self.logger.warning(f"⚠ FileMaker: {str(e)}")
        
        # Test target database if required
        try:
            if require_target:
                target_success, target_message = self.db_manager.target_db.test_connection()
                results['target'] = (target_success, target_message)
                self.connection_status['target'] = {
                    'connected': target_success, 
                    'message': target_message
                }
                
                if target_success:
                    self.logger.info(f"✓ Target: {target_message}")
                    # Set PostgreSQL version if available
                    if self.config.db_type == 'supabase':
                        _, version = self.db_manager.target_db.get_postgres_version()
                        self.exporter.set_postgres_version(version)
                else:
                    self.logger.error(f"✗ Target: {target_message}")
            else:
                # Still try to test but don't fail if it doesn't work
                try:
                    target_success, target_message = self.db_manager.target_db.test_connection()
                    results['target'] = (target_success, target_message)
                    self.connection_status['target'] = {
                        'connected': target_success, 
                        'message': target_message
                    }
                    self.logger.info(f"✓ Target: {target_message}")
                    
                    # Set PostgreSQL version if available
                    if target_success and self.config.db_type == 'supabase':
                        _, version = self.db_manager.target_db.get_postgres_version()
                        self.exporter.set_postgres_version(version)
                except Exception as e:
                    target_message = str(e)
                    results['target'] = (False, target_message)
                    self.connection_status['target'] = {
                        'connected': False, 
                        'message': target_message
                    }
                    self.logger.warning(f"⚠ Target: {target_message}")
        except Exception as e:
            results['target'] = (False, str(e))
            self.connection_status['target'] = {
                'connected': False, 
                'message': str(e)
            }
            if require_target:
                self.logger.error(f"✗ Target: {str(e)}")
            else:
                self.logger.warning(f"⚠ Target: {str(e)}")
        
        return results
    
    def validate_connections(self, require_filemaker: bool = True, require_target: bool = True) -> bool:
        """Test connections and validate based on requirements"""
        results = self.test_connections_selectively(require_filemaker, require_target)
        
        # Check if required connections are successful
        success = True
        
        if require_filemaker and 'filemaker' in results:
            if not results['filemaker'][0]:
                success = False
        
        if require_target and 'target' in results:
            if not results['target'][0]:
                success = False
        
        return success
    
    def get_table_list_safe(self, tables_arg: str = 'all') -> Tuple[List[str], bool]:
        """Get list of tables to process with error handling"""
        try:
            # Only try to get tables from FileMaker if connection is available
            if self.connection_status['filemaker']['connected']:
                available_tables = self.db_manager.get_filemaker_tables()
            else:
                # Fallback to hardcoded common table names or config
                self.logger.warning("FileMaker not connected, using fallback table list")
                available_tables = ['ratcatalogue', 'ratbuilders', 'ratroutes', 'ratcollections', 'ratlabels']
                
            if tables_arg == 'all':
                return available_tables, True
            
            # Parse table list from argument
            import re
            delimiters = [";", "|", ",", " "]
            pattern = "|".join(map(re.escape, delimiters))
            requested_tables = [t.strip() for t in re.split(pattern, tables_arg) if t.strip()]
            
            # Validate requested tables exist (only if we have a connection)
            if self.connection_status['filemaker']['connected']:
                invalid_tables = set(requested_tables) - set(available_tables)
                if invalid_tables:
                    self.logger.error(f"Invalid tables requested: {invalid_tables}")
                    self.logger.info(f"Available tables: {available_tables}")
                    return [], False
            
            return requested_tables, True
            
        except Exception as e:
            self.logger.error(f"Error getting table list: {e}")
            return [], False
    
    def run_source_count(self, tables: List[str] = None, output_json: bool = False) -> bool:
        """Get row counts for source FileMaker tables"""
        try:
            self.logger.info("Getting FileMaker Pro source table counts...")
            
            # Test FileMaker connection specifically
            if not self.validate_connections(require_filemaker=True, require_target=False):
                if output_json:
                    import json
                    error_result = {
                        'timestamp': datetime.now().isoformat(),
                        'database': 'FileMaker Pro',
                        'dsn': self.config.source_db.dsn,
                        'error': 'Connection failed',
                        'error_detail': self.connection_status['filemaker']['message'],
                        'tables': {},
                        'summary': {
                            'total_tables': 0,
                            'total_rows': 0,
                            'connection_error': True
                        }
                    }
                    print(json.dumps(error_result, indent=2))
                else:
                    self.logger.error(f"Cannot get source counts: {self.connection_status['filemaker']['message']}")
                return False
            
            if tables is None:
                tables, success = self.get_table_list_safe(getattr(self.args, 'tables_to_export', 'all'))
                if not success:
                    return False
            
            source_counts = self.db_manager.get_source_table_counts(tables)
            
            if output_json:
                import json
                result = {
                    'timestamp': datetime.now().isoformat(),
                    'database': self.config.source_db.name[1],
                    'dsn': self.config.source_db.dsn,
                    'tables': source_counts,
                    'summary': {
                        'total_tables': len(tables),
                        'total_rows': sum(count for count in source_counts.values() if count >= 0),
                        'tables_with_errors': sum(1 for count in source_counts.values() if count < 0),
                        'connection_error': False
                    }
                }
                print(json.dumps(result, indent=2))
            else:
                self.logger.info(f"FileMaker Pro Table Counts ({self.config.source_db.dsn}):")
                total_rows = 0
                for table, count in source_counts.items():
                    if count >= 0:
                        self.logger.info(f"  {table}: {count:,} rows")
                        total_rows += count
                    else:
                        self.logger.error(f"  {table}: Error getting count")
                
                self.logger.info(f"Total rows across {len(tables)} tables: {total_rows:,}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Source count failed: {e}")
            if output_json:
                import json
                error_result = {
                    'timestamp': datetime.now().isoformat(),
                    'database': 'FileMaker Pro', 
                    'error': str(e),
                    'tables': {},
                    'summary': {'total_tables': 0, 'total_rows': 0, 'connection_error': True}
                }
                print(json.dumps(error_result, indent=2))
            return False
    
    def run_target_count(self, tables: List[str] = None, output_json: bool = False) -> bool:
        """Get row counts for target Supabase tables"""
        try:
            self.logger.info(f"Getting {self.config.target_db.name[1]} target table counts...")
            
            # Test target connection specifically
            if not self.validate_connections(require_filemaker=False, require_target=True):
                if output_json:
                    import json
                    error_result = {
                        'timestamp': datetime.now().isoformat(),
                        'database': f"{self.config.target_db.name[1]} ({self.config.db_type})",
                        'host': self.config.target_db.host,
                        'schema': self.config.mig_schema,
                        'error': 'Connection failed',
                        'error_detail': self.connection_status['target']['message'],
                        'tables': {},
                        'summary': {
                            'total_tables': 0,
                            'total_rows': 0,
                            'connection_error': True
                        }
                    }
                    print(json.dumps(error_result, indent=2))
                else:
                    self.logger.error(f"Cannot get target counts: {self.connection_status['target']['message']}")
                return False
            
            if tables is None:
                tables, success = self.get_table_list_safe(getattr(self.args, 'tables_to_export', 'all'))
                if not success:
                    # For target counts, we can still proceed with fallback tables
                    tables = ['ratcatalogue', 'ratbuilders', 'ratroutes', 'ratcollections', 'ratlabels']
            
            target_counts = self.db_manager.get_target_table_counts(tables)
            
            if output_json:
                import json
                result = {
                    'timestamp': datetime.now().isoformat(),
                    'database': f"{self.config.target_db.name[1]} ({self.config.db_type})",
                    'host': self.config.target_db.host,
                    'schema': self.config.mig_schema,
                    'tables': target_counts,
                    'summary': {
                        'total_tables': len(tables),
                        'total_rows': sum(count for count in target_counts.values() if count >= 0),
                        'tables_migrated': sum(1 for count in target_counts.values() if count > 0),
                        'tables_empty': sum(1 for count in target_counts.values() if count == 0),
                        'tables_with_errors': sum(1 for count in target_counts.values() if count < 0),
                        'connection_error': False
                    }
                }
                print(json.dumps(result, indent=2))
            else:
                self.logger.info(f"{self.config.target_db.name[1]} Table Counts (Schema: {self.config.mig_schema}):")
                total_rows = 0
                migrated_tables = 0
                for table, count in target_counts.items():
                    if count > 0:
                        self.logger.info(f"  {table}: {count:,} rows")
                        total_rows += count
                        migrated_tables += 1
                    elif count == 0:
                        self.logger.info(f"  {table}: empty (not migrated)")
                    else:
                        self.logger.error(f"  {table}: Error getting count")
                
                self.logger.info(f"Total rows across {migrated_tables} migrated tables: {total_rows:,}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Target count failed: {e}")
            if output_json:
                import json
                error_result = {
                    'timestamp': datetime.now().isoformat(),
                    'database': f"{self.config.target_db.name[1]} ({self.config.db_type})",
                    'error': str(e),
                    'tables': {},
                    'summary': {'total_tables': 0, 'total_rows': 0, 'connection_error': True}
                }
                print(json.dumps(error_result, indent=2))
            return False
    
    def run_migration_status(self, tables: List[str] = None, output_json: bool = False) -> bool:
        """Get comprehensive migration status comparing source and target"""
        try:
            self.logger.info("Getting migration status comparison...")
            
            # Test both connections, but allow partial results
            fm_available = self.validate_connections(require_filemaker=True, require_target=False)
            target_available = self.validate_connections(require_filemaker=False, require_target=True)
            
            if not fm_available and not target_available:
                if output_json:
                    import json
                    error_result = {
                        'timestamp': datetime.now().isoformat(),
                        'error': 'Both database connections failed',
                        'filemaker_error': self.connection_status['filemaker']['message'],
                        'target_error': self.connection_status['target']['message'],
                        'summary': {'connection_error': True}
                    }
                    print(json.dumps(error_result, indent=2))
                else:
                    self.logger.error("Cannot get migration status: both database connections failed")
                return False
            
            if tables is None:
                tables, success = self.get_table_list_safe(getattr(self.args, 'tables_to_export', 'all'))
                if not success or not tables:
                    # Use fallback tables
                    tables = ['ratcatalogue', 'ratbuilders', 'ratroutes', 'ratcollections', 'ratlabels']
            
            # Get counts from available databases
            source_counts = {}
            target_counts = {}
            
            if fm_available:
                try:
                    source_counts = self.db_manager.get_source_table_counts(tables)
                except Exception as e:
                    self.logger.warning(f"Error getting source counts: {e}")
                    source_counts = {table: -1 for table in tables}
            else:
                source_counts = {table: -1 for table in tables}
            
            if target_available:
                try:
                    target_counts = self.db_manager.get_target_table_counts(tables)
                except Exception as e:
                    self.logger.warning(f"Error getting target counts: {e}")
                    target_counts = {table: -1 for table in tables}
            else:
                target_counts = {table: -1 for table in tables}
            
            status = {
                'timestamp': datetime.now().isoformat(),
                'source_database': self.config.source_db.name[1],
                'target_database': f"{self.config.target_db.name[1]} ({self.config.db_type})",
                'migration_schema': self.config.mig_schema,
                'connection_status': {
                    'filemaker': self.connection_status['filemaker'],
                    'target': self.connection_status['target']
                },
                'tables': {},
                'summary': {
                    'total_tables': len(tables),
                    'source_total_rows': sum(count for count in source_counts.values() if count >= 0),
                    'target_total_rows': sum(count for count in target_counts.values() if count >= 0),
                    'tables_migrated': 0,
                    'tables_empty_target': 0,
                    'tables_with_errors': 0
                }
            }
            
            for table in tables:
                source_count = source_counts.get(table, -1)
                target_count = target_counts.get(table, -1)
                
                # Calculate status
                if source_count == -1 and target_count == -1:
                    table_status = 'both_error'
                elif source_count == -1:
                    table_status = 'source_error'
                elif target_count == -1:
                    table_status = 'target_error'
                elif target_count == 0:
                    table_status = 'not_migrated'
                    status['summary']['tables_empty_target'] += 1
                elif target_count == source_count:
                    table_status = 'fully_migrated'
                    status['summary']['tables_migrated'] += 1
                else:
                    table_status = 'partially_migrated'
                    status['summary']['tables_migrated'] += 1
                
                if 'error' in table_status:
                    status['summary']['tables_with_errors'] += 1
                
                status['tables'][table] = {
                    'source_rows': source_count,
                    'target_rows': target_count,
                    'status': table_status,
                    'migration_percentage': (target_count / source_count * 100) if source_count > 0 else 0
                }
            
            if output_json:
                import json
                print(json.dumps(status, indent=2))
            else:
                self.logger.info("Migration Status Summary:")
                self.logger.info(f"  Source: {status['source_database']} (Connected: {fm_available})")
                self.logger.info(f"  Target: {status['target_database']} (Connected: {target_available})")
                self.logger.info(f"  Schema: {status['migration_schema']}")
                self.logger.info(f"  Total Tables: {status['summary']['total_tables']}")
                
                if fm_available:
                    self.logger.info(f"  Source Total Rows: {status['summary']['source_total_rows']:,}")
                else:
                    self.logger.warning(f"  Source Total Rows: Unable to retrieve (FileMaker not connected)")
                
                if target_available:
                    self.logger.info(f"  Target Total Rows: {status['summary']['target_total_rows']:,}")
                else:
                    self.logger.warning(f"  Target Total Rows: Unable to retrieve (Target not connected)")
                
                self.logger.info(f"  Tables Migrated: {status['summary']['tables_migrated']}")
                self.logger.info(f"  Tables Empty: {status['summary']['tables_empty_target']}")
                
                self.logger.info("\nPer-Table Status:")
                for table, info in status['tables'].items():
                    status_icon = {
                        'fully_migrated': '✓',
                        'partially_migrated': '⚠',
                        'not_migrated': '✗',
                        'source_error': '❌',
                        'target_error': '❌',
                        'both_error': '💥'
                    }.get(info['status'], '?')
                    
                    source_display = f"{info['source_rows']:,}" if info['source_rows'] >= 0 else "N/A"
                    target_display = f"{info['target_rows']:,}" if info['target_rows'] >= 0 else "N/A"
                    
                    self.logger.info(
                        f"  {status_icon} {table}: {source_display} → {target_display} "
                        f"({info['migration_percentage']:.1f}%)"
                    )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Migration status failed: {e}")
            if output_json:
                import json
                error_result = {
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e),
                    'summary': {'connection_error': True}
                }
                print(json.dumps(error_result, indent=2))
            return False
    
    def run_info_only(self, output_json: bool = False) -> bool:
        """Get basic information without requiring database connections"""
        try:
            self.logger.info("Getting system information...")
            
            # Test connections but don't require them
            self.test_connections_selectively(require_filemaker=False, require_target=False)
            
            # Try to get table list
            tables, table_success = self.get_table_list_safe(getattr(self.args, 'tables_to_export', 'all'))
            if not table_success or not tables:
                tables = ['ratcatalogue', 'ratbuilders', 'ratroutes', 'ratcollections', 'ratlabels']
                self.logger.warning("Using fallback table list")
            
            if output_json:
                import json
                info = {
                    'timestamp': datetime.now().isoformat(),
                    'source_database': self.config.source_db.name[1],
                    'source_dsn': self.config.source_db.dsn,
                    'target_database': f"{self.config.target_db.name[1]} ({self.config.db_type})",
                    'target_host': self.config.target_db.host,
                    'migration_schema': self.config.mig_schema,
                    'connection_status': self.connection_status,
                    'tables_available': tables,
                    'table_count': len(tables),
                    'export_settings': {
                        'export_path': self.config.export.path,
                        'export_prefix': self.config.export.prefix,
                        'image_formats': self.config.export.image_formats_supported
                    }
                }
                print(json.dumps(info, indent=2))
            else:
                self.logger.info(f"System Information:")
                self.logger.info(f"  Source: {self.config.source_db.name[1]} (DSN: {self.config.source_db.dsn})")
                self.logger.info(f"  Target: {self.config.target_db.name[1]} ({self.config.db_type})")
                self.logger.info(f"  Migration Schema: {self.config.mig_schema}")
                self.logger.info(f"  Export Path: {self.config.export.path}")
                self.logger.info(f"  FileMaker Status: {'Connected' if self.connection_status['filemaker']['connected'] else 'Not Connected'}")
                self.logger.info(f"  Target Status: {'Connected' if self.connection_status['target']['connected'] else 'Not Connected'}")
                self.logger.info(f"  Available tables ({len(tables)}): {', '.join(tables)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Info gathering failed: {e}")
            if output_json:
                import json
                error_result = {
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e),
                    'source_database': getattr(self.config.source_db, 'name', ['', 'Unknown'])[1],
                    'target_database': 'Unknown',
                    'connection_status': self.connection_status
                }
                print(json.dumps(error_result, indent=2))
            return False
    
    def run_migration(self) -> bool:
        """Run the complete migration process"""
        try:
            self.logger.info("Starting FileMaker migration process")
            
            # Handle special counting operations first (these may not need both connections)
            if getattr(self.args, 'src_cnt', False):
                return self.run_source_count(output_json=getattr(self.args, 'json', False))
            
            if getattr(self.args, 'tgt_cnt', False):
                return self.run_target_count(output_json=getattr(self.args, 'json', False))
            
            if getattr(self.args, 'migration_status', False):
                return self.run_migration_status(output_json=getattr(self.args, 'json', False))
            
            if getattr(self.args, 'info_only', False):
                return self.run_info_only(output_json=getattr(self.args, 'json', False))
            
            # For actual migration operations, we need both connections
            if not self.validate_connections(require_filemaker=True, require_target=True):
                self.logger.error("Migration requires both FileMaker and target database connections")
                return False
            
            # Handle special cases that require connections
            if getattr(self.args, 'get_images', False):
                return self.process_images()
            
            if getattr(self.args, 'get_schema', False):
                return self.run_schema_export()
            
            # Get tables to process
            tables, success = self.get_table_list_safe(getattr(self.args, 'tables_to_export', 'all'))
            if not success:
                return False
            
            self.logger.info(f"Processing {len(tables)} tables: {', '.join(tables)}")
            
            # Set up target database
            if self.export_options.export_to_database:
                self.logger.info("Setting up target database...")
                if not self.db_manager.setup_target_database(reset=self.export_options.reset_database):
                    self.logger.error("Failed to set up target database")
                    return False
            
            # Process each table
            success_count = 0
            for table_name in tables:
                self.logger.info(f"Processing table: {table_name}")
                
                try:
                    # Get sample data for DDL generation if needed
                    if self.export_options.include_ddl:
                        sample_sql = f'SELECT * FROM "{table_name}" FETCH FIRST 100 ROWS ONLY'
                        filemaker_conn = self.db_manager.filemaker.connect()
                        
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            sample_df = pd.read_sql(sample_sql, filemaker_conn)
                        
                        if not self.process_table_ddl(table_name, sample_df):
                            self.logger.error(f"DDL processing failed for {table_name}")
                            continue
                    
                    # Process table data
                    if self.export_options.include_dml:
                        if not self.process_table_data(table_name):
                            self.logger.error(f"Data processing failed for {table_name}")
                            continue
                    
                    success_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing table {table_name}: {e}")
                    continue
            
            # Generate summary
            summary = self.exporter.get_export_summary()
            self.logger.info("Migration Summary:")
            self.logger.info(f"  Tables processed: {summary['tables_processed']}/{len(tables)}")
            self.logger.info(f"  Rows inserted: {summary['rows_inserted']}")
            self.logger.info(f"  Rows failed: {summary['rows_failed']}")
            self.logger.info(f"  Duplicate entries: {summary['duplicate_entries']}")
            
            if summary['files_created'] > 0:
                self.logger.info(f"  Files created: {summary['files_created']}")
            
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            return False
        finally:
            # Clean up connections
            self.db_manager.close_all_connections()
    
    def process_table_ddl(self, table_name: str, sample_df: pd.DataFrame) -> bool:
        """Process DDL for a single table"""
        try:
            with self.db_manager.target_db.get_connection() as conn:
                # Generate DDL
                ddl = self.exporter.generate_ddl(sample_df, table_name, conn)
                
                # Export to file if requested
                if self.export_options.export_to_files:
                    self.exporter.export_to_file(ddl, table_name, 'ddl')
                
                # Export to database if requested
                if self.export_options.export_to_database:
                    success = self.exporter.export_ddl_to_database(
                        ddl, table_name, conn, self.export_options.reset_database
                    )
                    if not success:
                        return False
                
                self.logger.info(f"DDL processed for {table_name}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error processing DDL for {table_name}: {e}")
            return False
    
    def process_table_data(self, table_name: str, chunk_size: int = 1000) -> bool:
        """Process data for a single table"""
        try:
            # Build query
            sql = f'SELECT * FROM "{table_name}"'
            
            if self.export_options.max_rows != 'all':
                sql += f" FETCH FIRST {self.export_options.max_rows} ROWS ONLY"
            
            # Add ordering for ratcatalogue
            if table_name == 'ratcatalogue':
                sql += " ORDER BY image_no ASC"
            
            # Get connection and process in chunks
            filemaker_conn = self.db_manager.filemaker.connect()
            
            # Get total count for progress bar
            count_sql = f"SELECT COUNT(*) FROM \"{table_name}\""
            cursor = filemaker_conn.cursor()
            cursor.execute(count_sql)
            total_rows = cursor.fetchone()[0]
            
            if self.export_options.max_rows != 'all':
                total_rows = min(total_rows, int(self.export_options.max_rows))
            
            expected_chunks = (total_rows + chunk_size - 1) // chunk_size
            
            self.logger.info(f"Processing {total_rows} rows from {table_name} in {expected_chunks} chunks")
            
            # Process data in chunks
            chunk_count = 0
            header_written = False
            
            # Suppress pandas warnings about pyodbc
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                for chunk_df in tqdm(
                    pd.read_sql(sql, filemaker_conn, chunksize=chunk_size),
                    total=expected_chunks,
                    desc=f"Processing {table_name}"
                ):
                    chunk_count += 1
                    is_header = not header_written
                    is_footer = chunk_count >= expected_chunks
                    
                    # Generate DML
                    dml = self.exporter.df_to_sql_bulk_insert(chunk_df, table_name, is_header)
                    
                    # Export to file if requested
                    if self.export_options.export_to_files:
                        self.exporter.export_to_file(dml, table_name, 'dml', is_header, is_footer)
                    
                    # Export to database if requested
                    if self.export_options.export_to_database:
                        with self.db_manager.target_db.get_connection() as conn:
                            inserted, errors = self.exporter.export_dml_to_database(dml, table_name, conn)
                            self.exporter.stats.rows_inserted += inserted
                            self.exporter.stats.rows_failed += errors
                    
                    header_written = True
            
            # Export error log if there were errors
            if table_name in self.exporter.insert_errors and self.exporter.insert_errors[table_name]:
                self.exporter.export_error_log(table_name)
            
            self.logger.info(f"Data processing completed for {table_name}")
            self.exporter.stats.tables_processed += 1
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing data for {table_name}: {e}")
            return False
    
    def process_images(self) -> bool:
        """Process and export container images from FileMaker"""
        try:
            self.logger.info("Starting image extraction process...")
            
            # Query to get images from RATCatalogue
            sql = "SELECT image_no, GetAs(picture,'JPEG') picture, entry_date, date_taken FROM RATCatalogue"
            
            if self.export_options.max_rows != 'all':
                sql += f" FETCH FIRST {self.export_options.max_rows} ROWS ONLY"
            
            # Get connection and process images
            filemaker_conn = self.db_manager.filemaker.connect()
            
            # Process in chunks to manage memory
            chunk_size = 100  # Smaller chunks for images due to memory usage
            image_data = []
            
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                for chunk_df in pd.read_sql(sql, filemaker_conn, chunksize=chunk_size):
                    # Convert chunk to the format expected by exporter
                    chunk_data = {
                        'data': chunk_df[['image_no', 'picture']].values.tolist()
                    }
                    image_data.append(chunk_data)
            
            # Export images
            exported_count = self.exporter.export_images(image_data, 'images')
            self.logger.info(f"Exported {exported_count} images successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing images: {e}")
            return False
    
    def run_schema_export(self) -> bool:
        """Export FileMaker schema information"""
        try:
            self.logger.info("Exporting FileMaker schema information...")
            
            filemaker_conn = self.db_manager.filemaker.connect()
            schema_tables = self.config.source_db.schema
            
            for schema_table in schema_tables:
                self.logger.info(f"Exporting schema table: {schema_table}")
                
                try:
                    sql = f'SELECT * FROM "{schema_table}"'
                    
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        schema_df = pd.read_sql(sql, filemaker_conn)
                    
                    # Export schema data to file
                    if self.export_options.export_to_files:
                        # Convert to CSV for schema data
                        schema_file = self.exporter.export_paths['export'] / f"schema_{schema_table}_{self.exporter.date_string}.csv"
                        schema_df.to_csv(schema_file, index=False, encoding='utf-8')
                        self.logger.info(f"Schema exported to: {schema_file}")
                
                except Exception as e:
                    self.logger.error(f"Error exporting schema table {schema_table}: {e}")
                    continue
            
            return True
            
        except Exception as e:
            self.logger.error(f"Schema export failed: {e}")
            return False


def get_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Export data from FileMaker Pro database.")
    
    # Mutually exclusive group for export destination
    dest_group = parser.add_mutually_exclusive_group(required=False)
    dest_group.add_argument("--fn-exp", action="store_true", default=False, help="Export to Files")
    dest_group.add_argument("--db-exp", action="store_true", default=False, help="Export to Database")
    dest_group.add_argument("--info-only", action="store_true", default=False, help="Information only")
    dest_group.add_argument("-i", "--get-images", action="store_true", default=False, help="Export images")
    dest_group.add_argument("--src-cnt", action="store_true", default=False, help="Get source table row counts")
    dest_group.add_argument("--tgt-cnt", action="store_true", default=False, help="Get target table row counts")
    dest_group.add_argument("--migration-status", action="store_true", default=False, help="Get migration status comparison")
    
    # General arguments
    parser.add_argument('--export-dir', type=Path, help='Export directory')
    parser.add_argument('--log-dir', type=Path, help='Log directory')
    parser.add_argument('--config-file', type=str, default='config.toml', help='Configuration file path')
    parser.add_argument('--dsn', type=str, help='Override FileMaker DSN from config')
    parser.add_argument('--json', action="store_true", default=False, help='Output results in JSON format')
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("-t", "--tables-to-export", type=str, default='all', help="List of tables to export")
    parser.add_argument("--ddl", action="store_true", default=False, help="Export DDL definitions")
    parser.add_argument("--dml", action="store_true", default=False, help="Export DML data")
    parser.add_argument("--del-data", action="store_true", default=False, help="Delete data in target database")
    parser.add_argument("--del-db", action="store_true", default=False, help="Delete database objects")
    parser.add_argument("--get-schema", action="store_true", default=False, help="Get source database schema")
    parser.add_argument("-r", "--max-rows", type=str, default='all', help="Maximum rows to return")
    parser.add_argument("--db-type", type=str, choices=['mysql', 'supabase'], default='supabase', help="Target database type")
    parser.add_argument("--fn-fmt", type=str, choices=['single', 'multi'], default='multi', help="File export format")
    parser.add_argument("--start-from", type=str, help="Start migration from specific image_no")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Set defaults if no destination specified
    if not (args.fn_exp or args.db_exp or args.get_images or args.info_only or 
            args.src_cnt or args.tgt_cnt or args.migration_status):
        args.db_exp = True
    
    # Set default operations if none specified
    if args.db_exp or args.fn_exp:
        if not args.ddl and not args.dml:
            args.ddl = True
            args.dml = True
    
    return args


def main():
    """Main entry point"""
    try:
        # Parse arguments
        args = get_args()
        
        # Create migration manager
        migration_manager = FileMakerMigrationManager(args)
        
        # Run appropriate operation
        if getattr(args, 'get_schema', False):
            success = migration_manager.run_schema_export()
        elif getattr(args, 'src_cnt', False):
            success = migration_manager.run_source_count(output_json=getattr(args, 'json', False))
        elif getattr(args, 'tgt_cnt', False):
            success = migration_manager.run_target_count(output_json=getattr(args, 'json', False))
        elif getattr(args, 'migration_status', False):
            success = migration_manager.run_migration_status(output_json=getattr(args, 'json', False))
        else:
            success = migration_manager.run_migration()
        
        if success:
            migration_manager.logger.info("✓ Operation completed successfully")
            sys.exit(0)
        else:
            migration_manager.logger.error("✗ Operation failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()