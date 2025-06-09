#!/usr/bin/env python3
"""
FileMaker Extract - Refactored Main Script
Now uses modular components for better maintainability
"""

import sys
import argparse
import logging
import pandas as pd
import warnings
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
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
        
        # Set PostgreSQL version if available
        if self.config.db_type == 'supabase':
            _, version = self.db_manager.target_db.get_postgres_version()
            self.exporter.set_postgres_version(version)
    
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
    
    def validate_connections(self) -> bool:
        """Test all database connections"""
        self.logger.info("Testing database connections...")
        
        results = self.db_manager.test_all_connections()
        
        all_success = True
        for db_name, (success, message) in results.items():
            if success:
                self.logger.info(f"✓ {db_name.title()}: {message}")
            else:
                self.logger.error(f"✗ {db_name.title()}: {message}")
                all_success = False
        
        return all_success
    
    def get_table_list(self, tables_arg: str = 'all') -> List[str]:
        """Get list of tables to process"""
        available_tables = self.db_manager.get_filemaker_tables()
        
        if tables_arg == 'all':
            return available_tables
        
        # Parse table list from argument
        import re
        delimiters = [";", "|", ",", " "]
        pattern = "|".join(map(re.escape, delimiters))
        requested_tables = [t.strip() for t in re.split(pattern, tables_arg) if t.strip()]
        
        # Validate requested tables exist
        invalid_tables = set(requested_tables) - set(available_tables)
        if invalid_tables:
            self.logger.error(f"Invalid tables requested: {invalid_tables}")
            self.logger.info(f"Available tables: {available_tables}")
            raise ValueError(f"Invalid tables: {invalid_tables}")
        
        return requested_tables
    
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
    
    def run_source_count(self, tables: List[str] = None, output_json: bool = False) -> bool:
        """Get row counts for source FileMaker tables"""
        try:
            self.logger.info("Getting FileMaker Pro source table counts...")
            
            if tables is None:
                tables = self.get_table_list(getattr(self.args, 'tables_to_export', 'all'))
            
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
                        'tables_with_errors': sum(1 for count in source_counts.values() if count < 0)
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
            return False
    
    def run_target_count(self, tables: List[str] = None, output_json: bool = False) -> bool:
        """Get row counts for target Supabase tables"""
        try:
            self.logger.info(f"Getting {self.config.target_db.name[1]} target table counts...")
            
            if tables is None:
                tables = self.get_table_list(getattr(self.args, 'tables_to_export', 'all'))
            
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
                        'tables_with_errors': sum(1 for count in target_counts.values() if count < 0)
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
            return False
    
    def run_migration_status(self, tables: List[str] = None, output_json: bool = False) -> bool:
        """Get comprehensive migration status comparing source and target"""
        try:
            self.logger.info("Getting migration status comparison...")
            
            if tables is None:
                tables = self.get_table_list(getattr(self.args, 'tables_to_export', 'all'))
            
            status = self.db_manager.get_migration_status(tables)
            
            if output_json:
                import json
                print(json.dumps(status, indent=2))
            else:
                self.logger.info("Migration Status Summary:")
                self.logger.info(f"  Source: {status['source_database']}")
                self.logger.info(f"  Target: {status['target_database']}")
                self.logger.info(f"  Schema: {status['migration_schema']}")
                self.logger.info(f"  Total Tables: {status['summary']['total_tables']}")
                self.logger.info(f"  Source Total Rows: {status['summary']['source_total_rows']:,}")
                self.logger.info(f"  Target Total Rows: {status['summary']['target_total_rows']:,}")
                self.logger.info(f"  Tables Migrated: {status['summary']['tables_migrated']}")
                self.logger.info(f"  Tables Empty: {status['summary']['tables_empty_target']}")
                
                self.logger.info("\nPer-Table Status:")
                for table, info in status['tables'].items():
                    status_icon = {
                        'fully_migrated': '✓',
                        'partially_migrated': '⚠',
                        'not_migrated': '✗',
                        'source_error': '❌',
                        'target_error': '❌'
                    }.get(info['status'], '?')
                    
                    self.logger.info(
                        f"  {status_icon} {table}: {info['source_rows']:,} → {info['target_rows']:,} "
                        f"({info['migration_percentage']:.1f}%)"
                    )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Migration status failed: {e}")
            return False
    
    def run_migration(self) -> bool:
        """Run the complete migration process"""
        try:
            self.logger.info("Starting FileMaker migration process")
            
            # Test connections first
            if not self.validate_connections():
                self.logger.error("Connection validation failed")
                return False
            
            # Handle special counting operations
            if getattr(self.args, 'src_cnt', False):
                return self.run_source_count(output_json=getattr(self.args, 'json', False))
            
            if getattr(self.args, 'tgt_cnt', False):
                return self.run_target_count(output_json=getattr(self.args, 'json', False))
            
            if getattr(self.args, 'migration_status', False):
                return self.run_migration_status(output_json=getattr(self.args, 'json', False))
            
            # Handle special cases
            if getattr(self.args, 'get_images', False):
                return self.process_images()
            
            if getattr(self.args, 'info_only', False):
                self.logger.info("Information-only mode requested")
                tables = self.get_table_list(getattr(self.args, 'tables_to_export', 'all'))
                
                if getattr(self.args, 'json', False):
                    import json
                    info = {
                        'timestamp': datetime.now().isoformat(),
                        'source_database': self.config.source_db.name[1],
                        'target_database': f"{self.config.target_db.name[1]} ({self.config.db_type})",
                        'tables_available': tables,
                        'table_count': len(tables)
                    }
                    print(json.dumps(info, indent=2))
                else:
                    self.logger.info(f"Available tables ({len(tables)}): {', '.join(tables)}")
                
                return True
            
            # Get tables to process
            tables = self.get_table_list(getattr(self.args, 'tables_to_export', 'all'))
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