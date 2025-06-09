#!/usr/bin/env python3
"""
Data Export Module
Handles exporting data to files and target databases
"""

import os
import re
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError, ProgrammingError
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging
from PIL import Image
from io import BytesIO
from tqdm import tqdm

from config_manager import ConfigManager, AppConfig
from database_connections import DatabaseManager


@dataclass
class ExportOptions:
    """Configuration for export operations"""
    export_to_files: bool = False
    export_to_database: bool = True
    include_ddl: bool = True
    include_dml: bool = True
    reset_data: bool = False
    reset_database: bool = False
    file_format: str = 'multi'  # 'single' or 'multi'
    max_rows: str = 'all'
    start_from: Optional[str] = None
    debug: bool = False


@dataclass
class ExportStats:
    """Statistics for export operations"""
    tables_processed: int = 0
    rows_inserted: int = 0
    rows_failed: int = 0
    duplicate_entries: int = 0
    files_created: List[str] = None
    
    def __post_init__(self):
        if self.files_created is None:
            self.files_created = []


class DataExporter:
    """Handles data export operations"""
    
    def __init__(self, config: AppConfig, export_options: ExportOptions):
        self.config = config
        self.options = export_options
        self.logger = logging.getLogger(__name__)
        
        # Initialize paths
        self.export_paths = self._setup_export_paths()
        
        # Export tracking
        self.stats = ExportStats()
        self.insert_errors: Dict[str, Dict] = {}
        self.table_ddl: Dict[str, Dict] = {}
        self.insert_header = ""
        self.postgres_version: Optional[float] = None
        
        # Date string for file naming
        self.date_string = datetime.now().strftime('%Y%m%d')
    
    def _setup_export_paths(self) -> Dict[str, Path]:
        """Set up and create export directories"""
        base_path = Path(self.config.export.path)
        
        paths = {
            'export': base_path,
            'jpg': base_path / 'images' / 'jpg',
            'webp': base_path / 'images' / 'webp',
        }
        
        # Create directories if they don't exist
        for path_type, path in paths.items():
            path.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Created/verified directory: {path}")
        
        return paths
    
    def set_postgres_version(self, version: Optional[float]):
        """Set PostgreSQL version for formatting compatibility"""
        self.postgres_version = version
    
    def format_value_for_sql(self, val, db_type: str = None) -> str:
        """Format a value for SQL insertion"""
        if db_type is None:
            db_type = self.config.db_type
            
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, (int, float)):
            return str(val)
        else:
            val_str = str(val)
            if db_type == 'mysql':
                # MySQL formatting
                escaped_val = val_str.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r')
                escaped_val = escaped_val.replace('"', '""')
                if ',' in escaped_val or "'" in escaped_val or '\n' in val_str or '\r' in val_str:
                    return f'"{escaped_val}"'
                return f"'{escaped_val}'"
            
            elif db_type == 'supabase':  # PostgreSQL
                # Escape single quotes by doubling them
                val_str = val_str.replace("'", "''")
                if self.postgres_version and self.postgres_version >= 9.0:
                    return f"E'{val_str}'"
                else:
                    return f"'{re.sub(',', r'\\,', val_str)}'"
    
    def df_to_sql_bulk_insert(self, df: pd.DataFrame, table: str, include_header: bool = True, **kwargs) -> str:
        """Convert DataFrame to bulk INSERT SQL statement"""
        # Add any additional columns from kwargs
        df = df.copy().assign(**kwargs).replace({True: 1, False: 0})
        
        # Special handling for ratcatalogue table
        if table == 'ratcatalogue' and 'picture' in df.columns and 'image_no' in df.columns:
            df['picture'] = df.apply(lambda x: x['image_no'] if x['image_no'] != '' else x['picture'], axis=1)
        
        # Convert rows to SQL tuples
        tuples = df.apply(
            lambda row: '({})'.format(', '.join(
                self.format_value_for_sql(val) for val in row
            )), 
            axis=1
        )
        values = (",\n" + " " * 7).join(tuples)
        
        # Handle header generation
        if include_header:
            if self.config.db_type == 'mysql':
                columns = ', '.join("`%s`" % x for x in df.columns)
            else:  # PostgreSQL
                columns = ', '.join(f'"{x}"' for x in df.columns)
            
            self.insert_header = f"INSERT INTO {self.config.mig_schema}.{table} ({columns})\nVALUES"
            query = f"{self.insert_header} {values}"
        else:
            query = f"{values}"
        
        return query
    
    def convert_create_table_to_dict(self, create_table_sql: str) -> Dict[str, Any]:
        """Extract column information from CREATE TABLE SQL"""
        table_dict = {}
        
        start_pattern = r'\(\s*\n'
        end_pattern = r'\n\s*\)'
        column_pattern = r"\n\t[`]?([\w\s]+)[`]? ([A-Z]+)"
        
        table_name = re.search(r'CREATE TABLE\s+[`"]?(\w+)[`"]?', create_table_sql, re.IGNORECASE)
        if table_name:
            tab = table_name.group(1)
            table_dict = {tab: {}}
        
        columns = re.search(start_pattern + r'(.+)' + end_pattern, create_table_sql, re.DOTALL)
        if columns:
            column_definitions = columns.group(1)
            column_lines = re.findall(column_pattern, column_definitions, re.IGNORECASE)
            table_dict[tab]['columns'] = [
                {'column_name': column[0], 'column_type': column[1]} 
                for column in column_lines
            ]
        
        return table_dict
    
    def generate_ddl(self, df: pd.DataFrame, table_name: str, target_connection) -> str:
        """Generate DDL (CREATE TABLE) statement for a table"""
        try:
            # Generate schema using pandas and target connection
            ddl = pd.io.sql.get_schema(df, table_name, con=target_connection)
            
            # Store column information for later use
            self.table_ddl.update(self.convert_create_table_to_dict(ddl))
            
            # Modify DDL for our needs
            ddl = ddl.replace('TABLE', 'TABLE IF NOT EXISTS')
            ddl = ddl.replace(table_name, f"{self.config.mig_schema}.{table_name}")
            
            return ddl
            
        except Exception as e:
            self.logger.error(f"Error generating DDL for {table_name}: {e}")
            raise
    
    def export_to_file(self, content: str, table_name: str, export_type: str, 
                      is_header: bool = True, is_footer: bool = True) -> Optional[str]:
        """Export content to SQL file"""
        try:
            # Determine file naming
            prefix = self.config.export.prefix
            
            if self.options.file_format == 'multi':
                tag = f"{table_name}_{export_type}"
            else:
                tag = '_'.join([export_type])
            
            filename = f"{self.date_string}_{prefix}_{tag}.sql"
            file_path = self.export_paths['export'] / filename
            
            # Determine write mode
            mode = 'w' if is_header else 'a'
            
            with open(file_path, mode, encoding='utf-8') as f:
                if is_header and export_type == 'dml':
                    # Add file header for DML
                    timestamp = datetime.now().strftime('%Y.%m.%d %H:%M:%S')
                    f.write('\n/*\n')
                    f.write(f"\tTable: {table_name}\n")
                    f.write(f"\tDate: {timestamp}\n")
                    f.write('*/\n')
                
                # Write content
                if content:
                    if export_type == 'ddl':
                        f.write(f"{content};\n")
                    else:  # DML
                        semicolon = ';\n' if is_footer else ''
                        f.write(f'{content}{semicolon}\n')
            
            if filename not in self.stats.files_created:
                self.stats.files_created.append(filename)
            
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"Error writing to file for {table_name}: {e}")
            return None
    
    def create_primary_key(self, connection, table_name: str) -> bool:
        """Create primary key constraint for a table"""
        try:
            pk_columns = self.config.pk_config.get(table_name, [])
            if not pk_columns:
                self.logger.debug(f"No primary key defined for table {table_name}")
                return True
            
            pk_name = f"pk_{table_name}"
            columns = ', '.join(pk_columns)
            
            # Check if PK already exists
            existing_pk = connection.execute(text(
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_schema = :schema AND table_name = :table AND constraint_type = 'PRIMARY KEY'"
            ), {"schema": self.config.mig_schema, "table": table_name}).fetchone()
            
            if existing_pk:
                self.logger.debug(f"Primary key already exists for table {table_name}")
                return True
            
            # Create the primary key
            connection.execute(text(
                f"ALTER TABLE {self.config.mig_schema}.{table_name} "
                f"ADD CONSTRAINT {pk_name} PRIMARY KEY ({columns})"
            ))
            connection.commit()
            self.logger.info(f"Created primary key for table {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating primary key for {table_name}: {e}")
            return False
    
    def export_ddl_to_database(self, ddl: str, table_name: str, connection, reset: bool = False) -> bool:
        """Export DDL to target database"""
        try:
            # Check if table exists
            from database_connections import TargetDatabaseConnection
            target_db = TargetDatabaseConnection(self.config)
            table_exists = target_db.table_exists(table_name, self.config.mig_schema)
            
            if reset and table_exists:
                # Drop existing table
                table_ref = f'"{table_name}"' if self.config.db_type == 'supabase' else table_name
                connection.execute(text(f"DROP TABLE IF EXISTS {self.config.mig_schema}.{table_ref}"))
                self.logger.info(f"Dropped existing table {table_name}")
                table_exists = False
            
            if not table_exists or reset:
                # Create schema if it doesn't exist
                target_db.create_schema_if_not_exists(self.config.mig_schema)
                
                # Execute DDL
                adjusted_ddl = self.adjust_sql_syntax(ddl)
                connection.execute(text(adjusted_ddl))
                connection.commit()
                self.logger.info(f"Created table {table_name}")
                
                # Create primary key
                self.create_primary_key(connection, table_name)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting DDL for {table_name}: {e}")
            return False
    
    def export_dml_to_database(self, dml: str, table_name: str, connection) -> Tuple[int, int]:
        """Export DML to target database"""
        inserted_count = 0
        error_count = 0
        
        if table_name not in self.insert_errors:
            self.insert_errors[table_name] = {}
        
        try:
            # Split DML into individual INSERT statements
            for insert_sql in dml.split(',\n'):
                if not re.findall('INSERT INTO', insert_sql, re.IGNORECASE):
                    insert_sql = self.insert_header + insert_sql
                
                # Handle start_from logic for ratcatalogue
                if self.options.start_from and table_name == 'ratcatalogue':
                    image_no = self.extract_image_no(insert_sql)
                    if image_no != self.options.start_from and not hasattr(self, '_start_found'):
                        continue
                    else:
                        self._start_found = True
                
                try:
                    # Prepare SQL with conflict handling
                    adjusted_sql = self.adjust_sql_syntax(insert_sql)
                    pk_columns = ', '.join(self.config.pk_config.get(table_name, []))
                    
                    if pk_columns:
                        sql_with_conflict = f"{adjusted_sql} ON CONFLICT({pk_columns}) DO NOTHING"
                    else:
                        sql_with_conflict = adjusted_sql
                    
                    # Execute insert
                    connection.execute(text(sql_with_conflict))
                    connection.commit()
                    inserted_count += 1
                    
                except IntegrityError as e:
                    error_count += 1
                    error_detail = str(e.orig.diag.message_detail) if hasattr(e.orig, 'diag') else str(e)
                    error_primary = str(e.orig.diag.message_primary) if hasattr(e.orig, 'diag') else str(e)
                    
                    self.insert_errors[table_name][error_count] = {
                        "message": error_primary,
                        "detail": error_detail,
                        "sql": adjusted_sql
                    }
                    
                    if "already exists" in str(e):
                        self.stats.duplicate_entries += 1
                    
                    connection.rollback()
                    continue
                    
                except (OperationalError, SQLAlchemyError) as e:
                    error_count += 1
                    self.logger.error(f"Database error inserting into {table_name}: {e}")
                    connection.rollback()
                    continue
        
        except Exception as e:
            self.logger.error(f"Unexpected error during DML export for {table_name}: {e}")
        
        return inserted_count, error_count
    
    def extract_image_no(self, sql_statement: str) -> Optional[str]:
        """Extract image_no from INSERT statement for ratcatalogue"""
        pattern = r"INSERT INTO.*VALUES\s*\((.*?)\)"
        match = re.search(pattern, sql_statement, re.DOTALL | re.IGNORECASE)
        
        if match:
            values = match.group(1)
            image_no = values.split(',')[0].strip().strip("'").strip('E').strip("'")
            return image_no
        return None
    
    def adjust_sql_syntax(self, sql: str) -> str:
        """Adjust SQL syntax for target database"""
        if self.config.db_type == 'supabase':
            sql = sql.replace('`', '"')
        return sql
    
    def export_error_log(self, table_name: str) -> Optional[str]:
        """Export error log for a table to file"""
        if table_name not in self.insert_errors or not self.insert_errors[table_name]:
            return None
        
        try:
            error_file = self.export_paths['export'] / f"ins_err_{table_name}_{self.date_string}.sql"
            
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(f"-- Duplicate/Error entries for table {table_name}\n")
                for error_id, error_info in self.insert_errors[table_name].items():
                    f.write(f"-- ID {error_id}: {error_info['detail']}\n")
                    f.write(f"{error_info['sql']};\n\n")
            
            self.logger.info(f"Exported {len(self.insert_errors[table_name])} error entries for {table_name}")
            return str(error_file)
            
        except Exception as e:
            self.logger.error(f"Error writing error log for {table_name}: {e}")
            return None
    
    def export_images(self, image_data: List[Dict], table_name: str = 'images') -> int:
        """Export container images to files"""
        exported_count = 0
        
        try:
            for chunk_data in image_data:
                chunk_count = len(chunk_data['data'])
                
                for index, item in tqdm(enumerate(chunk_data['data']), 
                                      total=chunk_count, 
                                      desc=f"Exporting {table_name} images"):
                    
                    image_name = item[0]
                    image_bytes = item[1]
                    
                    if not image_name or not image_bytes:
                        continue
                    
                    # Clean image name
                    image_name = image_name.replace('\n', '').replace('\r', '').replace(' ', '')
                    
                    # Export JPG if requested
                    if 'jpg' in self.config.export.image_formats_supported:
                        jpg_file = self.export_paths['jpg'] / f"{image_name}.jpg"
                        if not jpg_file.exists():
                            with open(jpg_file, 'wb') as f:
                                f.write(image_bytes)
                            exported_count += 1
                    
                    # Export WebP if requested
                    if 'webp' in self.config.export.image_formats_supported:
                        webp_file = self.export_paths['webp'] / f"{image_name}.webp"
                        if not webp_file.exists():
                            image = Image.open(BytesIO(image_bytes))
                            webp_data = BytesIO()
                            image.save(webp_data, format="webp")
                            
                            with open(webp_file, 'wb') as f:
                                f.write(webp_data.getvalue())
                            exported_count += 1
            
            self.logger.info(f"Exported {exported_count} images")
            return exported_count
            
        except Exception as e:
            self.logger.error(f"Error exporting images: {e}")
            return 0
    
    def get_export_summary(self) -> Dict[str, Any]:
        """Get summary of export operations"""
        return {
            'tables_processed': self.stats.tables_processed,
            'rows_inserted': self.stats.rows_inserted,
            'rows_failed': self.stats.rows_failed,
            'duplicate_entries': self.stats.duplicate_entries,
            'files_created': len(self.stats.files_created),
            'file_list': self.stats.files_created,
            'errors_by_table': {
                table: len(errors) 
                for table, errors in self.insert_errors.items() 
                if errors
            }
        }


# Convenience functions
def create_exporter(config_file: str = 'config.toml', **export_options) -> DataExporter:
    """Create a DataExporter with configuration"""
    config_manager = ConfigManager(config_file)
    config = config_manager.load_config()
    
    options = ExportOptions(**export_options)
    return DataExporter(config, options)


def export_table_data(df: pd.DataFrame, table_name: str, config_file: str = 'config.toml', 
                     **export_options) -> Dict[str, Any]:
    """Export a single table's data - convenience function"""
    exporter = create_exporter(config_file, **export_options)
    
    # This would need to be implemented with database connections
    # Left as a stub for now
    pass


if __name__ == "__main__":
    # Demo usage
    try:
        # Create sample export options
        options = ExportOptions(
            export_to_files=True,
            export_to_database=False,
            include_ddl=True,
            include_dml=True
        )
        
        config_manager = ConfigManager()
        config = config_manager.load_config()
        
        exporter = DataExporter(config, options)
        
        print(f"Data exporter initialized")
        print(f"Export paths: {exporter.export_paths}")
        print(f"Options: DDL={options.include_ddl}, DML={options.include_dml}")
        
    except Exception as e:
        print(f"Error: {e}")