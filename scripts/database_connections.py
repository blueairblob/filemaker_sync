#!/usr/bin/env python3
"""
Database Connection Management Module
Handles connections to FileMaker (ODBC) and target databases (Supabase/MySQL)
"""

import pyodbc
import sqlalchemy as sa
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from contextlib import contextmanager
import logging
from typing import Optional, Tuple, List, Dict, Any
from config_manager import ConfigManager, AppConfig


class DatabaseConnectionError(Exception):
    """Custom exception for database connection issues"""
    pass


class FileMakerConnection:
    """Manages FileMaker ODBC connections"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._connection: Optional[pyodbc.Connection] = None
    
    def connect(self) -> pyodbc.Connection:
        """Establish connection to FileMaker database"""
        if self._connection and not self._connection.closed:
            return self._connection
        
        try:
            conn_str = f"DSN={self.config.source_db.dsn};UID={self.config.source_db.user};PWD={self.config.source_db.pwd};CHARSET='UTF-8';ansi=True"
            
            self.logger.info(f"Connecting to {self.config.source_db.name[1]} via DSN: {self.config.source_db.dsn}")
            self._connection = pyodbc.connect(conn_str)
            
            self.logger.info("FileMaker connection established successfully")
            return self._connection
            
        except pyodbc.Error as e:
            error_msg = str(e)
            if 'SQLDriverConnect' in error_msg:
                raise DatabaseConnectionError(
                    f"Connection to {self.config.source_db.name[1]} failed. "
                    f"Please check that FileMaker Pro is running and the ODBC connection '{self.config.source_db.dsn}' is defined."
                )
            else:
                raise DatabaseConnectionError(f"FileMaker connection error: {error_msg}")
    
    def get_cursor(self) -> pyodbc.Cursor:
        """Get database cursor for queries"""
        connection = self.connect()
        return connection.cursor()
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test the FileMaker connection"""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            # Try a simple query to verify connection works
            cursor.execute("SELECT COUNT(*) FROM FileMaker_BaseTableFields")
            result = cursor.fetchone()
            cursor.close()
            return True, f"Connection successful. Found {result[0] if result else 'unknown'} base table fields."
        except Exception as e:
            return False, str(e)
    
    def get_table_list(self) -> List[str]:
        """Get list of available tables from FileMaker"""
        try:
            cursor = self.get_cursor()
            sql = "SELECT DISTINCT baseTableName FROM FileMaker_BaseTableFields"
            cursor.execute(sql)
            
            rows = cursor.fetchall()
            table_list = [row[0] for row in rows]
            
            # Sanitize table names
            table_list = [table.replace(' ', '_').lower() for table in table_list]
            
            self.logger.debug(f"Found {len(table_list)} base tables: {table_list}")
            return table_list
            
        except pyodbc.ProgrammingError as e:
            self.logger.error(f"Error getting table list: {e}")
            raise DatabaseConnectionError(f"Failed to retrieve table list: {e}")
    
    def get_table_row_counts(self, tables: List[str]) -> Dict[str, int]:
        """Get row counts for FileMaker tables"""
        row_counts = {}
        
        try:
            cursor = self.get_cursor()
            
            for table in tables:
                try:
                    sql = f'SELECT COUNT(*) FROM "{table}"'
                    cursor.execute(sql)
                    count = cursor.fetchone()[0]
                    row_counts[table] = count
                    self.logger.debug(f"Table {table}: {count} rows")
                except Exception as e:
                    self.logger.error(f"Error counting rows in {table}: {e}")
                    row_counts[table] = -1  # Indicate error
            
            return row_counts
            
        except Exception as e:
            self.logger.error(f"Error getting table row counts: {e}")
            return {table: -1 for table in tables}
    
    def close(self):
        """Close the connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self.logger.debug("FileMaker connection closed")


class TargetDatabaseConnection:
    """Manages connections to target databases (Supabase/MySQL)"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._engine: Optional[sa.Engine] = None
        self._postgres_version: Optional[float] = None
    
    def get_engine(self, use_dsn: bool = True) -> sa.Engine:
        """Get SQLAlchemy engine for target database"""
        if self._engine is not None:
            return self._engine
        
        try:
            if self.config.db_type == 'mysql':
                dsn_str = self.config.target_db.dsn if use_dsn else ''
                url = f"mysql+pymysql://{self.config.target_db.user}:{self.config.target_db.pwd}@{self.config.target_db.host}:{self.config.target_db.port}/{dsn_str}"
            elif self.config.db_type == 'supabase':
                dsn_str = self.config.target_db.dsn if use_dsn else ''
                url = f"postgresql://{self.config.target_db.user}:{self.config.target_db.pwd}@{self.config.target_db.host}:{self.config.target_db.port}/{dsn_str}"
            else:
                raise ValueError(f"Unsupported database type: {self.config.db_type}")
            
            self.logger.debug(f"Creating engine for {self.config.target_db.name[1]}")
            self._engine = create_engine(url)
            
            return self._engine
            
        except SQLAlchemyError as e:
            self.logger.error(f"Error creating database engine: {e}")
            raise DatabaseConnectionError(f"Failed to create database engine: {e}")
    
    @contextmanager
    def get_connection(self, use_dsn: bool = True):
        """Context manager for database connections"""
        connection = None
        try:
            engine = self.get_engine(use_dsn)
            connection = engine.connect()
            self.logger.debug(f"Connected to {self.config.target_db.name[1]}")
            yield connection
        except OperationalError as e:
            if "Unknown database" in str(e) and use_dsn:
                self.logger.warning("Unknown database. Attempting to connect without DSN.")
                with self.get_connection(use_dsn=False) as conn:
                    yield conn
            else:
                self.logger.error(f"Error connecting to {self.config.target_db.name[1]}: {e}")
                raise DatabaseConnectionError(f"Connection failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected connection error: {e}")
            raise DatabaseConnectionError(f"Connection failed: {e}")
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test the target database connection"""
        try:
            with self.get_connection() as conn:
                if self.config.db_type == 'supabase':
                    # Test Supabase (PostgreSQL) connection and get version
                    result = conn.execute(text("SELECT version(), current_setting('server_version_num')::integer as version_number"))
                    row = result.fetchone()
                    version_string = row[0]
                    version_number = row[1]
                    self._postgres_version = version_number / 10000
                    return True, f"Supabase (PostgreSQL) connection successful. Version: {version_string}"
                else:
                    # Test MySQL connection
                    result = conn.execute(text("SELECT VERSION()"))
                    version = result.fetchone()[0]
                    return True, f"MySQL connection successful. Version: {version}"
        except Exception as e:
            return False, str(e)
    
    def get_postgres_version(self) -> Tuple[Optional[str], Optional[float]]:
        """Get PostgreSQL version information"""
        if self.config.db_type != 'supabase':
            return None, None
        
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("""
                    SELECT version(), 
                           current_setting('server_version_num')::integer as version_number
                """))
                row = result.fetchone()
                version_string = row[0]
                version_number = row[1]
                self._postgres_version = version_number / 10000
                return version_string, self._postgres_version
        except SQLAlchemyError as e:
            self.logger.error(f"Error getting PostgreSQL version: {e}")
            return None, None
    
    def verify_database_exists(self, db_name: str, create_db: bool = True, drop_db: bool = False) -> str:
        """Verify target database exists or create it"""
        try:
            with self.get_connection() as conn:
                if self.config.db_type == 'mysql':
                    # Get existing databases
                    existing_dbs = conn.execute(text("SHOW DATABASES;"))
                    existing_dbs = [d[0] for d in existing_dbs]
                    
                    if drop_db and db_name in existing_dbs:
                        conn.execute(text(f"DROP DATABASE {db_name};"))
                        self.logger.info(f"Deleted database {db_name}")
                    
                    if create_db and (db_name not in existing_dbs or drop_db):
                        conn.execute(text(f"CREATE DATABASE {db_name};"))
                        conn.execute(text(f"USE {db_name}"))
                        self.logger.info(f"Created database {db_name}")
                        return 'created'
                
                elif self.config.db_type == 'supabase':
                    # For PostgreSQL/Supabase
                    if drop_db:
                        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name};"))
                        self.logger.info(f"Deleted database {db_name}")
                    
                    if create_db:
                        # Check if database exists
                        exists = conn.execute(text(f"SELECT * FROM pg_database WHERE datname = '{db_name}';")).fetchone()
                        if not exists or drop_db:
                            conn.execute(text(f"CREATE DATABASE {db_name};"))
                            self.logger.info(f"Created database {db_name}")
                            return 'created'
                        else:
                            self.logger.debug(f"Database {db_name} already exists")
                
                return 'exists'
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error verifying database: {e}")
            raise DatabaseConnectionError(f"Database verification failed: {e}")
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if a table exists in the target database"""
        try:
            engine = self.get_engine()
            inspector = inspect(engine)
            found = inspector.has_table(table_name, schema=schema)
            if found:
                self.logger.debug(f"Found table {schema}.{table_name}" if schema else f"Found table {table_name}")
            return found
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def get_schema_names(self) -> List[str]:
        """Get list of available schemas"""
        try:
            engine = self.get_engine()
            inspector = inspect(engine)
            return inspector.get_schema_names()
        except Exception as e:
            self.logger.error(f"Error getting schema names: {e}")
            return []
    
    def create_schema_if_not_exists(self, schema_name: str) -> bool:
        """Create schema if it doesn't exist"""
        try:
            with self.get_connection() as conn:
                inspector = inspect(conn)
                if schema_name not in inspector.get_schema_names():
                    conn.execute(sa.schema.CreateSchema(schema_name))
                    conn.commit()
                    self.logger.info(f"Created schema: {schema_name}")
                    return True
                else:
                    self.logger.debug(f"Schema {schema_name} already exists")
                    return False
        except SQLAlchemyError as e:
            self.logger.error(f"Error creating schema {schema_name}: {e}")
            raise DatabaseConnectionError(f"Schema creation failed: {e}")
    
    def dispose(self):
        """Dispose of the engine and close connections"""
        if self._engine:
            self._engine.dispose()
            self.logger.debug("Database engine disposed")
    
    def get_table_row_counts(self, tables: List[str], schema: Optional[str] = None) -> Dict[str, int]:
        """Get row counts for target database tables"""
        row_counts = {}
        
        if not schema:
            schema = self.config.mig_schema
        
        try:
            with self.get_connection() as conn:
                for table in tables:
                    try:
                        # Check if table exists first
                        if not self.table_exists(table, schema):
                            row_counts[table] = 0
                            continue
                        
                        if self.config.db_type == 'supabase':
                            sql = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                        else:
                            sql = f'SELECT COUNT(*) FROM `{schema}`.`{table}`'
                        
                        result = conn.execute(text(sql))
                        count = result.fetchone()[0]
                        row_counts[table] = count
                        self.logger.debug(f"Target table {schema}.{table}: {count} rows")
                        
                    except Exception as e:
                        self.logger.error(f"Error counting rows in {schema}.{table}: {e}")
                        row_counts[table] = -1  # Indicate error
                
                return row_counts
                
        except Exception as e:
            self.logger.error(f"Error getting target table row counts: {e}")
            return {table: -1 for table in tables}
    
    def close(self):
        """Close the connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self.logger.debug("FileMaker connection closed")


class DatabaseManager:
    """High-level database management class"""
    
    def __init__(self, config_file: str = 'config.toml'):
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.load_config()
        self.logger = logging.getLogger(__name__)
        
        self.filemaker = FileMakerConnection(self.config)
        self.target_db = TargetDatabaseConnection(self.config)
    
    def test_all_connections(self) -> Dict[str, Tuple[bool, Optional[str]]]:
        """Test all database connections"""
        results = {}
        
        # Test FileMaker
        fm_success, fm_message = self.filemaker.test_connection()
        results['filemaker'] = (fm_success, fm_message)
        
        # Test target database
        target_success, target_message = self.target_db.test_connection()
        results['target'] = (target_success, target_message)
        
        return results
    
    def get_filemaker_tables(self) -> List[str]:
        """Get list of FileMaker tables"""
        return self.filemaker.get_table_list()
    
    def setup_target_database(self, reset: bool = False) -> bool:
        """Set up target database with required schemas"""
        try:
            # Verify/create database
            db_status = self.target_db.verify_database_exists(
                self.config.target_db.dsn, 
                create_db=True, 
                drop_db=reset
            )
            
            # Create migration schema
            self.target_db.create_schema_if_not_exists(self.config.mig_schema)
            
            self.logger.info("Target database setup completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Target database setup failed: {e}")
            return False
    
    def get_source_table_counts(self, tables: List[str] = None) -> Dict[str, int]:
        """Get row counts for FileMaker tables"""
        if tables is None:
            tables = self.get_filemaker_tables()
        return self.filemaker.get_table_row_counts(tables)
    
    def get_target_table_counts(self, tables: List[str] = None, schema: str = None) -> Dict[str, int]:
        """Get row counts for target database tables"""
        if tables is None:
            tables = self.get_filemaker_tables()
        return self.target_db.get_table_row_counts(tables, schema)
    
    def get_migration_status(self, tables: List[str] = None) -> Dict[str, Any]:
        """Get comprehensive migration status comparing source and target"""
        if tables is None:
            tables = self.get_filemaker_tables()
        
        source_counts = self.get_source_table_counts(tables)
        target_counts = self.get_target_table_counts(tables)
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'source_database': self.config.source_db.name[1],
            'target_database': f"{self.config.target_db.name[1]} ({self.config.db_type})",
            'migration_schema': self.config.mig_schema,
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
            if source_count == -1:
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
        
        return status
    
    def close_all_connections(self):
        """Close all database connections"""
        self.filemaker.close()
        self.target_db.dispose()
        self.logger.info("All database connections closed")


# Add missing import at the top
from datetime import datetime


# Convenience functions for backward compatibility
def get_filemaker_connection(config_file: str = 'config.toml') -> FileMakerConnection:
    """Get FileMaker connection - convenience function"""
    config_manager = ConfigManager(config_file)
    config = config_manager.load_config()
    return FileMakerConnection(config)


def get_target_connection(config_file: str = 'config.toml') -> TargetDatabaseConnection:
    """Get target database connection - convenience function"""
    config_manager = ConfigManager(config_file)
    config = config_manager.load_config()
    return TargetDatabaseConnection(config)


if __name__ == "__main__":
    # Demo usage
    try:
        manager = DatabaseManager()
        
        print("Testing database connections...")
        results = manager.test_all_connections()
        
        for db_name, (success, message) in results.items():
            status = "✓" if success else "✗"
            print(f"{status} {db_name.title()}: {message}")
        
        if all(result[0] for result in results.values()):
            print("\n✓ All connections successful!")
            
            # Get table list
            tables = manager.get_filemaker_tables()
            print(f"Found {len(tables)} FileMaker tables: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}")
        else:
            print("\n✗ Some connections failed")
        
        manager.close_all_connections()
        
    except Exception as e:
        print(f"Error: {e}")