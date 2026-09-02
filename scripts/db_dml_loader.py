#!/bin/env python3

"""
DML to Supabase Migration Script

This script automates the process of migrating data from DML exports scripts OR from a local database migration schema.
It handles multiple export files, performs batch processing, supports resuming interrupted migrations,
and populates audit columns.

Features:
- Processes multiple DML SQL export files
- Batch inserts/updates to manage large datasets efficiently
- Resumes interrupted migrations using a migration log
- Populates audit columns (created_by, created_date, modified_by, modified_date)
- Respects referential integrity by migrating tables in a specific order
- Provides detailed logging of the migration process
- Configurable batch size and user ID for audit trails

Usage:
python script_name.py --export-path <path_to_exports> --batch-size <batch_size> --user-id <user_id>

Arguments:
--export-path : Path to the directory containing the DML export files (SQL format)
--batch-size  : Number of records to process in each batch (default: 1000)
--user-id     : Identifier for the user performing the migration (for audit columns)

Environment Variables (in .env file):
SUPABASE_URL : URL of your Supabase project
SUPABASE_KEY : API key for your Supabase project

Dependencies:
- supabase
- pandas
- python-dotenv
- tqdm

Note: Ensure your Supabase database schema matches the structure expected by this script.
Always backup your Supabase database before running a full migration.

Author: [Your Name]
Date: [Current Date]
Version: 1.0
"""

import sys
import os
os.system('chcp 65001')  # Set console to UTF-8
from pathlib import Path
import re
import tomli
import pandas as pd
from PIL import Image
#from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData, Table, select, insert, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from datetime import datetime, timezone
import logging
import argparse
import glob
from tqdm import tqdm
import io
from collections import OrderedDict
from sqlalchemy.exc import IntegrityError, DataError
from psycopg2.errors import UniqueViolation, ForeignKeyViolation, StringDataRightTruncation
from psycopg2.errors import Error as PGError

# --- shared secret resolution (env/.env first, config fallback) ---------------
try:
    from env_secrets import resolve_secret, resolve_target_pwd, url_quote
except ImportError:                       # self-contained fallback (identical behaviour)
    import os as _os
    from urllib.parse import quote_plus as _qp
    def resolve_secret(env_key, cfg_val=None, cli_val=None, default=""):
        if cli_val is not None:
            return cli_val
        try:
            from dotenv import load_dotenv; load_dotenv()
        except ModuleNotFoundError:
            pass
        _v = _os.environ.get(env_key)
        return _v if _v else (cfg_val if cfg_val is not None else default)
    def resolve_target_pwd(profile, cfg_val=None, cli_val=None, default=""):
        if cli_val is not None:
            return cli_val
        pwd = resolve_secret(f"RAT_TARGET_PWD_{profile.upper()}", cfg_val=None)
        if pwd:
            return pwd
        if profile == "supabase":
            pwd = resolve_secret("RAT_TARGET_PWD", cfg_val=None)
            if pwd:
                return pwd
        return cfg_val if cfg_val is not None else default
    def url_quote(value):
        return _qp(str(value or ""))

        
# Global variables — populated by main() (via `global`) before any migration
# function runs; declared here (without a None default) so their static type
# is the real type, not Optional[...].
user_id = None
debug = False
logger: logging.Logger
engine: Engine
session: sessionmaker
config: dict
# In-memory id lookup caches (built incrementally -- see cache_lookup_table()),
# keyed by table name ('country', 'location', ...) -> {natural_key: uuid}.
lookup_caches: dict = {}


# Set up logging
def setup_logging(debug_mode=False):
    """Set up logging with proper Unicode handling."""
    # Create logs directory if it doesn't exist
    log_dir = Path('./logs')
    log_dir.mkdir(exist_ok=True)

    # Get current date for log file name
    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f'db_dml_loader_{timestamp}.log'

    # Create logger
    logger = logging.getLogger('db_dml_loader')
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    # Create formatters and add it to handlers
    log_format = '%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    date_format = '%Y-%m-%d:%H:%M:%S'
    formatter = logging.Formatter(log_format, date_format)

    # Set up file handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Set up console handler with UTF-8 encoding if in debug mode
    if debug_mode:
        import sys
        # Force UTF-8 encoding for stdout if on Windows
        if sys.platform == 'win32':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger, log_file
  
def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Migrate DML data to Supabase")
    parser.add_argument("--mode", choices=['dml_files', 'migration_schema'], required=True, help="Source data to migrate from DML files or from the Database Migration Schema.")
    parser.add_argument("--export-path", required=True, help="Path to the directory containing DML export files")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserts")
    parser.add_argument("--user-id", help="User ID for audit columns")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--upload-images", action="store_true", help="Upload images to Supabase storage")
    parser.add_argument("--target-profile", help="Target DB profile from config.toml's "
                        "[database.target.<profile>] (overrides config/env RAT_TARGET_PROFILE)")
    return parser.parse_args()

def load_config():
    cur_pth = Path(os.getcwd())
    cfg_fn = 'config.toml'
    cfg_pth = cur_pth
    try:
        return tomli.loads(Path(f'{cfg_pth}/{cfg_fn}').read_text(encoding='utf-8'))
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found: {cfg_pth}/{cfg_fn}")
    except tomli.TOMLDecodeError as e:
        raise ValueError(f"Error parsing TOML file: {e}")

def format_pg_error(e):
    """Format PostgreSQL error details for log entries."""
    try:
        return f"Database error: {e.orig.diag.message_primary}"
    except AttributeError:
        return f"Error: {str(e)}"
                
def clear_existing_reject_file(file_path):
    """Delete the existing reject file if it exists."""
    reject_file_path = f"{file_path}.reject"
    if os.path.exists(reject_file_path):
        os.remove(reject_file_path)
        print(f"Deleted existing reject file: {reject_file_path}")
        
def preprocess_values(values_part):
    """Preprocess the VALUES part to handle both single and double quotes."""
    # Replace escaped quotes with placeholder
    values_part = values_part.replace("''", "§SINGLE§").replace('""', "§DOUBLE§")
    
    # Replace content inside quotes with placeholders
    single_quoted = re.findall(r"'(.*?)'", values_part)
    double_quoted = re.findall(r'"(.*?)"', values_part)
    
    for i, content in enumerate(single_quoted):
        values_part = values_part.replace(f"'{content}'", f"§S{i}§", 1)
    for i, content in enumerate(double_quoted):
        values_part = values_part.replace(f'"{content}"', f"§D{i}§", 1)
    
    return values_part, single_quoted, double_quoted

def postprocess_dataframe(df, single_quoted, double_quoted):
    """Postprocess the DataFrame to restore quoted content."""
    for column in df.columns:
        df[column] = df[column].apply(lambda x: x.replace("§SINGLE§", "'").replace("§DOUBLE§", '"') if isinstance(x, str) else x)
        for i, content in enumerate(single_quoted):
            df[column] = df[column].apply(lambda x: content if x == f"§S{i}§" else x)
        for i, content in enumerate(double_quoted):
            df[column] = df[column].apply(lambda x: content if x == f"§D{i}§" else x)
    return df

def parse_insert_statement(insert_statement, file_path, first_reject=True):
    """Parse a SQL INSERT statement using pandas, handling both single and double quotes."""
    table_name = "Unknown"
    columns = []
    try:
        # Extract table name
        table_match = re.search(r'INSERT INTO `?(\w+)`?', insert_statement)
        table_name = table_match.group(1) if table_match else "Unknown"

        # Extract column names
        columns_match = re.search(r'\((.*?)\)[\s\n]*VALUES', insert_statement, re.DOTALL)
        if columns_match:
            columns = [col.strip('` ') for col in columns_match.group(1).split(',')]
        else:
            columns = []

        # Extract VALUES part
        values_part = re.split(r'VALUES\s*', insert_statement, flags=re.IGNORECASE)[1]
        values_part = values_part.strip().rstrip(';')
        
        # Preprocess VALUES part
        processed_values, single_quoted, double_quoted = preprocess_values(values_part)
        
        # Use pandas to read the values
        df = pd.read_csv(io.StringIO(processed_values), 
                         header=None, 
                         names=columns, 
                         skipinitialspace=True,
                         keep_default_na=False,
                         na_values=['NULL'])
        
        # Postprocess the DataFrame
        df = postprocess_dataframe(df, single_quoted, double_quoted)

        # Convert DataFrame to list of dictionaries
        data_list = df.to_dict('records')

        # Check for mismatches
        if len(df.columns) != len(columns):
            reason = f"Mismatch in column count. Expected: {len(columns)}, Found: {len(df.columns)}"
            print(reason)
            write_rejected_data(file_path, values_part, values_part, reason, table_name, columns, first_reject)
            first_reject = False

        return table_name, data_list
    except Exception as e:
        reason = f"Failed to parse INSERT statement: {str(e)}"
        print(reason)
        write_rejected_data(file_path, insert_statement, insert_statement, reason, table_name, columns, first_reject)
        return None, []

# The write_rejected_data and read_dml_extracts functions can remain the same as in the previous examples

def write_rejected_data(file_path, original_row, rejected_row, reason, table_name, columns, first_reject=False):
    """Write original and rejected data to a .sql.reject file."""
    reject_file_path = f"{file_path}.reject"
    
    if first_reject and os.path.exists(reject_file_path):
        os.remove(reject_file_path)
    
    with open(reject_file_path, 'a', encoding='utf-8') as reject_file:
        column_names = ', '.join(columns)
        reject_file.write(f"INSERT INTO {table_name} ({column_names}) VALUES {original_row};\n")
        reject_file.write(f"-- Rejected: {reason}\n")
        reject_file.write(f"-- {rejected_row}\n\n")
    print(f"Rejected data written to {reject_file_path}")

def read_dml_extracts(export_path):
    """Read and parse all DML SQL files in the given directory."""
    print(f"Reading DML extracts from {export_path}")
    all_data = {}
    for file_path in glob.glob(os.path.join(export_path, '*.sql')):
        print(f"Processing file: {file_path}")
        first_reject = True
        content = ""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()

            # Strip a leading /* ... */ header comment (every real FileMaker
            # export has one, e.g. "/* Table: ratcatalogue Rows: N Date: ... */").
            # Without this, the chunk before the first real INSERT INTO doesn't
            # start with that keyword, so the "prepend INSERT INTO" fallback
            # below glues the comment onto a doubled/malformed statement.
            content = re.sub(r'^\s*/\*.*?\*/\s*', '', content, flags=re.DOTALL)

            # Split content into individual INSERT statements
            insert_statements = re.split(r';[\s\n]*INSERT INTO', content)
            insert_statements = [stmt if stmt.strip().upper().startswith('INSERT INTO') else f'INSERT INTO {stmt}' 
                                 for stmt in insert_statements if stmt.strip()]
            
            print(f"Found {len(insert_statements)} INSERT statements in {file_path}")
            
            for stmt in insert_statements:
                table_name, data_list = parse_insert_statement(stmt, file_path, first_reject)
                first_reject = False
                if table_name:
                    if table_name not in all_data:
                        all_data[table_name] = []
                    all_data[table_name].extend(data_list)
            
            print(f"Processed {sum(len(data) for data in all_data.values())} rows from {file_path}")
                        
        except Exception as e:
            reason = f"Failed to process file: {str(e)}"
            print(reason)
            write_rejected_data(file_path, content, content, reason, "Unknown", [], first_reject)
    
    print(f"Parsed data for {len(all_data)} tables")
    for table_name, data in all_data.items():
        print(f"  {table_name}: {len(data)} records")
    
    return all_data
    
def resolve_active_profile(config, cli_val=None):
    """Which [database.target.<profile>] is active. Precedence: CLI > env
    RAT_TARGET_PROFILE > config.toml active_profile (or legacy 'db' key) > 'supabase'."""
    tgt = config['database']['target']
    return resolve_secret(
        'RAT_TARGET_PROFILE',
        cfg_val=tgt.get('active_profile') or tgt.get('db'),
        cli_val=cli_val,
        default='supabase',
    )

def get_db_engine(config, profile_override=None):
    tgt = config['database']['target']
    profile = resolve_active_profile(config, profile_override)
    db_config = tgt[profile]
    pwd = resolve_target_pwd(profile, cfg_val=db_config.get('pwd', ''))
    dbname = db_config.get('dbname') or db_config.get('dsn') or 'postgres'
    db_url = f"postgresql://{db_config['user']}:{url_quote(pwd)}@{db_config['host']}:{db_config['port']}/{dbname}"
    return create_engine(db_url)
  
def get_table(table_name, schema = 'public'):
    """Get a SQLAlchemy Table object for the given table name."""
    metadata = MetaData(schema = schema)
    return Table(table_name, metadata, autoload_with = engine)

def get_or_create_user(username):
    users_table = get_table('users', mig_schema)
    if not username:
        username = config['database']['target']['default_migration_user']
        result = supabase.execute(select(users_table).where(users_table.c.username == username)).first()
        if not result:
            raise ValueError("Default migration_user not found")
        return result.id
    
    result = supabase.execute(select(users_table).where(users_table.c.username == username)).first()
    if result:
        return result.id
    
    # Insert user
    new_user = {
        'username': username,
        'email': f"{username}@example.com"
    }
    result = supabase.execute(insert(users_table).values(**new_user).returning(users_table.c.id))
    supabase.commit()
    return result.scalar_one()
    
def read_data_from_migration_schema(table_name):
    """Read data from the migration schema in PostgreSQL."""  
    table = get_table(table_name, mig_schema)
    with engine.connect() as connection:
        result = connection.execute(select(table))
        columns = result.keys()
        data = []
        for row in result:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i] if i < len(row) else None
            data.append(row_dict)
        return data

def get_last_migrated_id(table_name):
    """Get the ID of the last migrated record for a given table."""
    migration_log_table = get_table('migration_log', mig_schema)
    result = supabase.execute(
        select(migration_log_table.c.last_migrated_id)
        .where(migration_log_table.c.table_name == table_name)
    ).first()
    return result[0] if result else None

def update_migration_log(table_name, last_id):
    """Update the migration log with the last migrated ID for a table."""
    migration_log_table = get_table('migration_log', mig_schema)
    stmt = (
        pg_insert(migration_log_table)
        .values(table_name=table_name, last_migrated_id=last_id)
        .on_conflict_do_update(
            index_elements=['table_name'],
            set_=dict(last_migrated_id=last_id)
        )
    )
    supabase.execute(stmt)
    supabase.commit()
    
def table_exists(table_name, schema):
    """Check if a table exists in the specified schema."""
    inspector = inspect(engine)
    return inspector.has_table(table_name, schema = schema)

def create_table_if_not_exists(table_name, schema):
    """Check if a table exists, and log a warning if it doesn't."""
    if not table_exists(table_name, schema):
        logger.warning(f"Table {schema}.{table_name} does not exist. Please create it manually.")
        return False
    return True

def clean_string_data(value):
    """Clean string data by removing leading/trailing whitespace."""
    if isinstance(value, str):
        return value.strip()
    return value

def clean_record_data(record):
    """Clean all string values in a record dictionary."""
    return {k: clean_string_data(v) for k, v in record.items()}
          
def batch_upsert(table_name, data, uniq_columns=['id'], return_after_batch=False, quiet=True, batch_data={}):
    """
    Perform batch upsert operations with bulk insert optimization while maintaining compatibility
    with existing error handling and logging.
    """
    global user_id, batch_size
    
    success_cnt = 0
    
    # Table comes with schema
    tab = table_name.split('.')
    tgt_schema = tab[0]
    table_name = tab[1]
    current_time = datetime.now(timezone.utc).isoformat()
    
    try:
        data_len = len(data) 
        if return_after_batch:
            # This is done at the callee level for efficiency
            batches = [data]
            tgt_table_obj = batch_data[table_name]['table_obj']
            columns_info = batch_data[table_name]['columns_info']
            error_counts = batch_data[table_name]['error_counts']
            error_records = batch_data[table_name]['error_records'] 
        else:
            # This is a one-off task so we can get our data here
            tgt_table_obj = get_table(table_name, tgt_schema)
            
            # Get column size constraints from table
            columns_info = {c.name: length for c in tgt_table_obj.columns
                              if (length := getattr(c.type, 'length', None)) is not None}
          
            # Check table exists
            if not create_table_if_not_exists(table_name, tgt_schema):
                return False

            error_counts = {
                'unique_violations': 0,
                'foreign_key_violations': 0,
                'other_integrity_errors': 0,
                'other_errors': 0,
                'truncation_errors': 0
            }

            error_records = []
            batches = [data[y:y + batch_size] for y in range(0, data_len, batch_size)]
        
        # Per batch
        for batch in tqdm(batches, desc=f"Upserting {data_len} to {table_name}", disable=quiet): 
            # Clean data in the batch
            cleaned_batch = [clean_record_data(record) for record in batch]
            
            # Check data length
            len_err = False     
            for record in cleaned_batch:                        
                # Check data lengths before insert
                for col, max_length in columns_info.items():
                    if (col in record and 
                        record[col] is not None and  
                        max_length is not None and  
                        isinstance(record[col], str) and  
                        len(record[col]) > max_length):
                        len_err = True
                        logger.error(f"{table_name}: Data too long for column '{col}' in {table_name}. Ref.: {uniq_columns}")
                        logger.error(f"{table_name}: Value (raw): '{batch[col]}'")
                        logger.error(f"{table_name}: Value (repr): {repr(batch[col])}")
                        logger.error(f"{table_name}: Length: {len(batch[col])} > Maximum: {max_length}")
            if len_err:
                return False if return_after_batch else sys.exit(1)
            
            # Add audit data
            for record in cleaned_batch:
                record['created_by'] = user_id
                record['created_date'] = current_time
                record['modified_by'] = user_id
                record['modified_date'] = current_time
            
            try:
                # Try bulk insert first
                stmt = pg_insert(tgt_table_obj).values(cleaned_batch)
                if uniq_columns != ['id']:
                    stmt = stmt.on_conflict_do_nothing(index_elements=uniq_columns)
                
                result = supabase.execute(stmt)
                supabase.commit()
                success_cnt += len(cleaned_batch)
                
            except (IntegrityError, DataError) as e:
                supabase.rollback()
                logger.debug(f"{table_name}: Bulk insert failed, falling back to individual inserts")
                
                # Fall back to individual inserts
                for record in cleaned_batch:
                    try:
                        stmt = pg_insert(tgt_table_obj).values(record)
                        if uniq_columns != ['id']:
                            stmt = stmt.on_conflict_do_nothing(index_elements=uniq_columns)
                        
                        result = supabase.execute(stmt)
                        supabase.commit()
                        success_cnt += 1
                        
                    except IntegrityError as e:
                        supabase.rollback()
                        err_msg = format_pg_error(e)
                        error_detail = {
                            'record': record,
                            'error_type': 'integrity_error',
                            'error_message': err_msg
                        }
                        
                        if isinstance(e.orig, UniqueViolation):
                            error_counts['unique_violations'] += 1
                            error_detail['specific_type'] = 'unique_violation'
                            if uniq_columns == ['id']:
                                error_detail['ignore'] = True
                        elif isinstance(e.orig, ForeignKeyViolation):
                            error_counts['foreign_key_violations'] += 1
                            error_detail['specific_type'] = 'foreign_key_violation'
                            logger.warning(f"{table_name}: Foreign key violation: {err_msg}")
                            logger.debug(f"Constraint: {e.orig.diag.constraint_name}")
                        else:
                            error_counts['other_integrity_errors'] += 1
                            error_detail['specific_type'] = 'other_integrity_error'
                            logger.warning(f"{table_name}: Other integrity error: {err_msg}")
                        
                        if error_detail != {}:
                            error_records.append(error_detail)
                            
                    except DataError as e:
                        supabase.rollback()
                        error_counts['truncation_errors'] += 1
                        err_msg = format_pg_error(e)
                        logger.warning(f"{table_name}: Data truncation: {err_msg}")
                        if isinstance(e.orig, StringDataRightTruncation):
                            logger.debug(f"Column: {e.orig.diag.column_name}, Length: {e.orig.diag.message_detail}")
                        
                    except Exception as e:
                        supabase.rollback()
                        error_msg = format_pg_error(e)
                        logger.error(f"{table_name}: Error upserting record: {error_msg}")
                        
                        if 'violates row-level security policy' in str(e):
                            logger.error(f"{table_name}: This error is likely due to Row Level Security (RLS) policies. "
                                      "Ensure you're using a service role key with necessary permissions.")
                        
                        error_records.append({
                            'record': record,
                            'error_type': 'other_error',
                            'error_message': error_msg
                        })
                        
                        # Check for repeated errors
                        if len(error_records) > 1:
                            if any(er.get('error_message') == error_msg for er in error_records[:-1]):
                                logger.error(f"{table_name}: Same error encountered after rollback - exiting")
                                sys.exit(1)
            
            except Exception as e:
                supabase.rollback()
                error_msg = format_pg_error(e)
                logger.error(f"{table_name}: Error in batch operation: {error_msg}")
                if 'violates row-level security policy' in str(e):
                    logger.error(f"{table_name}: RLS policy violation. Check service role permissions.")
                return False
        
        # Summary logging
        error_cnt = len([r for r in error_records if not r.get('ignore', False)])
        if success_cnt > 0:
            if debug: 
                logger.info(f"{table_name}: Successfully inserted {success_cnt} records")
        if error_cnt > 0:
            logger.info(f"{table_name}: Failed to insert {error_cnt} records")
        
        return error_cnt == 0
        
    except Exception as e:
        logger.error(f"{table_name}: Error in batch_insert function: {str(e)}")
        logger.exception(f"{table_name}: Exception details:")
        sys.exit(1)
    

def get_country_id(tab, data, ref_data = ''):
    cached_id = lookup_caches.get('country', {}).get(stripy(data))
    if cached_id is not None:
        return cached_id

    stmt = select(tab.c.id).where(tab.c.name == data)
    #print(stmt.compile(compile_kwargs={"literal_binds": True}))
    result = supabase.execute(stmt).first()
    id = result[0] if result is not None else None
    
    if id is None:
      def_name = 'unknown'
      if ref_data != '':
          ref_data = f" \"{ref_data}\":"
      logger.warning(f"{tab.name}:{ref_data} No id found in \"{tab.name}\" table. \"{data}\" returned, so defaulting to \"{def_name}\"")
      stmt = select(tab.c.id).where(tab.c.name == def_name)
      result = supabase.execute(stmt).first()
      id = result[0] if result is not None else None
      
    return id
  
def add_location(location_name: str, country_name: str = 'unknown', check_exists: bool = False) -> bool:
    """ Add a single location record to the database. """
    tgt_table = 'location'
    logger.info(f"{tgt_table}: Adding missing location: \"{location_name}\"")

    try:
        # Check if location already exists
        if check_exists:
            Location = get_table(tgt_table, tgt_schema)
            stmt = select(Location.c.id).where(Location.c.name == location_name)
            result = supabase.execute(stmt).first()
            if result:
                logger.info(f"{tgt_table}: Location {location_name} already exists")
                return True

        # Get the country table and ID
        Country = get_table('country', tgt_schema)
        country_id = get_country_id(Country, country_name, location_name)
        
        if not country_id:
            logger.error(f"{tgt_table}: Failed to add location {location_name}: Country {country_name} not found")
            return False

        # Create location data
        location_data = [{
            'name': location_name,
            'country_id': country_id
        }]

        # Attempt to insert/update
        batch_upsert(f"{tgt_schema}.{tgt_table}", location_data, uniq_columns=['name'], quiet=True)
        if debug: logger.info(f"{tgt_table}: Successfully added location: \"{location_name}\"")
            
        return True

    except Exception as e:
        logger.error(f"{tgt_table}: Error adding location {location_name}: {str(e)}")
        logger.exception(f"{tgt_table}: Exception details:")
        return False
      
def add_builder(builder_code: str, builder_name: str | None = None, location_name: str = 'unknown'):
    """ Add a single builder record to the database. """
    tgt_table = 'builder'
    logger.info(f"{tgt_table}: Adding missing builder: {builder_code}")
    
    # Clean up data. Assume code is always upper case
    builder_code = builder_code.upper()
    
    try:
        # Check if builder already exists
        Builder = get_table(tgt_table, tgt_schema)
        stmt = select(Builder.c.id).where(Builder.c.code == builder_code)
        result = supabase.execute(stmt).first()
        if result:
            #logger.info(f"{tgt_table}: Builder {builder_code} already exists")
            return True

        # Get the location ID
        Location = get_table('location', tgt_schema)
        location_id = get_location_id(Location, location_name, builder_code)
        
        if not location_id:
            logger.error(f"{tgt_table}: Failed to add builder {builder_code}: Location {location_name} not found")
            return False

        # If builder_name not provided, use code as name
        if not builder_name:
            builder_name = builder_code

        # Create builder data
        builder_data = [{
            'code': builder_code,
            'name': builder_name,
            'location_id': location_id
        }]

        # Attempt to insert/update
        batch_upsert(f"{tgt_schema}.{tgt_table}", builder_data, uniq_columns=['code'], quiet=True)
        logger.info(f"{tgt_table}: Successfully added builder: {builder_code}")
            
        # Look up new builder id
        try:
            def_builder_code = builder_code if builder_code not in null_data_lst else 'UNK'  # Default builder code
            stmt = select(Builder.c.id).where(Builder.c.code == def_builder_code)
            result = supabase.execute(stmt).first()
            id = result[0] if result is not None else None
        except Exception as e:
            id = -1
        
        return id

    except Exception as e:
        logger.error(f"{tgt_table}: Error adding builder {builder_code}: {str(e)}")
        logger.exception(f"{tgt_table}: Exception details:")
        return False
      
def get_location_id(tab, data, ref_data = ''):
    data = stripy(data)
    cached_id = lookup_caches.get('location', {}).get(data)
    if cached_id is not None:
        return cached_id

    stmt = select(tab.c.id).where(tab.c.name == data)
    #print(stmt.compile(compile_kwargs={"literal_binds": True}))
    result = supabase.execute(stmt).first()
    id = result[0] if result is not None else None

    if id is None:
        def_name = 'unknown'
        if ref_data != '':
            ref_data = f" Ref. \"{ref_data}\": "
        logger.warning(f"{tab.name}:{ref_data}No id found in table \"{tab.name}\" for \"{data}\", so will add it to location table")
        # Add location if we're just missing it
        if data != None:
            if add_location(data):
              def_name = data

        # Look up new location id
        stmt = select(tab.c.id).where(tab.c.name == def_name)
        result = supabase.execute(stmt).first()
        id = result[0] if result is not None else None

    # Populate/refresh the cache so a repeat reference to this name later in
    # the same run (e.g. another route's start/end location) hits the fast
    # path above instead of repeating a live lookup.
    if id is not None and 'location' in lookup_caches:
        lookup_caches['location'][data] = id

    return id
  
def get_builder_id(tab, builder_code, builder_name='', ref_data=''):
    """Get builder ID by code, creating the builder if it doesn't exist."""
    
    if builder_code != None:
        # Assume all codes are upper case
        builder_code = stripy(builder_code.upper())
    
        stmt = select(tab.c.id).where(tab.c.code == builder_code)
        result = supabase.execute(stmt).first()
        id = result[0] if result is not None else None
    else:
      id = None
      
    if id is None:
        def_builder_code = builder_code if builder_code not in null_data_lst else 'UNK'  # Default builder code
        def_builder_name = builder_name if builder_name not in null_data_lst else 'unknown'  # Default builder code
        
        if def_builder_code == 'UNK':
            stmt = select(tab.c.id).where(tab.c.code == def_builder_code)
            result = supabase.execute(stmt).first()
            id = result[0] if result is not None else None
        else:
            # Add builder if builder code missing
            if ref_data != '':
                ref_data = f" Ref. \"{ref_data}\": "
            logger.warning(f"{tab.name}:{ref_data}No id found for \"{tab.name}\": code=\"{def_builder_code}\" name=\"{def_builder_name}\" so will add it to builder table")
            assert def_builder_code is not None  # only 'UNK' (handled above) can be None-derived
            id = add_builder(def_builder_code, def_builder_name)
      
    return id
  

def get_entity_id(table_name: str, search_field: str, search_value: str | None) -> str | None:
    """
    Generic function to get an entity's ID based on a search field and value.
    
    Args:
        table_name: Name of the table to search
        search_field: Field to search on (e.g., 'name', 'image_no')
        search_value: Value to search for
        schema: Database schema name (defaults to target schema)
    
    Returns:
        The ID if found, else None
    """
    try:
        if search_value is None:
            return None
          
        search_value = stripy(search_value)
            
        # Get table reference
        Table = get_table(table_name, tgt_schema)
        
        # Build and execute query
        stmt = select(Table.c.id).where(getattr(Table.c, search_field) == search_value)
        result = supabase.execute(stmt).first()
        
        return result[0] if result else None
        
    except Exception as e:
        logger.error(f"Error getting {table_name} ID for {search_field}='{search_value}': {str(e)}")
        return None
      
def create_lookup_indexes():
    """Create necessary indexes for lookup tables if they don't exist."""
    indexes = [
        ('catalog', 'image_no'),
        ('organisation', 'name'),
        ('location', 'name'),
        ('route', 'name'),
        ('collection', 'name'),
        ('photographer', 'name'),
        ('builder', 'code')
    ]
    
    for table, column in indexes:
        index_name = f"idx_{table}_{column}"
        try:
            # Check if index exists
            check_sql = f"""
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = '{tgt_schema}'
                AND tablename = '{table}'
                AND indexname = '{index_name}'
            """
            result = supabase.execute(text(check_sql)).first()
            
            if not result:
                # Create index if it doesn't exist
                create_sql = f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {tgt_schema}.{table} ({column})
                """
                supabase.execute(text(create_sql))
                supabase.commit()
                logger.info(f"Created index {index_name} on {tgt_schema}.{table}({column})")
        except Exception as e:
            logger.warning(f"Error creating index on {table}.{column}: {str(e)}")

def create_lookup_cache(table_name, lookup_column):
    """Create a lookup dictionary for a table."""
    try:
        sql = f"""
            SELECT id, {lookup_column}
            FROM {tgt_schema}.{table_name}
        """
        result = supabase.execute(text(sql))
        
        # Create dictionary with stripped values
        cache = {
            stripy(row[1]): row[0]
            for row in result
            if row[1] is not None
        }
        
        logger.info(f"Created lookup cache for {table_name}: {len(cache)} entries")
        return cache
    except Exception as e:
        logger.error(f"Error creating lookup cache for {table_name}: {str(e)}")
        return {}

def get_entity_id_from_cache(cache, value):
    """Get entity ID from cache, handling None values."""
    if value is None:
        return None
    return cache.get(stripy(value))

def cache_lookup_table(table_name, lookup_column):
    """(Re)build the lookup cache for a single table, updating just that key in
    lookup_caches rather than replacing the whole dict -- safe to call early
    (e.g. right after a table's own migrate_* completes) without clobbering
    any other table's cache built earlier in the same run."""
    lookup_caches[table_name] = create_lookup_cache(table_name, lookup_column)

def create_lookup_index_cache():
    """Build (or rebuild) every lookup cache. Cheap to call more than once --
    each call is one bulk SELECT per table -- so later callers see any rows
    added since an earlier partial build (e.g. via add_location())."""
    cache_lookup_table('catalog', 'image_no')
    cache_lookup_table('organisation', 'name')
    cache_lookup_table('location', 'name')
    cache_lookup_table('route', 'name')
    cache_lookup_table('collection', 'name')
    cache_lookup_table('photographer', 'name')
    cache_lookup_table('builder', 'code')

def update_picture_catalog_ids(metadata_records):
    """Update catalog_ids using existing lookup cache."""
    for record in metadata_records:
        image_no = Path(record['file_name']).stem
        record['catalog_id'] = lookup_caches['catalog'].get(stripy(image_no))

def process_image_folder():
    """Process all images in a folder and return metadata records."""
    metadata_records = []
    folder_path = Path(f"{config['export']['path']}/{config['export']['image_path']}/webp").resolve()
    # Get all image files (adjust extensions as needed)
    image_files = []
    #'.jpg', '.jpeg', '.png', '.gif'
    #for ext in ['.webp']:
    ext = '.webp'
    image_files.extend(folder_path.glob(f'*{ext}'))
    
    for img_path in image_files:
        try:
            # Open image and get basic info
            with Image.open(img_path) as img:
                # Get filename without extension
                image_no = img_path.stem
                
                metadata = {
                    'catalog_id': None,  # Will need to look this up based on image_no
                    'file_name': img_path.name,
                    'file_location': None,
                    'file_type': img.format.lower(),
                    'file_size': os.path.getsize(img_path),
                    'width': img.width,
                    'height': img.height,
                    'resolution': f"{img.info.get('dpi', (None, None))[0]}",
                    'colour_space': img.mode
                }
                
                # Map PIL color modes to your valid colour_space values
                colour_space_mapping = {
                    'RGB': 'sRGB',
                    'RGBA': 'sRGB',
                    'CMYK': 'CMYK',
                    'L': 'Grayscale',
                    'LAB': 'LAB'
                }
                metadata['colour_space'] = colour_space_mapping.get(img.mode)
                
                # Determine colour_mode
                if img.mode in ['L', 'LA']:
                    metadata['colour_mode'] = 'grayscale'
                else:
                    metadata['colour_mode'] = 'colour'
                
                metadata_records.append(metadata)
                
        except Exception as e:
            logger.error(f"Error processing image {img_path}: {str(e)}")
            continue
            
    return metadata_records
  
def upload_images_to_supabase(storage_bucket="rat", storage_folder="images"):
    """Upload webp images to Supabase storage."""
    logger.info("Starting Supabase storage upload")
    
    # Get path to webp images from config
    folder_path = Path(f"{config['export']['path']}/{config['export']['image_path']}/webp").resolve()
    
    # Get image files
    image_files = list(folder_path.glob('*.webp'))
    uploaded_count = 0
    error_count = 0
    
    for img_path in tqdm(image_files, desc="Uploading images"):
        file_name = img_path.name
        try:
            # Read file content
            with open(img_path, 'rb') as f:
                file_content = f.read()
            
            # Upload to Supabase storage
            storage_path = f"{storage_folder}/{file_name}"
            
            # Use engine since we already have the SQLAlchemy connection
            with engine.connect() as conn:
                response = conn.execute(
                    text("""
                    SELECT storage.upload($1, $2, $3, $4)
                    """),
                    {
                        "bucket": storage_bucket,
                        "path": storage_path,
                        "file": file_content,
                        "content_type": "image/webp"
                    }
                )
                
            uploaded_count += 1
            if debug:
                logger.debug(f"Uploaded {file_name}")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Error uploading {file_name}: {str(e)}")
            continue
    
    logger.info(f"Supabase storage upload completed. Successfully uploaded {uploaded_count} images, {error_count} errors.")
      
# ---------- Target Schema Migration Functions ---------- 

def migrate_country(df):
    """Migrate country data to Supabase."""
    tgt_table = 'country'
    logger.info(f"{tgt_table}: Starting migration")
    countries = df[tgt_table].dropna().unique()
    # Add unknown to handle missing info
    countries = list(df[tgt_table].dropna().unique()) + ["unknown"]
    country_data = [{'name': country} for country in countries]
    batch_upsert(f"{tgt_schema}.{tgt_table}", country_data, uniq_columns=['name'])
    logger.info(f"{tgt_table}: Completed country migration. Migrated {len(countries)} countries")

def migrate_organisation(df):
    """Migrate organisation data to Supabase."""
    tgt_table = 'organisation'
    logger.info(f"{tgt_table}: Starting migration")
    organisations = df[[tgt_table, 'country', 'organisation_type']].dropna(subset=['organisation']).drop_duplicates()
    org_data = OrderedDict()
    Country = get_table('country', tgt_schema)
    for _, row in organisations.iterrows():
        try:
            if row['country'] != None:
                org_name = row['organisation']
                # Get FKs
                country_id = get_country_id(Country, row['country'], org_name)
                # Add PK as index so any "organisation" dupes get over written 
                org_data[org_name] = {
                    'name': org_name,
                    'type': row['organisation_type'],
                    'country_id': country_id
                }  
                #print(f"{country_id} : organisation = {row['organisation']} : organisation_type = {row['organisation_type']}")
        except Exception as e:
            logger.error(f"{tgt_table}: Error processing organisation: {str(e)}")


    # Convert the OrderedDict values to a list
    org_data_lst = list(org_data.values())
    batch_upsert(f"{tgt_schema}.{tgt_table}", org_data_lst, uniq_columns=['name'])
    logger.info(f"{tgt_table}: Completed migration. Migrated {len(org_data_lst)} organisations")

def migrate_location(df):
    """Migrate location data to Supabase."""
    tgt_table = 'location'
    logger.info(f"{tgt_table}: Starting migration")
    locations = df[[tgt_table, 'country']].dropna(subset=[tgt_table]).drop_duplicates()
    location_data = OrderedDict()
    Country = get_table('country', tgt_schema)
    
    for _, row in locations.iterrows():
        try:
            location_name = row['location']
            # Get FKs
            country_id = get_country_id(Country, row['country'], location_name)
      
            location_data[location_name] = {
                'name': location_name,
                'country_id': country_id
            }
        except Exception as e:
            logger.error(f"{tgt_table}: Error processing location: {str(e)}")
    
    # Convert the OrderedDict values to a list
    location_data_lst = list(location_data.values())
    batch_upsert(f"{tgt_schema}.{tgt_table}", location_data_lst, uniq_columns=['name'])
    logger.info(f"{tgt_table}: Completed migration. Migrated {len(locations)} locations")

def migrate_route(df):
    """Migrate route data to Supabase."""
    tgt_table = 'route'
    logger.info(f"{tgt_table}: Starting migration")
    routes = df[[tgt_table, 'start_location', 'end_location']].dropna(subset=['route']).drop_duplicates()
    route_data = OrderedDict()
    Location = get_table('location', tgt_schema)
    for _, row in routes.iterrows():
        route = row['route']
        # Get FKs
        start_location_id = get_location_id(Location, row['start_location'], route)
        end_location_id = get_location_id(Location, row['end_location'], route)
        route_data[route] = {
            'name': route,
            'start_location_id': start_location_id,
            'end_location_id': end_location_id
        }
    route_data_lst = list(route_data.values())
    batch_upsert(f"{tgt_schema}.{tgt_table}", route_data_lst, uniq_columns=['name'])
    logger.info(f"{tgt_table}: Completed migration. Migrated {len(routes)} routes")

def migrate_catalog(df):
    """Migrate catalog data to Supabase."""
    tgt_table = 'catalog'
    logger.info(f"{tgt_table}: Starting migration")
    # Get the target table structure
    Catalog = get_table('catalog', tgt_schema)
    # Get list of valid column names from the SQLAlchemy Table object
    valid_columns = [c.name for c in Catalog.columns] 
    #logger.debug(f"{tgt_table}: Valid columns in target schema: {valid_columns}")
    catalog_data = OrderedDict()
    
    for _, row in df.iterrows():
        try:
            image_no = stripy(row['image_no']) 
            # Create data dictionary with only valid columns
            record = {
                col: row[col] 
                for col in valid_columns 
                if col in row and col not in ['id', 'created_date']  # Exclude auto-generated columns
            }          
            catalog_data[image_no] = record    
        except Exception as e:
            logger.error(f"{tgt_table}: Error processing catalog entry {row.get('image_no', 'unknown')}: {str(e)}")
            logger.exception(f"{tgt_table}: Exception details:")
    
    try:
        catalog_data_lst = list(catalog_data.values())
        batch_upsert(f"{tgt_schema}.{tgt_table}", catalog_data_lst, uniq_columns=['image_no'], quiet=False)
        logger.info(f"{tgt_table}: Completed catalog migration. Migrated {len(catalog_data_lst)} entries")
    except Exception as e:
        logger.error(f"Error during batch upsert of catalog entries: {str(e)}")
        logger.exception("Exception details:")

def migrate_catalog_metadata(df):
    """Migrate catalog metadata to Supabase with optimized lookups."""
    
    global lookup_caches
    
    tgt_table = 'catalog_metadata'
    logger.info(f"{tgt_table}: Starting metadata migration")

    metadata_data = []
    skipped = 0
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing metadata"):
        catalog_id = lookup_caches['catalog'].get(stripy(row['image_no']))
        if catalog_id is None:
            # This source row's own catalog insert already failed/was rejected
            # (e.g. NULL image_no) -- metadata for a catalog entry that doesn't
            # exist is unlinkable and, unlike catalog_builder's builder_id, has
            # no sentinel that would make it meaningfully re-runnable: catalog_id
            # is UNIQUE-constrained (SQL NULL != NULL means every re-run would
            # insert yet another orphan row). Skip rather than accumulate them.
            skipped += 1
            continue
        metadata_data.append({
            'catalog_id': catalog_id,
            'organisation_id': lookup_caches['organisation'].get(stripy(row['organisation'])),
            'location_id': lookup_caches['location'].get(stripy(row['location'])),
            'route_id': lookup_caches['route'].get(stripy(row['route'])),
            'collection_id': lookup_caches['collection'].get(stripy(row['collection'])),
            'photographer_id': lookup_caches['photographer'].get(stripy(row['photographer']))
        })
    #uniq_columns=['catalog_id', 'collection_id', 'photographer_id' ,'organisation_id', 'location_id', 'route_id']
    batch_upsert(f'{tgt_schema}.catalog_metadata', metadata_data, uniq_columns=['catalog_id'])
    if skipped:
        logger.warning(f"{tgt_table}: Skipped {skipped} row(s) with no resolvable catalog_id (orphaned metadata)")
    logger.info(f"Completed catalog metadata migration. Migrated {len(metadata_data)} metadata entries")


def migrate_usage(df):
    """Migrate usage data to Supabase."""
    tgt_table = 'usage'
    batch_size = 10000
    total_processed = 0
    usage_data = []
    
    logger.info(f"{tgt_table}: Starting data migration")
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {tgt_table}", disable=True):
        catalog_id = lookup_caches['catalog'].get(stripy(row['image_no']))
        if catalog_id == None:
            logger.warning(f"{tgt_table}: Cannot obtain catalog_id for \"{row['image_no']}\"? Skipping.")
            continue

        usage_data.append({
            'catalog_id': catalog_id,
            'prints_allowed': row['prints_allowed'] == 'yes',
            'internet_use': row['internet_use'] == 'yes',
            'publications_use': row['publications_use'] == 'yes'
        })
        
        # When we reach batch_size, process the batch
        if len(usage_data) >= batch_size:
            batch_upsert(f'{tgt_schema}.usage', usage_data, uniq_columns=['catalog_id'])
            total_processed += len(usage_data)
            #if debug: logger.debug(f"{tgt_table}: Processed {total_processed} records")
            usage_data = []  # Clear the batch
            
    # Process any remaining records
    if usage_data:
        batch_upsert(f'{tgt_schema}.usage', usage_data)
        total_processed += len(usage_data)
        
    logger.info(f"{tgt_table}: Completed usage data migration. Migrated {total_processed} usage entries")
    
def migrate_collection(df):
    """Migrate collection data to Supabase."""
    logger.info("Starting collection migration")
    
    # Filter and prepare collection data
    collections = df[['collection', 'owner', 'donor', 'storage_location']].dropna(subset=['collection']).drop_duplicates()
    collection_data = []
    for _, row in collections.iterrows():              
        collection_data.append({
            'name': row['collection'],
            'owner': row['owner'],
            'donor': row['donor'],
            'storage_location': row['storage_location']
        })
    
    # Perform batch upsert
    batch_upsert(f'{tgt_schema}.collection', collection_data, uniq_columns=['name'])
    logger.info(f"Completed collection migration. Migrated {len(collection_data)} collections")
    
def migrate_photographer(df):
    logger.info("Starting photographer migration")
    
    # Debug: Print all unique values in the 'photographer' column
    unique_photographers = df['photographer'].dropna().unique()
    
    if debug and False: 
      logger.info(f"Unique photographers found: {unique_photographers}")
    
      # Count occurrences of each photographer
      photographer_counts = df['photographer'].value_counts(dropna=True)
      logger.info(f"Photographer counts:\n{photographer_counts}")
      
      # Additional check: Print the first few rows of the DataFrame
      logger.info(f"First few rows of the DataFrame:\n{df.head()}")
    
    photographer_data = [{'name': photographer} for photographer in unique_photographers]
    
    batch_upsert(f'{tgt_schema}.photographer', photographer_data, uniq_columns=['name'])
    logger.info(f"Completed photographer migration. Migrated {len(photographer_data)} photographers")

def ensure_unknown_builder():
    """Make sure a sentinel 'UNK' builder row exists. catalog_builder rows whose
    Builder code doesn't resolve fall back to this real row's id instead of
    NULL, so they get a stable, non-NULL natural key on every re-run (SQL
    NULL != NULL, so a NULL builder_id would never conflict with itself and
    would duplicate every time). Matches the existing 'unknown' sentinel
    convention already used for location/country."""
    batch_upsert(f"{tgt_schema}.builder", [{'code': 'UNK', 'name': 'Unknown'}], uniq_columns=['code'], quiet=True)

def migrate_builder(df):
    tgt_table = 'builder'
    logger.info(f"{tgt_table}: Starting builder migration")
    # Assuming 'builder' and 'builder_location' columns exist in the DataFrame
    builders = df[['Builder code', 'Builder name', 'Location']].dropna(subset=['Builder code']).drop_duplicates()
    builder_data = OrderedDict()
    Location = get_table('location', tgt_schema)
    # Fetch the location null value
    location_nvl = get_location_id(Location, 'unknown')
    for _, row in builders.iterrows():
        builder_code = row['Builder code']
        builder_name = row['Builder name']
        if pd.notna(row['Location']):
            location_id = get_location_id(Location, row['Location'], f"migrate_{tgt_table}: {builder_code}")
        else:
            location_id = location_nvl
          
        builder_data[builder_name] = {
            'code': builder_code,
            'name': builder_name,
            'location_id': location_id
        }
        
    builder_data_lst = list(builder_data.values())
    batch_upsert(f"{tgt_schema}.{tgt_table}", builder_data_lst, uniq_columns=['code']) 
    logger.info(f"Completed {tgt_table} migration. Migrated {len(builder_data)} builders")

def stripy(txt):
    # pandas represents a blank source cell as a float NaN, not None -- treat
    # it the same as None rather than crashing on NaN.strip() (Session 8).
    if not isinstance(txt, str):
        return None if txt is None or pd.isna(txt) else txt
    return txt.strip()

def migrate_catalog_builder(df):
    """
    Populate catalog_builder table
    """
    tgt_table = 'catalog_builder'
    logger.info(f"{tgt_table}: Starting migration")
    
    catalog_builders = df[['image_no', 'Builder code', 'Builder code2', 'Builder code3', 
                          'Works number', 'Works number2', 'Works number3', 
                          'Year built', 'Year built2', 'Year built3', 
                          'Plant code', 'Plant code2', 'Plant code3',
                          'Builder name1', 'Builder name2', 'Builder name3']].dropna(subset=['image_no'])
    
    # Increment 2: real key is (catalog_id, builder_id, builder_order) -- 5,927 catalog/builder
    # pairs legitimately repeat with a different builder_order and payload, so this must stay a
    # 3-column key. NEVER dedupe on (catalog_id, builder_id) alone. The constraint already exists
    # in the DB (catalog_builder_catalog_id_builder_id_builder_order_key), so on_conflict_do_nothing
    # below makes this table idempotent on re-run without a truncate.
    uniq_columns = ['catalog_id', 'builder_id', 'builder_order']

    catalog_builder_data = []
    Builder = get_table('builder', tgt_schema)
    Catalog = get_table('catalog', tgt_schema)
    Catalog_Builder = get_table(tgt_table, tgt_schema)
        
    total_processed = 0
    batch_size = 5061  # Increased from 1 for better performance
    error_records = []
    error_counts = {
        'unique_violations': 0,
        'foreign_key_violations': 0,
        'other_integrity_errors': 0,
        'other_errors': 0
    }
    
    # Get column size constraints from table
    columns_info = {c.name: length for c in Catalog_Builder.columns
                      if (length := getattr(c.type, 'length', None)) is not None}
  
    # Check table exists
    if not create_table_if_not_exists(tgt_table, tgt_schema):
        logger.info(f'{tgt_table}: Table not found')
        sys.exit(1)
    
    batch_data = {}
    # Data that we maintain fo reach batch
    batch_data[tgt_table] = {'table_obj' : Catalog_Builder, 'error_records' : error_records,  'error_counts' : error_counts, 'columns_info' : columns_info} 
    
    logger.info(f"{tgt_table}: Gathering Data")
    for _, row in catalog_builders.iterrows():
        image_no = stripy(row.get('image_no'))
        try:
            # Process up to 3 possible entries per image
            for i in [1, 2, 3]:
                suffix = str(i) if i > 1 else ""
                name_suffix = str(i) if i > 1 else "1"  # Handle special case for first builder name

                # Get the values for this train
                builder_code = str(stripy(row.get(f'Builder code{suffix}'))).upper()
                builder_name = stripy(row.get(f'Builder name{name_suffix}'))
                plant_code = stripy(row.get(f'Plant code{suffix}'))
                works_number = stripy(row.get(f'Works number{suffix}'))
                year_built = stripy(row.get(f'Year built{suffix}'))

                # Skip if no data for this train
                builder_code = None if builder_code in ['NONE'] else builder_code

                # Get keys
                catalog_id = lookup_caches['catalog'].get(stripy(image_no))
                if catalog_id is None:
                    # This source row's own catalog insert already failed/was
                    # rejected -- same reasoning as catalog_metadata: unlinkable,
                    # and catalog_id is part of the natural key, so a NULL here
                    # would duplicate on every re-run (SQL NULL != NULL).
                    continue
                builder_id = lookup_caches['builder'].get(stripy(builder_code))

                # Most will skip 2nd and 3rd values
                if not any([builder_id, plant_code, works_number, year_built]):
                    if False:
                        logger.debug(f"{tgt_table}: Skip processing {image_no}: Train {i}: " +
                           f"Builder: builder_id = {builder_id}, {builder_code}, " +
                           f"Plant: {plant_code}, Works: {works_number}, Year: {year_built}")
                    continue

                if builder_id == -1:
                    logger.warning(f"{tgt_table}: {image_no}: Problem getting builder id for builder code {builder_code}")
                    continue

                if builder_id is None:
                    # Builder code didn't resolve, but there's other real payload
                    # (plant_code/works_number/year_built) worth keeping -- fall
                    # back to the sentinel 'UNK' builder (same convention already
                    # used for location/country's 'unknown' row) instead of NULL,
                    # so this row has a real, stable, non-NULL key on every re-run.
                    builder_id = lookup_caches['builder'].get('UNK')

                if debug and False:
                    logger.debug(f"{tgt_table}: Processing image {image_no}: Train {i}: " +
                           f"Builder: {builder_code}/{builder_name}, " +
                           f"Plant: {plant_code}, Works: {works_number}, Year: {year_built}")
                
                catalog_builder_data.append({
                    'catalog_id': catalog_id,
                    'builder_id': builder_id,
                    'plant_code': plant_code,
                    'works_number': works_number,
                    'year_built': year_built,
                    'builder_order': int(i),
                })

        except Exception as e:
            logger.error(f"{tgt_table}: Error compiling data for image {image_no}: {str(e)}")
            logger.exception(f"{tgt_table}: Exception details:")
            continue  # Continue to next record on error
    
    logger.info(f"{tgt_table}: Applying Data")
    cb_data = []
    batch_cnt = 0
    batches, remainder_batch_size = divmod(len(catalog_builder_data), batch_size)
    for cb_data_itm in catalog_builder_data:
        cb_data.append(cb_data_itm)
        
        # Adjust batch_size to handle end of batch
        if batch_cnt + 1 > batches:
            batch_size = remainder_batch_size
            
        # Process batch if we've reached batch_size
        if len(cb_data) >= batch_size:
            try:
                success = batch_upsert(
                    f"{tgt_schema}.{tgt_table}", 
                    cb_data,
                    uniq_columns=uniq_columns,
                    return_after_batch=True,
                    quiet=True,
                    batch_data=batch_data
                )
                batch_cnt += 1
                if success:
                    total_processed += len(cb_data)
                    cb_data = []  # Clear the batch after successful processing
                else:
                    logger.warning(f"{tgt_table}: Batch processing returned Errors, continuing...")
                    if True: # abort
                        sys.exit(1)

                continue

            except Exception as e:
                exc_tb = e.__traceback__
                line_no = exc_tb.tb_lineno if exc_tb else None
                logger.error(f"{tgt_table}: Error processing batch on line {line_no}: {format_pg_error(e)}")
                continue  # Continue instead of raising to handle errors more gracefully

    logger.info(f"{tgt_table}: Migration completed. Total records processed: {total_processed}")
         
   
def migrate_picture_metadata(df):
    """Migrate picture metadata from image folder to database."""
    tgt_table = 'picture_metadata'
    logger.info(f"{tgt_table}: Starting migration")
    #picture_metadata = df[['image_no', 'file_location', 'file_type', 'file_size', 'width', 'height']].dropna(subset=['image_no'])
    #     picture_metadata = df[['image_no', 'cd_no', 'bw_image_no', 'cd_no_hr']].dropna(subset=['image_no'])
        
    try:
        # Process all images
        metadata_data = process_image_folder()
        logger.info(f"Processed {len(metadata_data)} images")
        
        # Update catalog IDs
        update_picture_catalog_ids(metadata_data)
        
        # Insert records
        batch_upsert(f"{tgt_schema}.picture_metadata", metadata_data, uniq_columns=['catalog_id'], quiet=True)
        logger.info(f"{tgt_schema}: Completed picture_metadata migration. Migrated {len(metadata_data)} picture metadata entries")
        
    except Exception as e:
        logger.error(f"Error during picture metadata migration: {str(e)}")
        logger.exception("Exception details:")
    

def check_tables(required_tables, schema):
    # Check
    all_tables_exist = all(create_table_if_not_exists(table, schema) for table in required_tables)
    if not all_tables_exist:
        logger.error(f"Some required tables from the {schema} are missing. Please create them before running the migration.")
        return False
    return True
      
def main():
    """Main function to orchestrate the migration process."""
    
    global user_id, debug, engine, session, logger, batch_size, mig_schema, tgt_schema, config, supabase
    global null_data_lst
    
    # Load config file
    config = load_config()
    
    # Load config from command line
    args = get_args()
       
    user_id = args.user_id
    batch_size = args.batch_size
    mig_schema = config['database']['target']['schema'][0]
    tgt_schema = config['database']['target']['schema'][1]
    debug = args.debug
    null_data_lst = ['', None, 'NULL', 'None']
    
    logger, log_file = setup_logging(debug)
    logger.info(f"Logging to {log_file}")
    logger.info("Starting migration process")
    
    engine = get_db_engine(config, args.target_profile)
    session = sessionmaker(bind = engine)

    # Check
    tgt_tables = ['country', 'organisation', 'location', 'route', 'collection', 'photographer', 'builder', 'catalog']
    mig_tables = ['migration_log', 'users', 'ratroutes', 'ratcatalogue', 'ratbuilders', 'ratcollections', 'prompts']
    if not (check_tables(tgt_tables, tgt_schema) and check_tables(mig_tables, mig_schema)):
        return

    # Get UUID for the specified user or defaults to a the target config user
    with session() as supabase:
        user_id = get_or_create_user(args.user_id)
        logger.info(f"Using user_id: {user_id}")
        
    all_data: dict = {}
    if args.mode == 'dml_files':
        if not args.export_path:
            logger.error("Export path is required for DML files mode")
            return
        all_data = read_dml_extracts(args.export_path)
        
    elif args.mode == 'migration_schema':

        all_data = {
            'ratroutes': read_data_from_migration_schema('ratroutes'),
            'ratcatalogue': read_data_from_migration_schema('ratcatalogue'),
            'ratbuilders': read_data_from_migration_schema('ratbuilders'),
            'ratcollections': read_data_from_migration_schema('ratcollections'),
            'prompts': read_data_from_migration_schema('prompts')
        }
    
    # Report
    logger.info("Data extracted from SQL files:")
    for table_name, data in all_data.items():
        logger.info(f"  {table_name}: {len(data)} rows")
        # if debug:
        #     logger.debug(f"    Columns: {', '.join(data[0].keys()) if data else 'No data'}")

    # Create DataFrames for each table
    routes_df = pd.DataFrame(all_data.get('ratroutes', []))
    catalog_df = pd.DataFrame(all_data.get('ratcatalogue', []))
    builders_df = pd.DataFrame(all_data.get('ratbuilders', []))
    collections_df = pd.DataFrame(all_data.get('ratcollections', []))
    #prompts_df = pd.DataFrame(all_data.get('prompts', []))
    

    # Migration order respects referential dependencies
    with session() as supabase:
        # Indexes first: cheap even on an empty table, and every batch_upsert/
        # ON CONFLICT below benefits from them, not just the lookups.
        create_lookup_indexes()
        migrate_country(catalog_df)
        cache_lookup_table('country', 'name')  # needed by migrate_organisation/migrate_location next
        migrate_organisation(catalog_df)
        migrate_location(catalog_df)
        cache_lookup_table('location', 'name')  # needed by migrate_route/migrate_builder next
        migrate_route(routes_df)
        migrate_collection(collections_df)
        migrate_photographer(catalog_df)
        migrate_builder(builders_df)
        ensure_unknown_builder()  # before the cache rebuild below, so 'UNK' gets cached like any other builder
        migrate_catalog(catalog_df)
        create_lookup_index_cache()  # full rebuild: picks up any add_location() additions above too
        migrate_catalog_metadata(catalog_df)
        migrate_catalog_builder(catalog_df)
        migrate_usage(catalog_df)
        migrate_picture_metadata(catalog_df)
        # migrate_prompts(prompts_df)  # not restored: migrate_prompts() doesn't exist in this file

    logger.info("Migration process completed")

if __name__ == "__main__":
    main()