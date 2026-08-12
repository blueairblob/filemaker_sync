#!/bin/env python3
"""
    filemaker_extract.py 
    RAT Database Migration Script
    This script extracts data and images from the FileMaker Pro database and 
    generates DDL/DML Files or populates the staging or migration schema directly.
    Assumptions:
     - Migrating to a staging database so data will be as is with minimal transformation.
     - Using pandas and SQLAlchemy
"""

"""
    Gotchas:
     - Pandas schema extraction needs all or a lot of rows to establish required datatypes.
     - pyodbc translates all numbers as floats

"""
import os
import sys
import argparse
import pyodbc
import pandas as pd
from pandas.io.sql import DatabaseError
import sqlalchemy as sa
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, ProgrammingError, OperationalError
#from sqlalchemy.schema import DDL, AddConstraint, PrimaryKeyConstraint
import warnings
from time import time
import logging
from datetime import datetime
import re
from pathlib import Path
import tomli
from PIL import Image
from io import BytesIO
from tqdm import tqdm
# Trash collection
import gc
import signal
from contextlib import contextmanager
#import atexit
import traceback

def handle_exit():
    try: 
      if cn:
        cn.dispose()
        cn.close()
    except:
      pass

    try:
        log.info('Finished')
    except:
        print('Finished - before logging set up!')
    
     
def signal_handler(signum, frame):
    log.info("Received termination signal. Cleaning up...")
    handle_exit()
    
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
#atexit.register(handle_exit)

def setup_logging():
    # Create a logs directory if it doesn't exist
    log_dir = Path('./logs')
    log_dir.mkdir(exist_ok=True)

    # Get the script's filename without extension
    script_name = Path(__file__).stem

    # Get current date for the log file name
    current_date = datetime.now().strftime("%Y%m%d")

    # Construct the log file name
    log_file_name = f"{script_name}_{current_date}.log"
    log_file_path = log_dir / log_file_name

    # Create a logger
    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)
    # Pillow module: When converting jpg -> webp the pillow module switched to DEBUG
    pil_logger = logging.getLogger('PIL')
    pil_logger.setLevel(logging.INFO)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file_path, mode='a')  # 'a' mode appends to the file

    # Create formatters and add it to handlers
    log_format = '%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    date_format = '%Y-%m-%d:%H:%M:%S'
    formatter = logging.Formatter(log_format, date_format)

    # Always set up file handler
    file_handler = logging.FileHandler(log_file_path, mode='a')  # 'a' mode appends to the file
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Set up console handler only if DEBUG is set
    if os.environ.get('DEBUG'):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger, log_file_path

def get_postgres_version(connection):
    try:
        result = connection.execute(text("""
            SELECT version(), 
                   current_setting('server_version_num')::integer as version_number
        """))
        row = result.fetchone()
        version_string = row[0]
        version_number = row[1]
        return version_string, version_number
    except SQLAlchemyError as e:
        print(f"Error getting PostgreSQL version: {e}")
        return None, None

def format_value(val, postgres_version):
    if pd.isna(val):
        return 'NULL'
    elif isinstance(val, (int, float)):
        return str(val)
    else:
        val_str = str(val)
        if dbt_type == 'mysql':
            # Replace newlines with \n and escape backslashes
            escaped_val =val_str.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r')
            # Escape double quotes by doubling them
            escaped_val = escaped_val.replace('"', '""')
            # If the value contains a comma, single quote, or was originally multiline, wrap it in double quotes
            if ',' in escaped_val or "'" in escaped_val or '\n' in val_str or '\r' in val_str:
                return f'"{escaped_val}"'
            return f"'{escaped_val}'"
        
        elif dbt_type == 'supabase':  # assuming supabase uses PostgreSQL
            # Escape single quotes by doubling them
            val_str = val_str.replace("'", "''")
            if postgres_version and postgres_version >= 9.0:
                # Use E'' syntax for PostgreSQL 9.0 and above
                return f"E'{val_str}'"
            else:
                # Escape commas for older PostgreSQL versions
                return f"'{re.sub(',', r'\\,', val_str)}'"
          

def df_to_sql_bulk_insert(df: pd.DataFrame, table: str, postgres_version=None, header: bool = False, **kwargs) -> str:
    """Converts DataFrame to bulk INSERT sql query
    >>> data = [(1, "_suffixnan", 1), (2, "Noneprefix", 0), (3, "fooNULLbar", 1, 2.34)]
    >>> df = pd.DataFrame(data, columns=["id", "name", "is_deleted", "balance"])
    >>> df
       id        name  is_deleted  balance
    0   1  _suffixnan           1      NaN
    1   2  Noneprefix           0      NaN
    2   3  fooNULLbar           1     2.34
    >>> query = df_to_sql_bulk_insert(df, "users", status="APPROVED", address=None)
    >>> print(query)
    INSERT INTO users (id, name, is_deleted, balance, status, address)
    VALUES (1, '_suffixnan', 1, NULL, 'APPROVED', NULL),
           (2, 'Noneprefix', 0, NULL, 'APPROVED', NULL),
           (3, 'fooNULLbar', 1, 2.34, 'APPROVED', NULL);
    """
    global insert_header

    # Substitute in a common value to a column
    df = df.copy().assign(**kwargs).replace({True: 1, False: 0})
    
    if table == 'ratcatalogue':
        # Picture would normally hold an image, instead make it the same as image_no
        df['picture'] = df.apply(lambda x: x['image_no'] if x['image_no']!='' else x['picture'], axis=1)
        
    # Convert to tuples and join
    tuples = df.apply(lambda row: '({})'.format(', '.join(format_value(val, postgres_version) for val in row)), axis=1)
    values = (",\n" + " " * 7).join(tuples)
        
    # Handle Chunking
    if header:
        # Preserve funny column names 
        if dbt_type == 'mysql': 
            columns = ', '.join("`%s`" % x for x in df.columns)
        else:  # PostgreSQL
            columns = ', '.join(f'"{x}"' for x in df.columns)
        
        insert_header = f"INSERT INTO {mig_schema}.{table} ({columns})\nVALUES"
        query = f"{insert_header} {values}"
    else:
        query = f"{values}"
       
    return query

def get_db_connect(db, dsn=True):
   
    if db['type'] == 'url':
        log.info(f"Connecting to {db[db_type]['name'][1]}.")
      
        dsn_str = ''
        if dsn:
            dsn_str = db['dsn']
        
        try:
            if dbt_type == 'mysql':
                url=f"mysql+pymysql://{db[db_type]['user']}:{db[db_type]['pwd']}@{db['host']}:{db[db_type]['port']}/{dsn_str}"
                engine = create_engine(url)
                
            elif dbt_type == 'supabase':
                url=f"postgresql://{db[db_type]['user']}:{db[db_type]['pwd']}@{db['host']}:{db[db_type]['port']}/{dsn_str}"
                log.debug(f"Db connect url: {url}")
                engine = create_engine(url)
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
        except SQLAlchemyError as e:
            log.error(f"Error connecting to the database: {e}")           
            engine.dispose()
            sys.exit()
            
        return engine
    else:
        try:
            log.info(f"Connecting to {db['name'][1]}.")
            conn_str = f"DSN={db['dsn']};UID={db['user']};PWD={db['pwd']};CHARSET='UTF-8';ansi=True"
            cn = pyodbc.connect(conn_str)

            # SQLAlchemy:
            #sa_cn = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
            #conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
            #engine = create_engine(sa_cn)
            #params = urllib.parse.quote_plus(conn_str)
            #engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        except pyodbc.Error as e:
            if 'SQLDriverConnect' in e.args[1]:
                log.info(f"Connection to {db['name'][1]} failed. Please check that {db['name'][1]} is running or/and the ODBC connection is defined.")
            else:
                log.error(f"Connection error: {e}")
            cn.dispose()
            sys.exit()
        
        return cn
      
@contextmanager
def get_db_connection(dbt, use_dsn=True):
    
    global engine
    
    try:
        engine = get_db_connect(dbt, use_dsn)
        connection = engine.connect()
        yield connection
    except OperationalError as e:
        if "Unknown database" in str(e) and use_dsn:
            log.warning(f"Unknown database. Attempting to connect without DSN.")
            with get_db_connection(dbt, use_dsn=False) as conn:
                yield conn
        else:
            log.error(f"{dbt[db_type]['name'][1]}: Error connecting: {e}")
            raise
    finally:
        if 'connection' in locals() and connection:
            connection.close()

def run_query(sql, concat_result=True, chunk=1000):
    df_qry = pd.read_sql(sql, cn,  index_col=None, coerce_float=False, chunksize=chunk)
    # Concatenate all of the chunks into a single DataFrame
    if concat_result: 
        df = pd.concat(df_qry)
    else:
        df = df_qry
    return df

def get_date_columns(tab: str) -> dict:
    date_col_list = {}
    #for d in table_data['schema'][tab]:
        #print(f'd = {d}')
        #date_col_list[tab].append('{}')

    return {}

def convert_create_table_to_dict(create_table_sql: str) -> dict:
    """
    Extracts column name and type from SQL CREATE TABLE statements.
    
    Why? Pyodbc is interpreting DATE type as objects at least from FMP, whereas the pandas function
    pd.io.sql.get_schema uses the target database to interpret from the data. 
    So we are using the generated CREATE TABLE to get the actual target database data type.

    """
    table_dict = {}
    
    start_pattern = r'\(\s*\n'
    end_pattern = r'\n\s*\)'
    column_pattern =r"\n\t[`]?([\w\s]+)[`]? ([A-Z]+)"
        
    table_name = re.search(rf'CREATE TABLE\s+[`"]?(\w+)[`"]?', create_table_sql, re.IGNORECASE)
    if table_name:
        tab = table_name.group(1)
        table_dict = {tab: {}}
    
    columns = re.search(start_pattern + r'(.+)' + end_pattern, create_table_sql, re.DOTALL)
    if columns:
        column_definitions = columns.group(1)
        column_lines = re.findall(column_pattern, column_definitions, re.IGNORECASE)
        table_dict[tab]['columns'] = [{'column_name': column[0], 'column_type': column[1]} for column in column_lines]
    
    return table_dict

def get_table_data(tab: str, actions: dict, sql: str=None, rows: str='all', purge: bool=True, parse_dates=True) -> dict:
    """
    Perform various actions on the source database:
        - Export data in terms of DML (Data Manipulation Language) e.g. INSERT statements
        - Export the structure in terms of DDL (Data Definition Language) e.g. CREATE statements
        - Get the row count
    """
    global tab_ddl, ins_err, ins_cnt

    cnt = ' '
    ins_cnt = 0
    tab_dat = {}
    tab_dat[tab] = {'ddl' : '', 'dml' : '', 'data' : [], 'cnt' : '', 'exp': {'f' : {}, 'd' : {}}}
    # Build query
    if sql == None:
        sql = f"SELECT * FROM \"{tab}\""
        
    if rows != 'all':
            sql = f"{sql} FETCH FIRST {rows} ROWS ONLY"
        
    try:
        """     
            Suppressed : "UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI 
            or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
            Ref.: https://stackoverflow.com/questions/71082494/getting-a-warning-when-using-a-pyodbc-connection-object-with-pandas
        """
        for action, required in actions.items():
        
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                action = action.upper()
                if required:
                    chunk_cnt = 0
                    t0 = time()
                    header_req = True
                    footer_req = False
                    # Option to break up DML and commit;
                    split_insert = True
                    
                    if action == 'DML':
                        msg = f"{tab}: DML: Fetching the data"
                        log.info(f"{dbs['name'][1]}: {msg}")
                        if cnt != '':# Assumes CNT is done first
                            prop_cnt = int(cnt)/chunk
                        else:
                          log.info(f"{dbs['name'][1]}: {msg}: No count of rows found ({cnt})")
                            
                        # Add ORDER BY to ensure consistent ordering
                        if tab == 'ratcatalogue':
                            sql += " ORDER BY image_no ASC"
                        
                        # Convert dates
                        if parse_dates:
                            date_cols = {}
                            fmt = '%Y-%m-%d %H:%M:%S'
                            for col in tab_ddl[tab]['columns']:
                                if col['column_type'] == 'DATE':
                                    date_cols.update({col['column_name']:fmt}) 

                        for sub_df in tqdm(pd.read_sql(sql, cn, index_col = None, coerce_float = False, chunksize = chunk, parse_dates = date_cols), total = prop_cnt, desc = f"{tab} Query"):
                            chunk_cnt += 1
                            # Images are being ignored here
                            tab_dat[tab]['dml'] = df_to_sql_bulk_insert(sub_df, tab, postgres_version, header_req)
                                
                            # Perform a export of the data so far    
                            if (chunk_cnt + 1) > prop_cnt:
                                footer_req = True

                            export_data(tab_dat, tab, header_req, footer_req, split_insert)
                            
                            # if images:
                                # export_images() ...
                                
                            # Clear data from memory unless we're using results to store image data
                            if purge:
                                tab_dat[tab]['dml'] = ''
                            else:
                                # Preserve data for image processing
                                tab_dat[tab]['data'].append(sub_df.to_dict(orient='split'))
                            
                            # No need for a header now
                            header_req = False
                        
                        start_from_txt = ''
                        if start_from != None:
                            start_from_txt = f", starting from {start_from}"
                        log.info(f"{dbt[db_type]['name'][1]}: {tab}: Inserted {ins_cnt} rows{start_from_txt}.")
                        
                        # Export any ins_err to a file
                        if len(ins_err[tab]) > 0:
                            err_file = f"{cur_pth}/{exp_pth}/ins_err_{tab}_{dt_ymd}.sql"
                            with open(err_file, 'w', encoding='utf-8') as f:
                                f.write(f"-- Duplicate entries for table {tab}\n")
                                for n in ins_err[tab]:
                                    f.write(f"-- ID: Detail: {ins_err[tab][n]['detail']}\n")
                                    f.write(f"{ins_err[tab][n]['sql']};\n\n")
                                log.info(f"Exported {len(ins_err[tab])} duplicate entries for {tab} to {err_file}")
                        
                        txt = f"{action}: Processed {cnt} rows, {dupe_entry_cnt} duplicates"
                    
                    if action == 'DDL':
                        log.info(f"{dbs['name'][1]}: {tab}: DDL: Fetching table definitions.")
                        # We have to run the full query as we need to ensure all data is correctly represented by the appropriate datatype
                        if True:
                            ddl_sql = f"{sql} FETCH FIRST {chunk} ROWS ONLY"
                        else:
                            ddl_sql = sql
                        df_ddl = run_query(ddl_sql)
                        # Drop index that auto added                                         
                        df_ddl.reset_index(drop=True, inplace=True)
                        ddl = pd.io.sql.get_schema(df_ddl, f"{tab}", con=cn_tgt)
                        # Get column data types
                        tab_ddl = convert_create_table_to_dict(ddl)
                        ddl = ddl.replace('TABLE','TABLE IF NOT EXISTS')
                        ddl = ddl.replace(tab, f"{mig_schema}.{tab}")
                        tab_dat[tab]['ddl'] = ddl
                        txt = f'{action}: Generated for {tab}'
                        
                        # 
                        #if not dml:
                        #    # Handle the export of this data
                        export_data(tab_dat, tab)

                    if action == 'CNT':
                        # Pre-Count of query results
                        log.info(f"{dbs['name'][1]}: {tab}: Getting a query count")
                        sql_cnt = re.sub(r'(SELECT).*(FROM)', r'\1 COUNT(*) AS n \2', sql)
                        df_cnt = run_query(sql_cnt)
                        cnt = int(df_cnt['n'][0])

                        # Over ride cnt to be the limit (rows) if set
                        if rows != 'all' and cnt > int(rows): 
                            cnt = int(rows)
                        tab_dat[tab]['cnt'] = cnt
                        txt = f"Has {cnt} rows"

                    tm = round(time() - t0, 2)
                    log.info(f"{dbs['name'][1]}: {tab}: {txt}. Time {tm} secs.")
                                        
    except DatabaseError as e:
        if 'no such table' in e.args[0]:
            log.error(f"\nNo such table: {tab}")
            raise DatabaseError
        else:
            err_msg = f"{tab}: {action}: Failed to get table data. Unknown error: {e}"
            log.error(err_msg)
            raise Exception(err_msg)

    return tab_dat

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def get_date_string():
    date = datetime.now()
    date_string = date.strftime('%Y%m%d')
    return date_string

def create_pk(engine, name, create=True):
    pk_name = f"pk_{name}"
    columns = ', '.join(dbt[mig_schema]['pk'][name])
    
    try:
        # Check if PK already exists
        existing_pk = engine.execute(text(
            "SELECT constraint_name FROM information_schema.table_constraints "
            "WHERE table_schema = :schema AND table_name = :table AND constraint_type = 'PRIMARY KEY'"
        ), {"schema": mig_schema, "table": name}).fetchone()
            
        if existing_pk:
            log.debug(f"{dbt[db_type]['name'][1]}: Primary key already exists for table \"{mig_schema}.{name}\" - skipping creation.")
            return
          
        #print(f"ALTER TABLE {mig_schema}.{name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({columns})")
        
        # Create the primary key
        if create:
          engine.execute(text(
              f"ALTER TABLE {mig_schema}.{name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({columns})"
          ))
          cn_tgt.commit()
          log.info(f"Created primary key for table \"{mig_schema}.{name}\"")

    except ProgrammingError as e:
        if "already exists" in str(e):
            log.debug(f"{dbt[db_type]['name'][1]}: PK for table \"{name}\" Found  - skipping creation.")
        raise
    except SQLAlchemyError as e:
        log.error(f"{dbt[db_type]['name'][1]}: Error in creating PK for table {name}: {e}")
        raise
      
def get_table_list():
    # Get Tables
    sql="SELECT distinct baseTableName FROM FileMaker_BaseTableFields"
    
    try:
        crsr.execute(sql)
    except pyodbc.ProgrammingError as e:
        log.error(f"Error running {sql}: {e}")
        
        sys.exit()

    rows = crsr.fetchall()
    tab_lst = [x for y in rows for x in y]
    # Sanitise
    tab_lst = [sub.replace(' ', '_').lower() for sub in tab_lst]
    tab_cnt = len(tab_lst)
    log.debug(f'{tab_cnt} base tables found: {tab_lst}')

    return tab_lst

def verify_target_database(db_name: str, create_db: bool=True, drop_db: bool=False, query_db: bool=False, force: bool=False) -> dict or str:
    """
    Various database functions
        - Verification the database exists (query_db)
        - Creation of database with option to drop if exists (create_db / drop_db)
    """
    global cn_tgt, engine
    
    if dbt_type == 'mysql':
      # Query for existing databases
      try:
          existing_databases = cn_tgt.execute(text("SHOW DATABASES;"))
          # Results are a list of single item tuples, so unpack each tuple
          existing_databases = [d[0] for d in existing_databases]
      except Exception as e:
          existing_databases = []
          force = True

      if query_db:
          log.info(f"Existing databases are {existing_databases}")
          return existing_databases

      if drop_db:
          cn_tgt.execute(text(f"DROP DATABASE {db_name};"))
          log.info(f"Deleted database {db_name}")
          
      # Create database if not exists
      if (db_name not in existing_databases) or force:
          if create_db:
              cn_tgt.execute(text(f"CREATE DATABASE {db_name};"))
              log.info(f"Created database {db_name}")
              cn_tgt.execute(text(f"USE {db_name}"))
              return 'created'
            
    elif dbt_type == 'supabase':
        try:
            if query_db:
                existing_databases = cn_tgt.execute(text("SELECT datname FROM pg_database;"))
                existing_databases = [d[0] for d in existing_databases]
                log.info(f"Existing databases are {existing_databases}")
                return existing_databases
            
            # Close existing connection and dispose of engine
            # if cn_tgt:
            #     cn_tgt.close()
            # if engine:
            #     engine.dispose()
            
            # # Connect to 'postgres' database
            # try:
            #     postgres_url = engine.url.set(database='postgres')
            #     postgres_engine = create_engine(postgres_url)
            # except Exception as e:
            #     log.debug(f"Error: {e}")
                
            # with postgres_engine.connect() as conn:
            #     # Run operations outside of a transaction
            #     conn.execution_options(isolation_level="AUTOCOMMIT")
                
            if drop_db:
                cn_tgt.execute(text(f"DROP DATABASE IF EXISTS {db_name};"))
                log.info(f"Deleted database {db_name}")
            
            if create_db:
                # Check if database exists
                exists = cn_tgt.execute(text(f"SELECT * FROM pg_database WHERE datname = '{db_name}';")).fetchone()
                if not exists:
                    cn_tgt.execute(text(f"CREATE DATABASE {db_name};"))
                    log.info(f"Created database {db_name}")
                else:
                    log.debug(f"Database {db_name} already exists")
            
            # Dispose of the temporary engine
            #postgres_engine.dispose()
            
            # Reconnect to the target database
            # new_url = engine.url.set(database=db_name)
            # engine = create_engine(new_url)
            # cn_tgt = engine.connect()
            
            return 'created' if create_db else 'exists'
            
        except SQLAlchemyError as e:
            log.error(f"Error in verify_target_database: {e}")
            raise

def drop_table_data(engine, tables, schema=None):
    """
    Drop all data from the specified table list.
    :param engine: SQLAlchemy engine instance
    :param table_name: Name of the table to clear
    :param schema: Schema name (optional)
    """
    
    for table_name in tables:
        if not table_exists(table_name, mig_schema):
            continue
        
        metadata = MetaData(schema=schema)
        table = Table(table_name, metadata, autoload_with=engine)
    
        log.info(f"Attempting to drop all data from table: {schema}.{table_name}")            
        try:
            result = engine.execute(table.delete())
            log.info(f"Successfully dropped {result.rowcount} rows from {schema}.{table_name}" if schema else f"Successfully dropped {result.rowcount} rows from {table_name}")
        except SQLAlchemyError as e:
            log.error(f"Error dropping data from {schema}.{table_name}: {str(e)}" if schema else f"Error dropping data from {table_name}: {str(e)}")
            raise
          
def get_actions(cnt: bool, ddl: bool, dml: bool):
    return {'cnt': cnt, 'ddl': ddl, 'dml': dml}

def clean_error(err):
  return str(err).replace('\n', ' ').strip()

def table_exists(table_name, schema):
    """Check if a table exists in the specified schema."""
    inspector = inspect(cn_tgt)
    return inspector.has_table(table_name, schema=schema)
  
def extract_image_no(sql_statement):
    # Regular expression pattern to match the INSERT statement and capture the VALUES
    pattern = r"INSERT INTO.*VALUES\s*\((.*?)\)"
    
    # Find the match in the SQL statement
    match = re.search(pattern, sql_statement, re.DOTALL | re.IGNORECASE)
    
    if match:
        # Get the values part
        values = match.group(1)
        
        # Split the values and get the first one (image_no)
        image_no = values.split(',')[0].strip().strip("'").strip('E').strip("'")
        
        return image_no
    else:
        return None

def export_data(tab: dict, name: str, header_req: bool = True, footer_req = True, split_insert = False): 
    """
    Export Data and Schema info either to file or a target database
        - tab: All the DDL and DML data
        - name: Name of table used as part of file name. 'ddl' or 'dml used if single file target
        - header: Whether to include a file header
        - exp_req: Dict of export actions to perform
            - 'dest': File (f) or Database (d) or Both (b)
            - 'type': Data type 'ddl' or 'dml' or both (b)
            - 'reset': delete data or database tables
            - 'fmt' : format of export single (s) of multiple (m) files
            - 'tag' : The tag given to the export file describing
    """
    global tab_ddl, dupe_entry_cnt, check_ddl, ins_err, ins_cnt, ok_to_insert_from, err_cnt

    # Export to File
    if exp_req['dest']['fn']:
      
        # Process and file identifiers
        prfx = cfg['export']['prefix']
        tag = '_'.join(key for key, value in exp_req['type'].items() if value is True)
        
        # Multiple export files, but only one per table. 
        if exp_req['fmt'] == 'multi':
            tag = f"{name}_{tag}"
        
        # Initial write/insert or additional mode
        #mode = 'w' if (exp_req['reset']['ddl'] and header_req) else 'a'
        mode = 'w' if header_req else 'a'
        mode = 'a'
        
        if mode == 'w':
            dupe_entry_cnt = 0
                        
        if tab[name]['exp']['f'].get(tag) != None:
            h =f"{cur_pth}/{exp_pth}/{tab[name]['exp']['f'][tag]}"
            output_file = Path(h)
        else:
            tab[name]['exp']['f'] = {tag : ''}
            # Set filename
            fn = f"{dt_ymd}_{prfx}_{tag}.sql"
            tab[name]['exp']['f'][tag] = fn
            output_file = Path(f"{cur_pth}/{exp_pth}/{fn}")
            
            # Reset file
            if mode == 'w':
                if os.path.isfile(output_file):
                    os.remove(output_file)
        
        # Generate export file    
        with open(Path(output_file), mode, encoding='utf-8') as f:
            
            if header_req:
                # Date
                timestamp = datetime.today().strftime('%Y.%m.%d %H:%M:%S')
                f.write('\n/*\n')
                f.write(f"\tTable: {name}\n")
                if tab[name]['cnt'] != '' and tag != 'ddl':
                    f.write(f"\tRows: {tab[name]['cnt']}\n")
                f.write(f"\tDate: {timestamp},\n")
                f.write('*/\n')   

            if exp_req['type']['ddl']:
                if tab[name]['exp']['f'].get('ddl') != None:
                    # Got data?
                    if tab[name]['ddl'] != '':
                        f.write(f"{text(tab[name]['ddl'])};\n")
                    tab[name]['exp']['f']['ddl'] = 'done'
                    
            if exp_req['type']['dml']:
                # Got data?
                if tab[name]['dml'] != '':
                    for sql in tab[name]['dml'].split(';'):
                        # Split the DML with another INSERT command if not at the start and ensuing data begins with "("
                        if not header_req:
                            if split_insert and sql.startswith('('):
                                f.write(f"\n{insert_header}")
                        # Some data cleansing
                        sc = ';\n' if footer_req else ''
                        sc = ';\n' if sql.endswith(')') else ''
                        # No need to remove \n from sql text as MySQL handles this
                        f.write(f'{text(sql)}{sc}\n')
    
    # Export to Database
    if exp_req['dest']['db']:
        # Create database object and data
        reset = exp_req['reset']['ddl']

        # DDL
        # Are we starting a fresh migration? Check we are resetting and table has not previously 
        # been dropped and recreated in this session
        if (reset or check_ddl) and tab[name]['exp']['d'].get('ddl') == None:
          
            # If required to export DDL and have the data
            if (exp_req['type']['ddl'] and
                tab[name]['ddl'] != '' and
                tab[name]['exp']['d'].get('ddl') == None):     # Flag to determine if table has been created yet
                try:
                    # Check table exists
                    table_found = table_exists(name, mig_schema)
                    
                    if table_found:
                        # Just check only
                        create_pk(cn_tgt, name)
                                       
                    # Drop table
                    if reset and table_found:
                        log.info(f"Recreating Table {name}.")
                    
                        if dbt_type == 'supabase':
                            tab_str = f"\"{name}\""
                        else:
                            tab_str = f"{name}"
                        cn_tgt.execute(text(f"DROP TABLE IF EXISTS {mig_schema}.{tab_str}"))
                        table_found = False
                    
                    # Recreate table if reset or checking ddl and does not exist
                    if reset or (check_ddl and not table_found):
                      
                        # Check Schema Exists
                        inspector = inspect(cn_tgt)
                        if mig_schema not in inspector.get_schema_names():
                          cn_tgt.execute(sa.schema.CreateSchema(mig_schema))
                          # optional. set the default schema to the new schema:
                          cn_tgt.dialect.default_schema_name = mig_schema
                      
                        adjusted_ddl = adjust_sql_syntax(tab[name]['ddl'], db_type)  
                        try:
                            #print(text(adjusted_ddl))
                            status = cn_tgt.execute(text(adjusted_ddl))
                            log.info(f"Table {name} creation successful")
                            # If you want to ensure the changes are committed:
                            cn_tgt.commit()
                        except SQLAlchemyError as e:
                            print(f"An error occurred while creating the table: {e}")
                            # If you want to roll back any changes:
                            cn_tgt.rollback()
                    
                        # Create PKs
                        create_pk(cn_tgt, name)
                        
                except Exception as e:
                    log.error(f'Recreating Table {name}: {e}')
                    sys.exit()
                    
            # Need to record that the table has been dropped and recreated
            tab[name]['exp']['d']['ddl'] = 'done'
            return
     
        # DML
        if 'dml' in exp_req['type']:
            lst_err_dtl = ''
            try:
              ins_err[name] == {}
            except:
              ins_err = {name: {}}
              err_cnt = 0
              
            try:
                batch_insert = False
                if not batch_insert:   #do check integrity instead ?
                    # Single Insert statements - useful for updating
                    for ins_sql in tab[name]['dml'].split(',\n'):
                        i = ''
                        if not re.findall('INSERT INTO', ins_sql, re.IGNORECASE):
                            i = insert_header
                        txt = i + ins_sql
                        adjusted_dml = adjust_sql_syntax(txt, db_type)
                        
                        if start_from != None:
                            if name == 'ratcatalogue':
                                image_no = extract_image_no(adjusted_dml)
                                if image_no != start_from and not ok_to_insert_from:
                                    continue
                                else:
                                    ok_to_insert_from = True

                        try:
                            pk_cols = ', '.join(dbt[mig_schema]['pk'][name])
                            pk_cols = pk_cols.replace('\\', '')
                            sql_txt = f"{adjusted_dml} ON CONFLICT({pk_cols}) DO NOTHING"
                            sql_txt = text(sql_txt.replace('\\\\', '\\'))
                            
                            status = cn_tgt.execute(sql_txt)
                            cn_tgt.commit()
                            ins_cnt += 1
                            
                        except IntegrityError as e:
                            cur_err_dtl = clean_error(e.orig.diag.message_detail)
                            cur_err_msg = clean_error(e.orig.diag.message_primary)

                            err_info = { "message": cur_err_msg, "detail": cur_err_dtl, "sql" : adjusted_dml }
                            err_cnt += 1
                            err_msg = ''
                            if "already exists" in str(e):
                                err_msg = 'Duplicate entry error.'
                                dupe_entry_cnt += 1
                            else:
                                # Only non dupe errors get logged                                                                        
                                log.error(f"{dbt[db_type]['name'][1]}: Integrity error inserting data into {name}: {cur_err_msg}")
                            ins_err[name][err_cnt] = err_info
                            cn_tgt.rollback()
                            continue
                        except OperationalError as e:
                            cn_tgt.rollback()
                            log.error(f"{dbt[db_type]['name'][1]}: Operational error inserting data into {name}: {clean_error(e.orig)}")
                            raise
                        except SQLAlchemyError as e:
                            cn_tgt.rollback()
                            log.error(f"{dbt[db_type]['name'][1]} SQLAlchemy error inserting data into {name}: {clean_error(e)}")
                            raise       
                else:
                    # Bulk insert when we are sure that there are no ins_err
                    i = ''
                    if mode == 'a':
                        i = insert_header
                    txt = text(i + tab[name]['dml'])
                    status = cn_tgt.execute(txt)
                    cn_tgt.commit()
                    
            except Exception as e:
                log.error(f"{dbt[db_type]['name'][1]}: Unexpected error inserting data into {name}: {clean_error(e)}")
                if hasattr(e, '__cause__') and e.__cause__:
                    cause = str(e.__cause__).replace('\n', ' ').strip()
                    log.error(f"Caused by: {cause}")
                raise
                
def export_images(table):
    # Export Content
    # Special Case of RATcatalogue.picture which is FMP "container" (blob) data - JPG images!
   
    # export_images
    img_cnt_tot = 0
    try:
        img_chk_data = table_data[dbs['dsn']][table]['data']
        img_row_cnt =  table_data[dbs['dsn']][table]['cnt']
        #img_chk_len = len(img_chk_data) 
    except Exception as e:
        log.error(f"Error: Unable to extract images: {e}")
        sys.exit(1)

    for img_data in img_chk_data:
    
        img_cnt = len(img_data['data'])
        img_cnt_tot += img_cnt  
        log.info(f"{table}: Exporting {img_cnt_tot}/{img_row_cnt} images")
        for index, item in tqdm(enumerate(img_data['data'], start=0), total = img_cnt, desc="Exporting images"): 
            image_name = item[0]
            image_data = item[1]
            # Clean up 
            if image_name and image_data:
                image_name = image_name.replace('\n', '').replace('\r', '').replace(' ', '')
            else:
                continue
            
            #jpg_b64 = base64.b64encode(image_data).decode('utf-8')
            # Temp
            #decoded_data=base64.b64decode((image_data))
            #write the decoded data back to original format in  file
            if 'jpg' in export_image_formats:
                with open(Path(f"{jpg_pth}/{image_name}.jpg"), 'wb') as f:
                    f.write(image_data)       
                f.close()

            if 'webp' in export_image_formats:
                # Convert to webp
                image = Image.open(BytesIO(image_data))
                webp_data = BytesIO()
                image.save(webp_data, format="webp")
                with open(Path(f"{webp_pth}/{image_name}.webp"), 'wb') as f:
                    f.write(webp_data.getvalue())
                f.close()

            # Convert to Base64
            #image_data_webp_b64 = base64.b64encode(webp_data.getvalue()).decode('utf-8')

def found_in(l1: list, l2: list) -> bool:
    """ Compares one list to another """
    return all(item in l1 for item in l2) 

def get_table_export_list(export_tables: str = 'all') -> list:

    # Convert to list
    if type(export_tables) == str:
        try:
            delimiters = [";", "|", ",", " "]
            pattern = "|".join(map(re.escape, delimiters))
            tables_to_export_list = re.split(pattern, tables_to_export)
        except Exception as e:
            log.error(f"Unable to convert {export_tables} to a list?: {e}")
    else:
        # Assume function being called to validate an existing list
        tables_to_export_list = export_tables
    
    # Get a list of all available tables from the source
    table_list_all = get_table_list()

    # Validate list
    if tables_to_export_list[0] != 'all':    # Must be in provided list
        # Check tables in list are valid.
        if not set(tables_to_export_list).issubset(table_list_all):
            log.error(f'Table list conflict? You provided {tables_to_export_list} where as expecting 1 or a combination of these {table_list_all}')
            sys.exit()
        else:
            table_list = tables_to_export_list
    else:
        table_list = table_list_all
    log.info(f"Tables to process: {table_list}")
        
    return table_list

def adjust_sql_syntax(sql, dbt_type):
    if dbt_type == 'supabase':
        sql = sql.replace('`', '"')
        # Add more replacements as needed for PostgreSQL syntax
    return sql

def get_args ():
    
    parser = argparse.ArgumentParser(description = f"Export data from {dbs['name'][1]} database.")
    
    # Mutually exclusive group for export destination
    dest_group = parser.add_mutually_exclusive_group(required=True)
    dest_group.add_argument("--fn-exp", action = "store_true", help = "Export to Files")
    dest_group.add_argument("--db-exp", action = "store_true", help = "Export to Database")
    dest_group.add_argument("--info-only", action = "store_true", help = "Information only")

    ddl_group = parser.add_mutually_exclusive_group(required = True)
    ddl_group.add_argument("--ddl", action = "store_true", default = False, help = "Export the DDL definitions of the source schema")
    ddl_group.add_argument("--check-ddl", action = "store_true", default = False, help = "Check that the DDL (TABLEs etc) is created")
    
    parser.add_argument("-q", "--quiet", action = "store_true")
    parser.add_argument("-t", "--tables-to-export", type = str, default = tables_to_export, help = "A list of tables to export \"...\" .")
    parser.add_argument("-i", "--get-images", action = "store_true", default = False, help ="Export images")
    parser.add_argument("--dml", action = "store_true", default = False, help = "Export DML from source and export")
    parser.add_argument("--del-data", action = "store_true", default = False, help = "Delete all the data in the target staging database")
    parser.add_argument("--del-db", action = "store_true", default = False, help = "Delete the objects in the target staging database")
    parser.add_argument("--get-schema", action = "store_true", default = False, help ="Get source database schema details")
    parser.add_argument("-r", "--max-rows", type = str, default = max_rows, help = "Maximum rows to return")
    parser.add_argument("--db-type", type = str, choices = ['mysql', 'supabase'], default = 'supabase', help = "Specify the target database type")
    parser.add_argument("--fn-fmt", type = str, choices = ['single', 'multi'], default = 'multi', help = "Single or Multi File export")
    parser.add_argument("--start-from", type=str, help="Start migration from this image_no in the ratcatalogue table")
    args = parser.parse_args()
    
    # Override
    if args.check_ddl:
        args.ddl = True

    # Add parsed arguments to the global scope
    globals().update(vars(args))
        
    return args
  
def get_exception_line():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = exc_tb.tb_frame.f_code.co_filename
    line_no = exc_tb.tb_lineno
    return fname, line_no
  
if __name__ == "__main__":
    #gc.get_threshold()
    #(700, 10, 10)
    gc.set_threshold(5000, 20, 20)
    # Pandas do not allow truncation of output
    pd.set_option('display.max_colwidth', None)

    # Set up the logger
    log, log_file_path = setup_logging()

    # Now you can use 'log' throughout your script
    log.info(f"Logging initialized. Log file: {log_file_path}")
    if os.environ.get('DEBUG'):
        log.info("Debug mode is ON. Logging to both file and console.")
    else:
        log.info("Debug mode is OFF. Logging to file only.")
    
    log.info("Starting the migration process")

    # Establish Filesystem details
    #cur_pth = Path(os.getcwd())
    pth = Path(os.path.abspath(__file__))
    cur_pth = pth.parent
    log.debug(f"cur_pth = {cur_pth}, scr_pth = {pth}")
    
    # Init
    table_data = {}
    dt_ymd = get_date_string()
    cfg_fn = 'config.toml'
    cfg_pth = cur_pth
    tables_to_export = 'all'
    cnt = True   # Always True
    info_only = False
    ok_to_insert_from = False
    ins_err = {} 
    err_cnt = 0  
    chunk = 100
    max_rows = 'all'
    export_image_formats = ['jpg', 'webp']
    
    # Load configs:
    log.debug('Loading configs')
    cfg = tomli.loads(Path(f'{cur_pth}/{cfg_fn}').read_text(encoding='utf-8'))
    dbs = cfg['database']['source']
    dbt = cfg['database']['target']
    mig_schema = cfg['database']['target']['schema'][cfg['database']['target']['mig_schema']]
    
    # Args
    args = get_args()
    
    # Disable insert until start_from (image_no) found 
    if start_from != None:
        ok_to_insert_from = False
    
    # Export Requirements based on args and config
    exp_req = {'dest': { 'fn': fn_exp, 'db': db_exp }, 'type': { 'ddl': ddl, 'dml': dml }, 'reset': {'data': del_data,'ddl': del_db}, 'fmt': fn_fmt }
    
    # Check File systems
    exp_pth = Path(cfg['export']['path'])
    jpg_pth = Path(f"{cur_pth}/{exp_pth}/images/jpg")
    webp_pth = Path(f"{cur_pth}/{exp_pth}/images/webp")
    # Check Paths exist
    for pth in [exp_pth, jpg_pth, webp_pth]:
        if not os.path.exists(pth):
            log.warning(f"{pth} does not exist creating")
            os.makedirs(pth)
            
    # Override for main actions:
    # Information Only
    if info_only:
        actions = get_actions(cnt, False, False)
    else:
        actions = get_actions(cnt, ddl, dml)
         
       
    # Actions to be performed
    #actions = get_actions(cnt, ddl, dml)
    
    # Perform main actions
    
    # Source Db: Connect to source and get cursor
    try:
        with get_db_connect(dbs) as cn:
          crsr = cn.cursor()

          # Get Tables to report on or Export
          table_list = get_table_export_list(tables_to_export)
          table_cnt = len(table_list)
          if table_cnt == 0:
              log.info('No tables found to export. Now exiting')
              sys.exit(1)
    except Exception as e:
      log.error(f"{dbs[db_type]['name'][1]}: Error connecting: {e}")
      sys.exit()
      
       
    # Target Db
    try:
        dbt_type = cfg['database']['target']['db']
        dbt_name = dbt['dsn']
                
        engine = get_db_connection(dbt)
        with engine as cn_tgt:
            # Get Version Info
            if dbt_type == 'supabase':
                postgres_version_string, postgres_version_number = get_postgres_version(cn_tgt)
                if postgres_version_number:
                    postgres_version = postgres_version_number / 10000  # Convert to major version number
                    log.debug(f"Connected to PostgreSQL version: {postgres_version}")
                else:
                    postgres_version = None
                    print("Unable to determine PostgreSQL version. Falling back to default behavior.")
            else:
                postgres_version = None
                
            # Ensure the parent database exists
            #status = verify_target_database(dbt_name)
                      
            if del_data:
                drop_table_data(cn_tgt, table_list, mig_schema)
                cn_tgt.commit()

            # Export Source Schema
            # The Databases internal data dictionary or schema data. Keep this data for reference.
            if get_schema:
                #table_list = get_table_export_list(tables_to_export)
                table_data['schema'] = {}
                # Select from provided data dictionary (schema) tables
                for table in dbs['schema']:
                    table_data['schema'][table] = get_table_data(table, actions, purge=False)

            # Export table data
            if db_exp or fn_exp:
                for table in table_list:
                    dupe_entry_cnt = 0
                    table_data[dbs['dsn']] = get_table_data(table, actions, rows=max_rows)

            # Export images
            if get_images and len(export_image_formats) != 0:
                # Get Image data
                table = 'images'
                # This table does not exist in the source Db
                actions = get_actions(cnt, ddl, dml)
                sql="SELECT image_no, GetAs(picture,'JPEG') picture, entry_date, date_taken FROM RATCatalogue"
                table_data[dbs['dsn']] = get_table_data(table, actions, sql, max_rows, False)
                export_data(table_data[dbs['dsn']], table)
                export_images(table)

        handle_exit()
          
    except Exception as e:
        fname, line_no = get_exception_line()
        error_msg = f"An error occurred in file '{fname}', line {line_no}: {str(e)}"
        log.error(error_msg)
        
        # If you want more detailed traceback
        traceback_msg = traceback.format_exc()
        log.debug(f"Full traceback:\n{traceback_msg}")
        sys.exit()