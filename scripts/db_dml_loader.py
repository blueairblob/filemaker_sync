#!/bin/env python3

"""
DML to Supabase Migration Script

This script automates the process of migrating data from DML exports to a Supabase database.
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

from pathlib import Path
import re
import os
import tomli
import pandas as pd
#from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData, Table, select, insert, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from datetime import datetime, timezone
import logging
import argparse
import glob
from tqdm import tqdm
import io
from collections import OrderedDict
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import UniqueViolation, ForeignKeyViolation


# Global variables
user_id = None
debug = False
logger = None
engine = None
session = None
config = None


# Set up logging
def setup_logging(debug_mode):
    log_level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)
  
def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Migrate DML data to Supabase")
    parser.add_argument("--mode", choices=['dml_files', 'migration_schema'], required=True, help="Source data to migrate from DML files or from the Database Migration Schema.")
    parser.add_argument("--export-path", required=True, help="Path to the directory containing DML export files")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserts")
    parser.add_argument("--user-id", help="User ID for audit columns")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
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
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
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
    
def get_db_engine(config):
    db_type = config['database']['target']['db']
    db_config = config['database']['target'][db_type]
    db_url = f"postgresql://{db_config['user']}:{db_config['pwd']}@{config['database']['target']['host']}:{db_config['port']}/{config['database']['target']['dsn']}"
    return create_engine(db_url)
  
def get_table(table_name, schema = 'public'):
    """Get a SQLAlchemy Table object for the given table name."""
    metadata = MetaData(schema = schema)
    return Table(table_name, metadata, autoload_with = engine)

def get_or_create_user(username):
    users_table = get_table('users', mig_schema)
    if not username:
        username = config['database']['target']['user']
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
        first_row = result.fetchone()
        print(f"Columns: {columns}")
        print(f"First row: {first_row}")
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

def batch_upsert(table_name, data, id_column='id'):
    """Perform batch upsert operations, resuming from the last migrated ID."""
    global user_id, batch_size
    
    if debug: logger.info(f"Starting batch upsert for table: {table_name}")
    
    # Table comes with schema
    tab = table_name.split('.')
    schema = tab[0]
    table_name = tab[1]
    # Check table exists
    if not create_table_if_not_exists(table_name, schema):
        return
    
    target_table = get_table(table_name, schema)

    total_affected = 0
    error_counts = {
        'unique_violations': 0,
        'foreign_key_violations': 0,
        'other_integrity_errors': 0,
        'other_errors': 0
    }

    # Check Resume: Only works with provided PK integers
    # last_id = get_last_migrated_id(table_name)
    # if last_id:
    #    data = [record for record in data if record[id_column] > last_id]

    current_time = datetime.now(timezone.utc).isoformat()
    data_len = len(data)
    #if data_len < batch_size:
    #    batch_size = data_len
    for i in tqdm(range(0, data_len, batch_size), desc=f"Upserting {data_len} to {table_name} (max batch {batch_size})"): 
        batch = data[i:i+batch_size]
        # Each batch gets audit data
        for record in batch:
            record['created_by'] = user_id
            record['created_date'] = current_time
            record['modified_by'] = user_id
            record['modified_date'] = current_time
    
        try:
            stmt = pg_insert(target_table).values(batch)
            # stmt = stmt.on_conflict_do_update(
            #     index_elements=[id_column],
            #     set_={c.key: c for c in stmt.excluded if c.key != id_column}
            # )
            stmt = stmt.on_conflict_do_nothing(index_elements=[id_column])
            
            try:
                result = supabase.execute(stmt)
                supabase.commit()
                total_affected += result.rowcount
            except IntegrityError as e:
                  supabase.rollback()
                  if isinstance(e.orig, UniqueViolation):
                      error_counts['unique_violations'] += 1
                  elif isinstance(e.orig, ForeignKeyViolation):
                      error_counts['foreign_key_violations'] += 1
                  else:
                      error_counts['other_integrity_errors'] += 1
                  logger.warning(f"IntegrityError in {table_name}: {str(e)}")
            except Exception as e:
                supabase.rollback()
                error_counts['other_errors'] += 1
                logger.error(f"Unexpected error in {table_name}: {str(e)}")

            logger.info(f"{table_name}: Upsert complete. Affected rows: {total_affected}")
            logger.info(f"Error counts for {table_name}: {error_counts}")
                  
            # if batch:
            #     update_migration_log(table_name, batch[-1][id_column])
            
            if debug:
                logger.debug(f"Upserted batch for {table_name}. Affected rows: {len(batch)}")
        except Exception as e:
            logger.error(f"Error upserting batch to rat.{table_name}: {str(e)}")
            if 'violates row-level security policy' in str(e):
                logger.error("This error is likely due to Row Level Security (RLS) policies. "
                              "Ensure you're using a service role key with necessary permissions.")
            supabase.rollback()
            raise
  
    if debug: logger.info(f"Completed batch upsert for table: {tgt_schema}.{table_name}")

def get_country_id(tab, data, ref_data = ''):
    stmt = select(tab.c.id).where(tab.c.name == data)
    #print(stmt.compile(compile_kwargs={"literal_binds": True}))
    result = supabase.execute(stmt).first()
    id = result[0] if result is not None else None
    
    if id is None:
      def_name = 'unknown'
      if ref_data != '':
          ref_data = f" \"{ref_data}\":"
      logger.warning(f"{tab.name}:{ref_data} No country_id found for country: {data} defaulting to {def_name}")
      stmt = select(tab.c.id).where(tab.c.name == def_name)
      result = supabase.execute(stmt).first()
      id = result[0] if result is not None else None
      
    return id

# ---------- Target Schema Migration Functions ---------- 

def migrate_country(df):
    """Migrate country data to Supabase."""
    logger.info("Starting country migration")
    countries = df['country'].dropna().unique()
    # Add unknown to handle missing info
    countries = list(df['country'].dropna().unique()) + ["unknown"]
    country_data = [{'name': country} for country in countries]
    batch_upsert(f"{tgt_schema}.country", country_data, id_column='name')
    logger.info(f"Completed country migration. Migrated {len(countries)} countries")

def migrate_organisation(df):
    """Migrate organisation data to Supabase."""
    logger.info("Starting organisation migration")
    organisations = df[['organisation', 'country', 'organisation_type']].dropna(subset=['organisation']).drop_duplicates()
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
            logger.error(f"Error processing organisation: {str(e)}")

    try:
        # Convert the OrderedDict values to a list
        org_data_lst = list(org_data.values())
        batch_upsert(f"{tgt_schema}.organisation", org_data_lst, id_column='name')
        logger.info(f"Completed organisation migration. Migrated {len(org_data_lst)} organisations")
    except Exception as e:
        logger.error(f"Error during batch upsert of organisations: {str(e)}")
        

def migrate_location(df):
    """Migrate location data to Supabase."""
    tgt_table = 'location'
    logger.info("Starting location migration")
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
    batch_upsert(f"{tgt_schema}.{tgt_table}", location_data_lst, id_column='name')
    logger.info(f"Completed {tgt_table} migration. Migrated {len(locations)} locations")

def migrate_route(df):
    """Migrate route data to Supabase."""
    tgt_table = 'route'
    logger.info(f"Starting {tgt_table} migration")
    routes = df[[tgt_table, 'start_location', 'end_location']].dropna(subset=['route']).drop_duplicates()
    route_data = []
    for _, row in routes.iterrows():
        # Get FKs
        start_location_id = supabase.from_(f"{tgt_schema}.location").select('id').eq('name', row['start_location']).execute().data
        end_location_id = supabase.table(f"{tgt_schema}.location").select('id').eq('name', row['end_location']).execute().data
        start_location_id = start_location_id[0]['id'] if start_location_id else None
        end_location_id = end_location_id[0]['id'] if end_location_id else None
        route_data.append({
            'id': row['route'],  # Using name as ID for simplicity
            'name': row['route'],
            'start_location_id': start_location_id,
            'end_location_id': end_location_id
        })
    batch_upsert(f"{tgt_schema}.route", route_data, id_column='name')
    logger.info(f"Completed route migration. Migrated {len(routes)} routes")

def migrate_catalog(df, batch_size, user_id):
    """Migrate catalog data to Supabase."""
    logger.info("Starting catalog migration")
    catalog_data = df.to_dict('records')
    for record in catalog_data:
        record['id'] = record['image_no']  # Use image_no as the ID
    batch_upsert(f"{tgt_schema}.catalog", catalog_data, id_column='image_no')
    logger.info(f"Completed catalog migration. Migrated {len(df)} catalog entries")

def migrate_catalog_metadata(df):
    """Migrate catalog metadata to Supabase."""
    logger.info("Starting catalog metadata migration")
    metadata_data = []
    for _, row in df.iterrows():
        catalog_id = supabase.table('catalog').select('id').eq('image_no', row['image_no']).execute().data
        organisation_id = supabase.table('organisation').select('id').eq('name', row['organisation']).execute().data
        location_id = supabase.table('location').select('id').eq('name', row['location']).execute().data
        route_id = supabase.table('route').select('id').eq('name', row['route']).execute().data
        collection_id = supabase.table('collection').select('id').eq('name', row['collection']).execute().data
        photographer_id = supabase.table('photographer').select('id').eq('name', row['photographer']).execute().data
        
        catalog_id = catalog_id[0]['id'] if catalog_id else None
        organisation_id = organisation_id[0]['id'] if organisation_id else None
        location_id = location_id[0]['id'] if location_id else None
        route_id = route_id[0]['id'] if route_id else None
        collection_id = collection_id[0]['id'] if collection_id else None
        photographer_id = photographer_id[0]['id'] if photographer_id else None
        
        if catalog_id is None:
            continue  # skip: no catalog row matched this image_no (would orphan / violate FK)
        metadata_data.append({
            'catalog_id': catalog_id,
            'organisation_id': organisation_id,
            'location_id': location_id,
            'route_id': route_id,
            'collection_id': collection_id,
            'photographer_id': photographer_id
        })
    batch_upsert(f'{tgt_schema}.catalog_metadata', metadata_data, id_column='catalog_id')
    logger.info(f"Completed catalog metadata migration. Migrated {len(df)} metadata entries")

def migrate_usage(df):
    """Migrate usage data to Supabase."""
    logger.info("Starting usage data migration")
    usage_data = []
    for _, row in df.iterrows():
        catalog_id = supabase.table('catalog').select('id').eq('image_no', row['image_no']).execute().data
        catalog_id = catalog_id[0]['id'] if catalog_id else None
        if catalog_id is None:
            continue  # skip: no catalog row matched this image_no (usage has no standalone id)
        usage_data.append({
            'catalog_id': catalog_id,
            'prints_allowed': row['prints_allowed'] == 'yes',
            'internet_use': row['internet_use'] == 'yes',
            'publications_use': row['publications_use'] == 'yes'
        })
    batch_upsert(f'{tgt_schema}.usage', usage_data, id_column='catalog_id')
    logger.info(f"Completed usage data migration. Migrated {len(df)} usage entries")
    
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
    batch_upsert(f'{tgt_schema}.collection', collection_data, id_column='id')
    logger.info(f"Completed collection migration. Migrated {len(collection_data)} collections")
    
def migrate_photographer(df):
    logger.info("Starting photographer migration")
    
    # Debug: Print all unique values in the 'photographer' column
    unique_photographers = df['photographer'].dropna().unique()
    
    if debug: 
      logger.info(f"Unique photographers found: {unique_photographers}")
    
      # Count occurrences of each photographer
      photographer_counts = df['photographer'].value_counts(dropna=True)
      logger.info(f"Photographer counts:\n{photographer_counts}")
      
      # Additional check: Print the first few rows of the DataFrame
      logger.info(f"First few rows of the DataFrame:\n{df.head()}")
    
    photographer_data = [{'name': photographer} for photographer in unique_photographers]
    
    batch_upsert(f'{tgt_schema}.photographer', photographer_data)
    logger.info(f"Completed photographer migration. Migrated {len(photographer_data)} photographers")

def migrate_builder(df):
    logger.info("Starting builder migration")
    # Assuming 'builder' and 'builder_location' columns exist in the DataFrame
    builders = df[['builder', 'builder_location']].dropna(subset=['builder']).drop_duplicates()
    builder_data = []
    for _, row in builders.iterrows():
        builder_data.append({
            'code': row['builder'][:20],  # Assuming 'code' is derived from 'builder' name
            'name': row['builder'],
            'location': row['builder_location'] if 'builder_location' in row else None
        })
    batch_upsert(f'{tgt_schema}.builder', builder_data)
    logger.info(f"Completed builder migration. Migrated {len(builder_data)} builders")

def migrate_catalog_builder(df):
    logger.info("Starting catalog_builder migration")
    # Assuming 'image_no' and 'builder' columns exist in the DataFrame
    catalog_builders = df[['image_no', 'builder']].dropna(subset=['image_no', 'builder'])
    catalog_builder_data = []
    for _, row in catalog_builders.iterrows():
        catalog_id = supabase.table('catalog').select('catalog_id').eq('image_no', row['image_no']).execute().data
        builder_id = supabase.table('builder').select('builder_id').eq('name', row['builder']).execute().data
        if catalog_id and builder_id:
            catalog_builder_data.append({
                'catalog_id': catalog_id[0]['catalog_id'],
                'builder_id': builder_id[0]['builder_id'],
                'builder_order': 1  # Assuming single builder per catalog, adjust if needed
            })
    batch_upsert(f'{tgt_schema}.catalog_builder', catalog_builder_data)
    logger.info(f"Completed catalog_builder migration. Migrated {len(catalog_builder_data)} catalog-builder relationships")

def migrate_picture_metadata(df):
    logger.info("Starting picture_metadata migration")
    # Assuming columns like 'image_no', 'file_location', 'file_type', 'file_size', 'width', 'height' exist
    picture_metadata = df[['image_no', 'file_location', 'file_type', 'file_size', 'width', 'height']].dropna(subset=['image_no'])
    metadata_data = []
    for _, row in picture_metadata.iterrows():
        metadata_data.append({
            'image_no': row['image_no'],
            'file_location': row['file_location'],
            'file_type': row['file_type'],
            'file_size': int(row['file_size']) if row['file_size'] else None,
            'width': int(row['width']) if row['width'] else None,
            'height': int(row['height']) if row['height'] else None,
            'resolution': None,  # Add if available in your data
            'color_space': None,  # Add if available in your data
            'ai_description': None,  # Add if available in your data
            'tags': None  # Add if available in your data
        })
    batch_upsert(f'{tgt_schema}.picture_metadata', metadata_data)
    logger.info(f"Completed picture_metadata migration. Migrated {len(metadata_data)} picture metadata entries")

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
    
    # Load config file
    config = load_config()
    
    # Load config from command line
    args = get_args()
    user_id = args.user_id
    batch_size = args.batch_size
    mig_schema = 'rat_migration'
    tgt_schema = 'rat'
    debug = args.debug
    logger = setup_logging(debug)
    
    logger.info("Starting migration process")
    
    engine = get_db_engine(config)
    session = sessionmaker(bind = engine)
  
    # Check
    tgt_tables = ['country', 'organisation', 'location', 'route', 'collection', 'photographer', 'builder', 'catalog']
    mig_tables = ['migration_log', 'user', 'ratroutes', 'ratcatalogue', 'ratbuilders', 'ratcollections', 'prompts']
    if not (check_tables(tgt_tables, tgt_schema) or check_tables(mig_tables, mig_schema)):
        return

    # Get UUID for the specified user or defaults to a the target config user
    with session() as supabase:
        user_id = get_or_create_user(args.user_id)
        logger.info(f"Using user_id: {user_id}")
        
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
        if debug:
            logger.debug(f"    Columns: {', '.join(data[0].keys()) if data else 'No data'}")

    # Create DataFrames for each table
    routes_df = pd.DataFrame(all_data.get('ratroutes', []))
    catalog_df = pd.DataFrame(all_data.get('ratcatalogue', []))
    builders_df = pd.DataFrame(all_data.get('ratbuilders', []))
    collections_df = pd.DataFrame(all_data.get('ratcollections', []))
    #prompts_df = pd.DataFrame(all_data.get('prompts', []))
    

    # Migration order respects referential dependencies
    with session() as supabase:
        migrate_country(catalog_df)
        migrate_organisation(catalog_df)
        migrate_location(catalog_df)
        migrate_route(routes_df)
        migrate_collection(collections_df)
        migrate_photographer(catalog_df)
        migrate_builder(builders_df)
        migrate_catalog(catalog_df)
        migrate_catalog_metadata(catalog_df)
        migrate_catalog_builder(catalog_df)
        migrate_usage(catalog_df)
        migrate_picture_metadata(catalog_df)
        #migrate_prompts(prompts_df)    
    
        
    logger.info("Migration process completed")

if __name__ == "__main__":
    main()