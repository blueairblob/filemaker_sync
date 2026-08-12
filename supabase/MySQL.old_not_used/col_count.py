#!/bin/env python3

import mysql.connector
#pip install mysql-connector-python

# Replace with your database connection details
config = {
    'user': 'trevour',
    'password': 'trevour',
    'host': '127.0.0.1',
    'database': 'rat',
    'port' : 3306
}

DEBUG = 0
# Replace with your table name
table_name = 'ratcatalogue'
table_name = 'ratroutes'
table_name = 'ratcollections'

try:
    # Connect to the MySQL database
    conn = mysql.connector.connect(**config)

    # Create a cursor
    cursor = conn.cursor()

     # Get the column names dynamically from the information schema
    cursor.execute(f"SELECT count(*) FROM {table_name}")
    row_cnt = [row[0] for row in cursor.fetchall()]
    print(f"{table_name}: {row_cnt[0]} rows")

    # Get the column names dynamically from the information schema
    cursor.execute(f"SELECT `COLUMN_NAME` FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")

    # Fetch all the column names
    columns = [f"`{row[0]}`" for row in cursor.fetchall()]

    # Iterate through each column and get the distinct value count
    for column_name in columns:
        cursor.execute(f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}")
        distinct_count = cursor.fetchone()[0]
        print(f"Column: {column_name}, Distinct value count: {distinct_count}")

        # Check if distinct count is less than 5, then fetch and print distinct values
        if distinct_count < 5:
            cursor.execute(f"SELECT DISTINCT {column_name} FROM {table_name}")
            distinct_values = cursor.fetchall()  
            print("\tValues:")
            for value in distinct_values:
                value_str = value[0]
                if value_str == None:
                    sql_end = 'IS NULL'
                else:
                    sql_end = f" = '{value_str}'"
                sql_txt = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} {sql_end}"
                if DEBUG: print(sql_txt)
                cursor.execute(sql_txt)
                row_count = cursor.fetchone()[0]
                print(f"\t\tValue: \"{value_str}\", Row Count: {row_count}")


except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()
