# -*- coding: utf-8 -*-
"""
Description: Python connectors and query

Created on Tue Jan 15 13:38:55 2024
@author: Isidro Jr Medina
"""
#%% Connect DB Server, specify the database: 
def pyConnect2DB(server, database):
    import pyodbc
  
    db_connxn = pyodbc.connect('Driver={SQL Server};'
                      f'Server={server};'
                      f'Database={database};'
                      'Trusted_Connection=yes;')
    return (db_connxn)

#%% Connect Linked Server
def pyConnect2LinkedServer(server):
    import pyodbc
    
    db_connxn = pyodbc.connect('Driver={SQL Server};'
                      f'Server={server};'
                      'Database=master;'
                      'Trusted_Connection=yes;')
    return (db_connxn)

#%% Query all columns from a table, returns actual data
def pyDBTableQuery(server, database, table, condition=False):
    import pandas as pd
    from datetime import datetime
    import warnings
    
    warnings.simplefilter('ignore')
    
    database_connection = pyConnect2DB(server, database)
    current_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    
    if condition:
        where_clause = condition
    else:
        where_clause = ""
        
    try:
        sql_data = pd.read_sql_query(f'SELECT * FROM {table} {where_clause}', database_connection )
        print(f"{current_time} - {table.title()} data fetched successfully.")
    except database_connection.Error as e:
        print(f"{current_time} - Error retrieving entry from {database}: {e}")
    finally:
        if database_connection:
           database_connection.close()
    return (sql_data )

#%% Query all columns from Linked Server table, returns actual data
def pyDBLinkedServerQuery(server, linked_server, database, table):
    import pandas as pd
    from datetime import datetime
    import warnings
    warnings.simplefilter('ignore')
    
    database_connection = pyConnect2LinkedServer(server)
    current_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        sql_data = pd.read_sql_query(f'SELECT * FROM {linked_server}.{database}.{table}', database_connection )
        print(f"{current_time} - {table.title()} data fetched successfully.")
    except database_connection.Error as e:
        print(f"{current_time} - Error retrieving entry from {database}: {e}")
    finally:
        if database_connection:
           database_connection.close()
    return (sql_data )
    
#%% convert date column to datetime format then remove millisec
def format_SQLdt(df, column):
    import pandas as pd
    df[column] = pd.to_datetime(df[column], format='%Y-%m-%d').dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    df[column] = [x[:-3] for x in df[column]]

#%% Load update to database; Mode options: 'replace' / 'append',
def df2sql(df, server, database, table, mode, echo_display):
    from sqlalchemy import create_engine
    import urllib
    from datetime import datetime
    
    if echo_display:
        display = True
    else:
        display = False
    
    table = table.replace('[','').replace(']','')
    schm, table = table.split('.')                                              # split input into schema & table name
    
    quoted = urllib.parse.quote_plus('Driver={SQL Server};'
                      f'Server={server};'
                      f'Database={database};'
                      'Trusted_Connection=yes')
    
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted),echo=display, use_setinputsizes=False)
    df.to_sql(table, schema=schm, con=engine, chunksize=200, method='multi', index=False, if_exists=mode)
    
    current_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{current_time} - Data successfully loaded to '{table}' table in '{database}' database.")

#%% pyDBqueryStatement: General table query where statement can be altered
def pyDBqueryStatement(database, statement):
    import pandas as pd
    from datetime import datetime
 
    database_connection = pyConnect2DB(database)
    current_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        sql_data = pd.read_sql_query(f'{statement}', database_connection )
        current_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{current_time} - Query executed successfully")
    
    except database_connection.Error as e:
      print(f"Error retrieving entry from database: {e}")
      
    finally:
        if database_connection:
           database_connection.close()
    return (sql_data)

#%% Connect and Load data to Azure
def load_data_to_azure(fname, src_path, container_name, blob_folder, env):
    from azure.storage.blob import BlobServiceClient
    from datetime import datetime
    from dotenv import load_dotenv
    import os

    """
    fname: name of the file in on-prem source and target blob
    src_path: source path (on-prem)
    blob_folder: target/ destination path (relative path in azure storage container)
    env: path to the .env file containing the credentials
    """
    
    # Get Azure Credentials
    load_dotenv(env)
    
    account_name = os.getenv('AZ_{container_name.upper()}_ACCOUNT_NAME')
    account_key = os.getenv('AZ_{container_name.upper()}_ACCOUNT_KEY')

    # Azure Storage account connection string
    conn_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'

    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    
    # Get a BlobClient for the target blob: f'{blob_folder}/{fname}'
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f'{blob_folder}/{fname}')
    
    # Upload the file
    with open(f'{src_path}/{fname}', 'rb') as data:
        blob_client.upload_blob(data)
    
    print(f"{(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')} - File uploaded to '{container_name}' container: '{blob_folder}/{fname}'")

#%% Connect to Snowflake
def sf_connection(env):
    import snowflake.connector as sf
    from datetime import datetime
    from dotenv import load_dotenv
    import os
    # env: path to the .env file containing the credentials
    load_dotenv(env)
    
    conn = sf.connect(
        user = os.getenv('SF_USER'),
        password = os.getenv('SF_PASSWORD'),
        account = os.getenv('SF_ACCOUNT'),
        warehouse = os.getenv('SF_WAREHOUSE'),
        database = os.getenv('SF_DATABASE'),
        schema = os.getenv('SF_SCHEMA')
    )
    print(f'{(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")} - Snowflake connection successfully established.')
    
    return conn
