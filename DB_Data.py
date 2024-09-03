# -*- coding: utf-8 -*-
"""
Created on Tue Jan 15 13:38:55 2024

Description: Python DB connection and query

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
    
    try: # database_connection = pyConnect2PDM('prod'); table = 'promotions'
        sql_data = pd.read_sql_query(f'{statement}', database_connection )
        current_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{current_time} - Query executed successfully")
    
    except database_connection.Error as e:
      print(f"Error retrieving entry from database: {e}")
      
    finally:
        if database_connection:
           database_connection.close()
    return (sql_data)
