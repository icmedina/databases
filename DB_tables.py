# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 12:21:23 2022

@author: Isidro Jr Medina
"""
#%% package, path, DB & input param
import os, pyodbc
from DB_Data import pyDBqueryStatement
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import warnings
# from openpyxl import Workbook

warnings.filterwarnings("ignore")

svr = 'server_name'
path =  "directory_name"; os.chdir(path)
today = (datetime.now()).strftime("%Y-%m-%d")
show_column_details = False                                     # export individual table details or not

file_1 = f"Tables_{today}_summary.xlsx"
file_2 = f"Tables-Columns_{today}_summary.xlsx"

xl_writer_1 = pd.ExcelWriter(os.path.join(path,file_1))# , engine = 'openpyxl')
xl_writer_2 = pd.ExcelWriter(os.path.join(path,file_2))

#%% Server Query - get list of databases

conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={svr}")    # Establish the connection
cursor = conn.cursor()                                          # Create a cursor object
cursor.execute("SELECT name FROM sys.databases")                # Execute a query to list databases
databases = cursor.fetchall()                                   # Fetch and print all the databases

dbs = []
for db in databases:
    db = db[0]
    if db not in ['master','tempdb','model','msdb', 'SSISDB','SSISTest']:
        dbs.append(db)

cursor.close(); conn.close()                                    # Close the cursor & connection

#%% Resize columns automatically

def resize_cols(df, ws):
    # auto resize column width
    for idx, col in enumerate(df):                              # loop through all columns in df
        series = df[col]
        max_len = max((
            series.astype(str).map(len).max(),                  # len of largest item
            len(str(series.name))                               # len of column name/header
            )) + 2                                              # add a little extra space
        ws.set_column(idx, idx, max_len)                        # set column width
                    
#%% DB Query

for db in dbs:
    sql_statement =  'SELECT * FROM INFORMATION_SCHEMA.TABLES GO'
    db_tables = pyDBqueryStatement(database=db, statement=sql_statement)
    db_tables.to_excel(xl_writer_1, sheet_name=db, index=False)
    ws_1 = xl_writer_1.sheets[db]
    resize_cols(df=db_tables, ws=ws_1)
    
    # Table Query
    table_cols = pd.DataFrame(columns=['Table','Columns'])
    
    for row in tqdm(range(len(db_tables))):
        table = f'{db_tables.iloc[row,1]}.{db_tables.iloc[row,2]}'; print (f"Table:\t{table}")
        sql_statement = f"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('{table}')"
        sql_table = pyDBqueryStatement(database=db, statement=sql_statement)
        
        try:
            # export table details to individual sheet
            if show_column_details:
                tb_name = table[:30].replace('[','').replace(']','')
                sql_table.to_excel(xl_writer_2, sheet_name=tb_name, index=False)
                worksheet = xl_writer_2.sheets[tb_name]                         # pull worksheet object
                resize_cols(df=sql_table, ws=worksheet)                         # auto resize column width
                
            for col in sql_table.name:
                new_row = {'Table':table, 'Columns':col}
                table_cols = table_cols.append(new_row, ignore_index=True)      # append row to the table cols df
        except:
            print (f"Parsing Table:\t{table} - failed!")
            pass
    
    #%% Export data

    table_cols.to_excel(xl_writer_2, sheet_name=db, index=False)
    ws_2 = xl_writer_2.sheets[db] 
    resize_cols(df=table_cols, ws=ws_2)                         # auto resize column width

# save and close excel objects
xl_writer_1.save(); xl_writer_1.close()
xl_writer_2.save(); xl_writer_2.close()

os.startfile(path)