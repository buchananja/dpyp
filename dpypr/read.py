'''
The 'read' module contains functionality for reading data of various datatypes
into data pipelines.
'''

# Dependencies ################################################################
import pandas as pd
import os
import sqlite3
import dpypr as dp

# Data Loading ################################################################
def read_all_json(path):
    r'''
    Iteratively loads data.json from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = dict()
    for file in files:
        if file.endswith('.json'):
            df = pd.read_json(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary

def read_all_csv(path):
    r'''
    Iteratively loads data.csv from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = dict()
    for file in files:
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary

def read_all_xlsx(path):
    r'''
    Iteratively loads data.xlsx from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = dict()
    for file in files:
        if file.endswith('.xlsx'):
            df = pd.read_excel(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary

def read_all_feather(path):
    r'''
    Iteratively loads data.feather from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = dict()
    for file in files:
        if file.endswith('.feather'):
            df = pd.read_feather(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary

def read_all_parquet(path):
    r'''
    Iteratively loads data.parquet from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = dict()
    for file in files:
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary

def read_all_pickle(path):
    r'''
    Iteratively loads data.pickle from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = dict()
    for file in files:
        if file.endswith('.pickle'):
            df = pd.read_pickle(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary
              
def read_all_sqlite(path):
    r'''
    Returns all data from a sqlite database as a dictionary.
    '''
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # queries all tables in database
    cur.execute('''
        SELECT name 
        FROM sqlite_master 
        WHERE type = 'table';
    ''')
    table_names = cur.fetchall()
    
    data_dictionary = dict()
    for table_name in table_names:
        # selects everything from each table.
        query = f"SELECT * FROM {table_name[0]}"
        data_dictionary[table_name[0]] = pd.read_sql_query(query, conn)
    conn.close()
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary     

def gather_data_dictionary(globals_dict):
    r'''
    Packages all pandas dataframes in input dictionary beginning with 'df\_' and returns
    output dictionary.
    '''
    data_dictionary = dict()
    for name, data in globals_dict.items():
        if name.startswith('df_') and isinstance(data, pd.DataFrame):
            data_dictionary.update({name: data})
    if not data_dictionary:
        dp.sleep_log('No files found.')
    return data_dictionary

def unpack_data_dictionary(
        data_dictionary, 
        globals_dict = globals(), 
        sleep_seconds = 0, 
        messaging = False
    ):
    r'''
    Loads all data from data_dictionary into global variables with record 
    counts.
    '''
    for key, value in data_dictionary.items():
        globals_dict[f'df_{key}'] = value
        if messaging:
            dp.sleep_log(
                    f'- Read df_{key} ({len(value):,}) records.', 
                    sleep_time = sleep_seconds
                )