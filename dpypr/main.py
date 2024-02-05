# Dependencies ################################################################
import pandas as pd
import os
import time
import logging
import sqlite3
import pyarrow.feather as feather
import openpyxl


# Data Cleaning ###############################################################
def headers_to_snakecase(df, uppercase = False):
    r'''
    Converts all column headers to lower snake case by defatul and uppercase if
    'uppercase' argument is True.
    '''
    if uppercase:
        df.columns = (df.columns.str.upper().str.replace(' ', '_'))
    else:
        df.columns = (df.columns.str.lower().str.replace(' ', '_'))
    return df

def values_to_lowercase(df):
    r'''
    Converts all string values in dataframe to lowercase.
    '''
    df = df.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    return df

def values_to_uppercase(df):
    r'''
    Converts all string values in dataframe to uppercase.
    '''
    df = df.apply(lambda x: x.str.upper() if x.dtype == "object" else x)
    return df

def values_strip_whitespace(df):
    r'''
    Converts all string values to lowercase.
    '''
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df

def optimise_numeric_datatypes(df):
    r'''
    Optimises the data types in a pandas DataFrame by attempting to convert
    strings to numerical data where possible and to the smallest possible 
    integer datatype.
    ''' 
    for col in df.columns:
        if df[col].dtype == object:
            pass
        else:
            if all(df[col] % 1 == 0):
                df[col] = pd.to_numeric(df[col], downcast = 'integer')
            else:
                df[col] = pd.to_numeric(df[col], downcast = 'float')
    return df


# Data Loading ################################################################
def read_all_json(path):
    r'''
    Iteratively loads data.json from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.json'):
            df = pd.read_json(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    return data_dictionary

def read_all_csv(path):
    r'''
    Iteratively loads data.csv from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    return data_dictionary

def read_all_xlsx(path):
    r'''
    Iteratively loads data.xlsx from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.xlsx'):
            df = pd.read_excel(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    return data_dictionary

def read_all_feather(path):
    r'''
    Iteratively loads data.feather from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.feather'):
            df = pd.read_feather(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    return data_dictionary

def read_all_parquet(path):
    r'''
    Iteratively loads data.parquet from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    return data_dictionary

def read_all_pickle(path):
    r'''
    Iteratively loads data.pickle from the data directory and assign to 
    dataframes. Unpacks dictionary to global variables names with the format 
    df_{filename}.
    '''
    files = os.listdir(path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.pickle'):
            df = pd.read_pickle(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
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
    
    data_dictionary = {}
    for table_name in table_names:
        # selects everything from each table.
        query = f"SELECT * FROM {table_name[0]}"
        data_dictionary[table_name[0]] = pd.read_sql_query(query, conn)
    conn.close()
    return data_dictionary     

def gather_data_dictionary(globals_dict):
    r'''
    Packages all objects in input dictionary beginning with 'df\_' and returns
    output dictionary.
    '''
    data_dictionary = dict()
    for name, data in globals_dict.items():
        if name.startswith('df_'):
            data_dictionary.update({name: data})
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
            sleep_log(
                f'- Loaded df_{key} ({len(value):,}) records.', 
                sleep_time = sleep_seconds
                )


# Data Writing ################################################################
def write_dict_to_json(
        globals_dict, 
        path, 
        file_prefix = 'processed',
        messaging = True,
        sleep_seconds = 0.1
    ):
    r'''
    - Writes all objects beginning with 'df\_' in global space to path as .json. 
    - Prefix allows user to rename processed files upon writing.
    - 'Messaging' allows user to output number of records to console.
    '''
    for name, data in globals_dict.items():
        if name.startswith('df_'):
            data.to_json(f'{path}/{file_prefix}_{name[3:]}.json')
            # ouputs filenames and number or records to console
            if messaging:
                sleep_log(
                    f'- Wrote {file_prefix}_{name[3:]} ({len(data):,}) records.', 
                    sleep_time = sleep_seconds
                    )

def write_dict_to_csv(
        globals_dict, 
        path, 
        file_prefix = 'processed',
        messaging = True,
        sleep_seconds = 0.1
    ):
    r'''
    - Writes all objects beginning with 'df\_' in global space to path as .csv. 
    - Prefix allows user to rename processed files upon writing.
    - 'Messaging' allows user to output number of records to console.
    '''
    for name, data in globals_dict.items():
        if name.startswith('df_'):
            data.to_csv(f'{path}/{file_prefix}_{name[3:]}.csv')
            # ouputs filenames and number or records to console
            if messaging:
                sleep_log(
                    f'- Wrote {file_prefix}_{name[3:]} ({len(data):,}) records.', 
                    sleep_time = sleep_seconds
                    )
            
def write_dict_to_xlsx(
        globals_dict, 
        path, 
        file_prefix = 'processed',
        messaging = True,
        sleep_seconds = 0.1
    ):
    r'''
    - Writes all objects beginning with 'df\_' in global space to path as .xlsx. 
    - Prefix allows user to rename processed files upon writing.
    - 'Messaging' allows user to output number of records to console.
    '''
    for name, data in globals_dict.items():
        if name.startswith('df_'):
            data.to_excel(f'{path}/{file_prefix}_{name[3:]}.xlsx')
            # ouputs filenames and number or records to console
            if messaging:
                sleep_log(
                    f'- Wrote {file_prefix}_{name[3:]} ({len(data):,}) records.', 
                    sleep_time = sleep_seconds
                    )

def write_dict_to_feather(
        globals_dict, 
        path, 
        file_prefix = 'processed',
        messaging = True,
        sleep_seconds = 0.1
    ):
    r'''
    - Writes all objects beginning with 'df\_' in global space to path as .feather. 
    - Prefix allows user to rename processed files upon writing.
    - 'Messaging' allows user to output number of records to console.
    '''
    for name, data in globals_dict.items():
        if name.startswith('df_'):
            data.to_feather(f'{path}/{file_prefix}_{name[3:]}.feather')
            # ouputs filenames and number or records to console
            if messaging:
                sleep_log(
                    f'- Wrote {file_prefix}_{name[3:]} ({len(data):,}) records.', 
                    sleep_time = sleep_seconds
                    )
            
def write_dict_to_parquet(
        globals_dict, 
        path, 
        file_prefix = 'processed',
        messaging = True,
        sleep_seconds = 0.1
    ):
    r'''
    - Writes all objects beginning with 'df\_' in global space to path as .parquet. 
    - Prefix allows user to rename processed files upon writing.
    - 'Messaging' allows user to output number of records to console.
    '''
    for name, data in globals_dict.items():
        if name.startswith('df_'):
            data.to_parquet(f'{path}/{file_prefix}_{name[3:]}.parquet')
            # ouputs filenames and number or records to console
            if messaging:
                sleep_log(
                    f'- Wrote {file_prefix}_{name[3:]} ({len(data):,}) records.', 
                    sleep_time = sleep_seconds
                    )
            
def write_dict_to_pickle(
        globals_dict, 
        path, 
        file_prefix = 'processed',
        messaging = True,
        sleep_seconds = 0.1
    ):
    r'''
    - Writes all objects beginning with 'df\_' in global space to path as .pickle. 
    - Prefix allows user to rename processed files upon writing.
    - 'Messaging' allows user to output number of records to console.
    '''
    for name, data in globals_dict.items():
        if name.startswith('df_'):
            data.to_pickle(f'{path}/{file_prefix}_{name[3:]}.pickle')
            # ouputs filenames and number or records to console
            if messaging:
                sleep_log(
                    f'- Wrote {file_prefix}_{name[3:]} ({len(data):,}) records.', 
                    sleep_time = sleep_seconds
                    )         
                
                
# Diagnostics #################################################################
def fetch_all_sqlite_tables(path):
    '''
    Returns all table names present in a sqlite database as a list.
    r'''
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # queries all tables in database
    cur.execute('''
        SELECT name 
        FROM sqlite_master 
        WHERE type = 'table';
    ''')
    table_names = cur.fetchall()
    table_list = list()
    for table in table_names:
        # gets first element of each tuple in list of tuples (e.g. table name)
        table_list.append(table[0])
    return table_list
    
def fetch_all_global_df(globals_dict):
    r'''
    Returns all objects beginning with 'df\_' in global space as a list.
    '''
    list_df = list()
    for name in globals_dict:
        if name.startswith('df_'):
            list_df.append(name)
    return list_df

def sleep_log(message, sleep_time = 0):
    r'''
    Outputs info logging mesage to console with variable sleep timer.
    '''
    time.sleep(sleep_time)
    logging.info(message)