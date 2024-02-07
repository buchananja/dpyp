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
def read_everything(path):
    '''
    - Iteratively loads all json, csv, xlsx, parquet, feather, and pickle files 
    from the data directory and assigns to dataframes. 
    - Messaging logs statements about number of records.
    '''

    files = os.listdir(path)
    data_dictionary = dict()
    for file in files:
            if file.endswith('.json'):
                df = pd.read_json(os.path.join(path, file))
                filename = os.path.splitext(file)[0]
                data_dictionary[f'df_{filename}'] = df
            elif file.endswith('.csv'):
                df = pd.read_csv(os.path.join(path, file))
                filename = os.path.splitext(file)[0]
                data_dictionary[f'df_{filename}'] = df
            elif file.endswith('.xlsx'):
                df = pd.read_excel(os.path.join(path, file))
                filename = os.path.splitext(file)[0]
                data_dictionary[f'df_{filename}'] = df
            elif file.endswith('.feather'):
                df = pd.read_feather(os.path.join(path, file))
                filename = os.path.splitext(file)[0]
                data_dictionary[f'df_{filename}'] = df
            elif file.endswith('.parquet'):
                df = pd.read_parquet(os.path.join(path, file))
                filename = os.path.splitext(file)[0]
                data_dictionary[f'df_{filename}'] = df
            elif file.endswith('.pickle'):
                df = pd.read_pickle(os.path.join(path, file))
                filename = os.path.splitext(file)[0]
                data_dictionary[f'df_{filename}'] = df
    if not data_dictionary:
        dp.sleep_log('No files read.')
    return data_dictionary
        
        
def read_all_json(path, messaging = True, sleep_seconds = 0.1):
    '''
    - Iteratively loads all json files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    files = os.listdir(path)
    data_dictionary = dict()
    
    if messaging:
        dp.sleep_log('\nWriting data...', sleep_time = sleep_seconds)
    
    for file in files:
        if file.endswith('.json'):
            df = pd.read_json(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                dp.sleep_log(
                        f'- read {f'df_{filename}'} ({len(filename):,}) records.',
                        sleep_time = sleep_seconds 
                    )
                
    if not data_dictionary:
        dp.sleep_log('No files read.')
    if messaging:
        dp.sleep_log('All data read successfully.\n', sleep_time = sleep_seconds)
        
    return data_dictionary


def read_all_csv(path, messaging = True, sleep_seconds = 0.1):
    '''
    - Iteratively loads all csv files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    files = os.listdir(path)
    data_dictionary = dict()
    
    if messaging:
        dp.sleep_log('\nWriting data...', sleep_time = sleep_seconds)
        
    for file in files:
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                dp.sleep_log(
                        f'- read {f'df_{filename}'} ({len(filename):,}) records.',
                        sleep_time = sleep_seconds 
                    )
                
    if not data_dictionary:
        dp.sleep_log('No files read.')
    if messaging:
        dp.sleep_log('All data read successfully.\n', sleep_time = sleep_seconds)
        
    return data_dictionary


def read_all_xlsx(path, messaging = True, sleep_seconds = 0.1):
    '''
    - Iteratively loads all xlsx files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    files = os.listdir(path)
    data_dictionary = dict()
    
    if messaging:
        dp.sleep_log('\nWriting data...', sleep_time = sleep_seconds)
        
    for file in files:
        if file.endswith('.xlsx'):
            df = pd.read_excel(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                dp.sleep_log(
                        f'- read {f'df_{filename}'} ({len(filename):,}) records.',
                        sleep_time = sleep_seconds 
                    )
                
    if not data_dictionary:
        dp.sleep_log('No files read.')
    if messaging:
        dp.sleep_log('All data read successfully.\n', sleep_time = sleep_seconds)
        
    return data_dictionary


def read_all_feather(path, messaging = True, sleep_seconds = 0.1):
    '''
    - Iteratively loads all feather files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    files = os.listdir(path)
    data_dictionary = dict()
    
    if messaging:
        dp.sleep_log('\nWriting data...', sleep_time = sleep_seconds)
        
    for file in files:
        if file.endswith('.feather'):
            df = pd.read_feather(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                dp.sleep_log(
                        f'- read {f'df_{filename}'} ({len(filename):,}) records.',
                        sleep_time = sleep_seconds 
                    )
                
    if not data_dictionary:
        dp.sleep_log('No files read.')
    if messaging:
        dp.sleep_log('All data read successfully.\n', sleep_time = sleep_seconds)
        
    return data_dictionary


def read_all_parquet(path, messaging = True, sleep_seconds = 0.1):
    '''
    - Iteratively loads all parquet files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    files = os.listdir(path)
    data_dictionary = dict()
    
    if messaging:
        dp.sleep_log('\nWriting data...', sleep_time = sleep_seconds)
        
    for file in files:
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                dp.sleep_log(
                        f'- read {f'df_{filename}'} ({len(filename):,}) records.',
                        sleep_time = sleep_seconds 
                    )
                
    if not data_dictionary:
        dp.sleep_log('No files read.')
    if messaging:
        dp.sleep_log('All data read successfully.\n', sleep_time = sleep_seconds)
        
    return data_dictionary


def read_all_pickle(path, messaging = True, sleep_seconds = 0.1):
    '''
    - Iteratively loads all pickle files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    files = os.listdir(path)
    data_dictionary = dict()
    
    if messaging:
        dp.sleep_log('\nWriting data...', sleep_time = sleep_seconds)
        
    for file in files:
        if file.endswith('.pickle'):
            df = pd.read_pickle(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                dp.sleep_log(
                        f'- read {f'df_{filename}'} ({len(filename):,}) records.',
                        sleep_time = sleep_seconds 
                    )
                
    if not data_dictionary:
        dp.sleep_log('No files read.')
    if messaging:
        dp.sleep_log('All data read successfully.\n', sleep_time = sleep_seconds)
        
    return data_dictionary
     
              
def read_all_sqlite(path):
    '''
    - Iteratively loads all tables from sqlite database and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
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
    '''
    Packages all dataframes in input dictionary beginning with 'df_' and 
    returns output dictionary.
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
        output_dictionary = '',
        sleep_seconds = 0, 
        messaging = False,
    ):
    '''
    - Loads all dataframes in input dictionary beginning with 'df_' into 
    output_dictionary if present.
    - Messaging logs statements about number of records.
    '''
    
    if not output_dictionary:
        output_dictionary = dict()
        
    for key, value in data_dictionary.items():
            if isinstance(value, pd.DataFrame):
                    output_dictionary[f'df_{key}'] = value
                    if messaging:
                        dp.sleep_log(
                            f'- Read df_{key} ({len(value):,}) records.', 
                            sleep_time = sleep_seconds
                        )    
    return output_dictionary