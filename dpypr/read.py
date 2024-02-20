'''
The 'read' module contains functionality for reading data of various datatypes
into data pipelines.
'''


# Dependencies ################################################################
import pandas as pd
import dpypr as dp
import os
import sqlite3
from sqlite3 import OperationalError


# Data Loading ################################################################
# def read_everything(
#         path, 
#         messaging = True,
#         file_extensions = ['.json', '.csv', '.xlsx', '.feather', '.parquet', '.pickle']
#     ):
#     '''
#     - Iteratively loads all json, csv, xlsx, parquet, feather, and pickle files 
#     from the data directory and assigns to dataframes within a dictionary.
#     - Messaging logs statements about number of records.
#     '''

#     files = os.listdir(path)
#     data_dictionary = dict()
    
#     if messaging:
#         print('\nReading data...')
        
#     for file in files:
#         filename, file_extension = os.path.splitext(file)
#         if file_extension in file_extensions:
#             if file_extension == '.json':
#                 df = pd.read_json(os.path.join(path, file))
#             elif file_extension == '.csv':
#                 df = pd.read_csv(os.path.join(path, file))
#             elif file_extension == '.xlsx':
#                 df = pd.read_excel(os.path.join(path, file))
#             elif file_extension == '.feather':
#                 df = pd.read_feather(os.path.join(path, file))
#             elif file_extension == '.parquet':
#                 df = pd.read_parquet(os.path.join(path, file))
#             elif file_extension == '.pickle':
#                 df = pd.read_pickle(os.path.join(path, file))
#             data_dictionary[f'df_{filename}_{file_extension[1:]}'] = df
            
#             if messaging:
#                 print(f'- read {f'df_{filename}_{file_extension[1:]}'} ({len(filename):,} records).')      
#     if not data_dictionary:
#         print('No files read.')
#     else:
#         if messaging:
#             print('All data read successfully.\n')
#     return data_dictionary
        
        
def read_all_json(path, messaging = True):
    '''
    - Iteratively loads all json files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        print('Please enter a valid path.')
        
    if messaging:
        print('\nReading data...')
    
    for file in files:
        if file.endswith('.json'):
            df = pd.read_json(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                print(f'- read df_{filename} ({len(filename):,} records).')
                
    if not data_dictionary:
        print('No files read.')
    if messaging:
        print('All data read successfully.\n')
        
    return data_dictionary


def read_all_csv(path, messaging = True):
    '''
    - Iteratively loads all csv files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        print('Please enter a valid path.')
    
    if messaging:
        print('\nReading data...')
        
    for file in files:
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                print(f'- read df_{filename} ({len(filename):,} records).')
                
    if not data_dictionary:
        print('No files read.')
    if messaging:
        print('All data read successfully.\n')
        
    return data_dictionary


def read_all_xlsx(path, messaging = True):
    '''
    - Iteratively loads all xlsx files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        print('Please enter a valid path.')
    
    if messaging:
        print('\nReading data...')
        
    for file in files:
        if file.endswith('.xlsx'):
            df = pd.read_excel(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                print(f'- read df_{filename} ({len(filename):,} records).')
                
    if not data_dictionary:
        print('No files read.')
    if messaging:
        print('All data read successfully.\n')
        
    return data_dictionary


def read_all_feather(path, messaging = True):
    '''
    - Iteratively loads all feather files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        print('Please enter a valid path.')
    
    if messaging:
        print('\nReading data...')
        
    for file in files:
        if file.endswith('.feather'):
            df = pd.read_feather(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                print(f'- read df_{filename} ({len(filename):,} records).')
                
    if not data_dictionary:
        print('No files read.')
    if messaging:
        print('All data read successfully.\n')
        
    return data_dictionary


def read_all_parquet(path, messaging = True):
    '''
    - Iteratively loads all parquet files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        print('Please enter a valid path.')
    
    if messaging:
        print('\nReading data...')
        
    for file in files:
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                print(f'- read df_{filename} ({len(filename):,} records).')
                
    if not data_dictionary:
        print('No files read.')
    if messaging:
        print('All data read successfully.\n')
        
    return data_dictionary


def read_all_pickle(path, messaging = True):
    '''
    - Iteratively loads all pickle files from the data directory and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        print('Please enter a valid path.')
    
    if messaging:
        print('\nReading data...')
        
    for file in files:
        if file.endswith('.pickle'):
            df = pd.read_pickle(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
            
            if messaging:
                print(f'- read df_{filename} ({len(filename):,} records).')
                
    if not data_dictionary:
        print('No files read.')
    if messaging:
        print('All data read successfully.\n')
        
    return data_dictionary
     
              
def read_all_sqlite(path, messaging = True):
    '''
    - Iteratively loads all tables from sqlite database and assigns to 
    dataframes. 
    - Messaging logs statements about number of records.
    '''
    
    try:
        if dp.check_path_valid(path):
            conn = sqlite3.connect(path)
            cur = conn.cursor()
    except OperationalError:
        print('WARNING: Failed to connect to database.')
        
    if messaging:
        print('\nReading data...')
    
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
        
        if messaging:
            print(f'- read df_{table_name[0]} ({len(data_dictionary[table_name[0]]):,} records).')
    if messaging:
        print('All data read successfully.\n')
            
    conn.close()
    
    if not data_dictionary:
        print('No files read.')
        
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
        print('No files found.')
        
    return data_dictionary


def unpack_data_dictionary(
        data_dictionary, 
        output_dict = None, 
        messaging = False
    ):
    '''
    - Loads all data from data_dictionary into global variables with record 
    counts.
    - If output_dict is provided, output_dict will be updated and not returned.
    - If output_dict is not provided, a new dictionary will be returned.
    '''
    # checks whether output dictionary provided
    if output_dict is None:
        output_dict = dict()
        return_dict = True
    else:
        return_dict = False
    
    if messaging:
        print('\nReading data...')

    # unpacks all dataframes to globals are prefixes name with 'df_'
    for key, value in data_dictionary.items():
        if isinstance(value, pd.DataFrame):
            output_dict[f'df_{key}'] = value
            
            if messaging:
                print(f'- Loaded df_{key} ({len(value):,} records).')
    if messaging:
        print('All data read successfully.\n')

    if return_dict:
        return output_dict