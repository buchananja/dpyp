'''
The 'write' module contains functionality for writing data of various datatypes
from data pipelines.
'''


# Dependencies ################################################################
import dpypr as dp
import pandas as pd
import sqlite3
from sqlite3 import OperationalError
import os


# Data Writing ################################################################
def write_dict_to_json(
        globals_dict, 
        path, 
        file_prefix = 'df',
        messaging = True
    ):
    '''
    - Writes all objects beginning with 'df_' in global space to path as .json. 
    - Prefix allows user to rename processed files upon writing.
    - Messaging logs statements about number of records.
    '''
    
    if messaging:
        dp.sleep_log('\nWriting data...')
        
    for name, data in globals_dict.items():
        if name.startswith('df_') & isinstance(data, pd.DataFrame):
            data.to_json(f'{path}/{file_prefix}_{name[3:]}.json')

            if messaging:
                dp.sleep_log(f'- Wrote {file_prefix}_{name[3:]} ({len(data):,} records).')
    if messaging:
        dp.sleep_log('All data written successfully.\n')


def write_dict_to_csv(
        globals_dict, 
        path, 
        file_prefix = 'df',
        messaging = True
    ):
    '''
    - Writes all objects beginning with 'df_' in global space to path as .csv. 
    - Prefix allows user to rename processed files upon writing.
    - Messaging logs statements about number of records.
    '''
    
    if messaging:
        dp.sleep_log('\nWriting data...')
        
    for name, data in globals_dict.items():
        if name.startswith('df_') & isinstance(data, pd.DataFrame):
            data.to_csv(f'{path}/{file_prefix}_{name[3:]}.csv')

            if messaging:
                dp.sleep_log(f'- Wrote {file_prefix}_{name[3:]} ({len(data):,} records).')
    if messaging:
        dp.sleep_log('All data written successfully.\n')
       
            
def write_dict_to_xlsx(
        globals_dict, 
        path, 
        file_prefix = 'df',
        messaging = True
    ):
    '''
    - Writes all objects beginning with 'df_' in global space to path as .xlsx. 
    - Prefix allows user to rename processed files upon writing.
    - Messaging logs statements about number of records.
    '''
    
    if messaging:
        dp.sleep_log('\nWriting data...')
        
    for name, data in globals_dict.items():
        if name.startswith('df_') & isinstance(data, pd.DataFrame):
            data.to_excel(f'{path}/{file_prefix}_{name[3:]}.xlsx')

            if messaging:
                dp.sleep_log(f'- Wrote {file_prefix}_{name[3:]} ({len(data):,} records).')
    if messaging:
        dp.sleep_log('All data written successfully.\n')


def write_dict_to_feather(
        globals_dict, 
        path, 
        file_prefix = 'df',
        messaging = True
    ):
    '''
    - Writes all objects beginning with 'df_' in global space to path as .feather. 
    - Prefix allows user to rename processed files upon writing.
    - Messaging logs statements about number of records.
    '''
    
    if messaging:
        dp.sleep_log('\nWriting data...')
        
    for name, data in globals_dict.items():
        if name.startswith('df_') & isinstance(data, pd.DataFrame):
            data.to_feather(f'{path}/{file_prefix}_{name[3:]}.feather')

            if messaging:
                dp.sleep_log(f'- Wrote {file_prefix}_{name[3:]} ({len(data):,} records).')
    if messaging:
        dp.sleep_log('All data written successfully.\n')
        
            
def write_dict_to_parquet(
        globals_dict, 
        path, 
        file_prefix = 'df',
        messaging = True
    ):
    '''
    - Writes all objects beginning with 'df_' in global space to path as .parquet. 
    - Prefix allows user to rename processed files upon writing.
    - Messaging logs statements about number of records.
    '''
    
    if messaging:
        dp.sleep_log('\nWriting data...')
        
    for name, data in globals_dict.items():
        if name.startswith('df_') & isinstance(data, pd.DataFrame):
            data.to_parquet(f'{path}/{file_prefix}_{name[3:]}.parquet')

            if messaging:
                dp.sleep_log(f'- Wrote {file_prefix}_{name[3:]} ({len(data):,} records).')
    if messaging:
        dp.sleep_log('All data written successfully.\n')
        
        
def write_dict_to_pickle(
        globals_dict, 
        path, 
        file_prefix = 'df',
        messaging = True
    ):
    '''
    - Writes all objects beginning with 'df_' in global space to path as .pickle. 
    - Prefix allows user to rename processed files upon writing.
    - Messaging logs statements about number of records.
    '''
    
    if messaging:
        dp.sleep_log('\nWriting data...')
        
    for name, data in globals_dict.items():
        if name.startswith('df_') & isinstance(data, pd.DataFrame):
            data.to_pickle(f'{path}/{file_prefix}_{name[3:]}.pickle')

            if messaging:
                dp.sleep_log(f'- Wrote {file_prefix}_{name[3:]} ({len(data):,} records).')
    if messaging:
        dp.sleep_log('All data written successfully.\n')
          
                
def write_dict_to_sqlite(
        data_dictionary, 
        path,
        overwrite = False,
        file_prefix = 'df',
        messaging = True
    ):
    '''
    - Writes all objects beginning with 'df_' in global space to path as
    tables in sqlite database. 
    - Prefix allows user to rename processed files upon writing.
    - Messaging logs statements about number of records.
    '''
    
    if overwrite:
        # remove old database
        try:
            os.remove(path)
        except PermissionError:
            dp.sleep_log('WARNING: File is being used by another process!')
        except FileNotFoundError:
            dp.sleep_log('WARNING: File does not exist!')
     
    # connect to database       
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    
    try:
        if messaging:
            dp.sleep_log('\nWriting data...')
            
        # create tables in new database
        for name, data in data_dictionary.items():
            if name.startswith('df_') & isinstance(data, pd.DataFrame):
                # write DataFrame to new database
                data.to_sql(f'{file_prefix}_{name[3:]}', conn, if_exists = 'replace')
                
                if messaging:
                    dp.sleep_log(f'- Wrote {file_prefix}_{name[3:]} ({len(data):,} records).')
        if messaging:
            dp.sleep_log('All data written successfully.\n')
            
    except Exception as e:
        dp.sleep_log(f'WARNING: {str(e)}')