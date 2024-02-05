# Dependencies ################################################################
import pandas as pd
import os
import time
import logging
import sqlite3
import pyarrow.feather as feather
import openpyxl

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