'''
This is a custom module containing functions useful to data pipeline
construction, maintenance, and testing.
'''

import pandas as pd
import os
import time
import logging

def headers_to_snakecase(df):
    '''
    Converts all column headers to lower snake case.
    '''
    df.columns = (df.columns.str.lower().str.replace(' ', '_'))
    return df

def values_to_lowercase(df):
    '''
    Converts all string values to lowercase.
    '''
    df = df.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
    return df

def values_strip_whitespace(df):
    '''
    Converts all string values to lowercase.
    '''
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df

def optimise_datatypes(df):
    '''
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

def read_all_json(directory_path):
    '''
    Iteratively loads data from the data directory and assign to dataframes.
    Unpacks dictionary to global variables names with the format df_{filename}.
    '''
    files = os.listdir(directory_path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.json'):
            df = pd.read_json(os.path.join(directory_path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    return data_dictionary

def read_all_csv(directory_path):
    '''
    Iteratively loads data from the data directory and assign to dataframes.
    Unpacks dictionary to global variables names with the format df_{filename}.
    '''
    files = os.listdir(directory_path)
    data_dictionary = {}
    for file in files:
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(directory_path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'df_{filename}'] = df
    return data_dictionary

def sleep_log(message, sleep_time = 0):
    '''
    Outputs info logging mesage to console with variable sleep timer.
    '''
    time.sleep(sleep_time)
    logging.info(message)
    
def unpack_data_dictionary(
    data_dictionary, 
    globals_dict = globals(), 
    sleep_seconds = 0, 
    messaging = False
    ):
    '''
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