import pandas as pd
import dpyp as dp
import os
import sqlite3
from sqlite3 import OperationalError
import logging


'''
the 'read' module contains functionality for reading data of various datatypes
into data pipelines
'''


# creates logging instance
logger = logging.getLogger(__name__)


def read_all_json(path, messaging = True):
    '''
    - iteratively loads all json files from the data directory and assigns to 
    dataframes
    - logger.debugs number of records
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        logger.debug('Please enter a valid path.')
        
    for file in files:
        if file.endswith('.json'):
            df = pd.read_json(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'{filename}'] = df
            
            if messaging:
                logger.debug(f'read {filename} ({len(filename):,} records)')
                
    if not data_dictionary:
        logger.debug('No files read.')
        
    return data_dictionary


def read_all_csv(path, seperator = ',', messaging = True):
    '''
    - iteratively loads all csv files from the data directory and assigns to 
    dataframes
    - logger.debugs number of records
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        logger.debug('Please enter a valid path.')
    
    for file in files:
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(path, file), sep = f'{seperator}')
            filename = os.path.splitext(file)[0]
            data_dictionary[f'{filename}'] = df
            
            if messaging:
                logger.debug(f'read {filename} ({len(filename):,} records)')
                
    if not data_dictionary:
        logger.debug('No files read.')
        
    return data_dictionary


def read_all_xlsx(path, messaging = True):
    '''
    - iteratively loads all xlsx files from the data directory and assigns to 
    dataframes
    - logger.debugs number of records
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        logger.debug('Please enter a valid path.')
        
    for file in files:
        if file.endswith('.xlsx'):
            df = pd.read_excel(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'{filename}'] = df
            
            if messaging:
                logger.debug(f'read {filename} ({len(filename):,} records)')
                
    if not data_dictionary:
        logger.debug('No files read.')
        
    return data_dictionary


def read_all_feather(path, messaging = True):
    '''
    - iteratively loads all feather files from the data directory and assigns to 
    dataframes
    - logger.debugs number of records
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        logger.debug('Please enter a valid path.')
        
    for file in files:
        if file.endswith('.feather'):
            df = pd.read_feather(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'{filename}'] = df
            
            if messaging:
                logger.debug(f'read {filename} ({len(filename):,} records)')
                
    if not data_dictionary:
        logger.debug('No files read.')
        
    return data_dictionary


def read_all_parquet(path, messaging = True):
    '''
    - iteratively loads all parquet files from the data directory and assigns to 
    dataframes
    - logger.debugs number of records
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        logger.debug('Please enter a valid path.')
        
    for file in files:
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'{filename}'] = df
            
            if messaging:
                logger.debug(f'read {filename} ({len(filename):,} records)')
                
    if not data_dictionary:
        logger.debug('No files read.')
        
    return data_dictionary


def read_all_pickle(path, messaging = True):
    '''
    - iteratively loads all pickle files from the data directory and assigns to 
    dataframes
    - logger.debugs number of records
    '''
    
    if dp.check_path_valid(path):
        files = os.listdir(path)
        data_dictionary = dict()
    else:
        logger.debug('Please enter a valid path.')
        
    for file in files:
        if file.endswith('.pickle'):
            df = pd.read_pickle(os.path.join(path, file))
            filename = os.path.splitext(file)[0]
            data_dictionary[f'{filename}'] = df
            
            if messaging:
                logger.debug(f'read {filename} ({len(filename):,} records)')
                
    if not data_dictionary:
        logger.debug('No files read.')
        
    return data_dictionary
     
              
def read_all_sqlite(path, messaging = True):
    '''
    - iteratively loads all tables from sqlite database and assigns to 
    dataframes
    - logger.debugs number of records
    '''
    
    try:
        if dp.check_path_valid(path):
            conn = sqlite3.connect(path)
            cur = conn.cursor()
    except OperationalError:
        logger.debug('WARNING: Failed to connect to database.')
    
    # queries all tables in database
    cur.execute('''
        SELECT name 
        FROM sqlite_master 
        WHERE type = 'table';
    ''')
    
    # returns list of table names
    table_names = cur.fetchall()
    
    data_dictionary = dict()
    for table_name in table_names:
        # selects everything from each table.
        query = f"SELECT * FROM {table_name[0]}"
        data_dictionary[table_name[0]] = pd.read_sql_query(query, conn)
        
        if messaging:
            logger.debug(f'read {table_name[0]} ({len(data_dictionary[table_name[0]]):,} records)')
    
    # closes cursor and connection to database
    cur.close() 
    conn.close()
    
    if not data_dictionary:
        logger.debug('No files read.')
        
    return data_dictionary


def unpack_data_dictionary(
        input_dictionary, 
        output_dict = None, 
        messaging = False
    ):
    '''
    - loads all data from data_dictionary into global variables with record 
    counts
    - if output_dict is provided, output_dict will be updated and not returned
    - if output_dict is not provided, a new dictionary will be returned
    '''
    
    # checks whether output dictionary provided
    if output_dict is None:
        output_dict = dict()
        return_dict = True
    else:
        return_dict = False
    
    # unpacks all dataframes to globals are prefixes name with ''
    for key, value in input_dictionary.items():
        if isinstance(value, pd.DataFrame):
            output_dict[f'{key}'] = value
            
            if messaging:
                logger.debug(f'Loaded {key} ({len(value):,} records)')

    if return_dict:
        return output_dict