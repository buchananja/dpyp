'''
the 'diagnose' module contains functionality for monitoring data pipelines and
retriving useful information
'''


import sqlite3
import os
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


def fetch_all_sqlite_tables(path):
    '''returns all table names present in a sqlite database as a list'''
    
    # connects to database
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
    '''returns all objects beginning with 'df_' in global space as a list'''
    
    list_df = list()
    for name in globals_dict:
        if name.startswith('df_'):
            list_df.append(name)
    return list_df


def get_last_modified_date(path, formatting = '%Y/%m/%d, %H:%M'):
    '''
    returns most recent modified date from path directory with formatting'''
    
    # gets list of files in path
    files = os.listdir(path)
    
    # gets last modified date for all files in path
    modified_dates = []
    for file in files:
        full_file_path = os.path.join(path, file) 
        modified_date = datetime.fromtimestamp(os.path.getmtime(full_file_path))
        modified_dates.append(modified_date)
    
    # return formatted most recent date
    return max(modified_dates).strftime(formatting)


def check_path_valid(path):
    '''returns True if path exists and False if not'''
    
    if os.path.exists(path):
        return True
    return False


def check_column_nulls(df):
    '''logs columns containing null values in dataframe'''

    for col in df.columns:
        if df[col].isna().any():
            logger.debug(col)