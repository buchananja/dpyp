# Dependencies ################################################################
import time
import logging
import sqlite3


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