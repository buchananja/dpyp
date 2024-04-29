'''
the 'read' module contains functionality for reading data of various datatypes
into data pipelines
'''


import pandas as pd
import dpyp as dp
import os
import sqlite3
from sqlite3 import OperationalError
import logging


logger = logging.getLogger(__name__)


class ReadData:
    '''contains functionality for reading data from various file formats'''
    
    
    @staticmethod     
    def read_all_json(path, messaging = True):
        '''loads all json files from directory and assigns to dataframes'''
        
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


    @staticmethod
    def read_all_csv(path, seperator = ',', messaging = True):
        '''loads all csv files from directory and assigns to dataframes'''
        
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


    @staticmethod
    def read_all_xlsx(path, messaging = True):
        '''loads all xlsx files from directory and assigns to dataframes'''
        
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


    @staticmethod
    def read_all_feather(path, messaging = True):
        '''loads all feather files from directory and assigns to dataframes'''
        
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


    @staticmethod
    def read_all_parquet(path, messaging = True):
        '''loads all parquet files from directory and assigns to dataframes'''
        
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


    @staticmethod
    def read_all_pickle(path, messaging = True):
        '''loads all pickle files from directory and assigns to dataframes'''
        
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
        
    
    @staticmethod   
    def read_all_sqlite(path, messaging = True):
        '''loads all tables from sqlite database and assigns to dataframes'''
        
        # TODO: find a less nested method for performing this function
        
        try:
            if dp.check_path_valid(path):
                with sqlite3.connect(path) as conn:
                    with conn.cursor() as cur:
                        table_names = cur.execute('''
                                SELECT name 
                                FROM sqlite_master 
                                WHERE type = 'table';
                            ''').fetchall()
                        
                        data_dictionary = dict()
                        for table_name in table_names:
                            query = f"SELECT * FROM {table_name[0]}"
                            data_dictionary[table_name[0]] = pd.read_sql_query(query, conn)
                            if messaging:
                                logger.debug(
                                    f'read {table_name[0]} '
                                    f'({len(data_dictionary[table_name[0]]):,}' 
                                    'records)'
                                )
                                
                        if not data_dictionary:
                            logger.debug('No files read.')                            
                        return data_dictionary 
                    
        except OperationalError:
            logger.debug('WARNING: Failed to connect to database.')