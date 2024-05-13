'''
the 'diagnose' module contains functionality for monitoring data pipelines and
retriving useful information
'''


import os
import logging
import sqlite3
import importlib
import pandas as pd
from typing import List
from datetime import datetime


logger = logging.getLogger(__name__)


class GetInfo:
    '''contains functionality for returning information about data'''
    
    
    @staticmethod
    def fetch_all_sqlite_tables(path: str) -> list:
        '''returns all table names present in a sqlite database as a list'''
        
        # connects to database
        table_list = list()
        with sqlite3.connect(path) as conn:
            with conn.cursor()as cur: 
                table_names = cur.execute('''
                    SELECT name 
                    FROM sqlite_master 
                    WHERE type = 'table';
                ''').fetchall()
                
                for table in table_names:
                    table_list.append(table[0])
                    
        return table_list
        

    @staticmethod 
    def fetch_all_global_df(globals_dict: dict) -> list:
        '''returns all objects beginning with 'df_' in global space as list'''
        
        list_df = list()
        for name in globals_dict:
            if name.startswith('df_'):
                list_df.append(name)
        return list_df


    @staticmethod
    def get_last_modified_date(
        path: str, 
        formatting: str = '%Y/%m/%d, %H:%M'
    ) -> str:
        '''
        returns most recent modified date from path with formatting'''
        
        files = os.listdir(path)
        
        # gets last modified date for all files in path
        modified_dates = []
        for file in files:
            full_file_path = os.path.join(path, file) 
            modified_date = datetime.fromtimestamp(
                os.path.getmtime(full_file_path)
            )
            modified_dates.append(modified_date)
            
        recent_date = max(modified_dates).strftime(formatting)
        return recent_date


    @staticmethod
    def check_path_valid(path: str) -> bool:
        '''returns True if path exists and False if not'''
        
        if os.path.exists(path):
            return True
        return False


    @staticmethod
    def check_column_nulls(df: pd.DataFrame) -> None:
        '''logs columns containing null values in dataframe'''

        for col in df.columns:
            if df[col].isna().any():
                logger.debug(col)
                
    
    @staticmethod
    def get_module_names(
        path: str, 
        exclude : List[str] = ['__pycache__', '__init__.py']
    ) -> List[str]:
        '''returns list of all module names in path without extension'''
        
        files = os.listdir(path)
        exclude = ['__pycache__', '__init__.py']
        modules = [f'modules.{file[:-3]}' for file in files if file not in exclude]
        return modules


    @staticmethod
    def import_loggers(path: str, modules_folder: str) -> None:
        '''import loggers from a modules/ directory into main'''
        
        snp_loggers = list()
        modules_dir = os.path.join(path, modules_folder)
        for module_name in GetInfo.get_module_names(modules_dir):
            module = importlib.import_module(module_name)
            logger = getattr(module, 'logger', None)
            if logger is not None:
                snp_loggers.append(logger)
        return snp_loggers