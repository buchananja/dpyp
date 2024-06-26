'''
the 'write' module contains functionality for writing data of various datatypes
from data pipelines
'''


import pandas as pd
import sqlite3
import os
import logging


logger = logging.getLogger(__name__)


class WriteData:
    '''contains functionality for writing data to various file formats'''
     
    @staticmethod       
    def write_dict_to_json(
        input_dict: dict, 
        path: str, 
        output_prefix: str = 'df',
        messaging: bool = True
    ) -> None:
        '''writes all dataframes in dict as json with modifiable prefix'''

        for name, data in input_dict.items():
            if name.startswith('df_') & isinstance(data, pd.DataFrame):
                data.to_json(f'{path}/{output_prefix}_{name[3:]}.json')
                if messaging:
                    logger.debug(
                        f'wrote {output_prefix}_{name[3:]}'
                        f'({len(data):,} records)'
                    )

    @staticmethod       
    def write_dict_to_csv(
        input_dict: dict, 
        path: str, 
        output_prefix: str = 'df',
        messaging: bool = True
    ) -> None:
        '''writes all dataframes in dict as csv with modifiable prefix'''
        
        for name, data in input_dict.items():
            if name.startswith('df_') & isinstance(data, pd.DataFrame):
                data.to_csv(f'{path}/{output_prefix}_{name[3:]}.csv')
                if messaging:
                    logger.debug(
                        f'wrote {output_prefix}_{name[3:]}'
                        f'({len(data):,} records)'
                    )
        
    @staticmethod       
    def write_dict_to_xlsx(
        input_dict: dict, 
        path: str, 
        output_prefix: str = 'df',
        messaging: bool = True
    ) -> None:
        '''writes all dataframes in dict as xlsx with modifiable prefix'''
        
        for name, data in input_dict.items():
            if name.startswith('df_') & isinstance(data, pd.DataFrame):
                data.to_excel(f'{path}/{output_prefix}_{name[3:]}.xlsx')
                if messaging:
                    logger.debug(
                        f'wrote {output_prefix}_{name[3:]}'
                        f'({len(data):,} records)'
                    )

    @staticmethod       
    def write_dict_to_feather(
        input_dict: dict, 
        path: str, 
        output_prefix: str = 'df',
        messaging: bool = True
    ) -> None:
        '''writes all dataframes in dict as feather with modifiable prefix'''
        
        for name, data in input_dict.items():
            if name.startswith('df_') & isinstance(data, pd.DataFrame):
                data.to_feather(f'{path}/{output_prefix}_{name[3:]}.feather')
                if messaging:
                    logger.debug(
                        f'wrote {output_prefix}_{name[3:]}'
                        f'({len(data):,} records)'
                    )
    
    @staticmethod       
    def write_dict_to_parquet(
        input_dict: dict, 
        path: str, 
        output_prefix: str = 'df',
        messaging: bool = True
    ) -> None:
        '''writes all dataframes in dict as parquet with modifiable prefix'''
        
        for name, data in input_dict.items():
            if name.startswith('df_') & isinstance(data, pd.DataFrame):
                data.to_parquet(f'{path}/{output_prefix}_{name[3:]}.parquet')
                if messaging:
                    logger.debug(
                        f'wrote {output_prefix}_{name[3:]}'
                        f'({len(data):,} records)'
                    )
            
    @staticmethod       
    def write_dict_to_pickle(
        input_dict: dict, 
        path: str, 
        output_prefix: str = 'df',
        messaging: bool = True
    ) -> None:
        '''
        - writes all objects beginning with 'df_' in input_dict to path as 
        pickle
        - prefix allows user to rename processed files upon writing
        '''
        
        for name, data in input_dict.items():
            if name.startswith('df_') & isinstance(data, pd.DataFrame):
                data.to_pickle(f'{path}/{output_prefix}_{name[3:]}.pickle')
                if messaging:
                    logger.debug(
                        f'wrote {output_prefix}_{name[3:]}'
                        f'({len(data):,} records)'
                    )

    # TODO: analyse to different methods and compare
    # @staticmethod                
    # def write_dict_to_sqlite(
    #         input_dict, 
    #         path,
    #         overwrite = False,
    #         output_prefix = 'df',
    #         messaging = True
    #     ):
    #     '''
        # - writes all beginning 'df_' in input_dict to path as tables in database 
        # - prefix allows user to rename processed files upon writing
        # - logs number of records
        # - overwrites table if set
        # - creates database if path does not exist
        # - appends to tables by default
    #     '''
        
    #     if overwrite and os.path.exists(path):
    #         # remove old database
    #         try:
    #             os.remove(path)
    #         except PermissionError:
    #             logger.debug('WARNING: File is being used by another process!')
                    
    #     # connect to database       
    #     conn = sqlite3.connect(path)
    #     if messaging:
    #         logger.debug('Writing data...')
    #     try:           
    #         # create tables in new database
    #         for name, data in input_dict.items():
    #             if name.startswith('df_') & isinstance(data, pd.DataFrame):
    #                 # write DataFrame to new table
    #                 if overwrite:
    #                     data.to_sql(
    #                         f'{output_prefix}_{name[3:]}', 
    #                         conn, 
    #                         if_exists = 'replace'
    #                     )
    #                 else:
    #                     data.to_sql(
    #                         f'{output_prefix}_{name[3:]}', 
    #                         conn, 
    #                         if_exists = 'append'
    #                     )
    #                 if messaging:
    #                     logger.debug(
    #                         f'- wrote {output_prefix}_{name[3:]} '\
    #                         f'({len(data):,} records).'
    #                     )
    #         if messaging:
    #             logger.debug('All data written successfully.') 
    #     except Exception as e:
    #         logger.debug(f'WARNING: {str(e)}')
    #     conn.close()

    @staticmethod       
    def write_dict_to_sqlite(
        input_dict: dict,
        path: str, 
        overwrite: bool = False, 
        messaging: bool = True
    ) -> None:
        '''
        - overwrites table if set
        - creates database if path does not exist
        - appends to tables by default
        '''
        
        # deletes old table
        if overwrite and os.path.exists(path):
            try:
                os.remove(path)
            except PermissionError:
                logger.debug('WARNING: File is being used by another process!')
                
        # writes new table
        with sqlite3.connect(path) as conn:
            try:           
                for name, data in input_dict.items():
                    if isinstance(data, pd.DataFrame):
                        if overwrite:
                            data.to_sql(name, conn, if_exists = 'replace')
                        else:
                            data.to_sql(name, conn, if_exists = 'append')
                        if messaging:
                            logger.debug(
                                f'wrote {name} ({len(data):,} records)'
                            )
            except Exception as e:
                logger.debug(f'WARNING: {str(e)}')