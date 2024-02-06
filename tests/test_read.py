'''
This file tests whether the 'dpypr.read' module is working correctly.
'''


# Dependencies ################################################################
import pytest
import pandas as pd
import dpypr as dp
import warnings

    
# Fixtures ####################################################################
@pytest.fixture
def df_sample():
    df = {
        'Student Number': [
            123456,
            123457,
            123458,
            123459,
            123460,
            123461
        ],
        'Programme School': [
            'school_of_biological_sciences',
            'school_of_natural_science',
            'school_of_engineering',
            'school_of_biological_sciences',
            'school_of_music',
            'school_of_medicine'
        ],
        'Fee Region': [
            'scot',
            'scot',
            'scot',
            'rUK',
            'international',
            'international'
        ],
        'Academic Year': [
            2024,
            2024,
            2024,
            2024,
            2024,
            2024
        ]
    }
    df = pd.DataFrame(df)
    return df


# Tests #######################################################################

# read_all_json ###############################################################
def test_read_all_json_empty_directory(tmp_path):
    '''
    Tests whether empty directory correctly handled by read_all_json.
    '''
    data_dictionary = dp.read_all_json(tmp_path)
    assert data_dictionary == dict()

def test_read_all_json_reads_correct_files(tmp_path, df_sample):
    '''
    Tests that read_all_json correclty reads json files and assigns correct
    name.
    '''
    df = df_sample
    df.to_json(tmp_path/'test.json')
    data_dictionary = dp.read_all_json(tmp_path)
    assert data_dictionary['df_test'].equals(df)
    
def test_read_all_json_reads_correct_multiple_files(tmp_path, df_sample):
    '''
    Tests that read_all_json correclty reads json files and assigns correct
    names when multiple are present and ignores non-json files.
    '''
    # write multiple identical dataframes to temporary path
    df_1 = df_sample
    df_1.to_json(tmp_path/'test_1.json')
    df_2 = df_sample
    df_2.to_json(tmp_path/'test_2.json')
    df_3 = df_sample
    df_3.to_json(tmp_path/'test_3.json')
    df_4 = df_sample
    df_4.to_csv(tmp_path/'filename.txt', sep = '\t', index = False)
    # read all json files in path and test that they have been correctly read
    data_dictionary = dp.read_all_json(tmp_path)
    assert data_dictionary['df_test_1'].equals(df_1)
    assert data_dictionary['df_test_2'].equals(df_2)
    assert data_dictionary['df_test_3'].equals(df_3)
    assert 'df_test_4' not in data_dictionary
    
    
# read_all_csv ################################################################
def test_read_all_csv_empty_directory(tmp_path):
    '''
    Tests whether empty directory correctly handled by read_all_csv.
    '''
    data_dictionary = dp.read_all_csv(tmp_path)
    assert data_dictionary == dict()

def test_read_all_csv_reads_correct_files(tmp_path, df_sample):
    '''
    Tests that read_all_csv correclty reads csv files and assigns correct
    name.
    '''
    df = df_sample
    df.to_csv(tmp_path/'test.csv', index = False)
    data_dictionary = dp.read_all_csv(tmp_path)
    assert data_dictionary['df_test'].equals(df)
    
def test_read_all_csv_reads_correct_multiple_files(tmp_path, df_sample):
    '''
    Tests that read_all_csv correclty reads csv files and assigns correct
    names when multiple are present and ignores non-csv files.
    '''
    # write multiple identical dataframes to temporary path
    df_1 = df_sample
    df_1.to_csv(tmp_path/'test_1.csv', index = False)
    df_2 = df_sample
    df_2.to_csv(tmp_path/'test_2.csv', index = False)
    df_3 = df_sample
    df_3.to_csv(tmp_path/'test_3.csv', index = False)
    df_4 = df_sample
    df_4.to_csv(tmp_path/'filename.txt', sep = '\t', index = False)
    # read all csv files in path and test that they have been correctly read
    data_dictionary = dp.read_all_csv(tmp_path)
    assert data_dictionary['df_test_1'].equals(df_1)
    assert data_dictionary['df_test_2'].equals(df_2)
    assert data_dictionary['df_test_3'].equals(df_3)
    assert 'df_test_4' not in data_dictionary
    

# read_all_xlsx ###############################################################
def test_read_all_xlsx_empty_directory(tmp_path):
    '''
    Tests whether empty directory correctly handled by read_all_xlsx.
    '''
    data_dictionary = dp.read_all_xlsx(tmp_path)
    assert data_dictionary == dict()

def test_read_all_xlsx_reads_correct_files(tmp_path, df_sample):
    '''
    Tests that read_all_xlsx correclty reads xlsx files and assigns correct
    name.
    '''
    df = df_sample
    df.to_excel(tmp_path/'test.xlsx', index = False)
    data_dictionary = dp.read_all_xlsx(tmp_path)
    assert data_dictionary['df_test'].equals(df)
    
def test_read_all_xlsx_reads_correct_multiple_files(tmp_path, df_sample):
    '''
    Tests that read_all_xlsx correclty reads xlsx files and assigns correct
    names when multiple are present and ignores non-xlsx files.
    '''
    # write multiple identical dataframes to temporary path
    df_1 = df_sample
    df_1.to_excel(tmp_path/'test_1.xlsx', index = False)
    df_2 = df_sample
    df_2.to_excel(tmp_path/'test_2.xlsx', index = False)
    df_3 = df_sample
    df_3.to_excel(tmp_path/'test_3.xlsx', index = False)
    df_4 = df_sample
    df_4.to_csv(tmp_path/'filename.txt', sep = '\t', index = False)
    # read all xlsx files in path and test that they have been correctly read
    data_dictionary = dp.read_all_xlsx(tmp_path)
    assert data_dictionary['df_test_1'].equals(df_1)
    assert data_dictionary['df_test_2'].equals(df_2)
    assert data_dictionary['df_test_3'].equals(df_3)
    assert 'df_test_4' not in data_dictionary
    
    
# read_all_feather ############################################################
def test_read_all_feather_empty_directory(tmp_path):
    '''
    Tests whether empty directory correctly handled by read_all_feather.
    '''
    data_dictionary = dp.read_all_feather(tmp_path)
    assert data_dictionary == dict()

def test_read_all_feather_reads_correct_files(tmp_path, df_sample):
    '''
    Tests that read_all_feather correclty reads feather files and assigns correct
    name.
    '''
    df = df_sample
    df.to_feather(tmp_path/'test.feather')
    data_dictionary = dp.read_all_feather(tmp_path)
    assert data_dictionary['df_test'].equals(df)
    
def test_read_all_feather_reads_correct_multiple_files(tmp_path, df_sample):
    '''
    Tests that read_all_feather correclty reads feather files and assigns correct
    names when multiple are present and ignores non-feather files.
    '''
    # write multiple identical dataframes to temporary path
    df_1 = df_sample
    df_1.to_feather(tmp_path/'test_1.feather')
    df_2 = df_sample
    df_2.to_feather(tmp_path/'test_2.feather')
    df_3 = df_sample
    df_3.to_feather(tmp_path/'test_3.feather')
    df_4 = df_sample
    df_4.to_csv(tmp_path/'test_4.txt')
    # read all feather files in path and test that they have been correctly read
    data_dictionary = dp.read_all_feather(tmp_path)
    assert data_dictionary['df_test_1'].equals(df_1)
    assert data_dictionary['df_test_2'].equals(df_2)
    assert data_dictionary['df_test_3'].equals(df_3)
    assert 'df_test_4' not in data_dictionary
    

# read_all_parquet ############################################################
def test_read_all_parquet_empty_directory(tmp_path):
    '''
    Tests whether empty directory correctly handled by read_all_parquet.
    '''
    data_dictionary = dp.read_all_parquet(tmp_path)
    assert data_dictionary == dict()

def test_read_all_parquet_reads_correct_files(tmp_path, df_sample):
    '''
    Tests that read_all_parquet correclty reads parquet files and assigns correct
    name.
    '''
    df = df_sample
    df.to_parquet(tmp_path/'test.parquet')
    data_dictionary = dp.read_all_parquet(tmp_path)
    assert data_dictionary['df_test'].equals(df)
    
def test_read_all_parquet_reads_correct_multiple_files(tmp_path, df_sample):
    '''
    Tests that read_all_parquet correclty reads parquet files and assigns correct
    names when multiple are present and ignores non-parquet files.
    '''
    # write multiple identical dataframes to temporary path
    df_1 = df_sample
    df_1.to_parquet(tmp_path/'test_1.parquet')
    df_2 = df_sample
    df_2.to_parquet(tmp_path/'test_2.parquet')
    df_3 = df_sample
    df_3.to_parquet(tmp_path/'test_3.parquet')
    df_4 = df_sample
    df_4.to_csv(tmp_path/'test_4.txt')
    # read all parquet files in path and test that they have been correctly read
    data_dictionary = dp.read_all_parquet(tmp_path)
    assert data_dictionary['df_test_1'].equals(df_1)
    assert data_dictionary['df_test_2'].equals(df_2)
    assert data_dictionary['df_test_3'].equals(df_3)
    assert 'df_test_4' not in data_dictionary


# read_all_pickle #############################################################
def test_read_all_pickle_empty_directory(tmp_path):
    '''
    Tests whether empty directory correctly handled by read_all_pickle.
    '''
    data_dictionary = dp.read_all_pickle(tmp_path)
    assert data_dictionary == dict()

def test_read_all_pickle_reads_correct_files(tmp_path, df_sample):
    '''
    Tests that read_all_pickle correclty reads pickle files and assigns correct
    name.
    '''
    df = df_sample
    df.to_pickle(tmp_path/'test.pickle')
    data_dictionary = dp.read_all_pickle(tmp_path)
    assert data_dictionary['df_test'].equals(df)
    
def test_read_all_pickle_reads_correct_multiple_files(tmp_path, df_sample):
    '''
    Tests that read_all_pickle correclty reads pickle files and assigns correct
    names when multiple are present and ignores non-pickle files.
    '''
    # write multiple identical dataframes to temporary path
    df_1 = df_sample
    df_1.to_pickle(tmp_path/'test_1.pickle')
    df_2 = df_sample
    df_2.to_pickle(tmp_path/'test_2.pickle')
    df_3 = df_sample
    df_3.to_pickle(tmp_path/'test_3.pickle')
    df_4 = df_sample
    df_4.to_csv(tmp_path/'test_4.txt')
    # read all pickle files in path and test that they have been correctly read
    data_dictionary = dp.read_all_pickle(tmp_path)
    assert data_dictionary['df_test_1'].equals(df_1)
    assert data_dictionary['df_test_2'].equals(df_2)
    assert data_dictionary['df_test_3'].equals(df_3)
    assert 'df_test_4' not in data_dictionary