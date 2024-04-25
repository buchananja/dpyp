'''this file tests whether the 'dpypr.calculate' module is working correctly'''


import pytest
import pandas as pd
import numpy as np
import dpyp as dp


@pytest.fixture
def df_sample():
    '''basic example of 'student number planning'-style data'''
    
    df = {
        'Programme School': [
            'school_of_biological_sciences',
            'school_of_natural_science',
            'school_of_engineering',
            'school_of_biological_sciences',
            'school_of_music',
            'school_of_medicine',
            'school_of_art'
        ],
        'Population_1': [
            13,
            40,
            23,
            19,
            5,
            10,
            np.nan
        ],
        'Population_2': [
            12,
            50,
            33,
            20,
            4,
            11,
            1
        ],
    }
    df = pd.DataFrame(df)
    return df


# row_rate_of_change ##########################################################
def test_row_rate_of_change_correct(df_sample):
    '''tests row_rate_of_change calculates correct value by row'''
    
    assert dp.row_rate_of_change(
        df_sample.iloc[0], 'Population_1', 'Population_2', 0
    ) == 0.07692307692307693
    

def test_row_rate_of_change_lambda(df_sample):
    '''tests row_rate_of_change returns dvalue between -1 and 1 for each row'''
    
    result = df_sample.apply(
        lambda row: dp.row_rate_of_change(
            row, 'Population_1', 'Population_2', 0
        ), 
        axis = 1
    )
    assert all(-1 <= value <= 1 for value in result)
    

# col_rate_of_change ##########################################################
def test_col_rate_of_change_correct(df_sample):
    '''
    tests col_rate_of_change calculates correct value-range with correct name
    and correct default rate filled into null values
    '''
    
    df = dp.col_rate_of_change(
        df = df_sample, 
        col_name = 'rate_of_change', 
        col1 = 'Population_1', 
        col2 = 'Population_2', 
        default_rate = 0
    )
    assert all(-1 <= row <= 1 for row in df['rate_of_change'])
    
    
# row_product #################################################################
def test_row_product_correct(df_sample):
    '''tests row_products calculates correct value by row'''
    
    assert dp.row_product(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 156
    
    
# col_product #################################################################
def test_col_product_correct(df_sample):
    '''tests col_products calculates correct values, including nulls'''
    
    df_test = dp.col_product(df_sample, 'Product', 'Population_1', 'Population_2')
    expected_series = pd.Series(
        [156, 2000, 759, 380, 20, 110, np.nan], 
        name = 'Product'
    )
    assert df_test['Product'].equals(expected_series)
    
    
# row_percentage ##############################################################
def test_row_percentage_correct(df_sample):
    '''tests row_percentage outputs the correct value'''
    
    assert dp.row_percentage(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 108
    

def test_row_percentage_decimal_points(df_sample):
    '''tests row_percentage outputs the correct decimal points'''
    
    assert dp.row_percentage(
        df_sample.iloc[0], 'Population_1', 'Population_2', dec_points = 1
    ) == 108.3
    
    assert dp.row_percentage(
        df_sample.iloc[0], 'Population_1', 'Population_2', dec_points = 2
    ) == 108.33
    
    
# col_percentage ##############################################################
def test_col_percentage_correct(df_sample):
    '''tests col_percentage calculates correct values, including nulls'''
    
    df_test = dp.col_percentage(
        df_sample, 'Percentage', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [108.0, 80.0, 70.0, 95.0, 125.0, 91.0, np.nan], 
        name = 'Percentage'
    )
    assert df_test['Percentage'].equals(expected_series)
    

# row_modulo ##################################################################
def test_row_modulo_correct(df_sample):
    '''tests row_modulo outputs the correct value'''
    
    assert dp.row_modulo(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 1.0
    

# col_modulo ##################################################################
def test_col_modulo_correct(df_sample):
    '''tests col_modulo calculates correct values, including nulls'''
    
    df_test = dp.col_modulo(
        df_sample, 'Modulo', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [1.0, 40.0, 23.0, 19.0, 1.0, 10.0, np.nan], 
        name = 'Modulo'
    )
    assert df_test['Modulo'].equals(expected_series)
    
    
# row_subrtaction #############################################################
def test_row_subtraction_correct(df_sample):
    '''tests row_subtraction outputs the correct value'''
    
    assert dp.row_subtraction(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 1.0
    
    
# col_subrtaction #############################################################
def test_col_subtraction_correct(df_sample):
    '''tests col_substraction calculates correct values, including nulls'''
    
    df_test = dp.col_subtraction(
        df_sample, 'Subtraction', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [1.0, -10.0, -10.0, -1.0, 1.0, -1.0, np.nan], 
        name = 'Subtraction'
    )
    assert df_test['Subtraction'].equals(expected_series)
    
    
# row_addition ################################################################
def test_row_addition_correct(df_sample):
    '''tests row_addition outputs the correct value'''
    
    assert dp.row_addition(
        df_sample.iloc[0], 'Population_1', 'Population_2',
    ) == 25
    
    
# col_addition ################################################################
def test_col_addition_correct(df_sample):
    '''tests col_substraction calculates correct values, including nulls'''
    
    df_test = dp.col_addition(
        df_sample, 'Addition', 'Population_1', 'Population_2'
    )
    expected_series = pd.Series(
        [25.0, 90.0, 56.0, 39.0, 9.0, 21.0, np.nan], 
        name = 'Addition'
    )
    assert df_test['Addition'].equals(expected_series)