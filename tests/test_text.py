'''this file tests whether the 'dpypr.text' module is working correctly'''


import pytest
import dpyp as dp
from io import StringIO


@pytest.fixture
def text_sample():
    '''
    basic example of web-scraped text for parsing into a dataframe
    source: https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/braemardata.txt
    '''
    
    return '''
        Braemar
        Location: 315200E 791400N, 339 metres amsl (1959 to Apr 2005), 
        & 315200E 791900N, Lat 57.006 Lon -3.396, 327 metres amsl (Aug 2005 onwards)
        Estimated data is marked with a * after the value.
        Missing data (more than 2 days missing in month) is marked by  ---.
        Sunshine data taken from an automatic Kipp & Zonen sensor marked with a #, otherwise sunshine data taken from a Campbell Stokes recorder.
        yyyy  mm   tmax    tmin      af    rain     sun
                    degC    degC    days      mm   hours
        1959   1    1.7    -5.7      27     ---    34.2
        1959   2    6.2    -3.2      15     ---    68.6
        1959   3    7.6     0.8       7     ---    80.9
        1959   4    ---     ---     ---     ---   105.0
        1959   5   15.6     4.6       1     ---   182.6
        1959   6   16.4     7.2       0     ---   164.8
        1959   7   18.0     8.5       0     ---   147.4
        1959   8   18.6    10.1       0     ---   106.3
        1959   9   17.0     5.4       1     ---   148.2
        1959  10   13.2     4.8       3     ---    87.8
        1959  11    7.0     1.3       8     ---    26.6
        1959  12    4.7    -0.6      14     ---     8.2
        1960   1    3.8    -2.4      21     ---    28.5
        1960   2    3.5    -5.6      23     ---    79.4
        1960   3    5.7     0.0      11     ---    50.9
    '''


@pytest.fixture
def month_sample():
    '''
    examples of string-formatted months with fomatting errors and variation
    '''
    
    return [
        'JAN',
        'feb',
        'mAr',
        'april',
        'MAY',
        'J3ne',
        'julyy',
        'august',
        'september',
        'Octobe',
        'nov-15-21',
        'dec',
        '111',
        '222',
        '---',
        '',
        'egg'
    ]
    

# get_month_numeric ###########################################################
def test_get_month_numeric_correct_month():
    '''tests that get_month_numeric returns correct numbers for months'''
    
    assert dp.get_month_numeric('jan') == 1
    assert dp.get_month_numeric('dec') == 12
    assert dp.get_month_numeric('Jul') == 7
    assert dp.get_month_numeric('MAY') == 5
    
    
def test_get_month_numeric_invalid_month():
    '''tests that get_month_numeric raises value error for invalid months'''
    
    with pytest.raises(ValueError):
        dp.get_month_numeric('abc')
    with pytest.raises(ValueError):
        dp.get_month_numeric('')
    with pytest.raises(ValueError):
        dp.get_month_numeric('---')
        
        
def test_get_month_numeric_numeric_input_month():
    '''tests that get_month_numeric raises value error for numeric inputs'''
    
    with pytest.raises(ValueError):
        dp.get_month_numeric('123')
    
    
def test_get_month_numeric_correct_prefixes(month_sample):
    '''
    tests that get_month_numeric returns the correct month numbers and 
    correctly raises errors for invalid months
    '''
    
    month_dict = {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr': 4,
        'may': 5,
        'jun': 6,
        'jul': 7,
        'aug': 8,
        'sep': 9,
        'oct': 10,
        'nov': 11,
        'dec': 12
    }
    pass_count = 0
    error_count = 0
    for month in month_sample:
        if month[:3].lower() in month_dict.keys():
            assert dp.get_month_numeric(month) == month_dict[month[:3].lower()]
            pass_count += 1
        else:
            with pytest.raises(ValueError, match = f"Invalid month string: {month}"):
                dp.get_month_numeric(month)
            error_count += 1
        
    assert pass_count == 11
    assert error_count == 6
    
    
# remove_trailing_char ########################################################
def test_remove_trailing_char_correct_char():
    '''
    tests that remove_trailing_char removes specified character at correct index
    '''
    
    assert dp.remove_trailing_char('braemar,', char = ',') == 'braemar'
    assert dp.remove_trailing_char('global_', char = '_') == 'global' 
    assert dp.remove_trailing_char('phrase**', char = '*') == 'phrase*'
    assert dp.remove_trailing_char('*phrase', char = '*') != 'phrase'
    assert dp.remove_trailing_char('1111^', char = '^') == '1111'
    assert dp.remove_trailing_char('^', char = '^') == ''


# remove_trailing_char ########################################################
def test_remove_leading_char_correct_char():
    '''
    tests that remove_leading_char removes specified character at correct index
    '''
    
    assert dp.remove_leading_char('braemar,', char = ',') != 'braemar'
    assert dp.remove_leading_char('global_', char = '_') != 'global' 
    assert dp.remove_leading_char('phrase**', char = '*') != 'phrase*'
    assert dp.remove_leading_char('*phrase', char = '*') == 'phrase'
    assert dp.remove_leading_char('1111^', char = '^') != '1111'
    assert dp.remove_leading_char('^', char = '^') == ''
    
    
# replace_consecutive_whitespace ##################################################
def test_replace_consecutive_whitespace_replace_correctly():
    '''tests that replace_consecutive_whitespace replaces whitespace correctly'''
    
    assert dp.replace_consecutive_whitespace('last     night', '-') == 'last-night'
    assert dp.replace_consecutive_whitespace('    ', '') == ''
    assert dp.replace_consecutive_whitespace('    ', '&&&') == '&&&'
    assert dp.replace_consecutive_whitespace('0 1 2 3 4', '') == '01234'
    assert dp.replace_consecutive_whitespace('0755 9878', '-') == '0755-9878'
    

def test_replace_consecutive_whitespace_tabs():
    '''tests that replace_consecutive_whitespace ignores tabs when specified'''
    
    assert dp.replace_consecutive_whitespace(
        'hello\tworld', '_'
    ) == 'hello_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\t\t\tworld', '_'
    ) == 'hello_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\tworld', '_', ignore_tab = True
    ) == 'hello\tworld'
    
    assert dp.replace_consecutive_whitespace(
        '\thello\tworld\t', '_', ignore_tab = True
    ) == '\thello\tworld\t'
    

def test_replace_consecutive_whitespace_returns():
    '''tests that replace_consecutive_whitespace ignores returns when specified'''
    
    assert dp.replace_consecutive_whitespace(
        'hello\nworld', '_'
    ) == 'hello_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\n\n\nworld', '_'
    ) == 'hello_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\nworld', '_', ignore_return = True
    ) == 'hello\nworld'
    
    assert dp.replace_consecutive_whitespace(
        '\nhello\nworld\n', '_', ignore_return = True
    ) == '\nhello\nworld\n'
    
    
def test_replace_consecutive_whitespace_carriage_returns():
    '''
    tests that replace_consecutive_whitespace ignores carriage returns when 
    specified
    '''
    
    assert dp.replace_consecutive_whitespace(
        'hello\rworld', '_'
    ) == 'hello_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\r\r\rworld', '_'
    ) == 'hello_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\rworld', '_', ignore_carriage_return = True
    ) == 'hello\rworld'
    
    assert dp.replace_consecutive_whitespace(
        '\rhello\rworld\r', '_', ignore_carriage_return = True
    ) == '\rhello\rworld\r'
    
    
def test_replace_consecutive_whitespace_combination_whitespace():
    '''
    tests that replace_consecutive_whitespace ignores combined whitespace when 
    specified (tabs & returns)
    '''
    
    assert dp.replace_consecutive_whitespace(
        'hello\r\n\tworld', '_'
    ) == 'hello_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\r\tworld', '_', ignore_carriage_return = True
    ) == 'hello\r_world'
    
    assert dp.replace_consecutive_whitespace(
        'hello\r\t\nworld', '_', ignore_tab = True
    ) == 'hello_\t_world'