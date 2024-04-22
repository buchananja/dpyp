'''
the 'text' module contains functionality for parsing text
text
'''

import re
import logging


# creates logging instance
logger = logging.getLogger(__name__)


def get_month_numeric(month_str):
    '''takes string month name and returns numeric calender position'''

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
    if month_str[:3].lower() in month_dict:
        return month_dict[month_str]
    else:
        raise ValueError(f"Invalid month string: {month_str}")


def remove_trailing_char(line, trailing_char):
    '''returns line with trailing character removed'''

    if line.endswith(trailing_char):
        line = line[:-1]
    return line


def remove_leading_char(line, leading_char):
    '''returns line with leading character removed'''

    if line.startswith(leading_char):
        line = line[1:]
    return line


def replace_consecutive_spaces(line, replacement_char):
    '''returns line with all consecutive whitespace replaced with character'''
    
    line = re.sub(r'[^\S\r\n]+', replacement_char, line)
    return line


def get_index_text(phrase, split_char, index):
    '''gets text from index in phrase split by character'''

    text = phrase.strip().split(split_char)[index]
    return text


def get_text_numerics(phrase, split_char, *indexes):
    '''
    takes a phrase and list of indexes to extract numbers from; appends numbers
    to a list and returns
    '''

    if all([isinstance(index, int) for index in indexes]):      
        numbers = list()
        for index in indexes:
            try:
                number = ''.join(filter(str.isdigit, phrase.split(split_char)[index]))
                numbers.append(number)
            except IndexError:
                print('Index out of bounds')
    else:
        print('Index not intiger')
        raise ValueError
    
    return tuple(numbers)


def get_string_numerics(phrase):
    '''extracts numeric digits from a string containing numberic characters'''

    number = ''.join(filter(str.isdigit, phrase))
    return number

    
def get_text_between_indexes(phrase, char_1, char_2):
    '''extracts text within a phrase between two characters'''

    start_index = phrase.index(char_1)
    end_index = phrase.index(char_2)
    extracted_text = phrase[start_index:end_index]
    return extracted_text