


def filter_odds(in_list: list) -> list:

    '''
    function that retrieves only the even numbers from a provided list

    :param in_list: list that possesses all a set of numbers to be filtered 
    '''

    return [i for i in in_list if i % 2 == 0]



def string_modifier(in_str: str) -> str:

    '''
    function that from a received string filters out all the spaces between all the words 
    convert the first characters of each word to a major characters and joins it

    :param in_str: provided setence full of words separated by blank spaces
    '''

    ans_list = [i[0].upper() for i in in_str.split('')]

    return ''.join(ans_list)