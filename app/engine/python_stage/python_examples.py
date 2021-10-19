


def filter_odds(in_list):

    '''
    function that retrieves only the even numbers from a provided list

    :param in_list: list that possesses all a set of numbers to be filtered 
    '''

    return [i for i in in_list if i % 2 == 0]



def string_modifier(in_str):

    '''
    function that from a received string filters out all the spaces between all the words 
    convert the first characters of each word to a major characters and joins it

    :param in_str: provided setence full of words separated by blank spaces
    '''

    ans_list = []

    for i in in_str.split(' '):

        split_word = list(i)
        split_word[0] = split_word[0].upper()
        ans_list.append("".join(split_word))
    

    return ''.join(ans_list)
