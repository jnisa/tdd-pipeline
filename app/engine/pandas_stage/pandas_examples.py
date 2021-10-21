

import math
import pandas as pd



def drop_nan_records(df):

    '''
    function responsible for removing records with nan values from a pandas dataframe

    :param df: pandas dataframe to be analized
    '''

    drop_records = [i for i in range(df.shape[0]) \
        if True in [str(v) == 'nan' or str(v) == 'None' for v in df.iloc[i].to_list()]]

    return df.drop(drop_records, axis = 0)


def schema_approval(df, schema):

    '''
    function responsible for removing records that go against the provided schema 

    :param df: pandas dataframe to be analized
    :param schema: schema of the dataframe provided
    '''

    for i in df.columns.to_list():

        bool_ans = [isinstance(v, schema[i]) for v in df[i].to_list()]

        if not all(bool_ans) == True:
            
            df = df.drop([bool_ans.index(b) for b in bool_ans if b == False]).reset_index(drop = True)

    return df

