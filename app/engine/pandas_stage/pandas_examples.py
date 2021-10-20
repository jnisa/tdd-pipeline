


import pandas as pd



def drop_nan_records(df):

    '''
    function responsible for removing records with nan values from a pandas dataframe

    :param df: pandas dataframe to be analized
    '''

    return [df.drop(df.iloc[i].to_list(), inplace = True) for i in df.shape[0] if None in df.iloc[i].to_list()]


def schema_approval(df, schema):

    '''
    function responsible for removing records that go against the provided schema 

    :param df: pandas dataframe to be analized
    :param schema: schema of the dataframe provided
    '''

    for i in df.columns.to_list():

        bool_ans = [isinstance(v, schema[i]) for v in df[i].to_list()]
        df.drop([bool_ans.index(b) for b in bool_ans if b == False])

    return df

