


import pyspark.sql.functions as F


def coords_validation(df, coord_x, coord_y):

    '''
    validates the coordinates from the geox and geoy columns

    :param df: dataframe whose geographical coordinates will be validated
    :param coord_x: column id that possesses all the latitude values
    :param coord_y: column id that possesses all the longitude values
    '''    
    
    condition = (F.col(coord_x) > 15.5) | (F.col(coord_y) > 48.5)
    df_ans = df.filter(~condition)

    return df_ans