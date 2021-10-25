


def remove_nan_records(df):

    '''
    function that removes all the records that possess any nan values on them

    :param df: spark dataframe that will be scanned in order to search nan records  
    '''


    return df.na.drop("any")


def validate_geo_coordinates(df):

    '''
    function that excludes all the records if their geographical coordinates are not valid

    :param df: spark dataframe that will be scanned in order to search for invalid geographical coordinates
    '''

    records_to_delete = df.filter((df.Latitude > 90) | (df.Latitude < -90) | (df.Longitude > 90) | (df.Longitude < -90))

    return df.join(records_to_delete, on=["ID"], how='left_anti')