


import unittest
import pandas as pd
from pandas._testing import assert_frame_equal



from app.engine.pandas_stage.pandas_examples import drop_nan_records, schema_approval



class DropNanRecordsTestScenarios(unittest.TestCase):

    def test_drop_nan_records_tc1(self):

        '''
        drop_nan_records - 1st Test Scenario
        Complexity - 1/4
        '''

        data  = {
            'Name': ['Tay', 'Bonito', 'Marques', 'Ricardo'],
            'Age': [23.0, None, 19.0, 50.0],
            'Position': ['Agile Enabler', 'Junior Data Engineer', 'DevOps', 'Data Engineer']
        }

        expected = {
            'Name': ['Tay', 'Marques', 'Ricardo'],
            'Age': [23.0, 19.0, 50.0],
            'Position': ['Agile Enabler', 'DevOps', 'Data Engineer']
        }


        df = pd.DataFrame(data)
        df_result = drop_nan_records(df).reset_index(drop = True)
        df_expected = pd.DataFrame(expected).reset_index(drop = True)

        return assert_frame_equal(df_result, df_expected)

    
    def test_drop_nan_records_tc2(self):

        '''
        drop_nan_records - 2nd Test Scenario
        Complexity - 2/4
        '''

        data = {
            'Name': ['Tay', 'Bonito', 'Marques', None],
            'Age': [23.0, None, 19.0, 50.0],
            'Position': ['Agile Enabler', 'Junior Data Engineer', None, 'Data Engineer']
        }

        expected = {
            'Name': ['Tay'],
            'Age': [23.0],
            'Position': ['Agile Enabler']
        }


        df = pd.DataFrame(data)
        df_result = drop_nan_records(df).reset_index(drop = True)
        df_expected = pd.DataFrame(expected).reset_index(drop = True)

        return assert_frame_equal(df_result, df_expected)



class SchemaApprovalTestScenarios(unittest.TestCase):

    def test_schema_approval_tc1(self):

        '''
        schema_approval: 1st Test Scenario
        Complexity - 1/4
        '''

        data = {
            'Name': ['Tay', 'Bonito', 'Marques', 'Ricardo'],
            'Age': [23.0, '18', 19.0, 50.0],
            'Position': ['Agile Enabler', 'Junior Data Engineer', 'DevOps', 'Data Engineer']
        }

        expected = {
            'Name': ['Tay', 'Marques', 'Ricardo'],
            'Age': [23.0, 19.0, 50.0],
            'Position': ['Agile Enabler', 'DevOps', 'Data Engineer']
        }

        schema = {
            'Name': str,
            'Age': float,
            'Position': str
        }

        df = pd.DataFrame(data)
        df_result = schema_approval(df, schema).reset_index(drop = True)
        df_expected = pd.DataFrame(expected).reset_index(drop = True)

        return assert_frame_equal(df_result, df_expected)


    def test_schema_approval_tc2(self):

        '''
        schema_approval: 2nd Test Scenario
        Complexity - 2/4
        '''

        data = {
            'Name': [1, 'Bonito', 'Marques', 'Ricardo'],
            'Age': [23, '18', 19, 50],
            'Position': ['Agile Enabler', 'Junior Data Engineer', 1, 'Data Engineer']
        }

        expected = {
            'Name': ['Ricardo'],
            'Age': [50],
            'Position': ['Data Engineer']
        }

        schema = {
            'Name': str,
            'Age': int,
            'Position': str
        }

        df = pd.DataFrame(data)
        df_result = schema_approval(df, schema).reset_index(drop = True)
        df_expected = pd.DataFrame(expected).reset_index(drop = True)

        return assert_frame_equal(df_result, df_expected)
