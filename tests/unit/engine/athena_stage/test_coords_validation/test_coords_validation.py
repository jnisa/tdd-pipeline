


from os import curdir
from os.path import abspath

from app.engine.cloud_stage.cloud_examples import coords_validation
from tests.utils.dataframe_creator import SparkDFCreator

from pyspark.sql import SparkSession
from tests.utils.spark_test_case import SparkTestCase


appName = "Spark Test Pipeline"
master = 'local'
spark = SparkSession.builder.appName(appName).master(master).getOrCreate()



class TestScenarioCoordsValidation(SparkTestCase):

    def test_coords_validation_tc1(self):

        '''
        coords_validation - 1st Test Case Scenario
        Complexity - 1/4
        '''

        ROOT_DIR = "/Users/joao.nisa/Desktop/Projects/tdd-pipeline/tests/unit/engine/athena_stage/test_coords_validation"

        test_folder = "test_case_1"
        data_sample = ["data_sample.csv", "data_schema.txt"]
        data_expected = ["data_expected.csv", "data_exp_schema.txt"]

        print(type(SparkDFCreator(
                "/".join([ROOT_DIR] + [test_folder, data_sample[0]]),
                "/".join([ROOT_DIR] + [test_folder, data_sample[1]]))
        ))
        
        df_result = coords_validation( 
            SparkDFCreator(
                "/".join([ROOT_DIR] + [test_folder, data_sample[0]]),
                "/".join([ROOT_DIR] + [test_folder, data_sample[1]])
            ),
            "geox",
            "geoy"
        )

        df_expected = SparkDFCreator(
            "/".join([ROOT_DIR] + [test_folder, data_expected[0]]),
            "/".join([ROOT_DIR] + [test_folder, data_expected[1]])
        )

        return self.assertDataFrameEqual(df_result, df_expected)