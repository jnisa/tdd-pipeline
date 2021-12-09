

import pdb
import json
from os import curdir
from os.path import abspath

from app.engine.athena_stage.cloud_examples import coords_validation
from tests.utils.dataframe_creator import SparkDFCreator

from pyspark.sql import SparkSession
from tests.utils.spark_test_case import SparkTestCase


appName = "Spark Test Pipeline"
master = 'local'
spark = SparkSession.builder.appName(appName).master(master).getOrCreate()



class TestScenarioCoordsValidation(SparkTestCase):

    def test_init(self):

        ROOT_DIR = abspath(curdir)

        with open("/".join([ROOT_DIR] + ["tests", "utils", "config_files", "config_file.json"])) as jsonFile:
            self.configs = json.load(jsonFile)
            jsonFile.close()

        self.dt_map = "/".join([ROOT_DIR] + self.configs['map_file'])
        self.data_prefix = [ROOT_DIR] + self.configs["test_path"] + ["CASE_ID"]

        return self

    
    def test_coords_validation_tc1(self):

        '''
        coords_validation - 1st Test Case Scenario
        Complexity - 1/4
        '''

        self.test_init()

        case_id = "test_case_1"

        df_result = coords_validation( 
            SparkDFCreator(
                "/".join(self.data_prefix + [self.configs['data_files'][0]]).replace("CASE_ID", case_id),
                "/".join(self.data_prefix + [self.configs['data_files'][1]]).replace("CASE_ID", case_id),
                self.dt_map
            ),
            "geox",
            "geoy"
        )

        df_expected = SparkDFCreator(
            "/".join(self.data_prefix + [self.configs['data_files'][2]]).replace("CASE_ID", case_id),
            "/".join(self.data_prefix + [self.configs['data_files'][3]]).replace("CASE_ID", case_id),
            self.dt_map
        )

        return self.assertDataFrameEqual(df_result, df_expected)
