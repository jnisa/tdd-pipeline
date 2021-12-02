

from app.engine.pyspark_stage.pyspark_examples import remove_nan_records, validate_geo_coordinates


from pyspark.sql import SparkSession
from tests.utils.spark_test_case import SparkTestCase


appName = "Spark Test Pipeline"
master = 'local'
spark = SparkSession.builder.appName(appName).master(master).getOrCreate()



class TestScenarioRemoveNanRecords(SparkTestCase):

    def test_remove_nan_records_tc1(self):

        '''
        remove_nan_records - 1st Test Case Scenario
        Complexity - 1/4
        '''

        data = [
            {"Location": 'Lisbon',  "ID": 1, "Latitude": 38.7223,  "Longitude": -9.1393},
            {"Location": None,      "ID": 2, "Latitude": 41.1579,  "Longitude": -8.6291},
            {"Location": 'Coimbra', "ID": 3, "Latitude": 40.2033,  "Longitude": -8.4103}
        ] 

        expected = [
            {"Location": 'Lisbon',  "ID": 1, "Latitude": 38.7223,  "Longitude": -9.1393},
            {"Location": 'Coimbra', "ID": 3, "Latitude": 40.2033,  "Longitude": -8.4103}
        ]

        df = spark.createDataFrame(data)
        df_expected = spark.createDataFrame(expected)
        df_result = remove_nan_records(df)


        return self.assertDataFrameEqual(df_result, df_expected)


    def test_remove_nan_records_tc2(self):

        '''
        remove_nan_records - 2nd Test Case Scenario
        Complexity - 2/4
        '''

        data = [
            {"Location": 'Lisbon',  "ID": 1, "Latitude": None,    "Longitude": -9.1393},
            {"Location": None,      "ID": 2, "Latitude": 41.1579, "Longitude": -8.6291},
            {"Location": 'Coimbra', "ID": 3, "Latitude": 40.2033, "Longitude": -8.4103}
        ] 

        expected = [
            {"Location": 'Coimbra', "ID": 3, "Latitude": 40.2033, "Longitude": -8.4103}
        ]

        df = spark.createDataFrame(data)
        df_expected = spark.createDataFrame(expected)
        df_result = remove_nan_records(df)


        return self.assertDataFrameEqual(df_expected, df_result)



class TestScenarioValidateGeoCoords(SparkTestCase):

    def test_validate_geo_coordinates_tc1(self):

        '''
        validate_geo_coordinates - 1st Test Case Scenario
        Complexity - 1/4
        '''

        data = [
            {"Location": 'Lisbon',  "ID": 1, "Latitude": 38.7223,  "Longitude": -9.1393},
            {"Location": None,      "ID": 2, "Latitude": 41.1579,  "Longitude": -8.6291},
            {"Location": 'Coimbra', "ID": 3, "Latitude": 40.2033,  "Longitude": -8.4103}
        ] 

        expected = [
            {"Location": 'Lisbon',  "ID": 1, "Latitude": 38.7223,  "Longitude": -9.1393},
            {"Location": None,      "ID": 2, "Latitude": 41.1579,  "Longitude": -8.6291},
            {"Location": 'Coimbra', "ID": 3, "Latitude": 40.2033,  "Longitude": -8.4103}
        ]

        df = spark.createDataFrame(data)
        df_expected = spark.createDataFrame(expected)
        df_result = validate_geo_coordinates(df)

        return self.assertDataFrameEqual(df_expected, df_result)

    
    def test_validate_geo_coordinates_tc2(self):

        '''
        validate_geo_coordinates - 2nd Test Case Scenario
        Complexity - 1/4
        '''

        data = [
            {"Location": 'Lisbon',  "ID": 1, "Latitude": -95.823,  "Longitude": -9.1393},
            {"Location": 'Fundao',  "ID": 2, "Latitude": 41.1579,  "Longitude": -8.6291},
            {"Location": 'Coimbra', "ID": 3, "Latitude": 98.234,   "Longitude": -8.4103}
        ] 

        expected = [
            {"Location": 'Fundao',      "ID": 2, "Latitude": 41.1579,  "Longitude": -8.6291}
        ]

        df = spark.createDataFrame(data)
        df_expected = spark.createDataFrame(expected)
        df_result = validate_geo_coordinates(df)

        return self.assertDataFrameEqual(df_expected, df_result)
