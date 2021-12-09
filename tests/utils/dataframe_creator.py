


import ast
import csv
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ShortType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType
)
from pyspark.sql.functions import col


appName = 'PySpark Dataframe Creator'
master = 'local'
spark = SparkSession.builder.appName(appName).master(master).getOrCreate()


def SparkDFCreator(data_path, schema_path, json_map):

    '''
    creates a pyspark dataframe from data stored on a csv and with schema mapped from hive to spark data types

    :param data_path: path to the csv file where the data is stored
    :param schema_path: path to the file that holds the schema that must be applied to the dataframe
    :param json_map: json used to map data types between hive and spark
    '''


    with open(json_map) as jsonFile:
        hive_to_spark = json.load(jsonFile)
        jsonFile.close()
    
    dispatcher = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "ShortType": ShortType(),
        "LongType": LongType(),
        "DoubleType": DoubleType(),
        "BooleanType": BooleanType(),
        "DateType": DateType(),
        "TimestampType": TimestampType()
    }


    def dataFromCSV(csv_path):

        '''
        reads a csv file into a python datatype more friendly to be used on the creation 
        process of a pyspark dataframe

        :param csv_path: file where the data samples can be found
        '''

        with open(csv_path, 'r') as data_file:
            data = list(csv.DictReader(data_file, delimiter = ","))

        data_file.close()

        return data


    def schemaFromTxt(schema_path):

        '''
        reads a txt file and extracts the schema from that file and converts it into a 
        python dictionary

        :param schema_path: file that specifies the schema of the data sample collected from the cloud
        '''

        with open(schema_path, 'r') as schema_file:
            schema_extracted = ast.literal_eval(schema_file.read())

        schema_file.close()

        return schema_extracted


    def createSparkDF(df_data):

        '''
        create a spark dataframe by iterating over each row from the athena query

        :param df_data: is the variable that comprises all the data that should be on the data sample dataframe 
        '''

        df = spark.createDataFrame(data=df_data)

        return df


    def applySchema(df, schema, dispatcher, data_types_map):

        '''
        applies the schema from the metadata produced by the athena query 
        to the produced dataframe

        :param df: dataframe that comprises all the collected data from the cloud environment
        :param schema: schema that will be applied to the already created dataframe
        :param dispatcher: this variable allows the convertion between string to the data type function
        :param data_types_map: variables that handles the map procedure between hive and spark data types
        '''

        for k in schema:

            df = df.withColumn(
                k, 
                col(k).cast(dispatcher[data_types_map[schema[k]]]) 
            )

        return df

    data_sample = dataFromCSV(data_path)
    data_schema = schemaFromTxt(schema_path)
    df_raw = createSparkDF(data_sample)
    df = applySchema(df_raw, data_schema, dispatcher, hive_to_spark)

    return df
