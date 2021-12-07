


import pdb
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


class SparkDFCreator:


    def __init__(self, csv_path, schema_path):

        with open("/Users/joao.nisa/Desktop/Projects/tdd-pipeline/tests/utils/dtypes_map.json") as jsonFile:
            jsonObject = json.load(jsonFile)
            jsonFile.close()
        
        self.csv_path = csv_path
        self.schema_path = schema_path
        self.hive2spark = jsonObject
        self.dispatcher = {
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
        '''

        with open(csv_path, 'r') as data_file:
            data = list(csv.DictReader(data_file, delimiter = ","))

        data_file.close()

        return data


    def schemaFromTxt(schema_path):

        '''
        reads a txt file and extracts the schema from that file and converts it into a 
        python dictionary
        '''

        with open(schema_path, 'r') as schema_file:
            schema_extracted = ast.literal_eval(schema_file.read())

        schema_file.close()

        return schema_extracted


    def createSparkDF(df_data):

        '''
        create a spark dataframe by iterating over each row from the athena query
        '''

        df = spark.createDataFrame(data=df_data)

        return df


    def applySchema(df, schema):

        '''
        applies the schema from the metadata produced by the athena query 
        to the produced dataframe
        '''

        with open("/Users/joao.nisa/Desktop/Projects/tdd-pipeline/tests/utils/dtypes_map.json") as jsonFile:
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

        for k in schema:

            df = df.withColumn(
                k, 
                col(k).cast(dispatcher[hive_to_spark[schema[k]]]) 
            )

        return df



ROOT_DIR = "/Users/joao.nisa/Desktop/Projects/tdd-pipeline/tests/unit/engine/athena_stage/test_coords_validation"

test_folder = "test_case_1"
data_sample = ["data_sample.csv", "data_schema.txt"]


'''
df = SparkDFCreator(
    "/".join([ROOT_DIR] + [test_folder, data_sample[0]]),
    "/".join([ROOT_DIR] + [test_folder, data_sample[1]])
)
'''

schema = SparkDFCreator.schemaFromTxt(
    "/".join([ROOT_DIR] + [test_folder, data_sample[1]])
)

data = SparkDFCreator.dataFromCSV(
    "/".join([ROOT_DIR] + [test_folder, data_sample[0]])
)

df = SparkDFCreator.createSparkDF(
    data
)

final_df = SparkDFCreator.applySchema(
    df,
    schema
)


pdb.set_trace()