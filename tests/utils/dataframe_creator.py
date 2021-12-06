
'''
TO-DO list:

1. change the values data type according to the schema obtained from the aws cli commands
'''


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

        self.dataFromCSV()
        self.schemaFromTxt()
        self.createSchema()
        df = self.createSparkDF()

        pdb.set_trace()

    
    def createSchema(self):

        '''
        defines the schema from the metadata produced by the athena query
        '''

        self.schema = StructType([
            StructField(k, self.dispatcher[self.hive2spark[self.schema_extracted[k]]])
            for k in self.schema_extracted.keys() 
        ])

        return self


    def createSparkDF(self):

        '''
        create a spark dataframe by iterating over each row from the athena query
        '''

        df = spark.createDataFrame(data=self.data)

        return df


    def dataFromCSV(self):

        '''
        reads a csv file into a python datatype more friendly to be used on the creation 
        process of a pyspark dataframe 
        '''

        with open(self.csv_path, 'r') as data_file:
            self.data = list(csv.DictReader(data_file, delimiter = ","))

        data_file.close()

        return self

    
    def schemaFromTxt(self):

        '''
        reads a txt file and extracts the schema from that file and converts it into a 
        python dictionary
        '''

        with open(self.schema_path, 'r') as schema_file:
            self.schema_extracted = ast.literal_eval(schema_file.read())

        schema_file.close()

        return self



dir = "/Users/joao.nisa/Desktop/Projects/tdd-pipeline/tests/unit/engine/athena_stage/test_coords_validation/test_case_1"
data_file = dir + "/data_sample.csv"
schema_file = dir + "/data_schema.txt"

df_result = SparkDFCreator(data_file, schema_file)