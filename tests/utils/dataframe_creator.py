


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


    def __init__(self, input_var):

        with open("/Users/joao.nisa/Desktop/Projects/tdd-pipeline/tests/utils/dtypes_map.json") as jsonFile:
            jsonObject = json.load(jsonFile)
            jsonFile.close()
        
        self.query_data = input_var
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

        self.createSchema()
        self.createSparkDF()


    
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

        df = spark.createDataFrame(data=self.data, schema=self.schema)

        return None

