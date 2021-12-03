


import pdb
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


appName = 'Pyspark Dataframe Creator'
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

        self.schemaToDict()
        self.dataToLists()
        self.createSchema()

        pdb.set_trace()

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

        pdb.set_trace()

        return None


    def dataToLists(self):

        '''
        convert a data set from a python dictionary to lists
        '''

        self.data = [
            tuple([
                value['VarCharValue']
                for value in record['Data']
            ])
            for record in self.query_data['ResultSet']['Rows']
        ]

        return self


    def schemaToDict(self):

        '''
        convert the resultant metadata from the athena cli query to a python dictionary
        '''

        self.schema_extracted = dict(
            (c['Name'],
                (c["Type"])
            )
            for c in self.query_data['ResultSet']['ResultSetMetadata']['ColumnInfo']
        )

        return self 



input_var = {
    "ResultSet": {
        "Rows": [
            {
                "Data": [
                    {
                        "VarCharValue": "workshop_id"
                    },
                    {
                        "VarCharValue": "create_timestamp"
                    },
                    {
                        "VarCharValue": "geox"
                    },
                    {
                        "VarCharValue": "geoy"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "FI0008"
                    },
                    {
                        "VarCharValue": "1553230807577"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "NL0024"
                    },
                    {
                        "VarCharValue": "1565067606896"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "GR0008"
                    },
                    {
                        "VarCharValue": "1569906007993"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "ES0086"
                    },
                    {
                        "VarCharValue": "1573534807510"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "AT1167"
                    },
                    {
                        "VarCharValue": "1574744408211"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "FR0085"
                    },
                    {
                        "VarCharValue": "1580792409437"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "PT0041"
                    },
                    {
                        "VarCharValue": "1583298008419"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "SE0025"
                    },
                    {
                        "VarCharValue": "1584594006582"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "IT0358"
                    },
                    {
                        "VarCharValue": "1588568411403"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            },
            {
                "Data": [
                    {
                        "VarCharValue": "NO0070"
                    },
                    {
                        "VarCharValue": "1588741207370"
                    },
                    {
                        "VarCharValue": "0.0"
                    },
                    {
                        "VarCharValue": "0.0"
                    }
                ]
            }
        ],
        "ResultSetMetadata": {
            "ColumnInfo": [
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "workshop_id",
                    "Label": "workshop_id",
                    "Type": "varchar",
                    "Precision": 2147483647,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "create_timestamp",
                    "Label": "create_timestamp",
                    "Type": "bigint",
                    "Precision": 19,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "geox",
                    "Label": "geox",
                    "Type": "double",
                    "Precision": 17,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                },
                {
                    "CatalogName": "hive",
                    "SchemaName": "",
                    "TableName": "",
                    "Name": "geoy",
                    "Label": "geoy",
                    "Type": "double",
                    "Precision": 17,
                    "Scale": 0,
                    "Nullable": "UNKNOWN",
                }
            ]
        }
    },
    "UpdateCount": 0
}

SparkDFCreator(input_var)