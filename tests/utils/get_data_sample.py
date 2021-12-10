


import csv
import ast
import json
import time
import subprocess
from os import walk, curdir
from os.path import abspath


class getDataSample:

    def __init__(self, path, output_bucket):

        self.path = path
        self.output_bucket = output_bucket

        self.listTestCases()
        self.listQueries()
        self.queryAWSData()
        self.getBufferTime()
        self.getDataFromQuery()

        dirs_lst = ['/'.join(f.split('/')[:-1]) for f in self.files_lst]

        for k in self.data_store.keys():
            self.query_data = json.loads(self.data_store[k].decode('utf-8').replace("true", "\"true\""))
            self.dataToLists()
            self.schemaToDict()
            self.writeDataFeatures(dirs_lst[list(self.data_store.keys()).index(k)])

    
    def listTestCases(self):

        '''
        list all the test cases that comprehend the queries to get the data samples
        '''

        self.files_lst = ['/'.join([f[0], f[2][0]]) for f in walk(self.path) if 'query_' in '/'.join([f[0], f[2][0]])]

        return self


    def listQueries(self):

        '''
        list the queries on the files obtained on the listTestCases
        '''

        self.query_lst = []

        for f in self.files_lst:
            self.query_lst.append(open(f, "r").read().replace("\n", " "))

        return self


    def writeDataFeatures(self, dir):

        '''
        creates two files one relative to the data sample from the athena query
        and another one relative to the schema of that data

        :param dir: directory where the data samples and schemas can be found
        '''
    
        with open('/'.join([dir, 'data_sample.csv']), 'w') as data_file:
            writer = csv.writer(data_file)
            writer.writerows(self.data)
        
        with open('/'.join([dir, 'data_schema.txt']), 'w') as schema_file:
            schema_file.write(json.dumps(self.schema_extracted))

        return self

    
    def queryAWSData(self):

        '''
        query the database and get the query execution id
        '''

        self.execution_map = {}

        for q in self.query_lst:
            query_obs = ["aws", "athena", "start-query-execution", 
                "--query-string", "'%s'" %(q), 
                "--result-configuration", "'OutputLocation=%s'" %(self.output_bucket)]
        
            query_execution = subprocess.Popen(" ".join(query_obs), shell=True, stdout=subprocess.PIPE)
            self.execution_map[q] = ast.literal_eval(query_execution.stdout.read().decode('utf-8'))['QueryExecutionId']

        return self


    def getDataFromQuery(self):

        '''
        get the query results into a spark dataframe
        
        Obs: some stopping times were added to adjust the buffer to the query performed
        '''

        self.data_store = {}

        for idx, pid in enumerate(list(self.execution_map.values())):

            time.sleep(self.buffer_times[idx])
            data = subprocess.check_output(
                'aws athena get-query-results --query-execution-id %s' %(pid),
                shell=True
            )
            self.data_store[pid] = data
            time.sleep(self.buffer_times[idx])

        return self


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

    
    def getBufferTime(self):

        '''
        defines the time needed to process the output of the queries performed to athena
        '''

        self.buffer_times = [
            ast.literal_eval(
                subprocess.check_output(
                    'aws athena get-query-execution --query-execution-id %s' %(pid),
                    shell=True
                ).decode('utf-8')
            )['QueryExecution']['Statistics']['TotalExecutionTimeInMillis'] / 1000
            for pid in list(self.execution_map.values())
        ] 

        return self




ROOT_DIR = abspath(curdir)

with open("/".join([ROOT_DIR] + ["tests", "utils", "config_files", "config_file.json"])) as jsonFile:
    configs = json.load(jsonFile)
    jsonFile.close()

ds = getDataSample(
    "/".join([ROOT_DIR] + configs['test_path']),  
    configs['test_coords_validation']['output_bucket']
    )