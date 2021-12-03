
'''
TO-DO list:

1. test the pipeline with bigger queries to study if it is necessary to adjust the sleeping times
    if yes:
        try to define the sleeping depeding on the number of records (probably this data can be obtained with another
        aws cli command)
    if no:
        keep it as it is

2. change some the variables present in this file to configuration files 

3. try to adapt a similar pipeline as this one to kafka

4. add some tests in scala as well

5. readjust the way that the spark dataframe is written on a text file
    a function to convert an output from the aws cli command into a pyspark dataframe needs to be created
    save the schema in another file (https://stackoverflow.com/questions/56707401/save-schema-of-dataframe-in-s3-location)
'''


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
        self.getDataFromQuery()
        self.writeDataSamples()

    
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


    def writeDataSamples(self):

        '''
        creates a file with the data samples retrieved from the queries defined
        '''

        dirs_lst = ['/'.join(f.split('/')[:-1]) for f in self.files_lst]

        for dir in dirs_lst:
            with open('/'.join([dir, 'data_sample.txt']), 'w') as convert_file:
                convert_file.write(json.dumps(list(self.data_store.values())[dirs_lst.index(dir)].decode("utf-8")))

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

        for pid in list(self.execution_map.values()):
            time.sleep(0.5)
            data = subprocess.check_output(
                'aws athena get-query-results --query-execution-id %s' %(pid),
                shell=True
            )
            self.data_store[pid] = data
            time.sleep(0.5)

        return self



# definition of the full path 
ROOT_DIR = abspath(curdir)
proj_path = ['tests', 'unit', 'engine', 'athena_stage']
full_path = '/'.join([ROOT_DIR, '/'.join(proj_path)])

# definition of the query output bucket location (configuration or text file)
bucket_out = "s3://aws-athena-query-results-854474629353-eu-west-1/"


# call class functions
ds = getDataSample(full_path, bucket_out)