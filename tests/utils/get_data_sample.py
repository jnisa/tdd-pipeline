

'''
Commands that work from the aws cli:
. aws athena start-query-execution --query-string "SELECT * FROM man_vehicledatalake_dev_gluedatabase_rio_landing.man_servicecare_workshops_country LIMIT 10" --result-configuration "OutputLocation=s3://aws-athena-query-results-854474629353-eu-west-1/"
. aws athena get-query-results --query-execution-id "dd5c38cb-f6a3-4165-a96f-33881e9f1fcb"
'''


import pdb
import os
from os import listdir
from os.path import isfile, join

import subprocess


class getDataSample:

    def __init__(self, path):

        self.path = path

    
    def listTestCases(self):

        '''
        list all the test cases that comprehend the queries to get the data samples
        '''

        self.files_lst = ['/'.join([f[0], f[2][0]]) for f in os.walk(self.path) if 'query_' in '/'.join([f[0], f[2][0]])]

        return self


    def listQueries(self):

        '''
        list the queries on the files obtained on the listTestCases
        '''

        self.query_lst = []

        for f in self.files_lst:
            self.query_lst.append(open(f, "r").read().replace("\n", " "))

        pdb.set_trace()

        return self


    def writeDataSamples(self):

        '''
        creates a file with the data samples retrieved from the queries defined
        '''

        return None

    
    def queryAWSData(self):

        '''
        query the database and get the query execution id
        '''

        return None


    def getDataFromQuery(self):

        '''
        get the query results into a spark dataframe
        '''

        return None



ROOT_DIR = os.path.abspath(os.curdir)
proj_path = ['tests', 'unit', 'engine', 'cloud_stage']


ds = getDataSample('/'.join([ROOT_DIR, '/'.join(proj_path)]))
ds.listTestCases()
ds.listQueries()