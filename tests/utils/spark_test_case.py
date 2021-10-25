


import unittest



from decimal import Decimal
from unittest import TestCase


from pyspark.sql import types as T


class SparkTestCase(TestCase):

    '''
    Custom TestCase fully dedicated to the unit test between pyspark dataframes
    '''

    def __init__(self, methodName=None):

        if methodName:
            unittest.TestCase.__init__(self, methodName)
    

    def assertDataFrameEqual(self, df1, df2, verbose=True, msg=None):
        
        '''
        When two dataframes are equal it means that not only the data but also the metadata is the same
        '''

        df1.cache()
        df2.cache()

        if df1.schema != df2.schema:

            raise self.failureException('DFs schemas are not equal %s : %s' % (df1.schema, df2.schema))

        if any([
            df1.subtract(df2).count() != 0,
            df2.subtract(df1).count() != 0,
        ]):
            df1.show(truncate = False)
            df2.show(truncate = False)

            if verbose:
                print("'{df1} \ {df2}'")
                df1.subtract(df2).show(truncate = False)
                print("'{df2} \ {df1}'")
                df2.subtract(df1).show(truncate = False)

            raise self.failureException('DFs values are not equal %s : %s' %(df1.collect(), df2.collect()))

    
    def assertDictAlmostEqual(self, d1, d2, places=7):

        '''
        Function that guarantees that the keys and values are the same
        Values being compared up to specified precision
        '''

        self.assertListEqual(sorted(d1.keys()), sorted(d2.keys()))

        def loop(l):

            if not l:
                
                return None

            i, j = l[0]
            key1, value1 = i
            key2, value2 = j

            if type(value1) != type(value2):

                raise self.failureException('Values are not equal %s : %s' %(value1, value2, key1))

            elif isinstance(value1, dict):

                self.assertDictAlmostEqual(value1, value2, places)

            elif isinstance(value1, Decimal):

                self.assertAlmostEqual(value1, value2, places)

            else:

                self.assertEqual(value1, value2)

            loop(l[1:])

        items = zip(sorted(d1.items()), sorted(d2.items()))

        loop(items)


    def assertDataFrameAlmostEqual(self, df1, df2, places=7):

        '''
        Asserts two dataframes are equal up to an acceptable precision
        '''

        df1.cache()
        df2.cache()

        if df1.schema != df2.schema:

            raise self.failureException('DFs schemas are not equal %s : %s' %(df1.schema, df2.schema))

        if any([
            df1.subtract(df2).count() != 0,
            df2.subtract(df1).count() != 0,
        ]):
            has_decimals = any([isinstance(f.dataType, T.DecimalType) for f in df1.schema.fields])
            
            if not has_decimals:
                
                df1.show()
                df2.show()
                raise self.failureException('DFs values are not equal %s : %s' %(df1.collect(), df2.collect()))

            for r1, r2 in zip(sorted(df1.collect(), sorted(df2.collect()))):

                self.assertDictAlmostEqual(r1.asDict(), r2.asDict(), places)
