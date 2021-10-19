


import os
import logging
import unittest



from app.engine.python_stage.python_examples import filter_odds, string_modifier



class FilterOddsTestScenarios(unittest.TestCase):

    def test_filter_odds_tc1(self):

        '''
        filter_odds - 1st Test Scenario
        Complexity - 1/4
        '''

        in_arg = [1, 2, 3, 4, 5, 6, 7, 8]

        result = filter_odds(in_arg)
        expected = [2, 4, 6, 8]

        return self.assertListEqual(result, expected)



    def test_filter_odds_tc2(self):

        '''
        filter_odds - 2nd Test Scenario
        Complexity - 1/4 
        '''

        in_arg = [12, 213, 42, 2, 421, 423]

        result = filter_odds(in_arg)
        expected = [12, 42, 2]

        return self.assertListEqual(result, expected)




class StringModTestScenarios(unittest.TestCase):

    def test_string_modifier_tc1(self):

        '''
        string_modifier - 1st Test Scenario
        Complexity - 1/4
        '''
    
        in_arg = 'today is a good day'

        result = string_modifier(in_arg)
        expected = 'TodayIsAGoodDay'

        return self.assertMultiLineEqual(result, expected)


    def test_string_modifier_tc2(self):

        '''
        string_modifier - 2nd Test Scenario
        Complexity - 1/4
        '''

        in_arg = 'your car. IS. yellow'

        result = string_modifier(in_arg)
        expected = 'YourCar.IS.Yellow'

        return self.assertMultiLineEqual(result, expected)

