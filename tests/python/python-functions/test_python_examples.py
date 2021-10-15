


import unittest as unt



from app.python import filter_odds, string_modifier



class FilterOddsTestScenarios:

    def filter_odds_tc1():

        '''
        filter_odds - 1st Test Scenario
        Complexity - 1/4
        '''

        in_arg = [1, 2, 3, 4, 5, 6, 7]

        result = filter_odds(in_arg)
        expected = [2, 4, 6, 8]

        return unt.assertListEqual(result, expected)



    def filter_odds_tc2():

        '''
        filter_odds - 2nd Test Scenario
        Complexity - 1/4 
        '''

        in_arg = [12, 213, 42, 2, 421, 423]

        result = filter_odds(in_arg)
        expected = [12, 42, 2]

        return unt.assertListEqual(result, expected)




class StringModTestScenarios:

    def string_modifier_tc1():

        '''
        string_modifier - 1st Test Scenario
        Complexity - 1/4
        '''
    
        in_arg = 'today is a good day'

        result = string_modifier(in_arg)
        expected = 'TodayIsAGoodDay'

        return unt.assertMultiLineEqual(result, expected)


    def string_modifier_tc2():

        '''
        string_modifier - 2nd Test Scenario
        Complexity - 1/4
        '''

        in_arg = 'your car. IS. yellow'

        result = string_modifier(in_arg)
        expected = 'YourCar.IS.Yellow'

        return unt.assertMultiLineEqual(result, expected)

