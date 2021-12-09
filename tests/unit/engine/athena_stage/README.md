## **TDD Pipeline - How to Interact with the Athena Stage**

To perform multiple unitary tests, in order to get bullet-proof functions, it's important to cover data exceptions and also to test the functions against different data.

In order to achieve the previously mentioned point it's important to take into consideration the following points:

1. First of all, it's important to create a folder under the `athena_stage` with the following name convention `test_[function_tested]`;

2. Under the directory created previously, we are going to create multiple test cases and each one must be within a different folder;

3. First thing to be created under a new test case is the query that we want run on athena in order to get test. It's also important to mention that this is what is going to differ between the multiple test cases;

4. After creating the athena query, run the script `utils/get_data_sample.py`. This script, will generate two files under the test case created: one will be data correspondent to the query defined, and the other one will be the schema of that same data;

5. Copy the files obtained from the previous point to create the expected scenario (i.e. data that must be obtained after applying the function that is under test scenario) and paste these files within the test case folder from where the files were copied from. The copied files must follow the naming convention defined, so the file that possess the expected data must be called `data_expected.csv` and the one with the schema must be called `data_exp_schema.txt`;

6. After setting up the previous mentioned files, we are in conditions to starting the test procedures to the function that we have developed. For that matter, a python script must be created (that follows the naming convention already mentioned `test_[function_tested].py`) a to create a dataframes needed for the test we can used `dataframe_creator.py` script that possesses a nested function that assures the creation of both result and expected dataframes.
