## **TDD Pipeline - How to Interact with the Athena Stage**

To perform multiple unitary tests, in order to get bullet-proof functions, it's important to cover data exceptions and also to test the functions against different data.

In order to achieve the previously mentioned point it's important to take into consideration the following points:

1. First of all, it's important to create a folder under the `athena_stage` with the following name convention `test_[function_tested]`;

2. Under the directory created previously, we are going to create multiple test cases and each one must be within a different folder;

3. First thing to be created under a new test case is the query that we want run on athena in order to get test. It's also important to mention that this is what is going to differ between the multiple test cases;

4. After creating the athena query, run the script `utils/get_data_sample.py`. This script, will generate two files under the test case created: one will be data correspondent to the query defined, and the other one will be the schema of that same data;

5. Copy the files obtained from the previous point to create the expected scenario (i.e. data that must be obtained after applying the function that is under test scenario) and paste these files within the test case folder from where the files were copied from. The copied files must follow the naming convention defined, so the file that possess the expected data must be called `data_expected.csv` and the one with the schema must be called `data_exp_schema.txt`;

6. After setting up the previous mentioned files, we are in conditions to starting the test procedures to the function that we have developed. For that matter, a python script must be created (that follows the naming convention already mentioned `test_[function_tested].py`) a to create a dataframes needed for the test we can used `dataframe_creator.py` script that possesses a nested function that assures the creation of both result and expected dataframes.

**ATTENTION**: It's important to not only adapt the bucket value on the configuration file but also to adapt your local aws credentials to the account where you're extracting data!

### **Example: Interaction with the Cloud Stage of the Test Pipeline**

__Scenario__: Let's suppose that we want to add a new test case to the already existent function `test_coords_validation`, in order to test another test case it's important to take into account the following steps:

- Step 1: create a new folder (under the `test_coords_validation` folder - already existent) for the new test_case, in this specific case the name of the folder must be `test_case_2`;

- Step 2: under the `test_case_2` folder create, create a .txt file with the query to extract the data that will be used on our tests. In this case, it can be something like:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`query_tc_2.txt`
<ul>

````
SELECT workshop_id, create_timestamp, geox, geoy 
FROM man_vehicledatalake_dev_gluedatabase_rio_landing.man_servicecare_workshops_country
WHERE isActive = true 
LIMIT 20
````
</ul>

- Step 3: after creating this file, run the script `get_data_sample.py` which is under the `utils` folder. If everything runned smoothly, two new files must be created under the `test_case_2` folder: `data_sample.csv` and `data_schema.txt`;

- Step 4: copy the `data_sample.csv` and the `data_schema.txt` files (both generated on the previous step) and adjust them, with what must be obtained after applying the function that is under test, to the data sample and schema extracted. __Example__: in this case, the function excludes records whose geo_x value is higher that 15.5 or if the geo_y is higher than 38.5, so records with these specifications must be excluded on the `expected_data.csv` file. **Attention**: if there is any changes on the schema of the resultant table, please adapt the `expected_schema.txt` file accordingly;

- Step 5: after setting up the files, go to the test script (in this case, `test_coords_validation.py`) and recreate the code template of a test scenario to this new case.

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**added code**

<ul>

````
def test_coords_validation_tc2(self):

        '''
        coords_validation - 2nd Test Case Scenario
        Complexity - 1/4
        '''

        self.test_init()

        case_id = "test_case_2"

        df_result = coords_validation( 
            SparkDFCreator(
                "/".join(self.data_prefix + [self.configs['data_files'][0]]).replace("CASE_ID", case_id),
                "/".join(self.data_prefix + [self.configs['data_files'][1]]).replace("CASE_ID", case_id),
                self.dt_map
            ),
            "geox",
            "geoy"
        )

        df_expected = SparkDFCreator(
            "/".join(self.data_prefix + [self.configs['data_files'][2]]).replace("CASE_ID", case_id),
            "/".join(self.data_prefix + [self.configs['data_files'][3]]).replace("CASE_ID", case_id),
            self.dt_map
        )

        return self.assertDataFrameEqual(df_result, df_expected)
````
</ul>

- Step 6: after all these steps, we can test the function throughout the command:

<ul>

````
python -m unittest discover
````
</ul>