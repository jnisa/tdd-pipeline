## **TDD Pipeline - Python, Pandas, PySpark, and Athena**

#### **Versions**
[![Generic badge](https://img.shields.io/badge/python-3.8-blue)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/pyspark-3.1.2-blue)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/pandas-1.2.4-blue)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/subprocess-0.0.8-blue)](https://shields.io/)
#### **Coverage**
[![Generic badge](https://img.shields.io/badge/macOS-passing-brightgreen)](https://shields.io/)

### **1. Intro**
TDD (__Test Driven Development__) is a methodology that should be instantiated in any software development environment. 

The reason for this initiative bears on the following dimensions:

- Applying it can reduce the existence of bugs by over 50%; 
- New projects built with the TDD approach had smaller bugs to lines of code ratio than non-TDD ones;
- Can increase the quality of the code;
- Save costs, since we would be performing tests on local environments and by developing test cases with different complexities we would be reducing the number of times that we need to run pipelines on cloud environment;
- Promote code reusability.

Taking this into account, this pipeline was built so that the points mentioned above can be aligned with the scope of this project. In addition to that, it also worth mentioning that this test concept was also combined with the concept of integration tests, since we wanted to have the possibility to cover exceptions that we verify in our data during the development stage.
### **2. Solution's Overview**
The solution defined to address the points highlighted above and to improve our development pipeline gathers our most common use cases of our development path:
- Test path fully dedicated to Python code and divided into the following distinct dimensions: 1) Python general code, 2) pandas use cases, 3) PySpark, and 4) Athena, this involved the creation of a pipeline that extracts data samples from our AWS Athena databases, which can be useful on the creation of Python Glue Jobs and more specifically to test those on your local environment;
- Travis CI to guarantee validation of the code syntax and also to highlighted functions that are not passing to the tests they are submitted.  

### **3. Depedencies/Plugins**
```
pip install pandas
pip install pyspark
pip install subprocess
```

### **4. Project Tree**
- `/app`
This directory is fully dedicated to the code developed in the course of the project. This folder is then divided into three different dimensions as I probably mentioned already:

<ul>

1. `python` - that can be used to test any python function;
2. `pandas` - whenever you want to test any pandas transformation;
3. `pyspark` - whenever you want to test pyspark procedures;
4. `athena`- whenever you want to test your glue jobs outside of the cloud environment.

</ul>

- `/tests`
Where you should place multiple complex levels tests to the function on the developed on the app side.

- `/utils`
This folder contains python scripts that handle the following core procedures of this repository:

<ul>

1. get data samples from the cloud environment and more specifically from Athena. This step comprises in a first instance the development of an Athena query, that query is then performed throughout an `aws cli` command, that returns data in a very peculiar format. The key procedures after the step stated on the previous point, is convert the data queried - that is returned in a very terminal looking to a more pythonic format;
2. creation of a dataframe. From the previous point and after obtaining the data and schema into a more pythonic format, is very straight forward to obtain a pyspark dataframe from the data and the schema arranged;
3. Spark test case, which is a class that handles the unitary tests between pyspark dataframes;
4. JSON map that establishes a correspondence between hive and spark data types.
</ul>

### **5. Future Work list**

In order to get this pipeline covering all the development scenarios, some additional work needs to be done, and it's important to highlight the following tasks:

1. Try to find out some way to perform some tests to kafka;
2. A Scala stage it's also missing;
3. Find a way to manage the automatic adaptation of the aws credentials on each local environment;
4. Optimizations on the configuration file:

<ul>
    1. at this moment, we have a output bucket - for the queries performed - transversal to every test case and it must be something that is automatically adapted from the existent tests;
</ul>

5. Optimization in terms of performance (ex. study the implementation of `boto` instead of having the `subprocess`);
6. Make heavy-weight test to the pipeline, to see how it behaves on exaustive and exception scenarios;
7. Study the option of splitting this into different repositories or have a switch to only perform specific tests, in order to optimize the runtime of the performed tests.