## **TDD Pipeline - Python, Pandas, and PySpark**

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

Taking this into account, this pipeline was built so that the points mentioned above can be aligned with the scope of this project.
### **2. Solution's Overview**
The solution defined to address the points highlighted above and to improve our development pipeline gathers our most common use cases of our development path:
- Test path fully dedicated to Python code and divided into the following distinct dimensions: 1) Python general code, 2) pandas use cases, and 3) PySpark;
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
1. `python` - that can be used to test any python function;
2. `pandas` - whenever you want to test any pandas transformation;
3. `pyspark` - whenever you want to test pyspark procedures.

- `/tests`
Where you should place multiple complex levels tests to the function on the developed on the app side.
