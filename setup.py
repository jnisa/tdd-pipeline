


from setuptools import setup
from setuptools import find_packages


# installation requirements
INSTALL_REQUIRES = [
    'pandas',
    'pyspark',
    'subprocess'
]


# setup the environment
setup(
    name="tdd-pipeline",
    version="0.1",
    description="Test Driven Development Pipeline",
    package=find_packages(),
    author="João Nisa",
    url="",
    include_package_data=True,
    zip_safe=False,
    install_requires = INSTALL_REQUIRES
)