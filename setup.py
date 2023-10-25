from setuptools import setup, find_packages

setup(
    name='dbclusters',
    version='1.0',
    url = 'https://github.com/aarnisi/databricks_cluster_optimization',
    packages=find_packages(),
    install_requires=[
        'databricks-connect'
       ,'pandas'
       ,'plotly'
       ,'matplotlib'
       ,'requests'
       ,'json'
       ,'pyspark'
    ],
)
