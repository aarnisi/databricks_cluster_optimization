from setuptools import setup, find_packages

setup(
    name='dbclusters',
    version='1.0',
    url = 'https://github.com/aarnisi/databricks-cluster-optimization',
    packages=find_packages(),
    install_requires=[
        'databricks'
       ,'pandas'
       ,'seaborn'
       ,'matplotlib'
       ,'requests'
    ],
)
