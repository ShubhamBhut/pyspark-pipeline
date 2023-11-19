from setuptools import setup, find_packages

setup(
    name='housing_analysis',
    version='0.1',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'pyspark==3.5.0',
        'py4j==0.10.9.7'
    ],
)

