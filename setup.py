from setuptools import setup, find_packages

setup(
    name='Data Transformers',
    author='Fundar',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pandas',
    ]
)