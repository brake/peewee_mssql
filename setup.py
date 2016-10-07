import os

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()
    
requires = [
    'pyodbc',
    'peewee'
]

version = '0.1.4'

setup(
    name='peewee_mssqlserv',
    version=version,
    py_modules=['peewee_mssqlserv'],
    description='MS SQL Server support for the peewee ORM',
    long_description=README,
    author='Constantin Roganov',
    author_email='rccbox@gmail.com',
    url='https://github.com/brake/peewee_mssql',
    download_url='https://github.com/brake/peewee_mssql/archive/' + version + '.zip', 
    keywords=['database', 'ORM', 'peewee', 'mssql'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    install_requires=requires,
)
