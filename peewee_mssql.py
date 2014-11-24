# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:    peewee_mssql.py        
# Package: peewee_mssql
# Project: peewee_mssql     
#
# Created: 19.11.2014 14:33    
# Copyright:  (c) Constantin Roganov, 2014 
# Licence:    MIT
#-------------------------------------------------------------------------------
#!/usr/bin/env/python

"""MS SQL Server database support for peewee ORM"""

__author__ = 'Constantin Roganov'

from peewee import QueryCompiler, Database, ImproperlyConfigured

try:
    import pyodbc

except ImportError:
    pyodbc = None


class MSSQLQueryCompiler(QueryCompiler):

    def quote(self, s):
        return '[%s]' % s


class MSSQLDatabase(Database):
    commit_select = False
    compiler_class = MSSQLQueryCompiler
    field_overrides = {
        'bool': 'BIT',
        'double': 'DOUBLE PRECISION',
        'float': 'FLOAT',
        'primary_key': 'INT IDENTITY(1,1)',
        'text': 'NTEXT',
    }

    def _connect(self, database, **kwargs):
        if not pyodbc:
            raise ImproperlyConfigured('pyodbc should be installed in order to use peewee_mssql')

        return pyodbc.connect(kwargs['string'] + ';DATABASE=%s' % database)

    def get_indexes_for_table(self, table):
        res = self.execute_sql("""select i.name, is_primary_key, is_unique
            from sys.indexes i join sys.objects o on i.object_id=o.object_id
            where i.object_id = object_id(?)
            """,
            (table,)
        )
        return sorted([(r[0], r[1]) for r in res.fetchall()])

    def get_tables(self):
        res = self.execute_sql('SELECT name FROM sys.Tables')
        return [r[0] for r in res.fetchall()]

    def last_insert_id(self, cursor, model):
        if model._meta.auto_increment:
            return cursor.execute('SELECT @@IDENTITY').fetchone()[0]

