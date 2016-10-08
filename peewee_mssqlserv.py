# -*- coding: UTF-8 -*-
# -------------------------------------------------------------------------------
# Name:    peewee_mssqlserv.py        
# Package: peewee_mssqlserv
# Project: peewee_mssql     
#
# Created: 19.11.2014 14:33    
# Copyright:  (c) Constantin Roganov, 2014-2016 
# Licence:    MIT
#-------------------------------------------------------------------------------
#!/usr/bin/env/python

"""MS SQL Server database support for peewee ORM"""

__author__ = 'Constantin Roganov'

from peewee import QueryCompiler, Database, ImproperlyConfigured, SQL, \
                    CompoundSelect, _StripParens, Clause, EnclosedClause, \
                    CommaClause, savepoint, ColumnMetadata, ForeignKeyMetadata

try:
    import pyodbc

except ImportError:
    pyodbc = None


class MSSQLQueryCompiler(QueryCompiler):

    def quote(self, s):
        return '[%s]' % s

    def generate_select(self, query, alias_map=None):
        model = query.model_class
        db = model._meta.database

        alias_map = self.calculate_alias_map(query, alias_map)

        if isinstance(query, CompoundSelect):
            clauses = [_StripParens(query)]
        else:
            if not query._distinct:
                clauses = [SQL('SELECT')]
            else:
                clauses = [SQL('SELECT DISTINCT')]
                if query._distinct not in (True, False):
                    clauses += [SQL('ON'), EnclosedClause(*query._distinct)]

            if query._limit:
                clauses.append(SQL('TOP %s' % query._limit))

            select_clause = Clause(*query._select)
            select_clause.glue = ', '

            clauses.extend((select_clause, SQL('FROM')))
            if query._from is None:
                clauses.append(model.as_entity().alias(alias_map[model]))
            else:
                clauses.append(CommaClause(*query._from))

        # WINDOW semantic is ignored due to lack of knowledge (OVER ...)
        # if query._windows is not None:
        #     clauses.append(SQL('WINDOW'))
        #     clauses.append(CommaClause(*[
        #         Clause(
        #             SQL(window._alias),
        #             SQL('AS'),
        #             window.__sql__())
        #         for window in query._windows]))

        join_clauses = self.generate_joins(query._joins, model, alias_map)
        if join_clauses:
            clauses.extend(join_clauses)

        if query._where is not None:
            clauses.extend([SQL('WHERE'), query._where])

        if query._group_by:
            clauses.extend([SQL('GROUP BY'), CommaClause(*query._group_by)])

        if query._having:
            clauses.extend([SQL('HAVING'), query._having])

        if query._order_by:
            clauses.extend([SQL('ORDER BY'), CommaClause(*query._order_by)])

        # if query._offset:
        #     clauses.append(SQL('OFFSET %s ROWS' % query._offset))
        #
        # if query._limit or (query._offset and db.limit_max):
        #     limit = query._limit or db.limit_max
        #     clauses.append(SQL('FETCH NEXT %s ROWS ONLY' % limit))

        if query._offset:
            raise NotImplementedError('OFFSET is not supported')

        # No locking semantics supported due to lack of knowledge (WITH ...)
        # for_update, no_wait = query._for_update
        # if for_update:
        #     stmt = 'FOR UPDATE NOWAIT' if no_wait else 'FOR UPDATE'
        #     clauses.append(SQL(stmt))

        return self.build_query(clauses, alias_map)


class savepoint_mssql(savepoint):
    def commit(self):
        self._execute('COMMIT TRANSACTION %s;' % self.quoted_sid)

    def rollback(self):
        self._execute('ROLLBACK TRANSACTION %s;' % self.quoted_sid)

    def __enter__(self):
        self._orig_autocommit = self.db.get_autocommit()
        self.db.set_autocommit(False)
        self._execute('SAVE TRANSACTION %s;' % self.quoted_sid)
        return self


class MSSQLDatabase(Database):
    commit_select = False
    compiler_class = MSSQLQueryCompiler
    field_overrides = {
        'bool': 'BIT',
        'double': 'DOUBLE PRECISION',
        'float': 'FLOAT',
        'primary_key': 'INT IDENTITY(1,1)',
        'text': 'NTEXT',
        'blob': 'varbinary'
    }

    def _connect(self, database, **kwargs):
        if not pyodbc:
            raise ImproperlyConfigured('pyodbc should be installed in order to use peewee_mssql')

        return pyodbc.connect(kwargs['string'] + ';DATABASE=%s' % database)

    def get_tables(self, schema=None):
        res = self.execute_sql('SELECT name FROM sys.Tables')
        return [r[0] for r in res.fetchall()]

    def last_insert_id(self, cursor, model):
        if model._meta.auto_increment:
            return cursor.execute('SELECT @@IDENTITY').fetchone()[0]

    def savepoint(self, sid=None):
        return savepoint_mssql(self, sid)

    def get_indexes(self, table, schema=None):
        res = self.execute_sql('''select i.name, is_primary_key, is_unique
            from sys.indexes i join sys.objects o on i.object_id=o.object_id
            where i.object_id = object_id(?)
            ''',
            (table,)
        )
        # TODO: return IndexMetadata
        return sorted([(r[0], r[1]) for r in res.fetchall()])

    def get_columns(self, table, schema=None):
        res = self.execute_sql('''select column_name, data_type, is_nullable
            from information_schema.columns
            where table_name=?
            order by ordinal_position''',
            (table,))

        pks = set(self.get_primary_keys(table, schema))

        return [ColumnMetadata(col_name, col_type, nullable == 'YES', col_name in pks, table)
                for col_name, col_type, nullable in res.fetchall()]

    def get_primary_keys(self, table, schema=None):
        res = self.execute_sql('''SELECT Col.Column_Name
            FROM
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab,
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE Col
            WHERE Col.Constraint_Name=Tab.Constraint_Name
                AND Col.Table_Name=Tab.Table_Name
                AND Constraint_Type='PRIMARY KEY'
                AND Col.Table_Name=?
            ORDER BY Col.ordinal_position''',
            (table,))
        return list(res.fetchall())

    def get_foreign_keys(self, table, schema=None):
        res = self.execute_sql('''SELECT
                COL_NAME(fc.parent_object_id, fc.parent_column_id),
                OBJECT_NAME (f.referenced_object_id),
                COL_NAME(fc.referenced_object_id, fc.referenced_column_id)
            FROM sys.foreign_keys AS f
                INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID=fc.constraint_object_id
            WHERE OBJECT_NAME(f.parent_object_id)=?''',
            (table,))
        return [ForeignKeyMetadata(col_name, ref_table, ref_col, table)
                for col_name, ref_table, ref_col in res.fetchall()]
