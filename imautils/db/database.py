# -*- coding: utf-8 -*-

"""Implementation of functions to handle database records."""

import os as _os
import json as _json
import sqlite3 as _sqlite
import numpy as _np

from . import utils as _utils


_empty_str = '--'


class DatabaseError(Exception):
    """Database exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


class DatabaseObject():
    """Database object."""

    _db_table = ''
    _db_dict = {}
    # Example of _db_dict:
    # _db_dict = _collections.OrderedDict([
    #     ('attribute_name', {
    #         'column': 'column_name', 'dtype': str, 'not_null': True}),
    # ])

    @staticmethod
    def database_exists(database):
        """Check if database file exists.

        Args:
        ----
            database (str): full file path to database.

        Return:
        ------
            True if database file exists, False otherwise.

        """
        return _os.path.isfile(database)

    @classmethod
    def create_database_table(cls, database, table=None):
        """Create database table.

        Args:
        ----
            database (str): full file path to database.
            table (str, optional): database table name.

        Return:
        ------
            True if successful, False otherwise.

        """
        if table is None:
            table = cls._db_table

        if len(table) == 0:
            return False

        variables = []
        variables.append(['id', 'INTEGER NOT NULL'])
        variables.append(['date', 'TEXT NOT NULL'])
        variables.append(['hour', 'TEXT NOT NULL'])

        for attr_name in cls._db_dict:
            column = cls._db_dict[attr_name]['column']
            dtype = cls._db_dict[attr_name]['dtype']
            not_null = cls._db_dict[attr_name]['not_null']

            if dtype == int:
                db_type = 'INTEGER'
            elif dtype == float:
                db_type = 'REAL'
            elif dtype == str:
                db_type = 'TEXT'
            elif dtype in (_np.ndarray, list, tuple, dict):
                db_type = 'TEXT'
            else:
                raise DatabaseError('Unknown database type.')

            if not_null:
                db_type = db_type + ' NOT NULL'

            variables.append((column, db_type))

        try:
            con = _sqlite.connect(database)
            cur = con.cursor()

            cmd = 'CREATE TABLE IF NOT EXISTS {0} ('.format(table)
            for var in variables:
                cmd = cmd + "\'{0}\' {1},".format(var[0], var[1])
            cmd = cmd + "PRIMARY KEY(\'id\'));"
            cur.execute(cmd)
            con.close()
            return True

        except Exception:
            con.close()
            return False

    @classmethod
    def database_table_exists(cls, database, table=None):
        """Check if table exists in database.

        Args:
        ----
            database (str): full file path to database.
            table (str, optional): database table name.

        Return:
        ------
            True if the table exists, False otherwise.

        """
        if table is None:
            table = cls._db_table

        if len(table) == 0:
            return False

        if not cls.database_exists(database):
            return False

        con = _sqlite.connect(database)
        cur = con.cursor()
        cur.execute("PRAGMA TABLE_INFO({0})".format(table))
        if len(cur.fetchall()) > 0:
            con.close()
            return True
        else:
            con.close()
            return False

    @classmethod
    def get_database_table_name(cls):
        """Return the database table name."""
        return cls._db_table

    @classmethod
    def get_database_table_value(cls, database, column, idn, table=None):
        """Get column value from entry id.

        Args:
        ----
            database (str): full file path to database.
            column (str): column name.
            idn (int): entry id.
            table (str, optional): database table name.

        Return:
        ------
            the parameter value.

        """
        if table is None:
            table = cls._db_table

        if not cls.database_table_exists(database, table):
            return None

        if len(cls.get_database_table_column_names(database, table)) == 0:
            return None

        con = _sqlite.connect(database)
        cur = con.cursor()

        try:
            cur.execute('SELECT {0} FROM {1} WHERE id = ?'.format(
                column, table), (idn,))
            value = cur.fetchone()[0]
            con.close()
            return value
        except Exception:
            con.close()
            return None

    @classmethod
    def get_database_table_last_id(cls, database, table=None):
        """Return the last id of the database table.

        Args:
        ----
            database (str): full file path to database.
            table (str, optional): database table name.

        Return:
        ------
            a id.

        """
        if table is None:
            table = cls._db_table

        if not cls.database_table_exists(database, table):
            return None

        con = _sqlite.connect(database)
        cur = con.cursor()

        try:
            cur.execute('SELECT MAX(id) FROM {0}'.format(table))
            idn = cur.fetchone()[0]
            con.close()
            return idn
        except Exception:
            return None

    @classmethod
    def get_database_table_column_names(cls, database, table=None):
        """Return the column names of the database table.

        Args:
        ----
            database (str): full file path to database.
            table (str, optional): database table name.

        Return:
        ------
            a list with table column names.

        """
        if table is None:
            table = cls._db_table

        if not cls.database_table_exists(database, table):
            return []

        con = _sqlite.connect(database)
        cur = con.cursor()
        cur.execute("PRAGMA TABLE_INFO({0})".format(table))
        info = cur.fetchall()
        column_names = [i[1] for i in info]
        con.close()
        return column_names

    @classmethod
    def get_database_table_column_types(cls, database, table=None):
        """Return the column types of the database table.

        Args:
        ----
            database (str): full file path to database.
            table (str, optional): database table name.

        Return:
        ------
            a dict with column names and types.

        """
        if table is None:
            table = cls._db_table

        if not cls.database_table_exists(database, table):
            return []

        db_type_dict = {
            'INTEGER': int,
            'REAL': float,
            'TEXT': str,
            }

        con = _sqlite.connect(database)
        cur = con.cursor()
        cur.execute("PRAGMA TABLE_INFO({0})".format(table))
        info = cur.fetchall()
        con.close()

        column_types = {}
        for i in info:
            column_types[i[1]] = db_type_dict[i[2]]

        return column_types

    @classmethod
    def get_database_table_column(cls, database, column, table=None):
        """Return column values of the database table.

        Args:
        ----
            database (str): full file path to database.
            column (str): column name.
            table (str, optional): database table name.

        """
        if table is None:
            table = cls._db_table

        if not cls.database_table_exists(database, table):
            return []

        con = _sqlite.connect(database)
        cur = con.cursor()
        cur.execute('SELECT {0} FROM {1}'.format(column, table))
        column = [d[0] for d in cur.fetchall()]
        con.close()
        return column

    @classmethod
    def search_database_table_column(cls, database, column, value, table=None):
        """Search column in database table.

        Args:
        ----
            database (str): full file path to database.
            column (str): column to search.
            value (str): value to search.
            table (str, optional): database table name.

        Return:
        ------
            a list of database entries.

        """
        if table is None:
            table = cls._db_table

        if not cls.database_table_exists(database, table):
            return []

        if len(cls.get_database_table_column_names(database, table)) == 0:
            return []

        con = _sqlite.connect(database)
        cur = con.cursor()

        try:
            cmd = 'SELECT * FROM {0} WHERE {1}="{2}"'.format(
                table, column, str(value))
            cur.execute(cmd)
            entries = cur.fetchall()
            con.close()
            return entries
        except Exception:
            con.close()
            return []

    @classmethod
    def filter_database_table_columns(
            cls, database, columns, filters,
            initial_idn=None, max_nr_lines=None, table=None):
        """Search paremeter in database.

        Args:
        ----
            database (str): full file path to database.
            columns (list): list of column names to filter.
            filters (list): list of filters to apply.
            initial_idn (int, optional): initial id to start filter.
            max_nr_lines (int, optional): maximum number of lines.
            table (str, optional): database table name.

        Return:
        ------
            a list of database entries.

        """
        if table is None:
            table = cls._db_table

        try:
            column_names_str = ''
            for column in columns:
                column_names_str = column_names_str + '"{0:s}", '.format(
                    column)
            column_names_str = column_names_str[:-2]
            cmd = 'SELECT {0:s} FROM {1:s}'.format(column_names_str, table)

            column_types = cls.get_database_table_column_types(
                database, table=table)

            if any(filt != '' for filt in filters):
                cmd = cmd + ' WHERE '

            and_flag = False
            for idx, column in enumerate(columns):
                data_type = column_types[column]

                filt = filters[idx]

                if filt != '':

                    if and_flag:
                        cmd = cmd + ' AND '
                    and_flag = True

                    if data_type == str:
                        cmd = cmd + column + ' LIKE "%' + filt + '%"'
                    else:
                        if '~' in filt:
                            fs = filt.split('~')
                            if len(fs) == 2:
                                cmd = cmd + column + ' >= ' + fs[0]
                                cmd = cmd + ' AND '
                                cmd = cmd + column + ' <= ' + fs[1]
                        elif filt.lower() == 'none' or filt.lower() == 'null':
                            cmd = cmd + column + ' IS NULL'
                        else:
                            try:
                                value = data_type(filt)
                                cmd = cmd + column + ' = ' + str(value)
                            except ValueError:
                                cmd = cmd + column + ' ' + filt

            if max_nr_lines is not None:
                limit_str = ' LIMIT {0:d}'.format(max_nr_lines)
            else:
                limit_str = ''

            if initial_idn is not None:
                if 'WHERE' in cmd:
                    cmd = (
                        'SELECT * FROM (' + cmd +
                        ' AND id >= {0:d}{1:s})'.format(
                            initial_idn, limit_str))
                else:
                    cmd = (
                        'SELECT * FROM (' + cmd +
                        ' WHERE id >= {0:d}{1:s})'.format(
                            initial_idn, limit_str))

            else:
                cmd = (
                    'SELECT * FROM (' + cmd +
                    ' ORDER BY id DESC{0:s}) ORDER BY id ASC'.format(
                        limit_str))

            con = _sqlite.connect(database)
            cur = con.cursor()
            cur.execute(cmd)
            data = cur.fetchall()
            con.close()
            return data
        except Exception:
            con.close()
            message = 'Could not filter values in table {0}.'.format(table)
            raise DatabaseError(message)

    def read_from_database_table(self, database, idn=None, table=None):
        """Read a table entry from database.

        Args:
        ----
            database (str): full file path to database.
            idn (int, optional): entry id
            table (str, optional): database table name.

        """
        if table is None:
            table = self._db_table

        if not self.database_table_exists(database, table):
            raise DatabaseError('Invalid database table name.')

        db_column_names = self.get_database_table_column_names(database, table)

        con = _sqlite.connect(database)
        cur = con.cursor()

        try:
            if idn is not None:
                cur.execute(
                    'SELECT * FROM {0} WHERE id = ?'.format(table), (idn,))
            else:
                cur.execute(
                    """SELECT * FROM {0}\
                    WHERE id = (SELECT MAX(id) FROM {0})""".format(table))
            entry = cur.fetchone()
            con.close()

        except Exception:
            con.close()
            message = ('Could not retrieve data from {0}'.format(table))
            raise DatabaseError(message)

        for attr_name in self._db_dict:
            column = self._db_dict[attr_name]['column']
            dtype = self._db_dict[attr_name]['dtype']

            if column not in db_column_names:
                raise DatabaseError('Failed to read data from database.')

            try:
                idx = db_column_names.index(column)
                if dtype in (_np.ndarray, list, tuple, dict):
                    if entry[idx] is None:
                        setattr(self, attr_name, entry[idx])
                    else:
                        _l = _json.loads(entry[idx])
                        if dtype == _np.ndarray:
                            setattr(self, attr_name, _utils.to_array(_l))
                        else:
                            setattr(self, attr_name, _l)
                else:
                    setattr(self, attr_name, entry[idx])
            except AttributeError:
                pass

        if (hasattr(self, '_timestamp') and
                'date' in db_column_names and 'hour' in db_column_names):
            idx_date = db_column_names.index('date')
            date = entry[idx_date]
            idx_hour = db_column_names.index('hour')
            hour = entry[idx_hour]
            self._timestamp = '_'.join([date, hour])

    def save_to_database_table(self, database, table=None):
        """Insert values into database table.

        Args:
        ----
            database (str): full file path to database.
            table (str, optional): database table name.

        Return:
        ------
            the id of the database record.

        """
        if table is None:
            table = self._db_table

        if len(table) == 0:
            return None

        if not self.database_table_exists(database, table):
            raise DatabaseError('Invalid database table name.')

        db_column_names = self.get_database_table_column_names(database, table)
        if len(db_column_names) == 0:
            raise DatabaseError('Failed to save data to database.')

        if hasattr(self, '_timestamp') and self._timestamp is not None:
            timestamp = self._timestamp.split('_')
        else:
            timestamp = _utils.get_timestamp().split('_')

        values = []
        values.append(None)
        values.append(timestamp[0])
        values.append(timestamp[1].replace('-', ':'))

        for attr_name in self._db_dict:
            column = self._db_dict[attr_name]['column']
            dtype = self._db_dict[attr_name]['dtype']

            if column not in db_column_names:
                raise DatabaseError('Failed to save data to database.')

            if dtype in (_np.ndarray, list, tuple, dict):
                value = getattr(self, attr_name)
                if value is None:
                    values.append(value)
                else:
                    if isinstance(value, _np.ndarray):
                        value = value.tolist()
                    values.append(_json.dumps(value))
            else:
                values.append(getattr(self, attr_name))

        if len(values) != len(db_column_names):
            message = 'Inconsistent number of values for table {0}.'.format(
                table)
            raise DatabaseError(message)

        _l = '(' + ','.join(['?']*len(values)) + ')'

        con = _sqlite.connect(database)
        cur = con.cursor()

        try:
            cur.execute(
                ('INSERT INTO {0} VALUES '.format(table) + _l), values)
            idn = cur.lastrowid
            con.commit()
            con.close()
            return idn

        except Exception:
            con.close()
            message = 'Could not insert values into table {0}.'.format(table)
            raise DatabaseError(message)

    def update_database_table(self, database, idn, table=None):
        """Update a table entry from database.

        Args:
        ----
            database (str): full file path to database.
            idn (int): entry id.
            table (str, optional): database table name.

        Return:
        ------
            True if update was sucessful.
            False if update failed.

        """
        if table is None:
            table = self._db_table

        if len(table) == 0:
            return False

        if not self.database_table_exists(database, table):
            raise DatabaseError('Invalid database table name.')

        db_column_names = self.get_database_table_column_names(database, table)
        if len(db_column_names) == 0:
            raise DatabaseError('Failed to save data to database.')

        if hasattr(self, '_timestamp') and self._timestamp is not None:
            timestamp = self._timestamp
        else:
            timestamp = _utils.get_timestamp().split('_')

        values = []
        values.append(idn)
        values.append(timestamp[0])
        values.append(timestamp[1].replace('-', ':'))

        updates = ''
        updates = updates + '`id`' + '=?, '
        updates = updates + '`date`' + '=?, '
        updates = updates + '`hour`' + '=?, '

        for attr_name in self._db_dict:
            column = self._db_dict[attr_name]['column']
            dtype = self._db_dict[attr_name]['dtype']

            if column not in db_column_names:
                raise DatabaseError('Failed to read data from database.')

            updates = updates + '`' + column + '`' + '=?, '
            if dtype in (_np.ndarray, list, tuple, dict):
                value = getattr(self, attr_name)
                if value is None:
                    values.append(value)
                else:
                    if isinstance(value, _np.ndarray):
                        value = value.tolist()
                    values.append(_json.dumps(value))
            else:
                values.append(getattr(self, attr_name))

        # strips last ', ' from updates
        updates = updates[:-2]

        try:
            con = _sqlite.connect(database)
            cur = con.cursor()

            if idn is not None:
                cur.execute("""UPDATE {0} SET {1} WHERE
                            id = {2}""".format(table, updates, idn), values)
                con.commit()
                con.close()
                return True
            else:
                message = 'Invalid entry id.'
                raise DatabaseError(message)

        except Exception:
            con.close()
            message = ('Could not update {0} entry.'.format(table))
            raise DatabaseError(message)
