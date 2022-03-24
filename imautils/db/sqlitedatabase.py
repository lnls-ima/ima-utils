# -*- coding: utf-8 -*-

"""Implementation of functions to handle sqlite database records."""

import os as _os
import sys as _sys
import json as _json
import sqlite3 as _sqlite
import traceback as _traceback
import numpy as _np

from . import utils as _utils


class SqliteDatabaseError(Exception):
    """Sqlite database exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


def db_database_exists(database_name):
    """Check if database file exists.

    Args:
        database_name (str): full file path to database.

    Returns:
        True if database file exists, False otherwise.

    """
    if database_name is None or len(database_name) == 0:
        msg = 'Invalid database name.'
        raise SqliteDatabaseError(msg)
    else:
        return _os.path.isfile(database_name)


def db_get_tables(database_name):
    """Get database table names.

    Args:
        database_name (str): full file path to database.

    Returns:
        a list with table names.

    """
    if not db_database_exists(database_name):
        msg = 'Database not found.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = [r[0] for r in cur.fetchall()]
        con.close()
        return table_names

    except Exception as e:
        con.close()
        raise e


def db_table_exists(database_name, table_name):
    """Check if table exists in database.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        True if the table exists, False otherwise.

    """
    if not db_database_exists(database_name):
        msg = 'Database not found.'
        raise SqliteDatabaseError(msg)

    if table_name is None or len(table_name) == 0 or table_name == 'table':
        msg = 'Invalid table name.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute("PRAGMA TABLE_INFO({0})".format(table_name))
        if len(cur.fetchall()) > 0:
            con.close()
            return True
        else:
            con.close()
            return False

    except Exception as e:
        con.close()
        raise e


def db_get_column_names(database_name, table_name):
    """Return the column names of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        a list with table column names.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute("PRAGMA TABLE_INFO({0})".format(table_name))
        info = cur.fetchall()
        column_names = [i[1] for i in info]
        con.close()
        return column_names

    except Exception as e:
        con.close()
        raise e


def db_get_column_types(database_name, table_name):
    """Return the column types of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        a dict with column names and types.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    db_type_dict = {
        'INTEGER': int,
        'REAL': float,
        'TEXT': str,
        }

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute("PRAGMA TABLE_INFO({0})".format(table_name))
        info = cur.fetchall()
        con.close()

        column_types = {}
        for i in info:
            column_types[i[1]] = db_type_dict[i[2]]

        return column_types

    except Exception as e:
        con.close()
        raise e


def db_get_first_id(database_name, table_name):
    """Return the first id of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        an id.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute('SELECT MIN(id) FROM {0}'.format(table_name))
        idn = cur.fetchone()[0]
        con.close()
        return idn

    except Exception as e:
        con.close()
        raise e


def db_get_last_id(database_name, table_name):
    """Return the last id of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        an id.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute('SELECT MAX(id) FROM {0}'.format(table_name))
        idn = cur.fetchone()[0]
        con.close()
        return idn

    except Exception as e:
        con.close()
        raise e


def db_delete(database_name, table_name, idns):
    """Delete entries from database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        idns (list): list of entry ids.

    Returns:
        True if successful, False otherwise.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    if idns is None or len(idns) == 0:
        msg = 'Invalid entry ids.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        seq = ','.join(['?']*len(idns))
        cmd = 'DELETE FROM {0} WHERE id IN ({1})'.format(
            table_name, seq)
        cur.execute(cmd, idns)
        con.commit()
        con.close()
        return True

    except Exception as e:
        con.close()
        raise e


def db_get_values(database_name, table_name, column):
    """Return column values of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        column (str): column name.

    Returns:
        a list with column values.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    if column is None or len(column) == 0:
        msg = 'Invalid column name.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute('SELECT {0} FROM {1}'.format(column, table_name))
        column = [d[0] for d in cur.fetchall()]
        con.close()
        return column

    except Exception as e:
        con.close()
        raise e


def db_get_value(database_name, table_name, column, idn):
    """Get column value from entry id.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        column (str): column name.
        idn (int): entry id.

    Returns:
        the parameter value.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    if column is None or len(column) == 0:
        msg = 'Invalid column name.'
        raise SqliteDatabaseError(msg)

    if idn is None:
        msg = 'Invalid id number.'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute('SELECT {0} FROM {1} WHERE id = ?'.format(
            column, table_name), (idn,))
        value = cur.fetchone()
        if value is not None:
            value = value[0]
        con.close()
        return value

    except Exception as e:
        con.close()
        raise e


def db_search_column(database_name, table_name, column, value):
    """Search column in database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        column (str): column to search.
        value (str): value to search.

    Returns:
        a list of dicts with database entries.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    if column is None or len(column) == 0:
        msg = 'Invalid column name.'
        raise SqliteDatabaseError(msg)

    if value is None:
        msg = 'Invalid value to search.'
        raise SqliteDatabaseError(msg)

    column_names = db_get_column_names(database_name, table_name)
    if column not in column_names:
        msg = 'Column "{0}" not found in database table.'.format(column)
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cmd = 'SELECT * FROM {0} WHERE {1}="{2}"'.format(
            table_name, column, str(value))
        cur.execute(cmd)
        entries = cur.fetchall()
        con.close()

        list_of_dicts = []
        for entry in entries:
            _d = {}
            for i, col in enumerate(column_names):
                _d[col] = entry[i]
            list_of_dicts.append(_d)

        return list_of_dicts

    except Exception as e:
        con.close()
        raise e


def db_search_table(
        database_name, table_name, columns=None, filters=None,
        initial_idn=None, max_nr_lines=None):
    """Search paremeter in database.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        columns (list, optional): list of column names to filter.
        filters (list, optional): list of filters to apply.
        initial_idn (int, optional): initial id to start filter.
        max_nr_lines (int, optional): maximum number of lines.

    Returns:
        a list of dicts with database entries.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    if columns is None or len(columns) == 0:
        columns = db_get_column_names(database_name, table_name)

    if isinstance(columns, str):
        columns = [columns]

    if filters is None or len(filters) == 0:
        filters_list = ['']*len(columns)
    else:
        filters_list = [str(f) for f in filters]

    column_names = [c for c in columns]
    if 'id' not in column_names:
        column_names.insert(0, 'id')
        filters_list.insert(0, '')

    if len(filters_list) != len(column_names):
        msg = 'Inconsistent columns and filters arguments.'
        raise SqliteDatabaseError(msg)

    column_names_str = ''
    for column in column_names:
        column_names_str = column_names_str + '"{0:s}", '.format(
            column)
    column_names_str = column_names_str[:-2]
    cmd = 'SELECT {0:s} FROM {1:s}'.format(column_names_str, table_name)

    column_types = db_get_column_types(
        database_name, table_name)

    if any(filt != '' for filt in filters_list):
        cmd = cmd + ' WHERE '

    and_flag = False
    for idx, column in enumerate(column_names):
        if column not in column_types.keys():
            msg = 'Column "{0}" not found in database table.'.format(column)
            raise SqliteDatabaseError(msg)

        data_type = column_types[column]

        filt = filters_list[idx]

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

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute(cmd)
        data = cur.fetchall()
        con.close()

        list_of_dicts = []
        for entry in data:
            _d = {}
            for i, col in enumerate(column_names):
                _d[col] = entry[i]
            list_of_dicts.append(_d)
        return list_of_dicts

    except Exception as e:
        con.close()
        raise e


def db_create_table(database_name, table_name, db_dict):
    """Create database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        db_dict (dict): dictionary with database field names and types.

    Returns:
        True if successful, False otherwise.

    """
    if database_name is None:
        msg = 'Invalid database name.'
        raise SqliteDatabaseError(msg)

    if len(table_name) == 0:
        msg = 'Invalid table name.'
        raise SqliteDatabaseError(msg)

    if db_dict is None:
        db_dict = {}

    fields = []
    for k in db_dict.keys():
        if 'field' in db_dict[k].keys():
            field = db_dict[k]['field']
        else:
            field = k
        fields.append(field)

    variables = []
    if 'id' not in fields:
        variables.append(['id', 'INTEGER NOT NULL'])
    if 'date' not in fields:
        variables.append(['date', 'TEXT NOT NULL'])
    if 'hour' not in fields:
        variables.append(['hour', 'TEXT NOT NULL'])

    for attr_name in db_dict:
        if 'field' in db_dict[attr_name].keys():
            column = db_dict[attr_name]['field']
        else:
            column = attr_name

        if 'dtype' in db_dict[attr_name].keys():
            dtype = db_dict[attr_name]['dtype']
        else:
            dtype = _utils.DEFAULT_DTYPE

        if 'not_null' in db_dict[attr_name].keys():
            not_null = db_dict[attr_name]['not_null']
        else:
            not_null = _utils.DEFAULT_NOT_NULL

        if 'unique' in db_dict[attr_name].keys():
            unique = db_dict[attr_name]['unique']
        else:
            unique = _utils.DEFAULT_UNIQUE

        if dtype == int:
            db_type = 'INTEGER'
        elif dtype == float:
            db_type = 'REAL'
        elif dtype == str:
            db_type = 'TEXT'
        elif dtype in (_np.ndarray, list, tuple, dict):
            db_type = 'TEXT'
        else:
            raise SqliteDatabaseError('Unknown database type.')

        if not_null:
            db_type = db_type + ' NOT NULL'

        if unique:
            db_type = db_type + ' UNIQUE'

        variables.append((column, db_type))

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cmd = 'CREATE TABLE IF NOT EXISTS {0} ('.format(table_name)
        for var in variables:
            cmd = cmd + "\'{0}\' {1},".format(var[0], var[1])
        cmd = cmd + "PRIMARY KEY(\'id\'));"
        cur.execute(cmd)
        con.close()
        return True

    except Exception as e:
        con.close()
        raise e


def db_save(database_name, table_name, values_dict):
    """Insert values into database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        values_dict (str): dict with values to save in database.

    Returns:
        The id of the saved database record.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    column_names = db_get_column_names(database_name, table_name)
    if len(column_names) == 0:
        msg = 'Empty database table'
        raise SqliteDatabaseError(msg)

    if values_dict is None or len(values_dict) == 0:
        msg = 'Invalid values to save in database.'
        raise SqliteDatabaseError(msg)

    if len(values_dict) != len(column_names):
        msg = 'Inconsistent number of values for table {0}.'.format(table_name)
        raise SqliteDatabaseError(msg)

    values = []
    for column in column_names:
        values.append(values_dict[column])
    aux_str = '(' + ','.join(['?']*len(values)) + ')'

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute(
            ('INSERT INTO {0} VALUES '.format(table_name) + aux_str), values)

        idn = cur.lastrowid
        con.commit()
        con.close()
        return idn

    except Exception as e:
        con.close()
        raise e


def db_read(database_name, table_name, idn=None):
    """Read a table entry from database.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        idn (int, optional): entry id (returns last id if not specified).

    Returns:
        a dict with values read from database.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    column_names = db_get_column_names(database_name, table_name)
    if len(column_names) == 0:
        msg = 'Empty database table'
        raise SqliteDatabaseError(msg)

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        if idn is not None:
            cur.execute(
                'SELECT * FROM {0} WHERE id = ?'.format(table_name), (idn,))
        else:
            cur.execute(
                """SELECT * FROM {0}\
                WHERE id = (SELECT MAX(id) FROM {0})""".format(table_name))
        entry = cur.fetchone()
        con.close()
        if entry is None:
            return {}

    except Exception as e:
        con.close()
        raise e

    values_dict = {}
    for idx, column in enumerate(column_names):
        values_dict[column] = entry[idx]

    return values_dict


def db_update(database_name, table_name, values_dict, idn):
    """Update a table entry from database.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        values_dict (str): dict with values to save in database.
        idn (int): entry id.

    Returns:
        True if update was successful, False otherwise.

    """
    if not db_table_exists(database_name, table_name):
        msg = 'Database table not found.'
        raise SqliteDatabaseError(msg)

    column_names = db_get_column_names(database_name, table_name)
    if len(column_names) == 0:
        msg = 'Empty database table {0}.'.format(table_name)
        raise SqliteDatabaseError(msg)

    if values_dict is None or len(values_dict) == 0:
        msg = 'Invalid values to save in database.'
        raise SqliteDatabaseError(msg)

    if len(values_dict) != len(column_names):
        msg = 'Inconsistent number of values for table {0}.'.format(table_name)
        raise SqliteDatabaseError(msg)

    if idn is None:
        msg = 'Invalid id number.'
        raise SqliteDatabaseError(msg)

    values = []
    aux_str = ''
    for column in column_names:
        values.append(values_dict[column])
        aux_str = aux_str + '`' + column + '`' + '=?, '
    aux_str = aux_str[:-2]

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        if idn is not None:
            cur.execute("""UPDATE {0} SET {1} WHERE
                        id = {2}""".format(table_name, aux_str, idn), values)
            con.commit()
            con.close()
            return True
        else:
            message = 'Invalid entry id.'
            raise SqliteDatabaseError(message)

    except Exception as e:
        con.close()
        raise e
