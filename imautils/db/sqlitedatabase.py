# -*- coding: utf-8 -*-

"""Implementation of functions to handle sqlite database records."""

import os as _os
import json as _json
import sqlite3 as _sqlite
import numpy as _np

from . import utils as _utils


class SqliteDatabaseError(Exception):
    """Sqlite database exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


def database_exists(database_name):
    """Check if database file exists.

    Args:
        database_name (str): full file path to database.

    Returns:
        True if database file exists, False otherwise.

    """
    return _os.path.isfile(database_name)


def get_tables(database_name):
    """Get database table names.

    Args:
        database_name (str): full file path to database.

    Returns:
        a list with table names.

    """
    if not database_exists(database_name):
        return []

    con = _sqlite.connect(database_name)
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [r[0] for r in cur.fetchall()]
    con.close()
    return table_names


def table_exists(database_name, table_name):
    """Check if table exists in database.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        True if the table exists, False otherwise.

    """
    if len(table_name) == 0:
        return False

    if not database_exists(database_name):
        return False

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    cur.execute("PRAGMA TABLE_INFO({0})".format(table_name))
    if len(cur.fetchall()) > 0:
        con.close()
        return True
    else:
        con.close()
        return False


def get_column_names(database_name, table_name):
    """Return the column names of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        a list with table column names.

    """
    if not table_exists(database_name, table_name):
        return []

    con = _sqlite.connect(database_name)
    cur = con.cursor()
    cur.execute("PRAGMA TABLE_INFO({0})".format(table_name))
    info = cur.fetchall()
    column_names = [i[1] for i in info]
    con.close()
    return column_names


def get_column_types(database_name, table_name):
    """Return the column types of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        a dict with column names and types.

    """
    if not table_exists(database_name, table_name):
        return []

    db_type_dict = {
        'INTEGER': int,
        'REAL': float,
        'TEXT': str,
        }

    con = _sqlite.connect(database_name)
    cur = con.cursor()
    cur.execute("PRAGMA TABLE_INFO({0})".format(table_name))
    info = cur.fetchall()
    con.close()

    column_types = {}
    for i in info:
        column_types[i[1]] = db_type_dict[i[2]]

    return column_types


def get_first_id(database_name, table_name):
    """Return the first id of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        an id.

    """
    if not table_exists(database_name, table_name):
        return None

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute('SELECT MIN(id) FROM {0}'.format(table_name))
        idn = cur.fetchone()[0]
        con.close()
        return idn
    except Exception:
        con.close()
        return None


def get_last_id(database_name, table_name):
    """Return the last id of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.

    Returns:
        an id.

    """
    if not table_exists(database_name, table_name):
        return None

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute('SELECT MAX(id) FROM {0}'.format(table_name))
        idn = cur.fetchone()[0]
        con.close()
        return idn
    except Exception:
        con.close()
        return None


def get_values(database_name, table_name, column):
    """Return column values of the database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        column (str): column name.

    Returns:
        a list with column values.

    """
    if not table_exists(database_name, table_name):
        return []

    con = _sqlite.connect(database_name)
    cur = con.cursor()
    cur.execute('SELECT {0} FROM {1}'.format(column, table_name))
    column = [d[0] for d in cur.fetchall()]
    con.close()
    return column


def get_value(database_name, table_name, column, idn):
    """Get column value from entry id.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        column (str): column name.
        idn (int): entry id.

    Returns:
        the parameter value.

    """
    if not table_exists(database_name, table_name):
        return None

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute('SELECT {0} FROM {1} WHERE id = ?'.format(
            column, table_name), (idn,))
        value = cur.fetchone()[0]
        con.close()
        return value
    except Exception:
        con.close()
        return None


def search_column(database_name, table_name, column, value):
    """Search column in database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        column (str): column to search.
        value (str): value to search.

    Returns:
        a list of dicts with database entries.

    """
    if not table_exists(database_name, table_name):
        return []

    column_names = get_column_names(database_name, table_name)
    if len(column_names) == 0:
        return []

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
    except Exception:
        con.close()
        return []


def search_table(
        database_name, table_name, columns, filters=None,
        initial_idn=None, max_nr_lines=None):
    """Search paremeter in database.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        columns (list): list of column names to filter.
        filters (list, optional): list of filters to apply.
        initial_idn (int, optional): initial id to start filter.
        max_nr_lines (int, optional): maximum number of lines.

    Returns:
        a list of dicts with database entries.

    """
    try:
        if filters is None:
            filters_list = ['']*len(columns)
        else:
            filters_list = [str(f) for f in filters]

        column_names = [c for c in columns]
        if 'id' not in column_names:
            column_names.insert(0, 'id')
            filters_list.insert(0, '')

        column_names_str = ''
        for column in column_names:
            column_names_str = column_names_str + '"{0:s}", '.format(
                column)
        column_names_str = column_names_str[:-2]
        cmd = 'SELECT {0:s} FROM {1:s}'.format(column_names_str, table_name)

        column_types = get_column_types(
            database_name, table_name)

        if any(filt != '' for filt in filters_list):
            cmd = cmd + ' WHERE '

        and_flag = False
        for idx, column in enumerate(column_names):
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

    except Exception:
        con.close()
        message = 'Could not filter values in table {0}.'.format(table_name)
        raise SqliteDatabaseError(message)


def create_table(database_name, table_name, db_dict):
    """Create database table.

    Args:
        database_name (str): full file path to database.
        table_name (str): database table name.
        db_dict (dict): dictionary with database field names and types.

    Returns:
        True if successful, False otherwise.

    """
    if database_name is None or len(table_name) == 0:
        return False

    fields = [db_dict[k]['field'] for k in db_dict.keys()]

    variables = []
    if 'id' not in fields:
        variables.append(['id', 'INTEGER NOT NULL'])
    if 'date' not in fields:
        variables.append(['date', 'TEXT NOT NULL'])
    if 'hour' not in fields:
        variables.append(['hour', 'TEXT NOT NULL'])

    for attr_name in db_dict:
        column = db_dict[attr_name]['field']
        dtype = db_dict[attr_name]['dtype']
        not_null = db_dict[attr_name]['not_null']

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

        variables.append((column, db_type))

    try:
        con = _sqlite.connect(database_name)
        cur = con.cursor()

        cmd = 'CREATE TABLE IF NOT EXISTS {0} ('.format(table_name)
        for var in variables:
            cmd = cmd + "\'{0}\' {1},".format(var[0], var[1])
        cmd = cmd + "PRIMARY KEY(\'id\'));"
        cur.execute(cmd)
        con.close()
        return True

    except Exception:
        con.close()
        return False


def save_to_database(record):
    """Insert values into database table.

    Args:
        record: object derived from DatabaseDocument object.

    Returns:
        True if update was sucessful, False otherwise.

    """
    database_name = record.database_name
    table_name = record.collection_name
    db_dict = record.db_dict

    if database_name is None or len(table_name) == 0:
        return False

    if len(table_name) == 0:
        return False

    db_column_names = get_column_names(database_name, table_name)
    if len(db_column_names) == 0:
        raise SqliteDatabaseError('Failed to save data to database.')

    values = []

    timestamp_split = _utils.get_timestamp().split('_')
    date = timestamp_split[0]
    hour = timestamp_split[1].replace('-', ':')

    reverse_db_dict = {}
    for k, v in db_dict.items():
        reverse_db_dict[v['field']] = k

    if 'id' in reverse_db_dict.keys():
        setattr(record, reverse_db_dict['id'], None)
    else:
        values.append(None)

    if 'date' in reverse_db_dict.keys():
        if getattr(record, reverse_db_dict['date']) is None:
            setattr(record, reverse_db_dict['date'], date)

    if 'hour' in reverse_db_dict.keys():
        if getattr(record, reverse_db_dict['hour']) is None:
            setattr(record, reverse_db_dict['hour'], hour)

    for attr_name in db_dict:
        column = db_dict[attr_name]['field']
        dtype = db_dict[attr_name]['dtype']

        if column not in db_column_names:
            raise SqliteDatabaseError('Failed to save data to database.')

        if dtype in (_np.ndarray, list, tuple, dict):
            value = getattr(record, attr_name)
            if value is None:
                values.append(value)
            else:
                if isinstance(value, _np.ndarray):
                    value = value.tolist()
                values.append(_json.dumps(value))
        else:
            values.append(getattr(record, attr_name))

    if len(values) != len(db_column_names):
        message = 'Inconsistent number of values for table {0}.'.format(
            table_name)
        raise SqliteDatabaseError(message)

    _l = '(' + ','.join(['?']*len(values)) + ')'

    con = _sqlite.connect(database_name)
    cur = con.cursor()

    try:
        cur.execute(
            ('INSERT INTO {0} VALUES '.format(table_name) + _l), values)

        idn = cur.lastrowid
        if 'id' in reverse_db_dict.keys():
            setattr(record, reverse_db_dict['id'], idn)

        con.commit()
        con.close()
        return True

    except Exception:
        con.close()
        message = 'Could not insert values into table {0}.'.format(table_name)
        raise SqliteDatabaseError(message)


def read_from_database(record, idn=None):
    """Read a table entry from database.

    Args:
        record: object derived from DatabaseDocument object.
        idn (int, optional): entry id (returns last id if not specified).

    Returns:
        True if update was sucessful, False otherwise.

    """
    database_name = record.database_name
    table_name = record.collection_name
    db_dict = record.db_dict

    if database_name is None or len(table_name) == 0:
        return False

    db_dict = record.db_dict

    db_column_names = get_column_names(database_name, table_name)
    if len(db_column_names) == 0:
        raise SqliteDatabaseError('Failed to read data from database.')

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
            return False

    except Exception:
        con.close()
        message = ('Could not retrieve data from {0}'.format(table_name))
        raise SqliteDatabaseError(message)

    for attr_name in db_dict:
        column = db_dict[attr_name]['field']
        dtype = db_dict[attr_name]['dtype']

        if column not in db_column_names:
            raise SqliteDatabaseError('Failed to read data from database.')

        try:
            idx = db_column_names.index(column)
            if dtype in (_np.ndarray, list, tuple, dict):
                if entry[idx] is None:
                    setattr(record, attr_name, entry[idx])
                else:
                    _l = _json.loads(entry[idx])
                    if dtype == _np.ndarray:
                        setattr(record, attr_name, _utils.to_array(_l))
                    else:
                        setattr(record, attr_name, _l)
            else:
                setattr(record, attr_name, entry[idx])
        except AttributeError:
            pass

    return True


def update_database(record, idn):
    """Update a table entry from database.

    Args:
        record: object derived from DatabaseDocument object.
        idn (int): entry id.

    Returns:
        True if update was sucessful, False otherwise.

    """
    database_name = record.database_name
    table_name = record.collection_name
    db_dict = record.db_dict

    if database_name is None or len(table_name) == 0:
        return False

    db_dict = record.db_dict

    if len(table_name) == 0:
        return False

    db_column_names = get_column_names(database_name, table_name)
    if len(db_column_names) == 0:
        raise SqliteDatabaseError('Failed to update database.')

    values = []
    updates = ''

    timestamp_split = _utils.get_timestamp().split('_')
    date = timestamp_split[0]
    hour = timestamp_split[1].replace('-', ':')

    reverse_db_dict = {}
    for k, v in db_dict.items():
        reverse_db_dict[v['field']] = k

    if 'id' in reverse_db_dict.keys():
        setattr(record, reverse_db_dict['id'], idn)
    else:
        values.append(idn)
        updates = updates + '`id`' + '=?, '

    if 'date' in reverse_db_dict.keys():
        if getattr(record, reverse_db_dict['date']) is None:
            setattr(record, reverse_db_dict['date'], date)

    if 'hour' in reverse_db_dict.keys():
        if getattr(record, reverse_db_dict['hour']) is None:
            setattr(record, reverse_db_dict['hour'], hour)

    for attr_name in db_dict:
        column = db_dict[attr_name]['field']
        dtype = db_dict[attr_name]['dtype']

        if column not in db_column_names:
            raise SqliteDatabaseError('Failed to update database.')

        updates = updates + '`' + column + '`' + '=?, '
        if dtype in (_np.ndarray, list, tuple, dict):
            value = getattr(record, attr_name)
            if value is None:
                values.append(value)
            else:
                if isinstance(value, _np.ndarray):
                    value = value.tolist()
                values.append(_json.dumps(value))
        else:
            values.append(getattr(record, attr_name))
    updates = updates[:-2]

    try:
        con = _sqlite.connect(database_name)
        cur = con.cursor()

        if idn is not None:
            cur.execute("""UPDATE {0} SET {1} WHERE
                        id = {2}""".format(table_name, updates, idn), values)
            con.commit()
            con.close()
            return True
        else:
            message = 'Invalid entry id.'
            raise SqliteDatabaseError(message)

    except Exception:
        con.close()
        message = ('Could not update {0} entry.'.format(table_name))
        raise SqliteDatabaseError(message)
