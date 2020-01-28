# -*- coding: utf-8 -*-

"""Implementation of functions to handle database documents."""

import json as _json
import numpy as _np

from . import sqlitedatabase as _sqlitedatabase
from . import mongodatabase as _mongodatabase
from . import utils as _utils


class DatabaseError(Exception):
    """Database exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


class FileDocumentError(Exception):
    """File document exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


class Database():
    """API for MongoDB or Sqlite database."""

    def __init__(
            self, database_name=None, mongo=False, server='localhost'):
        """Initialize the object and connect to Mongo server, if applicable.

        Args:
            database_name (str): database name.
            mongo (bool): flag indicating mongoDB (True) or sqlite (False).
            server (str): MongoDB server.

        """
        self.database_name = database_name
        self.mongo = mongo
        self._server = server

        if self.mongo:
            self.client = _mongodatabase.db_connect(server=self._server)
        else:
            self.client = None

    @property
    def server(self):
        """Return the MongoDB server."""
        if self.mongo:
            return self._server
        else:
            return None

    @server.setter
    def server(self, value):
        self._server = value
        if self.mongo:
            self.client = _mongodatabase.db_connect(server=self._server)

    def db_database_exists(self):
        """Check if database exists.

        Returns:
            True if database exists, False otherwise.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self._server)
            return _mongodatabase.db_database_exists(
                self.client, self.database_name)
        else:
            return _sqlitedatabase.db_database_exists(
                self.database_name)

    def db_get_collections(self):
        """Get database collection names.

        Returns:
            a list with collection names.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self._server)
            return _mongodatabase.db_get_collections(
                self.client, self.database_name)
        else:
            return _sqlitedatabase.db_get_tables(self.database_name)


class DatabaseCollection(Database):
    """API for MongoDB collection or Sqlite database table."""

    def __init__(
            self, database_name=None, collection_name=None,
            mongo=False, server='localhost'):
        """Initialize the object and connect to Mongo server, if applicable.

        Args:
            database_name (str): database name.
            collection_name (str): database collection name.
            mongo (bool): flag indicating mongoDB (True) or sqlite (False).
            server (str): MongoDB server.

        """
        self.collection_name = collection_name
        super().__init__(
            database_name=database_name, mongo=mongo, server=server)

    def db_collection_exists(self):
        """Check if the collection exists in database.

        Returns:
            True if the collection exists, False otherwise.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_collection_exists(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.db_table_exists(
                self.database_name, self.collection_name)

    def db_create_collection(self):
        """Create collection, with id as ascending index."""
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_create_collection(
                self.client, self.database_name, self.collection_name)
        else:
            msg = 'Empty tables are not supported in SQLite'
            raise NotImplementedError(msg)

    def db_get_field_names(self):
        """Return the field names of the last collection's document.

        Take care when using this function with Mongo since MongoDB is
        a no-SQL database, it doesn't require all documents (entries) to
        have the same fields. Each document can have it's own fields
        independent from the other documents inside the collection, which
        may be different from the last document's fields.

        Returns:
            a list with the last document's field names.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_get_field_names(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.db_get_column_names(
                self.database_name, self.collection_name)

    def db_get_field_types(self):
        """Return the field types of the last collection's document.

        Take care when using this function with Mongo since MongoDB is
        a no-SQL database, it doesn't require all documents (entries) to have
        the same fields or types. Each document can have it's own fields
        independent from the other documents inside the collection, which may
        be different from the last document's fields and types.

        Returns:
            a dict with field names and types.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_get_field_types(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.db_get_column_types(
                self.database_name, self.collection_name)

    def db_get_first_id(self):
        """Return the first document's id.

        Returns:
            a id.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_get_first_id(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.db_get_first_id(
                self.database_name, self.collection_name)

    def db_get_last_id(self):
        """Return the last document's id.

        Returns:
            a id.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_get_last_id(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.db_get_last_id(
                self.database_name, self.collection_name)

    def db_get_values(self, field):
        """Return field values of the database collection.

        Args:
            field (str): field name.

        Returns:
            a list of field values.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_get_values(
                self.client, self.database_name, self.collection_name, field)
        else:
            return _sqlitedatabase.db_get_values(
                self.database_name, self.collection_name, field)

    def db_get_value(self, field, idn):
        """Get field value from entry id.

        Args:
            field (str): field name.
            idn (int): entry id.

        Returns:
            the parameter value.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_get_value(
                self.client, self.database_name,
                self.collection_name, field, idn)
        else:
            return _sqlitedatabase.db_get_value(
                self.database_name, self.collection_name, field, idn)

    def db_search_field(self, field, value):
        """Search field in database collection.

        Args:
            field (str): field to search.
            value (value_type): value to search.

        Returns:
            a list of database entries.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_search_field(
                self.client, self.database_name,
                self.collection_name, field, value)
        else:
            return _sqlitedatabase.db_search_column(
                self.database_name, self.collection_name, field, value)

    def db_search_collection(
            self, fields=None, filters=None,
            initial_idn=None, max_nr_lines=None):
        """Filter collection entries.

        Args:
            fields (list, optional): list of field names to filter.
            filters (list, optional): list of filters to apply (must have the
                                      same lengh as 'fields').
            initial_idn (int, optional): initial id to start filter.
            max_nr_lines (int, optional): maximum number of lines.

        Returns:
            a list of database entries.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_search_collection(
                self.client, self.database_name, self.collection_name,
                fields=fields, filters=filters, initial_idn=initial_idn,
                max_nr_lines=max_nr_lines)
        else:
            return _sqlitedatabase.db_search_table(
                self.database_name, self.collection_name,
                columns=fields, filters=filters, initial_idn=initial_idn,
                max_nr_lines=max_nr_lines)


class DatabaseDocument(DatabaseCollection):
    """Database document or record."""

    label = ''
    collection_name = ''
    db_dict = {}

    # Example of db_dict:
    # db_dict = _collections.OrderedDict([
    #     ('attribute_name', {
    #         'field': 'field_name',
    #         'dtype' (optional): str,
    #         'not_null' (optional): False, # only used for sqlite databases
    #         'unique' (optional): False, # only used for sqlite databases
    #     }),
    # ])

    def __init__(
            self, database_name=None, mongo=False, server='localhost'):
        """Initialize the object.

        Args:
            database_name (str): database name.
            mongo (bool): flag indicating mongoDB (True) or sqlite (False).
            server (str): MongoDB server.

        """
        for attr in self.db_dict.keys():
            if not hasattr(self, attr):
                if 'dtype' in self.db_dict[attr].keys():
                    dtype = self.db_dict[attr]['dtype']
                else:
                    dtype = str

                if dtype == _np.ndarray:
                    setattr(self, attr, _np.array([]))
                elif dtype == dict:
                    setattr(self, attr, {})
                elif dtype == list:
                    setattr(self, attr, [])
                elif dtype == list:
                    setattr(self, attr, ())
                else:
                    setattr(self, attr, None)

        super().__init__(
            database_name=database_name,
            collection_name=self.collection_name,
            mongo=mongo,
            server=server)

    def __eq__(self, other):
        """Equality method."""
        if isinstance(other, self.__class__):
            if len(self.__dict__) != len(other.__dict__):
                return False

            for key in self.__dict__:
                if key not in other.__dict__:
                    return False

                self_value = self.__dict__[key]
                other_value = other.__dict__[key]

                if callable(self_value):
                    pass
                elif (isinstance(self_value, _np.ndarray) and
                      isinstance(other_value, _np.ndarray)):
                    if not self_value.tolist() == other_value.tolist():
                        return False
                elif (not isinstance(self_value, _np.ndarray) and
                      not isinstance(other_value, _np.ndarray)):
                    if not self_value == other_value:
                        return False
                else:
                    return False

            return True

        else:
            return False

    def __ne__(self, other):
        """Non-equality method."""
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __setattr__(self, name, value):
        """Set attribute."""
        if name not in self.db_dict:
            super().__setattr__(name, value)

        else:
            dtype = self.db_dict[name]['dtype']

            if isinstance(value, str) and value == _utils.EMPTY_STR:
                value = None

            if value is None:
                if dtype == _np.ndarray:
                    value = _np.array([])
                elif dtype == dict:
                    value = {}
                elif dtype == list:
                    value = []
                elif dtype == list:
                    value = ()
                else:
                    value = None

            if dtype in (_np.ndarray, list, tuple, dict) and isinstance(
                    value, str):
                value = _json.loads(value)

            if value is None or isinstance(value, dtype):
                super().__setattr__(name, value)
            elif dtype == _np.ndarray:
                super().__setattr__(name, _utils.to_array(value))
            else:
                super().__setattr__(name, dtype(value))

    def __str__(self):
        """Printable string representation of the object."""
        fmtstr = '{0:<18s} : {1}\n'
        r = ''
        for key, value in self.__dict__.items():
            r += fmtstr.format(key, str(value))
        return r

    def clear(self):
        """Clear object."""
        for key in self.__dict__:
            if isinstance(self.__dict__[key], _np.ndarray):
                self.__dict__[key] = _np.array([])
            elif isinstance(self.__dict__[key], dict):
                self.__dict__[key] = {}
            elif isinstance(self.__dict__[key], list):
                self.__dict__[key] = []
            elif isinstance(self.__dict__[key], tuple):
                self.__dict__[key] = ()
            else:
                self.__dict__[key] = None
        return True

    def copy(self):
        """Return a copy of the object."""
        _copy = type(self)()
        for key in self.__dict__:
            if isinstance(self.__dict__[key], _np.ndarray):
                _copy.__dict__[key] = _np.copy(self.__dict__[key])
            elif isinstance(self.__dict__[key], list):
                _copy.__dict__[key] = self.__dict__[key].copy()
            else:
                _copy.__dict__[key] = self.__dict__[key]
        return _copy

    def db_create_collection(self):
        """Create collection, with id as ascending index."""
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_create_collection(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.db_create_table(
                self.database_name, self.collection_name, self.db_dict)

    def db_save(self):
        """Insert a document into a database collection.

        Returns:
            The id of the saved database document.

        """
        values_dict = {}
        date, hour = _utils.get_date_hour()

        reverse_db_dict = {}
        for k, v in self.db_dict.items():
            reverse_db_dict[v['field']] = k

        if 'id' in reverse_db_dict.keys():
            setattr(self, reverse_db_dict['id'], None)
        else:
            values_dict['id'] = None

        if 'date' in reverse_db_dict.keys():
            if getattr(self, reverse_db_dict['date']) is None:
                setattr(self, reverse_db_dict['date'], date)
        else:
            values_dict['date'] = date

        if 'hour' in reverse_db_dict.keys():
            if getattr(self, reverse_db_dict['hour']) is None:
                setattr(self, reverse_db_dict['hour'], hour)
        else:
            values_dict['hour'] = hour

        for attr_name in self.db_dict:
            field = self.db_dict[attr_name]['field']

            if 'dtype' in self.db_dict[attr_name].keys():
                dtype = self.db_dict[attr_name]['dtype']
            else:
                dtype = _utils.DEFAULT_DTYPE

            value = getattr(self, attr_name)
            if value is not None and dtype in (_np.ndarray, list, tuple, dict):
                if isinstance(value, _np.ndarray):
                    value = value.tolist()

                if not self.mongo:
                    value = _json.dumps(value)

            values_dict[field] = value

        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            idn = _mongodatabase.db_save(
                self.client, self.database_name,
                self.collection_name, values_dict)
        else:
            idn = _sqlitedatabase.db_save(
                self.database_name, self.collection_name, values_dict)

        setattr(self, reverse_db_dict['id'], idn)
        return idn

    def db_read(self, idn=None):
        """Read a document (collection entry) from database.

        Args:
            idn (int, optional): entry id (returns last id if not specified).

        Returns:
            True if update was successful, False otherwise.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            values_dict = _mongodatabase.db_read(
                self.client, self.database_name, self.collection_name, idn=idn)
        else:
            values_dict = _sqlitedatabase.db_read(
                self.database_name, self.collection_name, idn=idn)

        if len(values_dict) == 0:
            return False

        field_names = self.db_get_field_names()

        for attr_name in self.db_dict:
            field = self.db_dict[attr_name]['field']

            if field not in field_names:
                msg = 'Field {0:s} not found in database.'.format(field)
                raise DatabaseError(msg)

            try:
                value = values_dict[field]
                setattr(self, attr_name, value)

            except AttributeError:
                pass

        return True

    def db_update(self, idn):
        """Update a collection's document from database.

        Args:
            idn (int): entry id.

        Returns:
            True if update was sucessful, False if update failed.

        """
        values_dict = {}
        field_names = self.db_get_field_names()
        date, hour = _utils.get_date_hour()

        reverse_db_dict = {}
        for k, v in self.db_dict.items():
            reverse_db_dict[v['field']] = k

        if 'id' in reverse_db_dict.keys():
            setattr(self, reverse_db_dict['id'], idn)
        else:
            values_dict['id'] = idn

        if 'date' in reverse_db_dict.keys():
            if getattr(self, reverse_db_dict['date']) is None:
                setattr(self, reverse_db_dict['date'], date)
        else:
            values_dict['date'] = date

        if 'hour' in reverse_db_dict.keys():
            if getattr(self, reverse_db_dict['hour']) is None:
                setattr(self, reverse_db_dict['hour'], hour)
        else:
            values_dict['hour'] = hour

        for attr_name in self.db_dict:
            field = self.db_dict[attr_name]['field']

            if 'dtype' in self.db_dict[attr_name].keys():
                dtype = self.db_dict[attr_name]['dtype']
            else:
                dtype = _utils.DEFAULT_DTYPE

            if field not in field_names:
                msg = 'Field {0:s} not found in database.'.format(field)
                raise DatabaseError(msg)

            value = getattr(self, attr_name)
            if value is not None and dtype in (_np.ndarray, list, tuple, dict):
                if isinstance(value, _np.ndarray):
                    value = value.tolist()

                if not self.mongo:
                    value = _json.dumps(value)

            values_dict[field] = value

        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.db_connect(server=self.server)
            return _mongodatabase.db_update(
                self.client, self.database_name,
                self.collection_name, values_dict, idn)
        else:
            return _sqlitedatabase.db_update(
                self.database_name, self.collection_name, values_dict, idn)


class DatabaseAndFileDocument(DatabaseDocument):
    """Base class for database and file documents."""

    label = ''
    collection_name = ''
    db_dict = {}

    def __init__(
            self, database_name=None, mongo=False, server='localhost'):
        """Initialize obejct.

        Args:
            database_name (str): database file path (sqlite) or name (mongo).
            mongo (bool): flag indicating mongoDB (True) or sqlite (False).
            server (str): MongoDB server.

        """
        super().__init__(
            database_name=database_name, mongo=mongo, server=server)

    @property
    def default_filename(self):
        """Return the default filename."""
        timestamp = _utils.get_timestamp()
        filename = '{0:1s}_{1:1s}.txt'.format(timestamp, self.label)
        return filename

    def read_file(self, filename):
        """Read from file.

        Args:
        ----
            filename (str): filepath.

        """
        flines = _utils.read_file(filename)
        for attr in self.db_dict:
            value = _utils.find_value(flines, attr, raise_error=False)
            try:
                setattr(self, attr, value)
            except Exception:
                pass

        sep_idx = _utils.find_index(flines, '---------------------')
        if sep_idx is not None:
            attrs = flines[sep_idx-1].split()

            data = []
            for line in flines[sep_idx+1:]:
                data_line = [float(d) for d in line.split('\t')]
                data.append(data_line)
            data = _np.array(data)

            if len(attrs) == data.shape[1]:
                for idx, attr in enumerate(attrs):
                    setattr(self, attr, data[:, idx])
            else:
                msg = 'Inconsistent number of columns in file: %s' % filename
                raise FileDocumentError(msg)

        return True

    def save_file(self, filename, columns=None):
        """Save to file.

        Args:
        ----
            filename (str): filepath.
            columns (list, optional): list of attributes to saved in columns.

        """
        if not self.valid_data():
            message = 'Invalid data.'
            raise FileDocumentError(message)

        if columns is None:
            columns = []

        date, hour = _utils.get_date_hour()

        if hasattr(self, 'date') and self.date is None:
            self.date = date

        if hasattr(self, 'hour') and self.hour is None:
            self.hour = hour

        with open(filename, mode='w') as f:
            if len(self.label) != 0:
                line = "{0:s} {1:s}\n\n".format(
                    _utils.COMMENT_CHAR, self.label)
                f.write(line)

            for attr in self.db_dict:
                if attr not in columns:
                    value = getattr(self, attr)

                    if value is None:
                        value = _utils.EMPTY_STR

                    else:
                        if 'dtype' in self.db_dict[attr].keys():
                            dtype = self.db_dict[attr]['dtype']
                        else:
                            dtype = _utils.DEFAULT_DTYPE

                        if dtype in (_np.ndarray, list, tuple, dict):
                            if dtype == _np.ndarray:
                                value = value.tolist()
                            value = _json.dumps(value).replace(' ', '')
                        elif dtype == str:
                            if len(value) == 0:
                                value = _utils.EMPTY_STR
                            value = value.replace(' ', '_')

                    line = '{0:s}\t{1}\n'.format(attr.ljust(30), value)
                    f.write(line)

            if len(columns) != 0:
                columns_values = []
                for attr in columns:
                    columns_values.append(getattr(self, attr))

                columns_header = '\t'.join(columns)
                columns_values = _np.column_stack(columns_values)

                f.write('\n')
                f.write('%s\n' % columns_header)
                f.write('--------------------------------------------' +
                        '--------------------------------------------\n')

                for i in range(columns_values.shape[0]):
                    line = ''
                    for j in range(columns_values.shape[1]):
                        line = line + '{0:+0.10e}\t'.format(
                            columns_values[i, j])
                    f.write(line.strip() + '\n')

        return True

    def valid_data(self):
        """Check if parameters are valid."""
        values = []
        for name in self.db_dict:
            if 'not_null' in self.db_dict[name].keys():
                not_null = self.db_dict[name]['not_null']
            else:
                not_null = _utils.DEFAULT_NOT_NULL

            if not_null and name not in ['idn', 'date', 'hour']:
                values.append(getattr(self, name))

        return all([v is not None for v in values])
