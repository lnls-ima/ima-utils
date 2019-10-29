"""MongoDB interface module."""

import numpy as _np
import pymongo as _pymongo
from . import utils as _utils

_empty_str = '--'


class DatabaseError(Exception):
    """Database exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


def MongoDatabase():
    """MongoDB object."""

    _db_collection = ''
    _db_dict = {}
    # Example of _db_dict:
    # _db_dict = _collections.OrderedDict([
    #     ('attribute_name', {
    #         'field': 'field_name', 'dtype': str, 'not_null': True}),
    # ])
    # obs: this class also accepts an 'column' instead of 'field' for SQLite
    #      combatibility.
    _filter_dict = {'=': '$eq',
                    '!=': '$ne',
                    '<>': '$ne',
                    '>': '$gt',
                    '>=': '$gte',
                    '<': '$lt',
                    '<=': '$lte'}

    def __init__(self, server):
        """Initializes MongoDB class."""
        self.server = server

    def connect(self):
        """Connects to a MongoDB server.

        Returns:
        -------
            True if connection was successful, False otherwise.
        """
        try:
            self.client = _pymongo.MongoClient(self.server)
            return True
        except Exception:
            return False

    def database_exists(self, database):
        """Check if database file exists.

        Args:
        ----
            database (str): database name.

        Return:
        ------
            True if database file exists, False otherwise.

        """

        if database in self.client.list_database_names():
            return True
        else:
            return False

    def create_database_table(self, database, table=None):
        """Create collection, with id as ascending index."""
        pass#

    def database_collection_exists(self, database, collection):
        """Check if table exists in database.

        Args:
        ----
            database (str): database name.
            collection (str, optional): database collection name.

        Return:
        ------
            True if the table exists, False otherwise.

        """
        _db = self.client[database]
        if collection in _db.list_collection_names():
            return True
        else:
            return False

    def get_database_collection_name(self):
        """Return the database table name."""
        return self._db_collection

    def get_database_collection_value(self, database, field, idn,
                                      collection=None):
        """Get field value from entry id.

        Args:
        ----
            database (str): database name.
            field (str): field name.
            idn (int): entry id.
            collection (str, optional): database table name.

        Return:
        ------
            the parameter value.

        """
        if collection is None:
            collection = self._db_collection

        if not self.database_collection_exists(database, collection):
            return None

        if len(self.get_database_collection_field_names(database,
                                                        collection)) == 0:
            return None

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            _doc = _col.find_one({'id': idn})

            return _doc[field]
        except Exception:
            return None

    def get_database_collection_last_id(self, database, collection=None):
        """Returns the last inserted document's id.

        Args:
        ----
            database (str): database name.
            collection (str, optional): database collection name.

        Return:
        ------
            an ObjectId.

        """
        if collection is None:
            collection = self._db_collection

        if not self.database_collection_exists(database, collection):
            return None

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
            _doc = _cursor.next()
            _cursor.close()

            return _doc['_id']
        except Exception:
            _message = "Could not find last collection's document."
            raise DatabaseError(_message)

    def get_database_collection_field_names(self, database, collection=None):
        """Return the field names of the last collection's doctument. Take care
        when using this function since MongoDB is a no-SQL database, it doesn't
        require all documents (entries) to have the same fields. Each document
        can have it's own fields idependent from the other documents inside the
        collection, which may be different from the last document's fields.

        Args:
        ----
            database (str): database name.
            collection (str, optional): database collection name.

        Return:
        ------
            a list with the last document's field names.

        """
        if collection is None:
            collection = self._db_collection
        if not self.database_collection_exists(database, collection):
            return []

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
            _doc = _cursor.next()
            _cursor.close()

            return list(_doc.keys())
        except Exception:
            _message = "Could not find last collection's document."
            raise DatabaseError(_message)

    def get_database_collection_field_types(self, database, collection=None):
        """Return the field types of the last collection's doctument. Take care
        when using this function since MongoDB is a no-SQL database, it doesn't
        require all documents (entries) to have the same fields os types. Each
        document can have it's own fields idependent from the other documents
        inside the collection, which may be different from the last document's
        fields and types.

        Args:
        ----
            database (str): database name.
            collection (str, optional): database collection name.

        Return:
        ------
            a dict with field names and types.

        """
        if collection is None:
            collection = self._db_collection
        if not self.database_collection_exists(database, collection):
            return []

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
            _doc = _cursor.next()
            _cursor.close()

            _field_types = {}
            for _field in _doc:
                _field_types[_field] = _doc[_field]
            return _field_types
        except Exception:
            _message = "Could not find last collection's document."
            raise DatabaseError(_message)

    def get_database_collection_field(self, database, fields, collection=None):
        """Return field values of the database table.

        Args:
        ----
            database (str): database name.
            fields (str or list): string containing the field name or a list of
                                  field names.
            collection (str, optional): database collection name.

        Return:
        ------
            a list of field values
        """
        if collection is None:
            collection = self._db_table

        if not self.database_collection_exists(database, collection):
            return []

        if isinstance(fields, str):
            fields = [fields]

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            _list = list(_col.find(projection=fields))
        except Exception:
            _message = "Failed while trying to retrieve collection fields."
            raise DatabaseError(_message)

        return _list

    def search_database_collection_field(self, database, field, value,
                                         collection=None):
        """Search field in database collection.

        Args:
        ----
            database (str): database name.
            field (str): field to search.
            value (value_type): value to search.
            collection (str, optional): database collection name.

        Return:
        ------
            a list of database entries.

        """
        if collection is None:
            collection = self._db_collection

        if not self.database_collection_exists(database, collection):
            return []

        if len(self.get_database_collection_field_names(database,
                                                        collection)) == 0:
            return []

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            _cursor = _col.find({field: value})
            return list(_cursor)
        except Exception:
            _message = "Failed to search for collection field values."
            raise DatabaseError(_message)

    def filter_database_collection_fields(self, database, fields, filters,
                                          initial_idn=None, max_nr_lines=None,
                                          collection=None):
        """Search paremeter in database.

        Args:
        ----
            database (str): full file path to database.
            fields (list): list of field names to filter.
            filters (list): list of filters to apply (must have the same lengh
                            as 'fields').
            initial_idn (int, optional): initial id to start filter.
            max_nr_lines (int, optional): maximum number of lines.
            collection (str, optional): database collection name.

        Return:
        ------
            a list of database entries.

        """
        if collection is None:
            collection = self._db_collection

        try:
            _field_types = self.get_database_collection_field_types(
                database, collection=collection)

            _limit = max_nr_lines

            _min = None
            if initial_idn is not None:
                _min = ['_id', initial_idn]

            _filters = {}
            for i in range(len(filters)):
                _data_type = _field_types[fields[i]]

                if '~' in filters[i]:
                    _split = filters[i].split('~')
                    _filters[fields[i]] = {'$lte': float(_split[0]),
                                           '$gte': float(_split[1])}
                elif '>=' in filters[i]:
                    _split_str = '>='
                elif '<=' in filters[i]:
                    _split_str = '<='
                elif '<>' in filters[i]:
                    _split_str = '<>'
                elif '>' in filters[i]:
                    _split_str = '>'
                elif '<' in filters[i]:
                    _split_str = '<'
                elif '!=' in filters[i]:
                    _split_str = '!='
                elif '=' in filters[i]:
                    _split_str = '='
                else:
                    _split_str = None

                if _split_str:
                    _operator = _filter_dict[_split_str]
                    _split = filters[i].split(_split_str)[1]
                else:
                    _operator = '$eq'
                    _split = filters[i]

                if 'none' in _split:
                    _value = None
                else:
                    _value = _data_type(_split[1])
                _filters[fields[i]] = {_operator: _value}

            _db = self.client[database]
            _col = getattr(_db, collection)
            _docs = _col.find(filter=_filters, limit=_limit, min=_min)
            return list(_docs)
        except Exception:
            _message = 'Could not filter values in collection {0}.'.format(
                collection)
            raise DatabaseError(_message)

    def read_from_database_collection(self, database, idn=None,
                                      collection=None):
        """Read a document (collection entry) from database.

        Args:
        ----
            database (str): database name.
            idn (int, optional): entry id (returns last id if not specified).
            collection (str, optional): database collection name.

        """
        if collection is None:
            collection = self._db_collection

        if not self.database_collection_exists(database, collection):
            raise DatabaseError('Invalid database collection name.')

        _db_field_names = self.get_database_collection_field_names(
            database, collection)

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            if idn:
                _doc = _col.find_one({'_id': idn})
            else:
                _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
                _doc = _cursor.next()
                _cursor.close()
        except Exception:
            _message = ('Could not retrieve data from {0}'.format(collection))
            raise DatabaseError(_message)

        # creates 'field' key if there isn't one in _db_dict
        if 'field' not in self._db_dict.keys():
            self._db_dict['field'] = self._db_dict['column']

        for _attr_name in self._db_dict:
            _field = self._db_dict[_attr_name]['field']
            _dtype = self._db_dict[_attr_name]['dtype']

            if _field not in _db_field_names:
                raise DatabaseError('Failed to read data from database.')

            try:
                _value = _doc[_field]
                if _dtype == _np.ndarray:
                    setattr(self, _attr_name, _utils.to_array(_value))
                else:
                    setattr(self, _attr_name, _value)
            except AttributeError:
                pass

        if (hasattr(self, '_timestamp') and
                'date' in _db_field_names and 'hour' in _db_field_names):
            _date = _doc['date']
            _hour = _doc['hour']
            self._timestamp = '_'.join([_date, _hour])

    def save_to_database_collection(self, database, collection=None):
        """Insert a doctument into a database collection.

        Args:
        ----
            database (str): database name.
            collection (str, optional): database collection name.

        Return:
        ------
            the id of the inserted document.

        """
        if collection is None:
            collection = self._db_collection

        if len(collection) == 0:
            return None

        if not self.database_collection_exists(database, collection):
            raise DatabaseError('Invalid database collection name.')

        _db_field_names = self.get_database_collection_field_names(database,
                                                                   collection)
        if len(_db_field_names) == 0:
            raise DatabaseError('Failed to save data to database.')

        if hasattr(self, '_timestamp') and self._timestamp is not None:
            _timestamp = self._timestamp.split('_')
        else:
            _timestamp = _utils.get_timestamp().split('_')

        _values = {'date': _timestamp[0],
                   'hour': _timestamp[1]}

        # creates 'field' key if there isn't one in _db_dict
        if 'field' not in self._db_dict.keys():
            self._db_dict['field'] = self._db_dict['column']

        for _attr_name in self._db_dict:
            _field = self._db_dict[_attr_name]['field']
            _dtype = self._db_dict[_attr_name]['dtype']

            if _field not in _db_field_names:
                raise DatabaseError('Failed to save data to database.')

            _value = getattr(self, _attr_name)
            if isinstance(_value, _np.ndarray):
                _value = _value.tolist()
            _values[_attr_name] = _value

        if len(_values) != len(_db_field_names):
            _message = 'Inconsistent number of values for collection \
                {0}.'.format(collection)
            raise DatabaseError(_message)

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)
            _col.insert_one(_values)
        except Exception:
            _message = 'Could not insert values into collection {0}.'.format(
                collection)
            raise DatabaseError(_message)

    def update_database_collection(self, database, idn, collection=None):
        """Update a collection's document from database.

        Args:
        ----
            database (str): database name.
            idn (int): entry id.
            collection (str, optional): database collection name.

        Return:
        ------
            True if update was sucessful.
            False if update failed.

        """
        if collection is None:
            collection = self._db_collection

        if len(collection) == 0:
            return False

        if not self.database_collection_exists(database, collection):
            raise DatabaseError('Invalid database collection name.')

        _db_field_names = self.get_database_collection_field_names(database,
                                                                   collection)
        if len(_db_field_names) == 0:
            raise DatabaseError('Failed to save data to database.')

        if hasattr(self, '_timestamp') and self._timestamp is not None:
            _timestamp = self._timestamp
        else:
            _timestamp = _utils.get_timestamp().split('_')

        _values = {'date': _timestamp[0],
                   'hour': _timestamp[1]}

        # creates 'field' key if there isn't one in _db_dict
        if 'field' not in self._db_dict.keys():
            self._db_dict['field'] = self._db_dict['column']

        for _attr_name in self._db_dict:
            _field = self._db_dict[_attr_name]['field']
            _dtype = self._db_dict[_attr_name]['dtype']

            if _field not in _db_field_names:
                raise DatabaseError('Failed to read data from database.')

            _value = getattr(self, _attr_name)
            if isinstance(_value, _np.ndarray):
                _value = _value.tolist()
            _values[_attr_name] = _value

        try:
            _db = self.client[database]
            _col = getattr(_db, collection)

            if idn is not None:
                _col.update_one({'_id': idn},
                                {'$set': _values})
                return True
            else:
                _message = 'Invalid entry id.'
                raise DatabaseError(_message)

        except Exception:
            _message = ('Could not update {0} entry.'.format(collection))
            raise DatabaseError(_message)
