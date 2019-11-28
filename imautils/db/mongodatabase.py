"""MongoDB interface module."""

import numpy as _np
import pymongo as _pymongo

from . import utils as _utils


class MongoDatabaseError(Exception):
    """Monog database exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


def connect(server='localhost'):
    """Connect to a MongoDB server.

    Returns:
        a MongoClient instance.

    """
    return _pymongo.MongoClient(server)


def database_exists(client, database_name):
    """Check if database exists.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.

    Returns:
        True if database exists, False otherwise.

    """
    return database_name in client.list_database_names()


def get_collections(client, database_name):
    """Get collection names.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.

    Returns:
        a list of collection names.

    """
    return client[database_name].list_collection_names()


def collection_exists(client, database_name, collection_name):
    """Check if collection exists in database.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        True if the collection exists, False otherwise.

    """
    return collection_name in client[database_name].list_collection_names()


def create_collection(client, database_name, collection_name):
    """Create collection, with id as ascending index.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        True if the collection exists, False otherwise.

    """
    if len(collection_name) == 0:
        return False

    if not collection_exists(client, database_name, collection_name):
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _col.create_index([('id', _pymongo.ASCENDING)], unique=True)

    return True


def get_field_names(client, database_name, collection_name):
    """Return the field names of the last collection's document.

    Take care when using this function since MongoDB is a no-SQL database,
    it doesn't require all documents (entries) to have the same fields.
    Each document can have it's own fields idependent from the other documents
    inside the collection, which may be different from the last document's
    fields.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        a list with the last document's field names.

    """
    if not collection_exists(client, database_name, collection_name):
        return []

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
        _doc = _cursor.next()
        _cursor.close()
        _list = list(_doc.keys())
        _list.remove('_id')

        return _list
    except Exception:
        _message = "Could not find last collection's document."
        raise MongoDatabaseError(_message)


def get_field_types(client, database_name, collection_name):
    """Return the field types of the last collection's document.

    Take care when using this function since MongoDB is a no-SQL database,
    it doesn't require all documents (entries) to have the same fields os
    types. Each document can have it's own fields independent from the other
    documents inside the collection, which may be different from the last
    document's fields and types.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        a dict with field names and types.

    """
    if not collection_exists(client, database_name, collection_name):
        return []

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
        _doc = _cursor.next()
        _cursor.close()

        _field_types = {}
        for _field in _doc:
            _field_types[_field] = type(_doc[_field])
        return _field_types
    except Exception:
        _message = "Could not find last collection's document."
        raise MongoDatabaseError(_message)


def get_first_id(client, database_name, collection_name):
    """Return the first inserted document's id.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        an id.

    """
    if not collection_exists(client, database_name, collection_name):
        return None

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _cursor = _col.find().sort('_id', _pymongo.ASCENDING)
        _doc = _cursor.next()
        _cursor.close()

        return _doc['id']
    except Exception:
        _message = "Could not find last collection's document."
        raise MongoDatabaseError(_message)


def get_last_id(client, database_name, collection_name):
    """Return the last inserted document's id.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        an id.

    """
    if not collection_exists(client, database_name, collection_name):
        return None

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
        _doc = _cursor.next()
        _cursor.close()

        return _doc['id']
    except Exception:
        _message = "Could not find last collection's document."
        raise MongoDatabaseError(_message)


def get_values(client, database_name, collection_name, field):
    """Return field values of the database table.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        field (str): string containing the field name.

    Returns:
        a list of field values.

    """
    if not collection_exists(client, database_name, collection_name):
        return []

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _cursor = _col.find(projection=[field])
        _docs = list(_cursor)
        _values = [doc[field] for doc in _docs]
        _cursor.close()
    except Exception:
        _message = "Failed while trying to retrieve collection fields."
        raise MongoDatabaseError(_message)

    return _values


def get_value(client, database_name, collection_name, field, idn):
    """Get field value from entry id.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        field (str): field name.
        idn (int): entry id.

    Returns:
        the parameter value.

    """
    if not collection_exists(client, database_name, collection_name):
        return None

    if len(get_field_names(client, database_name, collection_name)) == 0:
        return None

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _doc = _col.find_one({'id': idn})

        return _doc[field]
    except Exception:
        return None


def search_field(client, database_name, collection_name, field, value):
    """Search field in database collection.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        field (str): field to search.
        value (value_type): value to search.

    Returns:
        a list of dicts with database entries.

    """
    if not collection_exists(client, database_name, collection_name):
        return []

    if len(get_field_names(client, database_name, collection_name)) == 0:
        return []

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _cursor = _col.find({field: value})
        _docs = list(_cursor)
        _cursor.close()
        return _docs
    except Exception:
        _message = "Failed to search for collection field values."
        raise MongoDatabaseError(_message)


def search_collection(
        client, database_name, collection_name, fields, filters=None,
        initial_idn=None, max_nr_lines=None):
    """Filter collection entries.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        fields (list): list of field names to filter.
        filters (list, optional): list of filters to apply (must have the same
                                  lengh as 'fields').
        initial_idn (int, optional): initial id to start filter.
        max_nr_lines (int, optional): maximum number of lines.

    Returns:
        a list of dicts with database entries.

    """
    filter_dict = {
        '=': '$eq',
        '!=': '$ne',
        '<>': '$ne',
        '>': '$gt',
        '>=': '$gte',
        '<': '$lt',
        '<=': '$lte',
    }

    try:
        _field_types = get_field_types(client, database_name, collection_name)

        if max_nr_lines is None:
            _limit = 0
        else:
            _limit = max_nr_lines

        _min = None
        if initial_idn is not None:
            _min = {'id': initial_idn}

        if filters is None:
            filters_list = []
        else:
            filters_list = [str(f) for f in filters]

        _filters_dict = {}
        for i, _f in enumerate(filters_list):
            _data_type = _field_types[fields[i]]
            _f = _f.replace(' ', '')

            _split_str = None
            if '~' in _f:
                _split_str = '~'
                _split = _f.split('~')
                _filters_dict[fields[i]] = {
                    '$gte': float(_split[0]),
                    '$lte': float(_split[1])}
            elif '>=' in _f:
                _split_str = '>='
            elif '<=' in _f:
                _split_str = '<='
            elif '<>' in _f:
                _split_str = '<>'
            elif '>' in _f:
                _split_str = '>'
            elif '<' in _f:
                _split_str = '<'
            elif '!=' in _f:
                _split_str = '!='
            elif '=' in _f:
                _split_str = '='

            if _split_str != '~':
                if _split_str is not None:
                    _operator = filter_dict[_split_str]
                    _split = _f.split(_split_str)[1]
                else:
                    if _data_type is str:
                        _operator = '$regex'
                    else:
                        _operator = '$eq'
                    _split = _f

                if len(_split) != 0:
                    if 'none' in _split.lower():
                        _value = None
                    else:
                        _value = _data_type(_split)
                    _filters_dict[fields[i]] = {_operator: _value}

        _db = client[database_name]
        _col = getattr(_db, collection_name)
        if len(filters_list) != 0:
            _cursor = _col.find(
                projection=fields,
                filter=_filters_dict,
                limit=_limit,
                sort=[('id', -1)],
                min=_min,
                hint=[('id', 1)])
        else:
            _cursor = _col.find(
                projection=fields,
                limit=_limit,
                sort=[('id', -1)],
                min=_min,
                hint=[('id', 1)])
        _docs = list(_cursor)[::-1]
        _cursor.close()
        return _docs

    except Exception:
        _message = 'Could not filter values in collection {0}.'.format(
            collection_name)
        raise MongoDatabaseError(_message)


def save_to_database(document):
    """Insert a document into a database collection.

    Args:
        document: object derived from DatabaseDocument object.

    Returns:
        True if update was sucessful, False otherwise.

    """
    client = document.client
    database_name = document.database_name
    collection_name = document.collection_name
    db_dict = document.db_dict

    if len(collection_name) == 0:
        return None

    if not collection_exists(client, database_name, collection_name):
        raise MongoDatabaseError('Invalid database collection name.')

    reverse_db_dict = {}
    for k, v in db_dict.items():
        reverse_db_dict[v['field']] = k

    _db = client[database_name]
    _col = getattr(_db, collection_name)

    if _col.estimated_document_count() != 0:
        _db_field_names = get_field_names(
            client, database_name, collection_name)
        _id = get_last_id(client, database_name, collection_name)
        _id = _id + 1
    else:
        if 'id' in reverse_db_dict.keys():
            _db_field_names = []
        else:
            _db_field_names = ['id']
        for _item in db_dict:
            _db_field_names.append(db_dict[_item]['field'])
        _id = 1

    if len(_db_field_names) == 0:
        raise MongoDatabaseError('Failed to save data to database.')

    _values = {}

    timestamp_split = _utils.get_timestamp().split('_')
    date = timestamp_split[0]
    hour = timestamp_split[1].replace('-', ':')

    if 'id' in reverse_db_dict.keys():
        setattr(document, reverse_db_dict['id'], _id)
    else:
        _values['id'] = _id

    if 'date' in reverse_db_dict.keys():
        if getattr(document, reverse_db_dict['date']) is None:
            setattr(document, reverse_db_dict['date'], date)

    if 'hour' in reverse_db_dict.keys():
        if getattr(document, reverse_db_dict['hour']) is None:
            setattr(document, reverse_db_dict['hour'], hour)

    for _attr_name in db_dict:
        _field = db_dict[_attr_name]['field']

        if _field not in _db_field_names:
            print(_field, _db_field_names)
            raise MongoDatabaseError('Failed to save data to database.')

        _value = getattr(document, _attr_name)
        if isinstance(_value, _np.ndarray):
            _value = _value.tolist()
        _values[_field] = _value

    if len(_values) != len(_db_field_names):
        print(len(_values), len(_db_field_names))
        _message = 'Inconsistent number of values for collection \
            {0}.'.format(collection_name)
        raise MongoDatabaseError(_message)

    try:
        _col.insert_one(_values)
        return True
    except Exception:
        _message = 'Could not insert values into collection {0}.'.format(
            collection_name)
        raise MongoDatabaseError(_message)


def read_from_database(document, idn=None):
    """Read a document (collection entry) from database.

    Args:
        document: object derived from DatabaseDocument object.
        idn (int, optional): entry id (returns last id if not specified).

    Returns:
        True if update was sucessful, False otherwise.

    """
    client = document.client
    database_name = document.database_name
    collection_name = document.collection_name
    db_dict = document.db_dict

    if not collection_exists(client, database_name, collection_name):
        raise MongoDatabaseError('Invalid database collection name.')

    _db_field_names = get_field_names(client, database_name, collection_name)

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        if idn is not None:
            _doc = _col.find_one({'id': idn})
        else:
            _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
            _doc = _cursor.next()
            _cursor.close()
    except Exception:
        _message = ('Could not retrieve data from {0}'.format(collection_name))
        raise MongoDatabaseError(_message)

    for _attr_name in db_dict:
        _field = db_dict[_attr_name]['field']
        _dtype = db_dict[_attr_name]['dtype']

        if _field not in _db_field_names:
            raise MongoDatabaseError('Failed to read data from database.')

        try:
            _value = _doc[_field]
            if _dtype == _np.ndarray:
                setattr(document, _attr_name, _utils.to_array(_value))
            else:
                setattr(document, _attr_name, _value)
        except AttributeError:
            pass

    if (hasattr(document, 'timestamp') and
            'date' in _db_field_names and 'hour' in _db_field_names):
        _date = _doc['date']
        _hour = _doc['hour']
        document.timestamp = '_'.join([_date, _hour])

    return True


def update_database(document, idn):
    """Update a collection's document from database.

    Args:
        document: object derived from DatabaseDocument object.
        idn (int): entry id.

    Returns:
        True if update was sucessful, False if update failed.

    """
    client = document.client
    database_name = document.database_name
    collection_name = document.collection_name
    db_dict = document.db_dict

    if len(collection_name) == 0:
        return False

    if not collection_exists(client, database_name, collection_name):
        raise MongoDatabaseError('Invalid database collection name.')

    _db_field_names = get_field_names(client, database_name, collection_name)
    if len(_db_field_names) == 0:
        raise MongoDatabaseError('Failed to update database.')

    _values = {}

    reverse_db_dict = {}
    for k, v in db_dict.items():
        reverse_db_dict[v['field']] = k

    timestamp_split = _utils.get_timestamp().split('_')
    date = timestamp_split[0]
    hour = timestamp_split[1].replace('-', ':')

    if 'id' in reverse_db_dict.keys():
        setattr(document, reverse_db_dict['id'], idn)

    if 'date' in reverse_db_dict.keys():
        if getattr(document, reverse_db_dict['date']) is None:
            setattr(document, reverse_db_dict['date'], date)

    if 'hour' in reverse_db_dict.keys():
        if getattr(document, reverse_db_dict['hour']) is None:
            setattr(document, reverse_db_dict['hour'], hour)

    for _attr_name in db_dict:
        _field = db_dict[_attr_name]['field']

        if _field not in _db_field_names:
            raise MongoDatabaseError('Failed to read data from database.')

        _value = getattr(document, _attr_name)
        if isinstance(_value, _np.ndarray):
            _value = _value.tolist()
        _values[_field] = _value

    try:
        _db = client[database_name]
        _col = getattr(_db, collection_name)

        if idn is not None:
            _col.update_one({'id': idn},
                            {'$set': _values})
            return True
        else:
            _message = 'Invalid entry id.'
            raise MongoDatabaseError(_message)

    except Exception:
        _message = ('Could not update {0} entry.'.format(collection_name))
        raise MongoDatabaseError(_message)
