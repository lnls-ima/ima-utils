"""MongoDB interface module."""

import numpy as _np
import pymongo as _pymongo

from . import utils as _utils


class MongoDatabaseError(Exception):
    """Monog database exception."""

    def __init__(self, message, *args):
        """Initialize object."""
        self.message = message


def db_connect(server='localhost'):
    """Connect to a MongoDB server.

    Returns:
        a MongoClient instance.

    """
    return _pymongo.MongoClient(server)


def db_database_exists(client, database_name):
    """Check if database exists.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.

    Returns:
        True if database exists, False otherwise.

    """
    if not isinstance(client, _pymongo.MongoClient):
        msg = 'Invalid mongo client.'
        raise MongoDatabaseError(msg)
    
    if database_name is None or len(database_name) == 0:
        msg = 'Invalid database name.'
        raise MongoDatabaseError(msg)    
    
    return database_name in client.list_database_names()


def db_get_collections(client, database_name):
    """Get collection names.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.

    Returns:
        a list of collection names.

    """
    if not db_database_exists(client, database_name):
        msg = 'Database not found.'
        raise MongoDatabaseError(msg)
    
    return client[database_name].list_collection_names()


def db_collection_exists(client, database_name, collection_name):
    """Check if collection exists in database.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        True if the collection exists, False otherwise.

    """   
    if collection_name is None or len(collection_name) == 0:
        msg = 'Invalid collection name.'
        raise MongoDatabaseError(msg)
    
    if not db_database_exists(client, database_name):
        return False
    
    return collection_name in client[database_name].list_collection_names()


def db_create_collection(client, database_name, collection_name):
    """Create collection, with id as ascending index.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        True if successful, False otherwise.

    """
    if not db_collection_exists(client, database_name, collection_name):
        _db = client[database_name]
        _col = getattr(_db, collection_name)
        _col.create_index([('id', _pymongo.ASCENDING)], unique=True)
    
    return True


def db_get_field_names(client, database_name, collection_name):
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
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return []

    _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
    _doc = _cursor.next()
    _cursor.close()
    _list = list(_doc.keys())
    _list.remove('_id')

    return _list


def db_get_field_types(client, database_name, collection_name):
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
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return {}

    _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
    _doc = _cursor.next()
    _cursor.close()

    _field_types = {}
    for _field in _doc:
        _field_types[_field] = type(_doc[_field])
    return _field_types


def db_get_first_id(client, database_name, collection_name):
    """Return the first inserted document's id.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        an id.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return None

    _cursor = _col.find().sort('_id', _pymongo.ASCENDING)
    _doc = _cursor.next()
    _cursor.close()

    return _doc['id']


def db_get_last_id(client, database_name, collection_name):
    """Return the last inserted document's id.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.

    Returns:
        an id.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return None

    _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
    _doc = _cursor.next()
    _cursor.close()

    return _doc['id']


def db_delete(client, database_name, collection_name, idns):
    """Delete documents from database collection.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        idns (list): list of document ids.

    Returns:
        True if successful, False otherwise.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    if idns is None or len(idns) == 0:
        msg = 'Invalid document ids.'
        raise MongoDatabaseError(msg)    

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return False
    
    result =_col.delete_many({'id': { '$in': idns}})
    if result.deleted_count == len(idns):
        return True
    else:
        return False


def db_get_values(client, database_name, collection_name, field):
    """Return field values of the database table.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        field (str): string containing the field name.

    Returns:
        a list of field values.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    if field is None or len(field) == 0:
        msg = 'Invalid field name.'
        raise MongoDatabaseError(msg)

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return []
         
    _cursor = _col.find(projection=[field])
    _docs = list(_cursor)
    _values = [doc[field] for doc in _docs]
    _cursor.close()

    return _values


def db_get_value(client, database_name, collection_name, field, idn):
    """Get field value from entry id.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        field (str): field name.
        idn (int): document id.

    Returns:
        the parameter value.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    if field is None or len(field) == 0:
        msg = 'Invalid field name.'
        raise MongoDatabaseError(msg)

    if idn is None:
        msg = 'Invalid id number.'
        raise MongoDatabaseError(msg)  

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return None

    try:
        _doc = _col.find_one({'id': idn})
        return _doc[field]
    
    except Exception:
        return None


def db_search_field(client, database_name, collection_name, field, value):
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
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    if field is None or len(field) == 0:
        msg = 'Invalid field name.'
        raise MongoDatabaseError(msg)

    if value is None:
        msg = 'Invalid value to search.'
        raise MongoDatabaseError(msg)  

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return []

    _cursor = _col.find({field: value})
    _docs = list(_cursor)
    _cursor.close()
    
    return _docs


def db_search_collection(
        client, database_name, collection_name, fields=None, filters=None,
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
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    if fields is None or len(fields) == 0:
        fields = db_get_field_names(client, database_name, collection_name)
    
    if isinstance(fields, str):
        fields = [fields]
    
    filter_dict = {
        '=': '$eq',
        '!=': '$ne',
        '<>': '$ne',
        '>': '$gt',
        '>=': '$gte',
        '<': '$lt',
        '<=': '$lte',
    }

    if filters is None or len(filters) == 0:
        filters_list = []
    else:
        filters_list = [str(f) for f in filters]

    if max_nr_lines == 0:
        return []
    
    if max_nr_lines is None:
        _limit = 0
    else:
        _limit = max_nr_lines    
    
    _min = None
    if initial_idn is not None:
        _min = {'id': initial_idn}

    _field_types = db_get_field_types(client, database_name, collection_name)
    if len(_field_types) == 0:
        return []
    
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


def db_save(client, database_name, collection_name, values_dict):
    """Insert a document into a database collection.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        values_dict (str): dict with values to save in database.

    Returns:
        The id of the saved database document.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    if values_dict is None or len(values_dict) == 0:
        msg = 'Invalid values to save in database.'
        raise MongoDatabaseError(msg)

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()

    if _count == 0:
        idn = 1
    else:
        idn = db_get_last_id(client, database_name, collection_name)
        idn = idn + 1

    _values = {}
    for key, value in values_dict.items():
        _values[key] = value
    _values['id'] = idn

    _col.insert_one(_values)
    return idn


def db_read(client, database_name, collection_name, idn=None):
    """Read a document (collection entry) from database.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        idn (int, optional): entry id (returns last id if not specified).

    Returns:
        a dict with values read from database.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()
    
    if _count == 0:
        return {}
    
    if idn is not None:
        _doc = _col.find_one({'id': idn})
    else:
        _cursor = _col.find().sort('_id', _pymongo.DESCENDING)
        _doc = _cursor.next()
        _cursor.close()

    _doc.pop('_id', None)
    return _doc


def db_update(client, database_name, collection_name, values_dict, idn):
    """Update a collection's document from database.

    Args:
        client (MongoClient): a MongoClient instance.
        database_name (str): database name.
        collection_name (str): database collection name.
        idn (int): entry id.

    Returns:
        True if update was sucessful, False if update failed.

    """
    if not db_collection_exists(client, database_name, collection_name):
        msg = 'Database collection not found.'
        raise MongoDatabaseError(msg)

    if values_dict is None or len(values_dict) == 0:
        msg = 'Invalid values to save in database.'
        raise MongoDatabaseError(msg)

    if idn is None:
        msg = 'Invalid id number.'
        raise MongoDatabaseError(msg)  

    _db = client[database_name]
    _col = getattr(_db, collection_name)
    _count = _col.estimated_document_count()

    if _count == 0:
        return False

    _values = {}
    for key, value in values_dict.items():
        _values[key] = value
    _values['id'] = idn
    
    _col.update_one({'id': idn},
                    {'$set': _values})
    return True

