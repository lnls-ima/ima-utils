# -*- coding: utf-8 -*-

"""Implementation of functions to handle database documents."""

import collections as _collections

from . import sqlitedatabase as _sqlitedatabase
from . import mongodatabase as _mongodatabase


class Database():
    """API for MongoDB or Sqlite database."""

    def __init__(
            self, database_name=None, mongo=True, server='localhost'):
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
            self.client = _mongodatabase.connect(server=self._server)
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
            self.client = _mongodatabase.connect(server=self._server)

    def database_exists(self):
        """Check if database exists.

        Returns:
            True if database exists, False otherwise.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self._server)
            return _mongodatabase.database_exists(
                self.client, self.database_name)
        else:
            return _sqlitedatabase.database_exists(
                self.database_name)

    def get_collections(self):
        """Get database collection names.

        Returns:
            a list with collection names.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self._server)
            return _mongodatabase.get_collections(
                self.client, self.database_name)
        else:
            return _sqlitedatabase.get_tables(self.database_name)


class DatabaseCollection(Database):
    """API for MongoDB collection or Sqlite database table."""

    def __init__(
            self, database_name=None, collection_name=None,
            mongo=True, server='localhost'):
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

    def collection_exists(self):
        """Check if the collection exists in database.

        Returns:
            True if the collection exists, False otherwise.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.collection_exists(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.table_exists(
                self.database_name, self.collection_name)

    def create_collection(self):
        """Create collection, with id as ascending index."""
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.create_collection(
                self.client, self.database_name, self.collection_name)
        else:
            msg = 'Empty tables are not supported in SQLite'
            raise NotImplementedError(msg)

    def get_field_names(self):
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
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.get_field_names(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.get_column_names(
                self.database_name, self.collection_name)

    def get_field_types(self):
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
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.get_field_types(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.get_column_types(
                self.database_name, self.collection_name)

    def get_first_id(self):
        """Return the first document's id.

        Returns:
            a id.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.get_first_id(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.get_first_id(
                self.database_name, self.collection_name)

    def get_last_id(self):
        """Return the last document's id.

        Returns:
            a id.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.get_last_id(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.get_last_id(
                self.database_name, self.collection_name)

    def get_values(self, field):
        """Return field values of the database collection.

        Args:
            field (str): field name.

        Returns:
            a list of field values.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.get_values(
                self.client, self.database_name, self.collection_name, field)
        else:
            return _sqlitedatabase.get_values(
                self.database_name, self.collection_name, field)

    def get_value(self, field, idn):
        """Get field value from entry id.

        Args:
            field (str): field name.
            idn (int): entry id.

        Returns:
            the parameter value.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.get_value(
                self.client, self.database_name,
                self.collection_name, field, idn)
        else:
            return _sqlitedatabase.get_value(
                self.database_name, self.collection_name, field, idn)

    def search_field(self, field, value):
        """Search field in database collection.

        Args:
            field (str): field to search.
            value (value_type): value to search.

        Returns:
            a list of database entries.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.search_field(
                self.client, self.database_name,
                self.collection_name, field, value)
        else:
            return _sqlitedatabase.search_column(
                self.database_name, self.collection_name, field, value)

    def search_collection(
            self, fields, filters=None, initial_idn=None, max_nr_lines=None):
        """Filter collection entries.

        Args:
            fields (list): list of field names to filter.
            filters (list, optional): list of filters to apply (must have the
                                      same lengh as 'fields').
            initial_idn (int, optional): initial id to start filter.
            max_nr_lines (int, optional): maximum number of lines.

        Returns:
            a list of database entries.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.search_collection(
                self.client, self.database_name, self.collection_name,
                fields, filters, initial_idn=initial_idn,
                max_nr_lines=max_nr_lines)
        else:
            return _sqlitedatabase.search_table(
                self.database_name, self.collection_name,
                fields, filters, initial_idn=initial_idn,
                max_nr_lines=max_nr_lines)


class DatabaseDocument(DatabaseCollection):
    """Database document or record."""

    collection_name = ''
    db_dict = _collections.OrderedDict([
        ('idn', {'field': 'id', 'dtype': int, 'not_null': True}),
        ('date', {'field': 'date', 'dtype': str, 'not_null': True}),
        ('hour', {'field': 'hour', 'dtype': str, 'not_null': True}),
    ])
    # Example of db_dict:
    # db_dict = _collections.OrderedDict([
    #     ('attribute_name', {
    #         'field': 'field_name', 'dtype': str, 'not_null': True}),
    # ])

    def __init__(
            self, database_name=None, idn=None,
            mongo=True, server='localhost'):
        """Initialize the object.

        Args:
            database_name (str): database name.
            idn (int): id in database table (sqlite) / collection (mongo).
            mongo (bool): flag indicating mongoDB (True) or sqlite (False).
            server (str): MongoDB server.

        """
        self.idn = idn
        self.date = None
        self.hour = None
        super().__init__(
            database_name=database_name,
            collection_name=self.collection_name,
            mongo=mongo,
            server=server)

    def create_collection(self):
        """Create collection, with id as ascending index."""
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.create_collection(
                self.client, self.database_name, self.collection_name)
        else:
            return _sqlitedatabase.create_table(
                self.database_name, self.collection_name, self.db_dict)

    def save_to_database(self):
        """Insert a document into a database collection.

        Returns:
            True if update was sucessful, False otherwise.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.save_to_database(self)
        else:
            return _sqlitedatabase.save_to_database(self)

    def read_from_database(self, idn=None):
        """Read a document (collection entry) from database.

        Args:
            idn (int, optional): entry id (returns last id if not specified).

        Returns:
            True if update was sucessful, False otherwise.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.read_from_database(self, idn=idn)
        else:
            return _sqlitedatabase.read_from_database(self, idn=idn)

    def update_database(self, idn):
        """Update a collection's document from database.

        Args:
            idn (int): entry id.

        Returns:
            True if update was sucessful, False if update failed.

        """
        if self.mongo:
            if self.client is None:
                self.client = _mongodatabase.connect(server=self.server)
            return _mongodatabase.update_database(self, idn)
        else:
            return _sqlitedatabase.update_database(self, idn)
