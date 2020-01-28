
import os
import json
import unittest
import pymongo
import numpy as np
import collections

from imautils.db import database as dbm


_TEST_PATH = os.path.dirname(__file__)
_MONGO_SERVER = 'localhost'  # '127.0.0.1:27017'


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.mongo_server = _MONGO_SERVER
        self.mongo_database_name = 'mongo_database'

        self.sqlite_database_name = os.path.join(
            _TEST_PATH, 'sqlite_database.db')

        self.mongo_db = dbm.Database(
            database_name=self.mongo_database_name,
            mongo=True,
            server=self.mongo_server)

        self.sqlite_db = dbm.Database(
            database_name=self.sqlite_database_name,
            mongo=False)

    def tearDown(self):
        try:
            self.mongo_db.client.drop_database(self.mongo_database_name)
            os.remove(self.sqlite_database_name)
        except Exception:
            pass

    def test_db_database_exists(self):
        self.assertFalse(self.mongo_db.db_database_exists())
        self.assertFalse(self.sqlite_db.db_database_exists())

    def test_db_get_collections(self):
        with self.assertRaises(Exception):
            collection_names = self.mongo_db.db_get_collections()

        with self.assertRaises(Exception):
            collection_names = self.sqlite_db.db_get_collections()


class TestDatabaseCollection(unittest.TestCase):

    def setUp(self):
        self.mongo_server = _MONGO_SERVER
        self.mongo_database_name = 'mongo_database'

        self.sqlite_database_name = os.path.join(
            _TEST_PATH, 'sqlite_database.db')

        self.collection_name = "test_collection"

        self.mongo_db = dbm.DatabaseCollection(
            database_name=self.mongo_database_name,
            collection_name=self.collection_name,
            mongo=True,
            server=self.mongo_server)
        self.mongo_db.db_create_collection()

        self.sqlite_db = dbm.DatabaseCollection(
            database_name=self.sqlite_database_name,
            collection_name=self.collection_name,
            mongo=False)

    def tearDown(self):
        try:
            self.mongo_db.client.drop_database(self.mongo_database_name)
            os.remove(self.sqlite_database_name)
        except Exception:
            pass

    def test_db_database_exists(self):
        self.assertTrue(self.mongo_db.db_database_exists())
        self.assertFalse(self.sqlite_db.db_database_exists())

    def test_db_get_collections(self):
        collection_names = self.mongo_db.db_get_collections()
        self.assertEqual(len(collection_names), 1)
        self.assertEqual(collection_names[0], self.collection_name)

        with self.assertRaises(Exception):
            collection_names = self.sqlite_db.db_get_collections()

    def test_db_collection_exists(self):
        self.assertTrue(self.mongo_db.db_collection_exists())

        with self.assertRaises(Exception):
            self.sqlite_db.db_collection_exists()

    def test_db_get_field_names(self):
        field_names = self.mongo_db.db_get_field_names()
        self.assertEqual(len(field_names), 0)

        with self.assertRaises(Exception):
            self.sqlite_db.db_get_field_names()

    def test_db_get_field_types(self):
        field_types = self.mongo_db.db_get_field_types()
        self.assertEqual(len(field_types), 0)

        with self.assertRaises(Exception):
            self.sqlite_db.db_get_field_types()

    def test_db_get_first_id(self):
        idn = self.mongo_db.db_get_first_id()
        self.assertIsNone(idn)

        with self.assertRaises(Exception):
            self.sqlite_db.db_get_first_id()

    def test_db_get_last_id(self):
        idn = self.mongo_db.db_get_last_id()
        self.assertIsNone(idn)

        with self.assertRaises(Exception):
            self.sqlite_db.db_get_last_id()

    def test_db_get_values(self):
        values = self.mongo_db.db_get_values("field_name")
        self.assertEqual(len(values), 0)

        with self.assertRaises(Exception):
            self.sqlite_db.db_get_values("field_name")

    def test_db_get_values(self):
        value = self.mongo_db.db_get_value("field_name", 1)
        self.assertIsNone(value)

        with self.assertRaises(Exception):
            self.sqlite_db.db_get_value("field_name", 1)

    def test_db_search_field(self):
        entries = self.mongo_db.db_search_field("field_name", "field_value")
        self.assertEqual(len(entries), 0)

        with self.assertRaises(Exception):
            self.sqlite_db.db_search_field("field_name", "field_value")

    def test_db_search_collection(self):
        entries = self.mongo_db.db_search_collection()
        self.assertEqual(len(entries), 0)

        entries = self.mongo_db.db_search_collection(fields=['id'])
        self.assertEqual(len(entries), 0)

        entries = self.mongo_db.db_search_collection(
            fields=['id'], filters=[1], initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 0)

        with self.assertRaises(Exception):
            self.sqlite_db.db_search_collection()


class TestDatabaseDocument(unittest.TestCase):

    def setUp(self):
        self.mongo_server = _MONGO_SERVER
        self.mongo_database_name = 'mongo_database'

        self.sqlite_database_name = os.path.join(
            _TEST_PATH, 'sqlite_database.db')

        self.collection_name = "test_collection"
        self.label = self.collection_name.upper()
        self.db_dict = collections.OrderedDict([
            ('idn', {'field': 'id', 'dtype': int, 'not_null': True}),
            ('date', {'field': 'date', 'dtype': str}),
            ('hour', {'field': 'hour', 'dtype': str}),
            ('str_attr', {'field': 'str_attr', 'dtype': str}),
            ('dict_attr', {'field': 'dict_attr', 'dtype': dict}),
            ('list_attr', {'field': 'list_attr', 'dtype': list}),
            ('tuple_attr', {'field': 'tuple_attr', 'dtype': tuple}),
            ('array_attr', {'field': 'array_attr', 'dtype': np.ndarray}),
        ])
        self.field_names = [d['field'] for d in self.db_dict.values()]
        self.field_types = [d['dtype'] for d in self.db_dict.values()]
        self.values_1 = [
            1,
            '2020-01-14',
            '10:00:00',
            'string',
            {'a':1, 'b':2, 'c':3},
            [1, 2, 3],
            (10, 20, 30),
            np.array([100, 200, 300])]
        self.values_2 = [
            2,
            '2020-01-15',
            '10:00:00',
            'new_string',
            {'a':4, 'b':5, 'c':6},
            [4, 5, 6],
            (40, 50, 60),
            np.array([400, 500, 600])]

        self.mongo_db = dbm.DatabaseDocument(
            database_name=self.mongo_database_name,
            mongo=True,
            server=self.mongo_server)
        self.mongo_db.label = self.label
        self.mongo_db.collection_name = self.collection_name
        self.mongo_db.db_dict = self.db_dict
        self.mongo_db.db_create_collection()
        for attr in self.db_dict.keys():
            setattr(self.mongo_db, attr, None)

        self.sqlite_db = dbm.DatabaseDocument(
            database_name=self.sqlite_database_name,
            mongo=False)
        self.sqlite_db.label = self.label
        self.sqlite_db.collection_name = self.collection_name
        self.sqlite_db.db_dict = self.db_dict
        self.sqlite_db.db_create_collection()
        for attr in self.db_dict.keys():
            setattr(self.sqlite_db, attr, None)

        self.fn = 'filename.txt'

    def tearDown(self):
        try:
            self.mongo_db.client.drop_database(self.mongo_database_name)
            os.remove(self.sqlite_database_name)
            os.remove(self.fn)
        except Exception:
            pass

    def test_db_database_exists(self):
        self.assertTrue(self.mongo_db.db_database_exists())
        self.assertTrue(self.sqlite_db.db_database_exists())

    def test_db_get_collections(self):
        collection_names = self.mongo_db.db_get_collections()
        self.assertEqual(len(collection_names), 1)
        self.assertEqual(collection_names[0], self.collection_name)

        collection_names = self.sqlite_db.db_get_collections()
        self.assertEqual(len(collection_names), 1)
        self.assertEqual(collection_names[0], self.collection_name)

    def test_db_collection_exists(self):
        self.assertTrue(self.mongo_db.db_collection_exists())
        self.assertTrue(self.sqlite_db.db_collection_exists())

    def test_db_get_field_names(self):
        field_names = self.mongo_db.db_get_field_names()
        self.assertEqual(len(field_names), 0)

        field_names = self.sqlite_db.db_get_field_names()
        self.assertEqual(len(field_names), 8)

    def test_db_get_field_types(self):
        field_types = self.mongo_db.db_get_field_types()
        self.assertEqual(len(field_types), 0)

        field_types = self.sqlite_db.db_get_field_types()
        self.assertEqual(len(field_types), 8)

    def test_db_get_first_id(self):
        idn = self.mongo_db.db_get_first_id()
        self.assertIsNone(idn)

        idn = self.sqlite_db.db_get_first_id()
        self.assertIsNone(idn)

    def test_db_get_last_id(self):
        idn = self.mongo_db.db_get_last_id()
        self.assertIsNone(idn)

        idn = self.sqlite_db.db_get_last_id()
        self.assertIsNone(idn)

    def test_db_get_values(self):
        values = self.mongo_db.db_get_values("date")
        self.assertEqual(len(values), 0)

        values = self.sqlite_db.db_get_values("date")
        self.assertEqual(len(values), 0)

    def test_db_get_values(self):
        value = self.mongo_db.db_get_value("date", 1)
        self.assertIsNone(value)

        value = self.sqlite_db.db_get_value("date", 1)
        self.assertIsNone(value)

    def test_db_search_field(self):
        entries = self.mongo_db.db_search_field("date", "2020-01-14")
        self.assertEqual(len(entries), 0)

        entries = self.sqlite_db.db_search_field("date", "2020-01-14")
        self.assertEqual(len(entries), 0)

    def test_db_search_collection(self):
        entries = self.mongo_db.db_search_collection()
        self.assertEqual(len(entries), 0)

        entries = self.mongo_db.db_search_collection(fields=['id'])
        self.assertEqual(len(entries), 0)

        entries = self.sqlite_db.db_search_collection(
            fields=['id'], filters=[1], initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 0)

        entries = self.sqlite_db.db_search_collection()
        self.assertEqual(len(entries), 0)

        entries = self.sqlite_db.db_search_collection(fields=['id'])
        self.assertEqual(len(entries), 0)

        entries = self.sqlite_db.db_search_collection(
            fields=['id'], filters=[1], initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 0)

    def test_db_save_read_update(self):
        # mongo
        db_doc = self.mongo_db
        for idx, attr in enumerate(self.db_dict.keys()):
            setattr(db_doc, attr, self.values_1[idx])
        idn = db_doc.db_save()
        idn1 = idn

        temp_db_doc = dbm.DatabaseDocument(
            database_name=self.mongo_database_name,
            mongo=True,
            server=self.mongo_server)
        temp_db_doc.label = self.label
        temp_db_doc.collection_name = self.collection_name
        temp_db_doc.db_dict = self.db_dict

        success = temp_db_doc.db_read(idn=idn)
        self.assertTrue(success)

        self.assertTrue(temp_db_doc.date == db_doc.date)
        self.assertTrue(temp_db_doc.hour == db_doc.hour)
        self.assertTrue(temp_db_doc.str_attr == db_doc.str_attr)
        self.assertTrue(temp_db_doc.dict_attr == db_doc.dict_attr)
        self.assertTrue(temp_db_doc.list_attr == db_doc.list_attr)
        self.assertTrue(temp_db_doc.tuple_attr == db_doc.tuple_attr)
        np.testing.assert_equal(
            temp_db_doc.array_attr, db_doc.array_attr)

        for idx, attr in enumerate(self.db_dict.keys()):
            setattr(db_doc, attr, self.values_2[idx])
        idn = db_doc.db_save()

        temp_db_doc = dbm.DatabaseDocument(
            database_name=self.mongo_database_name,
            mongo=True,
            server=self.mongo_server)
        temp_db_doc.label = self.label
        temp_db_doc.collection_name = self.collection_name
        temp_db_doc.db_dict = self.db_dict

        success = temp_db_doc.db_read(idn=idn)
        self.assertTrue(success)

        self.assertTrue(temp_db_doc.date == db_doc.date)
        self.assertTrue(temp_db_doc.hour == db_doc.hour)
        self.assertTrue(temp_db_doc.str_attr == db_doc.str_attr)
        self.assertTrue(temp_db_doc.dict_attr == db_doc.dict_attr)
        self.assertTrue(temp_db_doc.list_attr == db_doc.list_attr)
        self.assertTrue(temp_db_doc.tuple_attr == db_doc.tuple_attr)
        np.testing.assert_equal(
            temp_db_doc.array_attr, db_doc.array_attr)

        success = db_doc.db_update(idn=idn1)
        self.assertTrue(success)

        db_doc = dbm.DatabaseAndFileDocument(
            database_name=self.mongo_database_name,
            mongo=True,
            server=self.mongo_server)
        db_doc.label = self.label
        db_doc.collection_name = self.collection_name
        db_doc.db_dict = self.db_dict
        db_doc.db_read(idn1)
        db_doc.save_file(self.fn)

        temp_db_doc = dbm.DatabaseAndFileDocument(
            database_name=self.mongo_database_name,
            mongo=True,
            server=self.mongo_server)
        temp_db_doc.label = self.label
        temp_db_doc.collection_name = self.collection_name
        temp_db_doc.db_dict = self.db_dict
        temp_db_doc.read_file(self.fn)

        self.assertTrue(temp_db_doc.date == db_doc.date)
        self.assertTrue(temp_db_doc.hour == db_doc.hour)
        self.assertTrue(temp_db_doc.str_attr == db_doc.str_attr)
        self.assertTrue(temp_db_doc.dict_attr == db_doc.dict_attr)
        self.assertTrue(temp_db_doc.list_attr == db_doc.list_attr)
        self.assertTrue(temp_db_doc.tuple_attr == db_doc.tuple_attr)
        np.testing.assert_equal(
            temp_db_doc.array_attr, db_doc.array_attr)

        # sqlite
        db_doc = self.sqlite_db
        for idx, attr in enumerate(self.db_dict.keys()):
            setattr(db_doc, attr, self.values_1[idx])
        idn = db_doc.db_save()

        temp_db_doc = dbm.DatabaseDocument(
            database_name=self.sqlite_database_name,
            mongo=False)
        temp_db_doc.label = self.label
        temp_db_doc.collection_name = self.collection_name
        temp_db_doc.db_dict = self.db_dict

        success = temp_db_doc.db_read(idn=idn)
        self.assertTrue(success)

        self.assertTrue(temp_db_doc.date == db_doc.date)
        self.assertTrue(temp_db_doc.hour == db_doc.hour)
        self.assertTrue(temp_db_doc.str_attr == db_doc.str_attr)
        self.assertTrue(temp_db_doc.dict_attr == db_doc.dict_attr)
        self.assertTrue(temp_db_doc.list_attr == db_doc.list_attr)
        self.assertTrue(temp_db_doc.tuple_attr == db_doc.tuple_attr)
        np.testing.assert_equal(
            temp_db_doc.array_attr, db_doc.array_attr)

        for idx, attr in enumerate(self.db_dict.keys()):
            setattr(db_doc, attr, self.values_2[idx])
        idn = db_doc.db_save()

        temp_db_doc = dbm.DatabaseDocument(
            database_name=self.sqlite_database_name,
            mongo=False)
        temp_db_doc.label = self.label
        temp_db_doc.collection_name = self.collection_name
        temp_db_doc.db_dict = self.db_dict

        success = temp_db_doc.db_read(idn=idn)
        self.assertTrue(success)

        self.assertTrue(temp_db_doc.date == db_doc.date)
        self.assertTrue(temp_db_doc.hour == db_doc.hour)
        self.assertTrue(temp_db_doc.str_attr == db_doc.str_attr)
        self.assertTrue(temp_db_doc.dict_attr == db_doc.dict_attr)
        self.assertTrue(temp_db_doc.list_attr == db_doc.list_attr)
        self.assertTrue(temp_db_doc.tuple_attr == db_doc.tuple_attr)
        np.testing.assert_equal(
            temp_db_doc.array_attr, db_doc.array_attr)

        db_doc = dbm.DatabaseAndFileDocument(
            database_name=self.sqlite_database_name,
            mongo=False)
        db_doc.label = self.label
        db_doc.collection_name = self.collection_name
        db_doc.db_dict = self.db_dict
        db_doc.db_read(idn1)
        db_doc.save_file(self.fn)

        columns = ['array_attr', 'list_attr']
        db_doc.save_file(self.fn, columns=columns)

        temp_db_doc = dbm.DatabaseAndFileDocument(
            database_name=self.sqlite_database_name,
            mongo=False)
        temp_db_doc.label = self.label
        temp_db_doc.collection_name = self.collection_name
        temp_db_doc.db_dict = self.db_dict
        temp_db_doc.read_file(self.fn)

        self.assertTrue(temp_db_doc.date == db_doc.date)
        self.assertTrue(temp_db_doc.hour == db_doc.hour)
        self.assertTrue(temp_db_doc.str_attr == db_doc.str_attr)
        self.assertTrue(temp_db_doc.dict_attr == db_doc.dict_attr)
        self.assertTrue(temp_db_doc.list_attr == db_doc.list_attr)
        self.assertTrue(temp_db_doc.tuple_attr == db_doc.tuple_attr)
        np.testing.assert_equal(
            temp_db_doc.array_attr, db_doc.array_attr)


if __name__ == '__main__':
    unittest.main()
