
import json
import unittest
import pymongo
import numpy as np

from imautils.db import mongodatabase as dbm

_MONGO_SERVER = 'localhost'


class TestMongoDatabaseEmptycollection(unittest.TestCase):

    def setUp(self):
        self.server = _MONGO_SERVER
        self.database_name = 'test_database'
        self.collection_name = 'test_collection'

        self.client = dbm.db_connect(server=self.server)
        dbm.db_create_collection(
            self.client, self.database_name, self.collection_name)

    def tearDown(self):
        try:
            self.client.drop_database(self.database_name)
        except Exception:
            pass

    def test_db_database_exists(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_database_exists(None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_database_exists(self.client, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_database_exists(self.client, "")

        self.assertFalse(dbm.db_database_exists(self.client, "other_name"))

        self.assertTrue(dbm.db_database_exists(
            self.client, self.database_name))

    def test_db_get_collections(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_collections(None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_collections(self.client, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_collections(self.client, "")

        self.assertEqual(
            dbm.db_get_collections(
                self.client, self.database_name)[0], self.collection_name)

    def test_db_collection_exists(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_collection_exists(None, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_collection_exists(self.client, "", "")

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_collection_exists(self.client, self.database_name, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_collection_exists(self.client, self.database_name, "")

        self.assertFalse(dbm.db_collection_exists(
            self.client, self.database_name, "other_name"))

        self.assertTrue(
            dbm.db_collection_exists(
                self.client, self.database_name, self.collection_name))

    def test_db_get_field_names(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_field_names(None, None, None)

        field_names = dbm.db_get_field_names(
            self.client, self.database_name, self.collection_name)
        self.assertEqual(len(field_names), 0)

    def test_db_get_field_types(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_field_types(None, None, None)

        field_types = dbm.db_get_field_types(
            self.client, self.database_name, self.collection_name)
        self.assertEqual(len(field_types), 0)

    def test_db_get_first_id(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_first_id(None, None, None)

        self.assertIsNone(dbm.db_get_first_id(
            self.client, self.database_name, self.collection_name))

    def test_db_get_last_id(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_last_id(None, None, None)

        self.assertIsNone(dbm.db_get_last_id(
            self.client, self.database_name, self.collection_name))

    def test_db_get_values(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_values(None, None, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_values(
                self.client, self.database_name, self.collection_name, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_values(
                self.client, self.database_name, self.collection_name, "")

        values = dbm.db_get_values(
            self.client, self.database_name, self.collection_name, "id")
        self.assertEqual(len(values), 0)

        values = dbm.db_get_values(
            self.client, self.database_name, self.collection_name, "date")
        self.assertEqual(len(values), 0)

        values = dbm.db_get_values(
            self.client, self.database_name, self.collection_name, "id")
        self.assertEqual(len(values), 0)

        values = dbm.db_get_values(
            self.client, self.database_name,
            self.collection_name, "other_name")
        self.assertEqual(len(values), 0)

    def test_db_get_value(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_value(None, None, None, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_get_value(
                self.client, self.database_name,
                self.collection_name, 'id', None)

        value = dbm.db_get_value(
            self.client, self.database_name, self.collection_name, 'id', 1)
        self.assertIsNone(value)

    def test_db_search_field(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_search_field(None, None, None, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_search_field(
                self.client, self.database_name,
                self.collection_name, 'id', None)

        entries = dbm.db_search_field(
            self.client, self.database_name, self.collection_name, 'id', 1)
        self.assertEqual(len(entries), 0)

    def test_db_search_collection(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_search_collection(None, None, None)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name)
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_collection(
            self.client, self.database_name,
            self.collection_name, fields=['id'])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_collection(
            self.client, self.database_name,
            self.collection_name, fields=[], filters=[])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id'], filters=[1])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id'], filters=[1], initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 0)

    def test_db_create_collection(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_create_collection(None, None, None)

        sucess = dbm.db_create_collection(
            self.client, self.database_name, self.collection_name)
        self.assertTrue(sucess)

        sucess = dbm.db_create_collection(
            self.client, self.database_name, "other_name")
        self.assertTrue(sucess)

        self.client[self.database_name].drop_collection("other_name")

    def test_db_save(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            idn =  dbm.db_save(None, None, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            idn = dbm.db_save(
                self.client, self.database_name, self.collection_name, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            idn = dbm.db_save(
                self.client, self.database_name, self.collection_name, {})

    def test_db_read(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_read(None, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            dbm.db_read(None, None, None, None)

        values_dict = dbm.db_read(
            self.client, self.database_name, self.collection_name)
        self.assertEqual(len(values_dict), 0)

        values_dict = dbm.db_read(
            self.client, self.database_name, self.collection_name, idn=1)
        self.assertEqual(len(values_dict), 0)

    def test_db_update(self):
        with self.assertRaises(dbm.MongoDatabaseError):
            sucess =  dbm.db_update(None, None, None, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            sucess = dbm.db_update(
                self.client, self.database_name,
                self.collection_name, None, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            sucess = dbm.db_update(
                self.client, self.database_name,
                self.collection_name, {}, None)

        with self.assertRaises(dbm.MongoDatabaseError):
            sucess = dbm.db_update(
                self.client, self.database_name, self.collection_name, {}, 1)


class TestMongoDatabaseNonEmptycollection(unittest.TestCase):

    def setUp(self):
        self.server = _MONGO_SERVER
        self.database_name = 'test_database'
        self.collection_name = 'test_collection'

        self.client = dbm.db_connect(server=self.server)
        dbm.db_create_collection(
            self.client, self.database_name, self.collection_name)

        self.field_names = [
            'id',
            'date',
            'hour',
            'str_attr',
            'dict_attr',
            'list_attr',
            'tuple_attr',
            'array_attr']
        self.field_types = [
            int,
            str,
            str,
            str,
            dict,
            list,
            tuple,
            np.ndarray]
        self.not_null = [
            True,
            True,
            True,
            False,
            False,
            False,
            False,
            False]
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

        self.db_dict = {}
        for i, name in enumerate(self.field_names):
            self.db_dict[name] = {
                'field': name,
                'dtype': self.field_types[i],
                'not_null': self.not_null[i],
                }

        self.processed_values_1 = []
        for i, v in enumerate(self.values_1):
            if self.field_types[i] == np.ndarray:
                v = v.tolist()
            if self.field_types[i] in (dict, list, tuple, np.ndarray):
                v = json.dumps(v)
            self.processed_values_1.append(v)

        self.processed_values_2 = []
        for i, v in enumerate(self.values_2):
            if self.field_types[i] == np.ndarray:
                v = v.tolist()
            if self.field_types[i] in (dict, list, tuple, np.ndarray):
                v = json.dumps(v)
            self.processed_values_2.append(v)

        self.values_dict_1 = {}
        for i, name in enumerate(self.field_names):
            self.values_dict_1[name] = self.processed_values_1[i]

        self.values_dict_2 = {}
        for i, name in enumerate(self.field_names):
            self.values_dict_2[name] = self.processed_values_2[i]

        dbm.db_save(
            self.client, self.database_name,
            self.collection_name, self.values_dict_1)

        dbm.db_save(
            self.client, self.database_name,
            self.collection_name, self.values_dict_2)

    def tearDown(self):
        try:
            self.client.drop_database(self.database_name)
        except Exception:
            pass

    def test_db_get_first_id(self):
        self.assertTrue(dbm.db_get_first_id(
            self.client, self.database_name, self.collection_name), 1)

    def test_db_get_last_id(self):
        self.assertTrue(dbm.db_get_last_id(
            self.client, self.database_name, self.collection_name), 2)

    def test_db_get_values(self):
        for name in self.field_names:
            rvalues = dbm.db_get_values(
                self.client, self.database_name, self.collection_name, name)

            values = [self.values_dict_1[name], self.values_dict_2[name]]
            np.testing.assert_equal(rvalues, values)

        with self.assertRaises(Exception):
            rvalues = dbm.db_get_values(
                self.client, self.database_name,
                self.collection_name, 'other_name')

    def test_db_get_value(self):
        rvalue = dbm.db_get_value(
            self.client, self.database_name,
            self.collection_name, 'other_name', 1)
        self.assertIsNone(rvalue)

        for name in self.field_names:
            rvalue = dbm.db_get_value(
                self.client, self.database_name, self.collection_name, name, 1)

            value = self.values_dict_1[name]
            np.testing.assert_equal(rvalue, value)

        for name in self.field_names:
            rvalue = dbm.db_get_value(
                self.client, self.database_name, self.collection_name, name, 2)

            value = self.values_dict_2[name]
            np.testing.assert_equal(rvalue, value)

        for name in self.field_names:
            rvalue = dbm.db_get_value(
                self.client, self.database_name, self.collection_name, name, 3)

            self.assertIsNone(rvalue)

    def test_db_search_field(self):
        for name in self.field_names:
            idns = [self.values_dict_1['id'], self.values_dict_2['id']]
            values = [self.values_dict_1[name], self.values_dict_2[name]]
            for i, value in enumerate(values):
                if name != 'dict_attr':
                    entries = dbm.db_search_field(
                        self.client, self.database_name,
                        self.collection_name, name, value)
                    if name == 'hour':
                        self.assertEqual(len(entries), 2)
                    else:
                        self.assertEqual(entries[0]['id'], idns[i])

        entries = dbm.db_search_field(
            self.client, self.database_name, self.collection_name, 'id', 3)
        self.assertEqual(len(entries), 0)

    def test_db_search_collection(self):
        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name)
        self.assertEqual(len(entries), 2)

        for name in self.field_names:
            if name != 'dict_attr':
                entries = dbm.db_search_collection(
                    self.client, self.database_name, self.collection_name,
                    fields=[name], filters=[self.values_dict_1[name]])
                np.testing.assert_equal(
                    entries[0][name], self.values_dict_1[name])

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['1', '2020-01-14'])
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['2', '2020-01-15'])
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['1', '2020-01-15'])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=2)
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1, max_nr_lines=0)
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1, max_nr_lines=100)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['id', 'hour'], filters=['1', '10:00:00'],
            initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['hour'], filters=['10:00:00'],
            initial_idn=1, max_nr_lines=2)
        self.assertEqual(len(entries), 2)

        entries = dbm.db_search_collection(
            self.client, self.database_name, self.collection_name,
            fields=['hour'], filters=['10:00:00'],
            initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 1)

    def test_db_read(self):
        rvalues_dict = dbm.db_read(
            self.client, self.database_name, self.collection_name, 1)
        np.testing.assert_equal(rvalues_dict, self.values_dict_1)

        rvalues_dict = dbm.db_read(
            self.client, self.database_name, self.collection_name, 2)
        np.testing.assert_equal(rvalues_dict, self.values_dict_2)

    def test_db_save(self):
        new_values_dict = {}
        for key, value in self.values_dict_1.items():
            new_values_dict[key] = value
        new_values_dict['id'] = 3

        ridn = dbm.db_save(
            self.client, self.database_name,
            self.collection_name, new_values_dict)

        self.assertEqual(ridn, 3)

        rvalues_dict = dbm.db_read(
            self.client, self.database_name, self.collection_name, 3)
        np.testing.assert_equal(rvalues_dict, new_values_dict)

        new_values_dict['id'] = None
        ridn = dbm.db_save(
            self.client, self.database_name,
            self.collection_name, new_values_dict)
        idn = dbm.db_get_last_id(
            self.client, self.database_name, self.collection_name)
        self.assertEqual(ridn, idn)
        self.assertEqual(ridn, 4)

    def test_db_update(self):
        dbm.db_update(
            self.client, self.database_name,
            self.collection_name, self.values_dict_1, 1)

        rvalues_dict = dbm.db_read(
            self.client, self.database_name, self.collection_name, 1)
        np.testing.assert_equal(rvalues_dict, self.values_dict_1)

        new_values_dict = {}
        for key, value in self.values_dict_1.items():
            new_values_dict[key] = value
        new_values_dict['str_attr'] = 'other_string'

        dbm.db_update(
            self.client, self.database_name,
            self.collection_name, new_values_dict, 1)

        rvalues_dict = dbm.db_read(
            self.client, self.database_name, self.collection_name, 1)
        np.testing.assert_equal(rvalues_dict, new_values_dict)


if __name__ == '__main__':
    unittest.main()
