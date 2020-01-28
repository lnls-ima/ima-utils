
import os
import json
import unittest
import sqlite3
import numpy as np

from imautils.db import sqlitedatabase as dbm


_TEST_PATH = os.path.dirname(__file__)


class TestSqliteDatabaseEmptyTable(unittest.TestCase):
    
    def setUp(self):
        self.database_name = os.path.join(_TEST_PATH, 'test_database.db')
        self.table_name = 'test_table'
        dbm.db_create_table(
            self.database_name, self.table_name, {})
       
    def tearDown(self):
        try:
            os.remove(self.database_name)
        except Exception:
            pass
       
    def test_db_database_exists(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_database_exists(None)
        
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_database_exists("")
        
        self.assertFalse(dbm.db_database_exists("other_name"))
        
        self.assertTrue(dbm.db_database_exists(self.database_name))

    def test_db_get_tables(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_tables(None)        
        
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_tables("")
        
        self.assertEqual(
            dbm.db_get_tables(self.database_name)[0], self.table_name)

    def test_db_table_exists(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_table_exists(None, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_table_exists("", "")

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_table_exists(self.database_name, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_table_exists(self.database_name, "")

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_table_exists(self.database_name, 'table')

        self.assertFalse(
            dbm.db_table_exists(self.database_name, "other_name"))
        
        self.assertTrue(
            dbm.db_table_exists(self.database_name, self.table_name))

    def test_db_get_column_names(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_column_names(None, None)

        column_names = dbm.db_get_column_names(
            self.database_name, self.table_name)

        self.assertEqual(len(column_names), 3)
        self.assertIn('id', column_names)
        self.assertIn('date', column_names)
        self.assertIn('hour', column_names)

    def test_db_get_column_types(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_column_types(None, None)

        column_types = dbm.db_get_column_types(
            self.database_name, self.table_name)

        self.assertEqual(len(column_types), 3)
        self.assertEqual(column_types['id'], int)
        self.assertEqual(column_types['date'], str)
        self.assertEqual(column_types['hour'], str)

    def test_db_get_first_id(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_first_id(None, None)

        self.assertIsNone(dbm.db_get_first_id(
            self.database_name, self.table_name))

    def test_db_get_last_id(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_last_id(None, None)

        self.assertIsNone(dbm.db_get_last_id(
            self.database_name, self.table_name))

    def test_db_get_values(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_values(None, None, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_values(
                self.database_name, self.table_name, None)       

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_values(self.database_name, self.table_name, "")       

        values = dbm.db_get_values(
            self.database_name, self.table_name, "id")
        self.assertEqual(len(values), 0)

        values = dbm.db_get_values(
            self.database_name, self.table_name, "date")
        self.assertEqual(len(values), 0)

        values = dbm.db_get_values(
            self.database_name, self.table_name, "id")
        self.assertEqual(len(values), 0)

        with self.assertRaises(Exception):
            dbm.db_get_values(
            self.database_name, self.table_name, "other_name")

    def test_db_get_value(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_value(None, None, None, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_get_value(
                self.database_name, self.table_name, 'id', None)       

        value = dbm.db_get_value(
            self.database_name, self.table_name, 'id', 1)
        self.assertIsNone(value)

    def test_db_search_column(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_search_column(None, None, None, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_search_column(
                self.database_name, self.table_name, 'id', None)       

        entries = dbm.db_search_column(
            self.database_name, self.table_name, 'id', 1)  
        self.assertEqual(len(entries), 0)
       
    def test_db_search_table(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_search_table(None, None)    

        entries = dbm.db_search_table(
            self.database_name, self.table_name)
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_table(
            self.database_name, self.table_name, columns=['id'])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_table(
            self.database_name, self.table_name, columns=[], filters=[])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_table(
            self.database_name, self.table_name, columns=['id'], filters=[1])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id'], filters=[1], initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 0)

    def test_db_create_table(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_create_table(None, None, None)

        sucess = dbm.db_create_table(
            self.database_name, self.table_name, None)
        self.assertTrue(sucess)

        sucess = dbm.db_create_table(
            self.database_name, self.table_name, {})
        self.assertTrue(sucess)

        sucess = dbm.db_create_table(
            self.database_name, "other_name", {})
        self.assertTrue(sucess)

    def test_db_save(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            idn =  dbm.db_save(None, None, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            idn = dbm.db_save(self.database_name, self.table_name, None)
        
        with self.assertRaises(dbm.SqliteDatabaseError):
            idn = dbm.db_save(self.database_name, self.table_name, {})

    def test_db_read(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_read(None, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            dbm.db_read(None, None, None)

        values_dict = dbm.db_read(self.database_name, self.table_name)
        self.assertEqual(len(values_dict), 0)
        
        values_dict = dbm.db_read(self.database_name, self.table_name, idn=1)
        self.assertEqual(len(values_dict), 0)

    def test_db_update(self):
        with self.assertRaises(dbm.SqliteDatabaseError):
            sucess =  dbm.db_update(None, None, None, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            sucess = dbm.db_update(
                self.database_name, self.table_name, None, None)
        
        with self.assertRaises(dbm.SqliteDatabaseError):
            sucess = dbm.db_update(
                self.database_name, self.table_name, {}, None)

        with self.assertRaises(dbm.SqliteDatabaseError):
            sucess = dbm.db_update(
                self.database_name, self.table_name, {}, 1)


class TestSqliteDatabaseNonEmptyTable(unittest.TestCase):
    
    def setUp(self):
        self.database_name = os.path.join(_TEST_PATH, 'test_database.db')
        self.table_name = 'test_table'
       
        self.column_names = [
            'id',
            'date',
            'hour',
            'str_attr',
            'dict_attr',
            'list_attr',
            'tuple_attr',
            'array_attr']
        self.column_types = [
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
        for i, name in enumerate(self.column_names):
            self.db_dict[name] = {
                'field': name,
                'dtype': self.column_types[i],
                'not_null': self.not_null[i],
                }

        dbm.db_create_table(
            self.database_name, self.table_name, self.db_dict)
        
        self.processed_values_1 = []
        for i, v in enumerate(self.values_1):
            if self.column_types[i] == np.ndarray:
                v = v.tolist()
            if self.column_types[i] in (dict, list, tuple, np.ndarray):
                v = json.dumps(v)
            self.processed_values_1.append(v)

        self.processed_values_2 = []
        for i, v in enumerate(self.values_2):
            if self.column_types[i] == np.ndarray:
                v = v.tolist()
            if self.column_types[i] in (dict, list, tuple, np.ndarray):
                v = json.dumps(v)
            self.processed_values_2.append(v)

        self.values_dict_1 = {}
        for i, name in enumerate(self.column_names):
            self.values_dict_1[name] = self.processed_values_1[i]

        self.values_dict_2 = {}
        for i, name in enumerate(self.column_names):
            self.values_dict_2[name] = self.processed_values_2[i]
        
        dbm.db_save(
            self.database_name, self.table_name, self.values_dict_1)

        dbm.db_save(
            self.database_name, self.table_name, self.values_dict_2)
        
    def tearDown(self):
        try:
            os.remove(self.database_name)
        except Exception:
            pass
       
    def test_db_get_first_id(self):
        self.assertTrue(dbm.db_get_first_id(
            self.database_name, self.table_name), 1)

    def test_db_get_last_id(self):
        self.assertTrue(dbm.db_get_last_id(
            self.database_name, self.table_name), 2)

    def test_db_get_values(self):
        for name in self.column_names:
            rvalues = dbm.db_get_values(
                self.database_name, self.table_name, name)
            
            values = [self.values_dict_1[name], self.values_dict_2[name]]
            np.testing.assert_equal(rvalues, values)

        with self.assertRaises(Exception):
            rvalues = dbm.db_get_values(
                self.database_name, self.table_name, 'other_name')

    def test_db_get_value(self):
        with self.assertRaises(Exception):
            rvalue = dbm.db_get_value(
                self.database_name, self.table_name, 'other_name', 1)
        
        for name in self.column_names:
            rvalue = dbm.db_get_value(
                self.database_name, self.table_name, name, 1)
            
            value = self.values_dict_1[name]
            np.testing.assert_equal(rvalue, value)

        for name in self.column_names:
            rvalue = dbm.db_get_value(
                self.database_name, self.table_name, name, 2)
            
            value = self.values_dict_2[name]
            np.testing.assert_equal(rvalue, value)

        for name in self.column_names:
            rvalue = dbm.db_get_value(
                self.database_name, self.table_name, name, 3)
            
            self.assertIsNone(rvalue)

    def test_db_search_column(self):
        for name in self.column_names:
            idns = [self.values_dict_1['id'], self.values_dict_2['id']]
            values = [self.values_dict_1[name], self.values_dict_2[name]]
            for i, value in enumerate(values):
                if name != 'dict_attr':
                    entries = dbm.db_search_column(
                        self.database_name, self.table_name, name, value)
                    if name == 'hour':
                        self.assertEqual(len(entries), 2)
                    else:
                        self.assertEqual(entries[0]['id'], idns[i])
       
        entries = dbm.db_search_column(
            self.database_name, self.table_name, 'id', 3)
        self.assertEqual(len(entries), 0)
       
    def test_db_search_table(self):
        entries = dbm.db_search_table(
            self.database_name, self.table_name)
        self.assertEqual(len(entries), 2)

        for name in self.column_names:
            if name != 'dict_attr':
                entries = dbm.db_search_table(
                    self.database_name, self.table_name,
                    columns=[name], filters=[self.values_dict_1[name]])
                np.testing.assert_equal(
                    entries[0][name], self.values_dict_1[name])
    
        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['1', '2020-01-14'])
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['2', '2020-01-15'])
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['1', '2020-01-15'])
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=2)
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1, max_nr_lines=0)
        self.assertEqual(len(entries), 0)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'date'], filters=['1', '2020-01-14'],
            initial_idn=1, max_nr_lines=100)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['id', 'hour'], filters=['1', '10:00:00'],
            initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 1)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['hour'], filters=['10:00:00'],
            initial_idn=1, max_nr_lines=2)
        self.assertEqual(len(entries), 2)

        entries = dbm.db_search_table(
            self.database_name, self.table_name,
            columns=['hour'], filters=['10:00:00'],
            initial_idn=1, max_nr_lines=1)
        self.assertEqual(len(entries), 1)
    
    def test_db_create_table(self):
        sucess = dbm.db_create_table(
            self.database_name, 'new_table', self.db_dict)
        self.assertTrue(sucess)

        np.testing.assert_array_equal(
            dbm.db_get_column_names(self.database_name, 'new_table'),
            self.column_names)

    def test_db_read(self):
        rvalues_dict = dbm.db_read(self.database_name, self.table_name, 1)
        np.testing.assert_equal(rvalues_dict, self.values_dict_1)

        rvalues_dict = dbm.db_read(self.database_name, self.table_name, 2)
        np.testing.assert_equal(rvalues_dict, self.values_dict_2)

    def test_db_save(self):
        with self.assertRaises(Exception):
            dbm.db_save(
                self.database_name, self.table_name, self.values_dict_1)
        
        new_values_dict = {}
        for key, value in self.values_dict_1.items():
            new_values_dict[key] = value
        new_values_dict['id'] = 3
        
        ridn = dbm.db_save(
            self.database_name, self.table_name, new_values_dict)
        
        self.assertEqual(ridn, 3)
        
        rvalues_dict = dbm.db_read(self.database_name, self.table_name, 3)
        np.testing.assert_equal(rvalues_dict, new_values_dict)

        new_values_dict['id'] = None
        ridn = dbm.db_save(
            self.database_name, self.table_name, new_values_dict)
        idn = dbm.db_get_last_id(self.database_name, self.table_name)
        self.assertEqual(ridn, idn)
        self.assertEqual(ridn, 4)

    def test_db_update(self):
        dbm.db_update(
            self.database_name, self.table_name, self.values_dict_1, 1)

        rvalues_dict = dbm.db_read(self.database_name, self.table_name, 1)
        np.testing.assert_equal(rvalues_dict, self.values_dict_1)

        new_values_dict = {}
        for key, value in self.values_dict_1.items():
            new_values_dict[key] = value
        new_values_dict['str_attr'] = 'other_string'
        
        dbm.db_update(
            self.database_name, self.table_name, new_values_dict, 1)

        rvalues_dict = dbm.db_read(self.database_name, self.table_name, 1)
        np.testing.assert_equal(rvalues_dict, new_values_dict)


if __name__ == '__main__':
    unittest.main()
