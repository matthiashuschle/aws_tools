import os
from unittest import TestCase
import sqlite3
from .. import database


class TestForeignKey(TestCase):

    def test_fk_default(self):
        fk = database.ForeignKey('col_a', 'tab_b', 'col_b')
        self.assertEqual(fk.this_column, 'col_a')
        self.assertEqual(fk.other_table, 'tab_b')
        self.assertEqual(fk.other_column, 'col_b')
        self.assertEqual(fk.fk_statement, 'FOREIGN KEY (col_a) REFERENCES tab_b (col_b)')
        self.assertEqual(fk.get_constraint('tab_a'),
                         'CONSTRAINT fk_tab_a_tab_b_col_b FOREIGN KEY (col_a) REFERENCES tab_b (col_b)')

    def test_fk_identical(self):
        fk = database.ForeignKeyIdentical('col_a', 'tab_b')
        self.assertEqual(fk.this_column, 'col_a')
        self.assertEqual(fk.other_table, 'tab_b')
        self.assertEqual(fk.other_column, 'col_a')
        self.assertEqual(fk.fk_statement, 'FOREIGN KEY (col_a) REFERENCES tab_b (col_a)')
        self.assertEqual(fk.get_constraint('tab_a'),
                         'CONSTRAINT fk_tab_a_tab_b_col_a FOREIGN KEY (col_a) REFERENCES tab_b (col_a)')


class TestInventoryLog(TestCase):

    filename = 'test_database.sqlite'

    @classmethod
    def wipe_test_db(cls):
        if os.path.isfile(cls.filename):
            os.remove(cls.filename)

    @classmethod
    def setUpClass(cls):
        cls.wipe_test_db()

    @classmethod
    def tearDownClass(cls):
        cls.wipe_test_db()

    def test_init(self):
        database.InventoryLog('dummy_vault', filename=self.filename)
        self.assertTrue(os.path.exists(self.filename))
        database.InventoryLog('dummy_vault', filename=self.filename)
        self.assertTrue(os.path.exists(self.filename))
        inst = database.InventoryLog('dummy_vault', filename=self.filename)
        inst.store_request({'jobId': 'abc'})
        del inst
        inst = database.InventoryLog('dummy_vault', filename=self.filename)
        self.assertTrue(len(inst.get_open_requests()))
        self.wipe_test_db()

    def test_store_request(self):
        inst = database.InventoryLog('dummy_vault', filename=self.filename)
        inst.store_request({'jobId': 'abc'})
        with sqlite3.Connection(database=self.filename) as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM REQUEST')
            table = cur.fetchall()
        self.assertEqual(1, len(table))
        row = table[0]
        self.assertEqual(5, len(row))
        row = inst.RequestRow(*row)
        self.assertEqual(row.vault_name, 'dummy_vault')
        self.assertEqual(row.job_id, 'abc')
        self.assertEqual(row.retrieved, 0)
        # store another one
        inst.store_request({'jobId': 'abcd'})
        with sqlite3.Connection(database=self.filename) as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM REQUEST WHERE request_id = %i' % row.request_id)
            table = cur.fetchall()
        self.assertEqual(1, len(table))
        row = table[0]
        self.assertEqual(5, len(row))
        row = inst.RequestRow(*row)
        self.assertEqual(row.vault_name, 'dummy_vault')
        self.assertEqual(row.job_id, 'abc')
        self.assertEqual(row.retrieved, 0)
        with sqlite3.Connection(database=self.filename) as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM REQUEST WHERE request_id != %i' % row.request_id)
            table = cur.fetchall()
        self.assertEqual(1, len(table))
        row = table[0]
        self.assertEqual(5, len(row))
        row = inst.RequestRow(*row)
        self.assertEqual(row.vault_name, 'dummy_vault')
        self.assertEqual(row.job_id, 'abcd')
        self.assertEqual(row.retrieved, 0)


