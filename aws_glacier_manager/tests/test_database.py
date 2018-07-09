import os
from unittest import TestCase
import sqlite3
from io import BytesIO
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

    def tearDown(self):
        self.wipe_test_db()

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

    def test_store_request(self):
        inst = database.InventoryLog('dummy_vault', filename=self.filename)
        inst.store_request({'jobId': 'abc'})
        with sqlite3.Connection(database=self.filename) as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM REQUEST')
            table = cur.fetchall()
        self.assertEqual(1, len(table))
        row = table[0]
        self.assertEqual(4, len(row))
        row = inst.RequestRow(*row)
        self.assertEqual(row.vault_name, 'dummy_vault')
        self.assertEqual(row.job_id, 'abc')
        # store another one
        inst.store_request({'jobId': 'abcd'})
        with sqlite3.Connection(database=self.filename) as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM REQUEST WHERE request_id = %i' % row.request_id)
            table = cur.fetchall()
        self.assertEqual(1, len(table))
        row = table[0]
        self.assertEqual(4, len(row))
        row = inst.RequestRow(*row)
        self.assertEqual(row.vault_name, 'dummy_vault')
        self.assertEqual(row.job_id, 'abc')
        with sqlite3.Connection(database=self.filename) as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM REQUEST WHERE request_id != %i' % row.request_id)
            table = cur.fetchall()
        self.assertEqual(1, len(table))
        row = table[0]
        self.assertEqual(4, len(row))
        row = inst.RequestRow(*row)
        self.assertEqual(row.vault_name, 'dummy_vault')
        self.assertEqual(row.job_id, 'abcd')

    def test_store_response(self):
        inst = database.InventoryLog('dummy_vault', filename=self.filename)
        inst.store_request({'jobId': 'abc'})
        response = {
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'accept-ranges': 'bytes',
                    'content-length': '26915',
                    'content-type': 'application/json',
                    'date': 'Sun, 08 Jul 2018 18:39:41 GMT',
                    'x-amzn-requestid': 'yHW7Xuf_3DEjJdACLccRrT11TePr8VVKhC9TOBHm9rxOvcs'
                },
                'HTTPStatusCode': 200,
                'RequestId': 'yHW7Xuf_3DEjJdACLccRrT11TePr8VVKhC9TOBHm9rxOvcs',
                'RetryAttempts': 0
            },
            'acceptRanges': 'bytes',
            'body': BytesIO(b'1234'),
            'contentType': 'application/json',
            'status': 200}
        latest = inst.get_open_requests()[0]
        inst.store_response(latest.request_id, response)
        stored = inst.get_latest_response()
        self.assertEqual(stored.request_id, latest.request_id)
        self.assertEqual(stored.content_type, 'application/json')
        self.assertEqual(stored.status, 200)
        self.assertEqual(stored.body, b'1234')
        response = {
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'accept-ranges': 'bytes',
                    'content-length': '26915',
                    'content-type': 'application/json',
                    'date': 'Sun, 08 Jul 2018 18:39:41 GMT',
                    'x-amzn-requestid': 'yHW7Xuf_3DEjJdACLccRrT11TePr8VVKhC9TOBHm9rxOvcs'
                },
                'HTTPStatusCode': 200,
                'RequestId': 'yHW7Xuf_3DEjJdACLccRrT11TePr8VVKhC9TOBHm9rxOvcs',
                'RetryAttempts': 0
            },
            'acceptRanges': 'bytes',
            'body': BytesIO(b'12345'),
            'contentType': 'application/json',
            'status': 200}
        inst.store_response(latest.request_id, response)
        stored = inst.get_latest_response()
        self.assertEqual(stored.request_id, latest.request_id)
        self.assertEqual(stored.content_type, 'application/json')
        self.assertEqual(stored.status, 200)
        self.assertEqual(stored.body, b'12345')

    def test_get_open_requests(self):
        inst = database.InventoryLog('dummy_vault', filename=self.filename)
        self.assertEqual(0, len(inst.get_open_requests()))
        inst.store_request({'jobId': 'abc'})
        self.assertEqual(1, len(inst.get_open_requests()))
        inst.store_request({'jobId': 'abcd'})
        self.assertEqual(2, len(inst.get_open_requests()))
        response = {
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'accept-ranges': 'bytes',
                    'content-length': '26915',
                    'content-type': 'application/json',
                    'date': 'Sun, 08 Jul 2018 18:39:41 GMT',
                    'x-amzn-requestid': 'yHW7Xuf_3DEjJdACLccRrT11TePr8VVKhC9TOBHm9rxOvcs'
                },
                'HTTPStatusCode': 200,
                'RequestId': 'yHW7Xuf_3DEjJdACLccRrT11TePr8VVKhC9TOBHm9rxOvcs',
                'RetryAttempts': 0
            },
            'acceptRanges': 'bytes',
            'body': BytesIO(b'12345'),
            'contentType': 'application/json',
            'status': 200}
        inst.store_response(inst.get_open_requests()[0].request_id, response)
        self.assertEqual(1, len(inst.get_open_requests()))


