import os
import sys
from unittest import TestCase
from io import BytesIO
from .. import config
from .. import database
from .. import datatypes
database.set_test()
config.load_test()


class DatabaseSetup(TestCase):

    filename = '_unittest.sqlite'

    @classmethod
    def wipe_test_db(cls):
        if os.path.isfile(cls.filename):
            os.remove(cls.filename)

    @classmethod
    def setUpClass(cls):
        assert 'unittest' in str(database.make_session.engine), 'wrong database: %s' % str(database.make_session.engine)
        cls.wipe_test_db()

    @classmethod
    def tearDownClass(cls):
        cls.wipe_test_db()

    def setUp(self):
        self.wipe_test_db()
        database.create_tables()

    def tearDown(self):
        self.wipe_test_db()


class TestInventoryLog(DatabaseSetup):

    def test_init(self):
        handler = datatypes.InventoryHandler(config.config['vault']['name'])
        # print(config.config['vault']['name'])
        handler.store_request({'jobId': 'abc'})
        del handler
        handler = datatypes.InventoryHandler(config.config['vault']['name'])
        self.assertTrue(len(handler.get_open_requests()))

    def test_store_request(self):
        handler = datatypes.InventoryHandler(config.config['vault']['name'])
        request = handler.store_request({'jobId': 'abc'})
        self.assertIsInstance(request, datatypes.InventoryRequest)
        self.assertEqual(request.request_id, 1)
        self.assertEqual(request.job_id, 'abc')
        self.assertIsNotNone(request.sent_dt)
        self.assertEqual(request.vault_name, config.config['vault']['name'])
        del handler
        with database.make_session() as session:
            request = datatypes.InventoryRequest.from_db(session, row_id=1)
        self.assertEqual(request.request_id, 1)
        self.assertEqual(request.job_id, 'abc')
        self.assertIsNotNone(request.sent_dt)
        self.assertEqual(request.vault_name, config.config['vault']['name'])
        handler = datatypes.InventoryHandler(config.config['vault']['name'])
        self.assertEqual(len(handler.get_open_requests()), 1)
        # store another one
        request = handler.store_request({'jobId': 'abcd'})
        self.assertEqual(request.request_id, 2)
        del request
        del handler
        with database.make_session() as session:
            request = datatypes.InventoryRequest.from_db(session, row_id=1)
        self.assertEqual(request.request_id, 1)
        self.assertEqual(request.job_id, 'abc')
        self.assertIsNotNone(request.sent_dt)
        self.assertEqual(request.vault_name, config.config['vault']['name'])
        with database.make_session() as session:
            request = datatypes.InventoryRequest.from_db(session, row_id=2)
        self.assertEqual(request.request_id, 2)
        self.assertEqual(request.job_id, 'abcd')
        self.assertIsNotNone(request.sent_dt)
        self.assertEqual(request.vault_name, config.config['vault']['name'])
        handler = datatypes.InventoryHandler(config.config['vault']['name'])
        self.assertEqual(len(handler.get_open_requests()), 2)

    def test_store_response(self):
        handler = datatypes.InventoryHandler(config.config['vault']['name'])
        request = handler.store_request({'jobId': 'abc'})
        response_dict = {
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
        response = handler.store_response(request.request_id, response_dict)
        self.assertEqual(response.request_id, request.request_id)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, b'1234')
        stored_response = handler.get_latest_response()
        self.assertEqual(response.response_id, stored_response.response_id)
        self.assertEqual(response.request_id, stored_response.request_id)
        self.assertEqual(response.retrieved_dt, stored_response.retrieved_dt)
        self.assertEqual(response.content_type, stored_response.content_type)
        self.assertEqual(response.status, stored_response.status)
        self.assertEqual(response.body, stored_response.body)
        response_dict = {
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
        response = handler.store_response(request.request_id, response_dict)
        self.assertEqual(response.request_id, request.request_id)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, b'12345')

    def test_get_open_requests(self):
        handler = datatypes.InventoryHandler(config.config['vault']['name'])
        self.assertEqual(0, len(handler.get_open_requests()))
        handler.store_request({'jobId': 'abc'})
        self.assertEqual(1, len(handler.get_open_requests()))
        handler.store_request({'jobId': 'abcd'})
        self.assertEqual(2, len(handler.get_open_requests()))
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
        open_requests = handler.get_open_requests()
        fill_id = open_requests[1].request_id
        handler.store_response(fill_id, response)
        open_requests = handler.get_open_requests()
        self.assertEqual(1, len(open_requests))
        self.assertTrue(fill_id not in [x.request_id for x in open_requests])


class TestBackupLog(DatabaseSetup):

    def test_create(self):
        self.fail()
        inst = database.BackupLog('foo', filename=self.filename)
        with inst.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT COUNT(*) FROM project')
            self.assertEqual(0, cur.fetchall()[0][0])
        inst.create()
        with inst.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT COUNT(*) FROM project')
            self.assertEqual(1, cur.fetchall()[0][0])
        self.assertRaises(RuntimeError, inst.create)
        with inst.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT COUNT(*) FROM project')
            self.assertEqual(1, cur.fetchall()[0][0])
        with inst.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT COUNT(*) FROM project WHERE name="foo"')
            self.assertEqual(1, cur.fetchall()[0][0])


class TestOther(DatabaseSetup):

    def test_project_load_all(self):
        self.fail()
        self.assertEqual([], datatypes.Project.load_all())
        project = datatypes.Project(base_path=os.path.abspath('.'), name='testproject',
                                    vault=config.config['vault']['name'])
        with database.make_session() as session:
            project.create_db_entry(session)
        handler = inst = database.BackupLog('foo', filename=self.filename)
        inst.create()
        self.assertEqual([database.ProjectInfo('foo', '/', 0)], database.get_projects_for_file(self.filename))
        inst = database.BackupLog('bar', filename=self.filename)
        inst.create()
        self.assertEqual([database.ProjectInfo('foo', '/', 0), database.ProjectInfo('bar', '/', 0)],
                         database.get_projects_for_file(self.filename))
