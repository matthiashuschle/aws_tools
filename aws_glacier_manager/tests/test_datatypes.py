import os
from unittest import TestCase
from io import BytesIO
from collections import defaultdict
from tempfile import TemporaryDirectory
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
        assert 'unittest' in str(datatypes.make_session.engine), \
            'wrong database: %s' % str(datatypes.make_session.engine)
        cls.wipe_test_db()

    def setUp(self):
        self.wipe_test_db()
        database.create_tables()
        datatypes.MappedBase.object_index = defaultdict(dict)

    def test_make_session(self):
        self.assertTrue('_unittest' in str(datatypes.make_session.engine))


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
        with datatypes.make_session() as session:
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
        with datatypes.make_session() as session:
            request = datatypes.InventoryRequest.from_db(session, row_id=1)
        self.assertEqual(request.request_id, 1)
        self.assertEqual(request.job_id, 'abc')
        self.assertIsNotNone(request.sent_dt)
        self.assertEqual(request.vault_name, config.config['vault']['name'])
        with datatypes.make_session() as session:
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
        response = handler.store_response(request, response_dict)
        self.assertEqual(response.request_id, request.request_id)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, b'1234')
        self.assertIs(response.request, request)
        stored_response = handler.get_latest_response()
        self.assertEqual(response.response_id, stored_response.response_id)
        self.assertEqual(response.request_id, stored_response.request_id)
        self.assertEqual(response.retrieved_dt, stored_response.retrieved_dt)
        self.assertEqual(response.content_type, stored_response.content_type)
        self.assertEqual(response.status, stored_response.status)
        self.assertEqual(response.body, stored_response.body)
        self.assertIs(stored_response.request, request)
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
        response = handler.store_response(request, response_dict)
        self.assertEqual(response.request_id, request.request_id)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, b'12345')
        self.assertIs(response.request, request)

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
        handler.store_response(open_requests[1], response)
        open_requests = handler.get_open_requests()
        self.assertEqual(1, len(open_requests))
        self.assertTrue(fill_id not in [x.request_id for x in open_requests])


class TestDerivedKeySetup(TestCase):

    def test_creation(self):
        setup = datatypes.DerivedKeySetup.create_default()
        self.assertIsNone(setup.row_id)
        setup.key_size_sig = None
        setup.salt_key_sig = None
        self.assertIsNone(setup.key_size_sig)
        self.assertIsNone(setup.salt_key_sig)
        with datatypes.make_session() as session:
            setup.create_db_entry(session)
        # key is set
        self.assertIsInstance(setup.row_id, int)
        # default values are assigned
        self.assertEqual(setup.key_size_sig, 0)
        self.assertEqual(setup.salt_key_sig, b'')
        with datatypes.make_session() as session:
            setup_from_db = datatypes.DerivedKeySetup.from_db(session, row_id=setup.row_id)
        self.assertIsNotNone(setup_from_db)
        self.assertIs(setup_from_db, setup)
        
    def test_update(self):
        setup = datatypes.DerivedKeySetup.create_default()
        self.assertIsNone(setup.row_id)
        setup.key_size_sig = None
        setup.salt_key_sig = None
        with datatypes.make_session() as session:
            setup.create_db_entry(session)
        self.assertIsInstance(setup.row_id, int)
        # default values are assigned
        self.assertEqual(setup.key_size_sig, 0)
        self.assertEqual(setup.salt_key_sig, b'')
        # modify, update, and check
        setup.key_size_sig = 42
        setup.construct = 'foo'
        old_id = setup.row_id
        with datatypes.make_session() as session:
            setup.update_db(session)
        self.assertEqual(setup.row_id, old_id)
        self.assertEqual(setup.key_size_sig, 42)
        self.assertEqual(setup.salt_key_sig, b'')
        self.assertEqual(setup.construct, 'foo')
        # delete from cache and retrieve again
        setup.clear_object_index()
        with datatypes.make_session() as session:
            setup_from_db = datatypes.DerivedKeySetup.from_db(session, row_id=old_id)
        self.assertEqual(setup_from_db.key_size_sig, 42)
        self.assertEqual(setup_from_db.salt_key_sig, b'')
        self.assertEqual(setup_from_db.construct, 'foo')

    def test_delete(self):
        setup = datatypes.DerivedKeySetup.create_default()
        with datatypes.make_session() as session:
            setup.create_db_entry(session)
            self.assertIsNotNone(setup.row_id)
            old_id = setup.row_id
            setup.remove_from_db(session)
        self.assertTrue(setup.is_deleted)
        self.assertIsNone(setup.row_id)
        setup.key_size_sig = 5
        with datatypes.make_session() as session:
            with self.assertRaises(RuntimeError):
                setup.update_db(session)
            with self.assertRaises(datatypes.UnknownId):
                datatypes.DerivedKeySetup.from_db(row_id=old_id, session=session)


class TestChunk(TestCase):

    def test_creation(self):
        # creation and storage
        chunk = datatypes.Chunk(start_offset=0, size=2**16, file_id=1)
        with datatypes.make_session() as session:
            chunk.create_db_entry(session)
        cid = chunk.row_id
        with datatypes.make_session() as session:
            loaded_chunk = datatypes.Chunk.from_db(session, row_id=cid)
        self.assertEqual(
            [loaded_chunk.start_offset, loaded_chunk.size, loaded_chunk.file_id],
            [0, 2**16, 1]
        )
        self.assertIs(loaded_chunk, chunk)
        # delete object storage and try again
        datatypes.Chunk.clear_object_index()
        with datatypes.make_session() as session:
            loaded_chunk = datatypes.Chunk.from_db(session, row_id=cid)
        self.assertEqual(
            [loaded_chunk.start_offset, loaded_chunk.size, loaded_chunk.file_id],
            [0, 2**16, 1]
        )
        self.assertIsNot(loaded_chunk, chunk)

    def test_key_setup(self):
        chunk = datatypes.Chunk(start_offset=0, size=2**16, file_id=1)
        key_setup = datatypes.DerivedKeySetup.create_default()
        with datatypes.make_session() as session:
            key_setup.create_db_entry(session)
        chunk.set_key_setup(key_setup)
        self.assertEqual(chunk.derived_key_setup_id, key_setup.row_id)
        with datatypes.make_session() as session:
            chunk.update_db(session)
        # clear chunk cache. chunks should be different instances, but the key setup not.
        chunk.clear_object_index()
        with datatypes.make_session() as session:
            loaded_chunk = datatypes.Chunk.from_db(session, row_id=chunk.row_id)
        self.assertEqual(loaded_chunk.derived_key_setup_id, key_setup.row_id)
        self.assertIsNotNone(loaded_chunk.derived_key_setup)
        self.assertIs(loaded_chunk.derived_key_setup, chunk.derived_key_setup)
        self.assertEqual(loaded_chunk.derived_key_setup.row_id, key_setup.row_id)


class TestFile(DatabaseSetup):

    @staticmethod
    def create_project():
        tmp_dir = TemporaryDirectory()
        os.makedirs(os.path.join(tmp_dir.name, 'subdir'))
        project = datatypes.Project(base_path=tmp_dir.name, name='foo')
        with datatypes.make_session() as session:
            project.create_db_entry(session)
        return tmp_dir, project

    def test_init(self):
        tmp_dir, project = self.create_project()
        path_a = os.path.join(tmp_dir.name, 'tmpfile_a')
        path_b = os.path.join(tmp_dir.name, 'subdir', 'tmpfile_b')
        with open(path_a, 'wb') as f_out:
            f_out.write(bytearray([5, 5, 5, 5, 5]))
        with open(path_b, 'wb') as f_out:
            f_out.write(bytearray([6, 6, 6, 6, 6, 6]))
        testfiles = [
            datatypes.File(path='', name='tmpfile_a', project_id=project.row_id),
            datatypes.File(path='subdir', name='tmpfile_b', project_id=project.row_id),
            datatypes.File(path='', name='subdir', project_id=project.row_id, is_folder=True)
        ]
        with datatypes.make_session() as session:
            for f in testfiles:
                f.create_db_entry(session)
        self.assertEqual([False, False, True], [x.is_folder for x in testfiles])
        self.assertEqual([None, None, None], [x.size for x in testfiles])
        self.assertEqual([False, False, False], [x.outdated for x in testfiles])
        for f in testfiles:
            f.set_size(project.base_path)
        self.assertEqual([False, False, True], [x.is_folder for x in testfiles])
        self.assertEqual([5, 6, None], [x.size for x in testfiles])

    def test_chunks(self):
        tmp_dir, project = self.create_project()
        path_a = os.path.join(tmp_dir.name, 'tmpfile_a')
        with open(path_a, 'wb') as f_out:
            f_out.write(bytearray([5, 5, 5, 5, 5]))
        testfile = datatypes.File(path='', name='tmpfile_a', project_id=project.row_id)
        testfile.set_size(project.base_path)
        with datatypes.make_session() as session:
            testfile.create_db_entry(session)
            # testfile.load_chunks(session)
        self.assertEqual(len(testfile.chunks), 0)
        chunks = [
            datatypes.Chunk(0, 2, testfile.row_id),
            datatypes.Chunk(2, 3, testfile.row_id),
        ]
        with datatypes.make_session() as session:
            for x in chunks:
                x.create_db_entry(session)
        # with datatypes.make_session() as session:
        #     testfile.load_chunks(session)
        self.assertEqual(len(testfile.chunks), 2)
        self.assertEqual([1, 2], [x for x in testfile.chunks])
        self.assertIs(testfile.chunks[1], chunks[0])
        self.assertIs(testfile.chunks[2], chunks[1])
        # add a trailing chunk
        chunk_trailing = datatypes.Chunk(5, 1, testfile.row_id)
        with datatypes.make_session() as session:
            chunk_trailing.create_db_entry(session)
            with self.assertRaises(datatypes.ChunkBoundaryError):
                testfile.verify_chunk_ranges()
            chunk_trailing.remove_from_db(session)
            chunks[0].remove_from_db(session)
            chunks[1].remove_from_db(session)
        # overlapping chunks
        chunks = [
            datatypes.Chunk(0, 2, testfile.row_id),
            datatypes.Chunk(1, 4, testfile.row_id),
        ]
        with datatypes.make_session() as session:
            for x in chunks:
                x.create_db_entry(session)
            with self.assertRaises(datatypes.ChunkBoundaryError):
                testfile.verify_chunk_ranges()
            chunks[0].remove_from_db(session)
            chunks[1].remove_from_db(session)
        # disjoined chunks
        chunks = [
            datatypes.Chunk(0, 1, testfile.row_id),
            datatypes.Chunk(2, 3, testfile.row_id),
        ]
        with datatypes.make_session() as session:
            for x in chunks:
                x.create_db_entry(session)
            with self.assertRaises(datatypes.ChunkBoundaryError):
                testfile.verify_chunk_ranges()

    def test_chunk_dropping(self):
        tmp_dir, project = self.create_project()
        path_a = os.path.join(tmp_dir.name, 'tmpfile_a')
        with open(path_a, 'wb') as f_out:
            f_out.write(bytearray([5, 5, 5, 5, 5]))
        testfile = datatypes.File(path='', name='tmpfile_a', project_id=project.row_id)
        testfile.set_size(project.base_path)
        with datatypes.make_session() as session:
            testfile.create_db_entry(session)
        chunks = [
            datatypes.Chunk(0, 2, testfile.row_id),
            datatypes.Chunk(2, 3, testfile.row_id),
        ]
        with datatypes.make_session() as session:
            for x in chunks:
                x.create_db_entry(session)
        with datatypes.make_session() as session:
            testfile.load_chunks(session)
        testfile.drop_chunks()
        self.assertEqual(len(testfile.chunks), 0)
        with datatypes.make_session() as session:
            testfile.load_chunks(session)
        self.assertEqual(len(testfile.chunks), 0)


# ToDo: basic tests for Project
class TestProject(DatabaseSetup):

    def test_init(self):
        tmp_dir = TemporaryDirectory()
        project = datatypes.Project(base_path=tmp_dir.name, name='foo')
        with datatypes.make_session() as session:
            project.create_db_entry(session)
        project = datatypes.Project(base_path=tmp_dir.name, name='bar', vault='abc')
        with datatypes.make_session() as session:
            project.create_db_entry(session)
        del project
        datatypes.Project.clear_object_index()
        with datatypes.make_session() as session:
            p1 = datatypes.Project.from_db(session, 1)
            self.assertEqual('foo', p1.name)
            self.assertEqual(tmp_dir.name, p1.base_path)
            self.assertIsNone(p1.vault)
            p2 = datatypes.Project.from_db(session, 2)
            self.assertEqual('bar', p2.name)
            self.assertEqual(tmp_dir.name, p2.base_path)
            self.assertEqual('abc', p2.vault)
        project = datatypes.Project(base_path=os.path.join(tmp_dir.name, '..'), name='foo')
        self.assertEqual(os.path.dirname(os.path.abspath(tmp_dir.name)), project.base_path)



class TestOther(DatabaseSetup):

    def test_get_overview(self):
        projects = datatypes.get_overview()
        self.assertFalse(len(projects))
        pfoo = datatypes.Project(name='foo', base_path='/', vault='fooo')
        pbar = datatypes.Project(name='bar', base_path='/', vault='fooo')
        with datatypes.make_session() as session:
            pfoo.create_db_entry(session)
            pbar.create_db_entry(session)
        projects = datatypes.get_overview()
        self.assertEqual([('fooo', 'bar'), ('fooo', 'foo')], [(x.vault, x.name) for x in projects])
