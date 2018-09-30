import os
import datetime
from collections import OrderedDict
import glob
from contextlib import suppress
from abc import ABC
from itertools import chain
from . import database
from .database import TabProject, TabFile, TabDerivedKeySetup, TabChunk, TabInventoryRequest, TabInventoryResponse
from sqlalchemy.sql.expression import null, func
from nacl import pwhash, utils, secret


# the ORM-classes are in database.py. They should not be used by any classes outside of this module.
# Here Project, File, etc. are implemented as classes, that use the underlying ORM.


class UnknownId(Exception):
    pass


class MappedBase(ABC):

    table_class = database.BackupLogBase  # ORM class
    required_cols = []
    optional_cols = []

    def __init__(self, *args, **kwargs):
        for arg in self.required_cols:
            if arg not in kwargs:
                raise AttributeError('missing required argument: %s' % arg)
            setattr(self, arg, kwargs[arg])
            del kwargs[arg]
        for arg in self.optional_cols:
            setattr(self, arg, kwargs.get(arg, None))
            with suppress(KeyError):
                del kwargs[arg]
        if len(kwargs):
            raise AttributeError('unexpected attributes: %s' % [x for x in kwargs])
        self.is_deleted = False

    def update_from(self, **kwargs):
        for arg, val in kwargs.items():
            if arg not in self.columns_for_update():
                raise AttributeError('argument %s is unknown' % arg)
            setattr(self, arg, val)

    @classmethod
    def columns_for_update(cls):
        return {x for x in chain(cls.required_cols, cls.optional_cols)}

    @classmethod
    def id_col_property(cls):
        return getattr(cls.table_class, cls.table_class.id_column)

    @classmethod
    def col_property(cls, column_name):
        return getattr(cls.table_class, column_name)

    def create_db_row(self):
        """ Create ORM instance """
        colnames = self.table_class.__table__.columns.keys()
        kwargs = {name: getattr(self, name) for name in colnames}
        row = self.table_class(**kwargs)
        return row

    @property
    def row_id(self):
        """ Universal row ID.

        :rtype: int
        """
        return getattr(self, self.table_class.id_column)

    @row_id.setter
    def row_id(self, val):
        """ Assign row ID.

        :param int val: new ID
        """
        setattr(self, self.table_class.id_column, val)

    @classmethod
    def _get_row_by_id(cls, row_id, session):
        """ Retrieve the ORM instance to the given ID.

        :param int row_id: ID value
        :param Session session:
        :return: ORM instance
        :raises: UnknownId
        """
        row = session.query(cls.table_class).filter(cls.id_col_property() == row_id).first()
        if row is None:
            raise UnknownId('unknown id for %s: %i' % (cls.table_class.__tablename__, row_id))
        return row

    @classmethod
    def _create_and_get_id(cls, session, row):
        """ Push the ORM-Instance to the database and return the assigned ID.

        :param Session session: database session
        :param row: ORM instance representing a table row
        :return: new ID
        :rtype: int
        """
        session.add(row)
        session.flush()
        session.refresh(row)
        return getattr(row, cls.table_class.id_column)

    def create_db_entry(self, session):
        """ Verifies that the given ID is valid, or creates a new row, if it is empty. """
        if self.row_id:
            return
        # write and set project ID
        row = self.create_db_row()
        self.row_id = self._create_and_get_id(session, row)
        assert self.row_id is not None, 'id field is None!'

    def update_db(self, session):
        row = self._get_row_by_id(self.row_id, session)
        for col in self.columns_for_update():
            this_val = getattr(self, col)
            if this_val != getattr(row, col):
                setattr(row, col, getattr(self, col))

    def remove_from_db(self, session):
        assert not self.is_deleted, 'double deletion of id %s' % repr(self.row_id)
        session.query(self.table_class).filter(self.id_col_property() == self.row_id).delete()
        self.is_deleted = True

    @classmethod
    def from_db(cls, session, row_id=None, row=None):
        """ Create instance from database row ID or row """
        assert row_id or row
        row = row or cls._get_row_by_id(row_id, session=session)
        colnames = cls.table_class.__table__.columns.keys()
        kwargs = {name: getattr(row, name) for name in colnames}
        return cls(**kwargs)


class Project(MappedBase):

    table_class = TabProject
    optional_cols = ['vault', table_class.id_column]
    required_cols = ['base_path', 'name']

    def __init__(self, **kwargs):
        self.name = None
        self.base_path = None
        self.vault = None
        self.project_id = None
        if kwargs.get('base_path'):
            kwargs['base_path'] = os.path.abspath(kwargs['base_path'])
        super(Project, self).__init__(**kwargs)
        self.files = {}  # relative path as key -> easier folder comparison

    def load_files(self, session):
        file_rows = session.query(TabFile)\
            .filter(not TabFile.outdated, TabFile.project_id == self.project_id)\
            .all()
        loaded_files = {row.path: File.from_db(session, row=row) for row in file_rows}
        self.files.update(loaded_files)
        self.update_folders()

    def update_folders(self):
        for file in self.files.values():
            if not file.is_folder:
                continue
            self.update_folder(file)

    def update_folder(self, file):
        paths = [self._to_relative_path(x) for x in glob.glob(os.path.join(self.base_path, file.path, '*'))]
        # ignore known stuff. its subfolders are handled by another loop run in self.update_folders
        paths = [x for x in paths if x not in self.files]
        if not len(paths):
            return
        files = self.add_files(paths, update=False)
        for file in files:
            if file.is_folder:
                self.update_folder(file)

    def add_file(self, path):
        path, relative_path = self._to_relative_path(path)
        if relative_path in self.files:
            return None
        file = File(
            name=os.path.basename(path),
            path=relative_path,
            project_id=self.project_id,
            is_folder=os.path.isdir(path)
        )
        if not file.is_folder:
            file.set_size(self.base_path)
        self.files[relative_path] = file
        return file

    def _to_relative_path(self, path):
        path = os.path.abspath(path)
        assert os.path.commonpath([path, self.base_path]) == self.base_path
        relative_path = os.path.relpath(path, self.base_path)
        return path, relative_path

    def add_files(self, paths, update=True):
        files = []
        with database.make_session() as session:
            for path in paths:
                file = self.add_file(path)
                if file is not None:
                    file.create_db_entry(session)
                    files.append(file)
        if update:
            self.update_folders()
        return files

    @classmethod
    def load_all(cls):
        with database.make_session() as session:
            return [cls.from_db(session, row=row) for row in session.query(cls.table_class).all()]

    def drop_files(self):
        with database.make_session() as session:
            for file in self.files:
                file.remove_from_db(session)
        self.files = {x: y for x, y in self.files.items() if not y.is_deleted}

    def drop_file(self, file, session):
        assert file.row_id in {x.row_id for x in self.files}
        file.remove_from_db(session)
        del self.files[file.path]


class File(MappedBase):

    table_class = TabFile
    optional_cols = ['is_folder', 'size', 'outdated', table_class.id_column]
    required_cols = ['path', 'name', 'project_id']

    def __init__(self, **kwargs):
        """ init signature reflects non-nullable (args), nullable (=None) and default values (=value)
        of ORM-class. """
        self.file_id = None
        self.name = None
        self.path = None
        self.project_id = None
        self.is_folder = None
        self.size = None
        self.outdated = None
        super(File, self).__init__(**kwargs)
        self.chunks = None

    def set_size(self, base_path):
        if self.is_folder:
            return
        self.size = get_file_size(os.path.join(base_path, self.path))

    def load_chunks(self, session):
        if not self.file_id:
            raise RuntimeError('file does not have an ID!')
        chunk_rows = session.query(TabChunk)\
            .filter(TabChunk.file_id == self.file_id)\
            .all()
        loaded_chunks = [Chunk.from_db(session, row=row) for row in chunk_rows]
        if sum(x.size for x in loaded_chunks) != self.size:
            raise ValueError('Chunk size invalid!')
        self.chunks = to_ordered_dict({x.row_id: x for x in loaded_chunks})
        
    def drop_chunks(self):
        with database.make_session() as session:
            for chunk in self.chunks:
                chunk.remove_from_db(session)
        self.chunks = OrderedDict([x for x in self.chunks.items() if not x[1].is_deleted])
        
    def drop_chunk(self, chunk, session):
        assert chunk.row_id in {x.row_id for x in self.chunks}
        chunk.remove_from_db(session)
        del self.chunks[chunk.row_id]


class Chunk(MappedBase):

    table_class = TabChunk
    optional_cols = [table_class.id_column, 'upload_id', 'checksum', 'signature_type', 'signature',
                     'verify_key', 'derived_key_setup_id', 'encrypted']
    required_cols = ['start_offset', 'size', 'file_id']

    def __init__(self, **kwargs):
        """ init signature reflects non-nullable (args), nullable (=None) and default values (=value)
        of ORM-class. """
        self.chunk_id = None
        self.upload_id = None
        self.checksum = None
        self.signature_type = None
        self.signature = None
        self.verify_key = None
        self.derived_key_setup_id = None
        self.start_offset = None
        self.size = None
        self.encrypted = None
        self.file_id = None
        super(Chunk, self).__init__(**kwargs)
        self.derived_key_setup = None

    def load_derived_key_setup(self, session):
        if not self.derived_key_setup_id:
            raise RuntimeError('chunk does not have an ID for derived key setup!')
        self.derived_key_setup = session.query(TabDerivedKeySetup)\
            .filter(TabDerivedKeySetup.id_col_property() == self.derived_key_setup_id)\
            .first()


class DerivedKeySetup(MappedBase):

    table_class = TabDerivedKeySetup
    optional_cols = [table_class.id_column, 'key_size_sig', 'salt_key_sig']
    required_cols = ['construct', 'ops', 'mem', 'key_size_enc', 'salt_key_enc']

    def __init__(self, **kwargs):
        self.derived_key_setup_id = None
        self.construct = None
        self.ops = None
        self.mem = None
        self.key_size_enc = None
        self.key_size_sig = None
        self.salt_key_enc = None
        self.salt_key_sig = None
        super(DerivedKeySetup, self).__init__(**kwargs)

    @classmethod
    def create_default(cls, enable_auth_key=False):
        """ Create default settings for encryption key derivation from password.

        original source: https://pynacl.readthedocs.io/en/stable/password_hashing/#key-derivation
        :param bool enable_auth_key: generate a key for full data signatures via HMAC
        :rtype: DerivedKeySetup
        """
        return cls(
            ops=pwhash.argon2i.OPSLIMIT_SENSITIVE,
            mem=pwhash.argon2i.MEMLIMIT_SENSITIVE,
            construct='argon2i',
            salt_key_enc=utils.random(pwhash.argon2i.SALTBYTES),
            salt_key_sig=utils.random(pwhash.argon2i.SALTBYTES) if enable_auth_key else b'',
            key_size_enc=secret.SecretBox.KEY_SIZE,
            key_size_sig=64 if enable_auth_key else 0
        )


class InventoryRequest(MappedBase):

    table_class = TabInventoryRequest
    required_cols = ['vault_name', 'sent_dt', 'job_id']
    optional_cols = [table_class.id_column]

    def __init__(self, **kwargs):
        self.request_id = None
        self.vault_name = None
        self.sent_dt = None
        self.job_id = None
        super(InventoryRequest, self).__init__(**kwargs)


class InventoryResponse(MappedBase):

    table_class = TabInventoryResponse
    required_cols = ['retrieved_dt', 'request_id']
    optional_cols = [table_class.id_column, 'content_type', 'status', 'body']

    def __init__(self, **kwargs):
        self.response_id = None
        self.request_id = None
        self.retrieved_dt = None
        self.content_type = None
        self.status = None
        self.body = None
        super(InventoryResponse, self).__init__(**kwargs)


class InventoryHandler:

    def __init__(self, vault_name):
        self.vault_name = vault_name

    def store_request(self, request_dict):
        job_id = request_dict['jobId']
        sent_dt = datetime.datetime.utcnow()
        request = InventoryRequest(
            vault_name=self.vault_name,
            sent_dt=sent_dt,
            job_id=job_id
        )
        with database.make_session() as session:
            request.create_db_entry(session)
        return request

    @staticmethod
    def store_response(request_id, response_dict):
        kwargs = {
            'retrieved_dt': datetime.datetime.utcnow(),
            'content_type': response_dict['contentType'],
            'status': response_dict['status'],
            'body': response_dict['body'].read(),
            'request_id': request_id
        }
        with database.make_session() as session:
            response_row = session.query(TabInventoryResponse)\
                .filter(TabInventoryResponse.request_id == request_id)\
                .first()
            if response_row is None:
                response = InventoryResponse(**kwargs)
                response.create_db_entry(session)
            else:
                response = InventoryResponse.from_db(session, row=response_row)
                response.update_from(**kwargs)
                response.update_db(session)
        return response

    def get_open_requests(self):
        with database.make_session() as session:
            requests = session.query(TabInventoryRequest)\
                .outerjoin(TabInventoryRequest.response)\
                .filter(
                TabInventoryRequest.vault_name == self.vault_name,
                TabInventoryResponse.response_id == null()).all()
            return [InventoryRequest.from_db(session, row=row) for row in requests or []]

    def get_latest_response(self):
        with database.make_session() as session:
            latest_response = session.query(func.max(TabInventoryRequest.sent_dt))\
                .join(TabInventoryRequest.response)\
                .filter(
                TabInventoryRequest.vault_name == self.vault_name,
                TabInventoryResponse.response_id != null()
            ).scalar()
            if latest_response is None:
                return None
            latest_response = session.query(TabInventoryResponse)\
                .join(TabInventoryResponse.request)\
                .filter(
                TabInventoryRequest.vault_name == self.vault_name,
                TabInventoryRequest.sent_dt == latest_response).first()
            if latest_response is not None:
                return InventoryResponse.from_db(session, row=latest_response)


def get_file_size(filepath):
    return os.stat(filepath).st_size


def to_ordered_dict(some_dict, **kwargs):
    return OrderedDict([(key, some_dict[key]) for key in sorted(some_dict.keys(), **kwargs)])


def get_overview():
    """ Returns the basic project overview: pairs of (vault, name), sorted.

    :rtype: list
    """
    projects = [x for x in sorted(Project.load_all(), key=lambda p: (p.vault, p.name))]
    return projects
