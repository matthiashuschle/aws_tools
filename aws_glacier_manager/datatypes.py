import os
import datetime
from collections import OrderedDict, defaultdict
import glob
from abc import ABC, abstractmethod
from contextlib import suppress
from itertools import chain
from . import database
from .database import (make_session, TabProject, TabFile, TabDerivedKeySetup, TabChunk, 
                       TabInventoryRequest, TabInventoryResponse)
from sqlalchemy.sql.expression import null, func
from nacl import pwhash, utils, secret


# the ORM-classes are in database.py. They should not be used by any classes outside of this module.
# Here Project, File, etc. are implemented as classes, that use the underlying ORM.


class UnknownId(Exception):
    pass


class ChunkBoundaryError(Exception):
    pass


class MappedBase(ABC):

    object_index = defaultdict(dict)  # stores objects by IDs to avoid multiple instances
    table_class = database.BackupLogBase  # ORM class
    required_cols = []
    optional_cols = []

    @abstractmethod
    def __init__(self):
        """ The inheriting contructors MUST be able to handle all required and optional database
        fields as keyword arguments! """
        self.is_deleted = False

    @classmethod
    def from_row(cls, row):
        colnames = cls.table_class.__table__.columns.keys()
        kwargs = {name: getattr(row, name) for name in colnames}
        return cls(**kwargs)

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
    def _create_and_get_in_db(cls, session, row):
        """ Push the ORM-Instance to the database and return the assigned values incl. ID.

        :param Session session: database session
        :param row: ORM instance representing a table row
        :return: new ID
        :rtype: int
        """
        session.add(row)
        session.flush()
        session.refresh(row)
        return row

    def create_db_entry(self, session):
        """ Creates a new row, if ID is empty. """
        if self.is_deleted:
            raise RuntimeError('trying to operate on deleted row.')
        if self.row_id:
            return
        # write and set fields
        row = self._create_and_get_in_db(session, self.create_db_row())
        self._update_from_row(row)
        assert self.row_id is not None, 'id field is None!'
        # store in the object index
        # noinspection PyTypeChecker
        self.object_index[self.__class__.__name__][self.row_id] = self
        self.update_dependencies(session)

    def _update_from_row(self, row):
        for arg in self.columns_for_update():
            if arg == self.table_class.id_column:
                self.row_id = getattr(row, arg)
            else:
                setattr(self, arg, getattr(row, arg))

    def update_db(self, session):
        """ Update the database with the current values.

        - fetches default values afterwards
        - tested in TestDerivedKeySetup.test_update

        :param session: database session
        :return: None
        """
        if self.is_deleted:
            raise RuntimeError('trying to operate on deleted row.')
        if self.row_id is None:
            return self.create_db_entry(session)
        row = self._get_row_by_id(self.row_id, session)
        for col in self.columns_for_update():
            this_val = getattr(self, col)
            if this_val != getattr(row, col):
                setattr(row, col, getattr(self, col))
        session.flush()
        session.refresh(row)
        self._update_from_row(row)
        self.update_dependencies(session)

    def remove_from_db(self, session):
        assert not self.is_deleted, 'double deletion of id %s' % repr(self.row_id)
        session.query(self.table_class).filter(self.id_col_property() == self.row_id).delete()
        self.is_deleted = True
        self.update_dependencies(session)
        del self.object_index[self.__class__.__name__][self.row_id]
        self.row_id = None

    def update_dependencies(self, session):
        """ Called if an instance is created from a DataBase entry. Loads the linked objects. """
        pass

    @classmethod
    def clear_object_index(cls):
        cls.object_index[cls.__name__].clear()

    @classmethod
    def from_db(cls, session, row_id=None, row=None):
        """ Create instance from database row ID or row """
        assert row_id or row
        if row:
            # get the id from the row
            row_id = getattr(row, cls.table_class.id_column)
        if row_id:
            # try to get the existing object
            inst = cls.object_index[cls.__name__].get(row_id)
            if inst is not None and not inst.is_deleted:
                return inst
        # create new object and store
        row = row or cls._get_row_by_id(row_id, session=session)
        inst = cls.from_row(row)
        inst.update_dependencies(session)
        # noinspection PyTypeChecker
        cls.object_index[cls.__name__][inst.row_id] = inst
        return inst


class Project(MappedBase):

    table_class = TabProject
    optional_cols = ['vault', table_class.id_column]
    required_cols = ['base_path', 'name']

    def __init__(self, name, base_path, vault=None, project_id=None):
        super(Project, self).__init__()
        # == begin DB mapping
        self.name = name
        self.base_path = os.path.abspath(base_path)
        self.vault = vault
        self.project_id = project_id
        # == end DB mapping
        self.files = {}  # relative path as key -> easier folder comparison

    def update_dependencies(self, session):
        self.load_files(session)
        # ToDo: delete File objects on delete

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
        with make_session() as session:
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
        with make_session() as session:
            return [cls.from_db(session, row=row) for row in session.query(cls.table_class).all()]

    def drop_files(self):
        with make_session() as session:
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

    def __init__(self, path, name, project_id,
                 is_folder=None, size=None, outdated=None, file_id=None):
        """ init signature reflects non-nullable (args), nullable (=None) and default values (=value)
        of ORM-class. """
        super(File, self).__init__()
        # == begin DB mapping
        self.file_id = file_id
        self.name = name
        self.path = path
        self.project_id = project_id
        self.is_folder = is_folder  # this should be set on creation!
        self.size = size
        self.outdated = outdated
        # == end DB mapping

    def update_dependencies(self, session):
        self.load_chunks(session)
        # ToDo: drop chunks on delete

    def set_size(self, base_path):
        if self.is_folder:
            return
        self.size = get_file_size(os.path.join(base_path, self.path, self.name))

    @property
    def project(self):
        """ Get the project from object cache.

        :rtype: Project
        """
        return self.object_index['Project'].get(self.project_id)

    @property
    def chunks(self):
        """ Get the chunks from object cache.

        :rtype: dict
        """
        return to_ordered_dict({x.row_id: x for x in self.object_index['Chunk'].values()
                                if x.file_id == self.file_id and not x.is_deleted})

    def load_chunks(self, session):
        if not self.file_id:
            raise RuntimeError('file does not have an ID!')
        chunk_rows = session.query(TabChunk)\
            .filter(TabChunk.file_id == self.file_id)\
            .all()
        # load into object_cache
        for row in chunk_rows:
            Chunk.from_db(session, row=row)

    def verify_chunk_ranges(self):
        # ToDo: use on operations
        offsets = {x.start_offset for x in self.chunks.values()}
        ends = {x.start_offset + x.size for x in self.chunks.values()}
        remaining = (offsets - ends) | (ends - offsets)
        if 0 not in remaining:
            raise ChunkBoundaryError('no Chunk with offset 0!')
        if self.size not in remaining:
            raise ChunkBoundaryError('Chunks don\'t add up to file size!')
        if len(remaining) > 2:
            raise ChunkBoundaryError('Chunks boundaries are not aligned!')

    def drop_chunks(self, session=None):
        with make_session(session) as session:
            for chunk in self.chunks.values():
                chunk.remove_from_db(session)
        
    def drop_chunk(self, chunk, session):
        assert chunk.row_id in self.chunks
        chunk.remove_from_db(session)


class Chunk(MappedBase):

    table_class = TabChunk
    optional_cols = [table_class.id_column, 'upload_id', 'checksum', 'signature_type', 'signature',
                     'verify_key', 'derived_key_setup_id', 'encrypted']
    required_cols = ['start_offset', 'size', 'file_id']

    def __init__(self, start_offset, size, file_id,
                 upload_id=None, checksum=None, signature_type=None, signature=None,
                 verify_key=None, derived_key_setup_id=None, encrypted=None,
                 chunk_id=None):
        """ Defines a part of a file that becomes the uploaded object.

        required keywords: ['start_offset', 'size', 'file_id']
        optional keywords: ['chunk_id', 'upload_id', 'checksum', 'signature_type', 'signature',
            'verify_key', 'derived_key_setup_id', 'encrypted']
        """
        super(Chunk, self).__init__()
        # == begin DB mapping
        self.chunk_id = chunk_id
        self.start_offset = start_offset
        self.size = size
        self.file_id = file_id
        self.upload_id = upload_id
        self.checksum = checksum
        self.signature_type = signature_type
        self.signature = signature
        self.verify_key = verify_key
        self.derived_key_setup_id = derived_key_setup_id
        self.encrypted = encrypted
        # == end DB mapping
        # relationships are reserved names in the underlying mapping class, but not synced to this class
        self.derived_key_setup = None

    def update_dependencies(self, session):
        if self.derived_key_setup_id is not None:
            self.derived_key_setup = DerivedKeySetup.from_db(session, row_id=self.derived_key_setup_id)

    def set_key_setup(self, derived_key_setup):
        """ Set the encryption key setup to use.

        :param DerivedKeySetup derived_key_setup: existing setup to use
        :returns: None
        """
        assert derived_key_setup.row_id is not None, 'derived_key_setup is not saved yet!'
        self.derived_key_setup_id = derived_key_setup.row_id
        self.derived_key_setup = derived_key_setup


class DerivedKeySetup(MappedBase):

    table_class = TabDerivedKeySetup
    optional_cols = [table_class.id_column, 'key_size_sig', 'salt_key_sig']
    required_cols = ['construct', 'ops', 'mem', 'key_size_enc', 'salt_key_enc']

    def __init__(self, construct, ops, mem, key_size_enc, salt_key_enc,
                 key_size_sig=None, salt_key_sig=None, derived_key_setup_id=None):
        super(DerivedKeySetup, self).__init__()
        # == begin DB mapping
        self.derived_key_setup_id = derived_key_setup_id
        self.construct = construct
        self.ops = ops
        self.mem = mem
        self.key_size_enc = key_size_enc
        self.key_size_sig = key_size_sig
        self.salt_key_enc = salt_key_enc
        self.salt_key_sig = salt_key_sig
        # == end DB mapping

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

    def __init__(self, vault_name, sent_dt, job_id, request_id=None):
        super(InventoryRequest, self).__init__()
        # == begin DB mapping
        self.request_id = request_id
        self.vault_name = vault_name
        self.sent_dt = sent_dt
        self.job_id = job_id
        # == end DB mapping


class InventoryResponse(MappedBase):

    table_class = TabInventoryResponse
    required_cols = ['retrieved_dt', 'request_id']
    optional_cols = [table_class.id_column, 'content_type', 'status', 'body']

    def __init__(self, request_id, retrieved_dt, content_type=None, status=None,
                 body=None, response_id=None):
        super(InventoryResponse, self).__init__()
        # == begin DB mapping
        self.response_id = response_id
        self.request_id = request_id
        self.retrieved_dt = retrieved_dt
        self.content_type = content_type
        self.status = status
        self.body = body
        # == end DB mapping
        self.request = None

    def update_dependencies(self, session):
        if self.request_id is None:
            return
        self.request = InventoryRequest.from_db(session, row_id=self.request_id)


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
        with make_session() as session:
            request.create_db_entry(session)
        return request

    @staticmethod
    def store_response(request, response_dict):
        kwargs = {
            'retrieved_dt': datetime.datetime.utcnow(),
            'content_type': response_dict['contentType'],
            'status': response_dict['status'],
            'body': response_dict['body'].read(),
            'request_id': request.row_id
        }
        with make_session() as session:
            response_row = session.query(TabInventoryResponse)\
                .filter(TabInventoryResponse.request_id == request.row_id)\
                .first()
            if response_row is None:
                response = InventoryResponse(**kwargs)
            else:
                response = InventoryResponse.from_db(session, row=response_row)
                response.update_from(**kwargs)
            response.update_db(session)
        return response

    def get_open_requests(self):
        with make_session() as session:
            requests = session.query(TabInventoryRequest)\
                .outerjoin(TabInventoryRequest.response)\
                .filter(
                TabInventoryRequest.vault_name == self.vault_name,
                TabInventoryResponse.response_id == null()).all()
            return [InventoryRequest.from_db(session, row=row) for row in requests or []]

    def get_latest_response(self):
        with make_session() as session:
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
