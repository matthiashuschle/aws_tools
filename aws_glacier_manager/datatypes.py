import os
import datetime
from collections import OrderedDict, defaultdict
import glob
from typing import Union, List
from pathlib import Path
import logging
from abc import ABC, abstractmethod
from contextlib import suppress
from itertools import chain
from . import database
from .database import (make_session, TabProject, TabFile, TabDerivedKeySetup, TabChunk, 
                       TabInventoryRequest, TabInventoryResponse)
from .encryption import DerivedKeySetup as DKS
from sqlalchemy.sql.expression import null, func


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
    required_cols = ['name']

    def __init__(self, name, vault=None, project_id=None, base_path=None):
        super(Project, self).__init__()
        # == begin DB mapping
        self.name = name
        self.vault = vault
        self.project_id = project_id
        # == end DB mapping
        self._base_path = None if base_path is None else str(Path(base_path).resolve())  # only local
        self.files = {}  # relative path as key -> easier folder comparison

    @property
    def base_path(self) -> str:
        return self._base_path

    @base_path.setter
    def base_path(self, val):
        self._base_path = None if val is None else str(Path(val).resolve())

    def update_dependencies(self, session):
        self.load_files(session)
        if self.is_deleted:
            self.remove_all_files(session)

    def load_files(self, session):
        file_rows = session.query(TabFile)\
            .filter(not TabFile.outdated, TabFile.project_id == self.project_id)\
            .all()
        loaded_files = {row.path: File.from_db(session, row=row) for row in file_rows}
        self.files.update(loaded_files)

    def _get_folder_content(self, path: Path) -> List[Path]:
        """ Get content of a single folder, excluding known files. """
        content = [x for x in Path(self.base_path).joinpath(path).iterdir()
                   if str(x.relative_to(self.base_path)) not in self.files]
        return content

    def _add_file(self, path: Path) -> 'Union[File, None]':
        """ Add a single file.

        Returns None if it is already known.
        May raise IsADirectoryError.

        :param str path: file path
        :rtype: File
        """
        assert not path.is_absolute()
        full_path = Path(self.base_path).joinpath(path)
        if str(path) in self.files:
            return None
        if not full_path.exists():
            raise FileNotFoundError
        if full_path.is_dir():
            raise IsADirectoryError
        file = File(
            name=path.name,
            path=str(path.parent),
            project_id=self.project_id,
        )
        file.set_size(self.base_path)
        self.files[str(path)] = file
        return file

    def _to_relative_paths(self, paths: List[Path]) -> List[Path]:
        """ Convert list of paths to relative to base_path. """
        assert self.base_path
        for path in paths:
            if '..' in path.parts:
                raise OSError('can not handle ".." in path, might break due to symlinks: %s' % path)
            if not len(path.name):
                raise OSError('empty file name is illegal: %s' % path)
        return [path.absolute().relative_to(self.base_path) for path in paths]

    def add_files(self, paths: List[Path], session=None) -> List['File']:
        """ Add files given in list, including subdirectory content.

        Folders are not added, but their containing files are.
        """
        paths = self._to_relative_paths(paths)
        files = []
        with make_session(session=session) as session:
            for path in paths:
                print(path)
                try:
                    file = self._add_file(path)
                except FileNotFoundError:
                    logging.getLogger(__name__).warning('file not found: %s' % path)
                    continue
                except IsADirectoryError:
                    files.extend(self.add_files(
                        paths=self._get_folder_content(path),
                        session=session,
                    ))
                    continue
                if file is not None:
                    file.create_db_entry(session)
                    files.append(file)
        return files

    @classmethod
    def load_all(cls):
        with make_session() as session:
            return [cls.from_db(session, row=row) for row in session.query(cls.table_class).all()]

    @classmethod
    def load_named(cls, name):
        with make_session() as session:
            row = session.query(cls.table_class).filter(TabProject.name == name).first()
            if not row:
                return None
            return cls.from_db(session=session, row=row)

    def remove_all_files(self, session=None):
        with make_session(session) as session:
            for file in self.files:
                file.remove_from_db(session)
        self.files = {x: y for x, y in self.files.items() if not y.is_deleted}

    def remove_files(self, paths: List[Path], session=None):
        """ Remove files. Acts like add_files, but removes them.
        """
        paths = self._to_relative_paths(paths)
        files_rel = {}
        for path in paths:
            print(path)
            path_rel = str(path)
            if path_rel in self.files:
                files_rel[path_rel] = self.files[path_rel]
                continue
            # could be folder
            subcontent = {x: y for x, y in self.files.items() if self._path_contains(path, Path(y.path))}
            if not len(subcontent):
                logging.getLogger(__name__).warning('file not found: %s' % path)
            else:
                files_rel.update(subcontent)
        with make_session(session=session) as session:
            for path_rel, file in files_rel.items():
                self._drop_file(file, session)

    @staticmethod
    def _path_contains(parent: Path, child: Path) -> bool:
        if parent == child:
            return True
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False

    def _drop_file(self, file, session):
        assert file.row_id in {x.row_id for x in self.files.values()}
        file.remove_from_db(session)
        del self.files[str(file.path_obj)]


class File(MappedBase):

    table_class = TabFile
    optional_cols = ['size', 'outdated', table_class.id_column]
    required_cols = ['path', 'name', 'project_id']

    def __init__(self, path, name, project_id,
                 size=None, outdated=None, file_id=None):
        """ init signature reflects non-nullable (args), nullable (=None) and default values (=value)
        of ORM-class. """
        super(File, self).__init__()
        # == begin DB mapping
        self.file_id = file_id
        self.name = name
        self.path = path
        self.project_id = project_id
        self.size = size
        self.outdated = outdated
        # == end DB mapping

    def update_dependencies(self, session):
        self.load_chunks(session)
        if self.is_deleted:
            self.drop_chunks(session)

    def set_size(self, base_path):
        print(base_path, self.path, self.name)
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

    @property
    def path_obj(self):
        return Path(self.path).joinpath(Path(self.name))

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


class DerivedKeySetup(MappedBase, DKS):

    table_class = TabDerivedKeySetup
    optional_cols = [table_class.id_column, 'key_size_sig', 'salt_key_sig']
    required_cols = ['construct', 'ops', 'mem', 'key_size_enc', 'salt_key_enc']

    def __init__(self, *args, derived_key_setup_id=None, **kwargs):
        super(DerivedKeySetup, self).__init__(*args, **kwargs)
        # == begin DB mapping
        self.derived_key_setup_id = derived_key_setup_id
        # == end DB mapping


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
    database.create_tables()
    projects = [x for x in sorted(Project.load_all(), key=lambda p: (p.vault, p.name))]
    return projects
