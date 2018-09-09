import os
import datetime
import glob
from contextlib import suppress
from abc import ABC, abstractmethod
from collections import OrderedDict, namedtuple
from itertools import chain
import sqlite3
from sqlalchemy import create_engine
from . import database
from .database import TabProject, TabFile, TabDerivedKeySetup, TabChunk, TabInventoryRequest, TabInventoryResponse
from sqlalchemy.sql.expression import and_
from nacl import pwhash, utils, secret
from contextlib import contextmanager


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
        with database.make_session() as session:
            self._create_db_entry(session)

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
        func_params = self.table_class.__init__.__code__.co_varnames[1:]
        print(func_params)
        args = [getattr(self, name) for name in func_params]
        row = self.table_class(*args)
        for col in self.columns_for_update():
            if col not in func_params:
                setattr(row, col, getattr(self, col))
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

    def _create_db_entry(self, session):
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

    @classmethod
    def from_db(cls, session, row_id=None, row=None):
        """ Create instance from database row ID or row """
        assert row_id or row
        row = row or cls._get_row_by_id(row_id, session=session)
        func_params = cls.table_class.__init__.__code__.co_varnames[1:]
        args = [getattr(row, name) for name in func_params]
        return cls(*args)


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
            .filter(TabFile.outdated == False, TabFile.project_id == self.project_id)\
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
        self.files[relative_path] = file
        return file

    def _to_relative_path(self, path):
        path = os.path.abspath(path)
        assert os.path.commonpath([path, self.base_path]) == self.base_path
        relative_path = os.path.relpath(path, self.base_path)
        return path, relative_path

    def add_files(self, paths, update=True):
        files = []
        for path in paths:
            file = self.add_file(path)
            if file is not None:
                files.append(file)
        if update:
            self.update_folders()
        return files


class File(MappedBase):

    table_class = TabFile
    id_column = 'file_id'
    nullable = [id_column, 'size']
    non_nullable = ['name', 'path', 'project_id']
    default_values = {'is_folder': False, 'outdated': False}

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

    def set_size(self, base_path):
        if self.is_folder:
            return
        self.size = get_file_size(os.path.join(base_path, self.path))


class DerivedKeySetup(MappedBase):

    table_class = TabDerivedKeySetup  # ORM class
    id_column = 'derived_key_setup_id'  # name of the ID in this class, and the property of the ORM class
    unique_columns = []  # columns that have a unique constraint. Checked on creation.
    nullable = [id_column]
    non_nullable = ['construct', 'ops', 'mem', 'key_size_enc', 'salt_key_enc']
    default_values = {'key_size_sig': 0, 'salt_key_sig'}

    def __init__(self, *args, **kwargs):



class Chunk(MappedBase):

    table_class = TabChunk
    id_column = 'chunk_id'
    nullalbe = [id_column, 'upload_id', 'checksum', 'signature_type', 'signature', 'verify_key', 'derived_key_setup_id']
    non_nullable = ['start_offset', 'size', 'encrypted', 'file_id']
    default_values = {'is_folder': False, 'outdated': False}

    def __init__(self, **kwargs):
        """ init signature reflects non-nullable (args), nullable (=None) and default values (=value)
        of ORM-class. """
        self.chunk_id = None
        self.upload_id = None
        self.checksum = None
        self.signature_type= None
        self.signature = None
        self.verify_key = None
        self.derived_key_setup_id = None
        self.start_offset = None
        self.size = None
        self.encrypted = None
        self.file_id = None
        self.is_folder = None
        self.outdated = None
        super(Chunk, self).__init__(**kwargs)

# --------------------------------------------------------------------------------
# old stuff

class DerivedKeySetup:
    
    _item_names = 'ops', 'mem', 'construct', 'salt_key_enc', 'salt_key_sig', 'key_size_enc', 'key_size_sig'

    def __init__(self, ops, mem, construct, salt_key_enc, salt_key_sig, key_size_enc, key_size_sig):
        self.ops = ops
        self.mem = mem
        self.construct = construct
        self.salt_key_enc = salt_key_enc
        self.salt_key_sig = salt_key_sig
        self.key_size_enc = key_size_enc
        self.key_size_sig = key_size_sig

    def __repr__(self):
        return repr('DerivedKeySetup(%s)' % ','.join('%s=%r' % (x, self[x]) for x in self._item_names))
    
    def __getitem__(self, item):
        return getattr(self, item)

    @classmethod
    def db_columns(cls):
        return ', '.join(cls._item_names)
    
    def to_db_tuple(self):
        return (
            self.ops,
            self.mem,
            self.construct,
            self.key_size_enc,
            self.key_size_sig,
            self.salt_key_enc,
            self.salt_key_sig
        )

    @classmethod
    def from_db_tuple(cls, db_tuple):
        return cls(
            ops=db_tuple[0],
            mem=db_tuple[1],
            construct=db_tuple[2],
            key_size_enc=db_tuple[3],
            key_size_sig=db_tuple[4],
            salt_key_enc=db_tuple[5],
            salt_key_sig=db_tuple[6]
        )

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


DerivedKeys = namedtuple('DerivedKeys', ['key_enc', 'key_sig', 'setup'])


ProjectInfo = namedtuple('ProjectInfo', 'name base_path num_files')


class Constraint:

    def __init__(self, this_column, other_table, other_column):
        self.this_column = this_column
        self.other_table = other_table
        self.other_column = other_column

    @property
    def fk_statement(self):
        return 'FOREIGN KEY ({this_column}) REFERENCES {other_table} ({other_column})'.format(
            other_table=self.other_table,
            other_column=self.other_column,
            this_column=self.this_column
        )

    def get_constraint(self, tablename):
        return 'CONSTRAINT fk_{t}_{other_table}_{other_column} {fk_statement}'.format(
            t=tablename,
            other_table=self.other_table,
            other_column=self.other_column,
            fk_statement=self.fk_statement
        )


class ConstraintIdentical(Constraint):

    def __init__(self, column, other_table):
        super(ConstraintIdentical, self).__init__(column, other_table, column)


class ConstraintUniqueIdx(Constraint):

    def __init__(self, column):
        super(ConstraintUniqueIdx, self).__init__(None, None, None)
        self.column = column

    def fk_statement(self):
        return None

    def get_constraint(self, tablename):
        return 'CONSTRAINT "uix_%s" UNIQUE (%s)' % (self.column, self.column)


class SQLiteLog(ABC):

    TABLE_DEF = OrderedDict([])
    TABLE_FK = {}
    DEFAULT_FILENAME = None

    def __init__(self, filename=None):
        self.filename = filename or self.DEFAULT_FILENAME
        if self.filename is None:
            raise NotImplementedError('Your derived class can not handle empty filenames!')
        self.init_db()

    @contextmanager
    def connect(self):
        with sqlite3.Connection(database=self.filename) as con:
            yield con

    def init_db(self):
        with self.connect() as con:
            cur = con.cursor()
            for tablename, coldefs in self.TABLE_DEF.items():
                elements = ['%s %s' % (x, y) for x, y in coldefs.items()]
                for foreign_key in self.TABLE_FK.get(tablename, []):
                    elements.append(foreign_key.get_constraint(tablename))
                cur.execute('CREATE TABLE IF NOT EXISTS %s (%s)' % (
                    tablename,
                    ', '.join(elements)
                ))


class BackupLogOld(SQLiteLog):

    TABLE_DEF = OrderedDict([
        ('project', OrderedDict([
            ('project_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('name', 'TEXT NOT NULL'),
            ('base_path', 'TEXT NOT NULL')
        ])),
        ('file', OrderedDict([
            ('file_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('is_folder', 'INTEGER DEFAULT 0'),
            ('name', 'TEXT NOT NULL'),
            ('path', 'TEXT NOT NULL'),
            ('size', 'INTEGER NOT NULL'),
            ('outdated', 'INTEGER DEFAULT 0'),
            ('project_id', 'INTEGER')
        ])),
        ('derived_key_setup', OrderedDict([
            ('derived_key_setup_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('construct', 'TEXT'),
            ('ops', 'INTEGER'),
            ('mem', 'INTEGER'),
            ('key_size_enc', 'INTEGER'),
            ('key_size_sig', 'INTEGER'),
            ('salt_key_enc', 'BLOB'),
            ('salt_key_sig', 'BLOB')
        ])),
        ('chunk', OrderedDict([
            ('chunk_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('upload_id', 'TEXT'),
            ('checksum', 'TEXT'),
            ('signature_type', 'TEXT'),
            ('signature', 'BLOB'),
            ('verify_key', 'BLOB'),
            ('start_offset', 'INTEGER NOT NULL'),
            ('size', 'INTEGER NOT NULL'),
            ('encrypted', 'INTEGER DEFAULT 0'),
            ('derived_key_setup_id', 'INTEGER'),
            ('file_id', 'INTEGER')
        ])),
    ])
    TABLE_FK = {
        'project': [ConstraintUniqueIdx('name')],
        'file': [ConstraintIdentical('project_id', 'project')],
        'chunk': [ConstraintIdentical('file_id', 'file')]
    }


class DBHandler:

    def __init__(self, connector_str, timeout_minutes=9):
        self.connector_str = connector_str
        self.timeout = datetime.timedelta(minutes=timeout_minutes)
        self._connection = None
        self._last_access = None

    @property
    def connection(self):
        if self._connection is None or self._last_access is None or \
                self._last_access + self.timeout < datetime.datetime.utcnow():
            self._connection = create_engine(self.connector_str, echo=True)
        self._last_access = datetime.datetime.utcnow()
        return self._connection


class BackupLog:
    DEFAULT_CONNECTOR_STRING = 'sqlite:///backup_log.sqlite'

    def __init__(self, project, base_path='/', connector_str=None, project_id=None):
        self.project = project
        self.base_path = base_path
        self.session_context = SessionContext(connector_str or self.DEFAULT_CONNECTOR_STRING)
        datatypes.BackupLogBase.metadata.create_all(bind=self.session_context.engine)
        self.id = project_id
        self._create_db_entry()

    @classmethod
    def get_project_row_by_name(cls, name, session):
        return session.query(datatypes.TabProject).filter_by(name=name).one()

    @classmethod
    def from_db_entry(cls, project_name, connector_str=None):
        session_context = SessionContext(connector_str or cls.DEFAULT_CONNECTOR_STRING)
        with session_context() as session:
            project = cls.get_project_row_by_name(project_name, session)
            if project is None:
                return None
            project = cls(project.name, project.base_path, connector_str, project_id=project.projecct_id)
        return project

    def _create_db_entry(self):
        with self.session_context() as session:
            if self.id is not None:
                # verify, that the id exists and return
                if session.query(datatypes.TabProject).filter_by(project_id=self.id).one() is None:
                    raise ValueError('Given project_id does not exist.')
                return
            # create the entry and set id
            project =

    def create(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT project_id FROM project WHERE name = ?', (self.project,))
            if len(cur.fetchall()):
                raise RuntimeError('project %s already exists!' % self.project)
            cur.execute('INSERT INTO project (name, base_path) VALUES (?, ?)', (self.project, self.base_path))

    def add_file(self, recursive=True):
        """ Add a file to the database. If it is a directory, descend. Should not follow symlinks to folders.

        This must be either complex or handled externally. Having directories and files is not a trivial task,
        as a directory may expand to more files over time, or files might be excluded from a directory"""
        pass

    def get_files_for_upload(self):
        """ Get list of stored files, for uploading, i.e. verify that they are unmodified and not uploaded yet."""
        pass

    def define_chunks(self, file):
        """ Create chunk information for a file. """
        pass

    def add_derived_key_setup(self, setup):
        """ Add a key derivation setup.

        :param DerivedKeySetup setup: key derivation settings
        :returns table entry ID
        :rtype: int
        """
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                'INSERT INTO derived_key_setup (%s) VALUES '
                '(?, ?, ?, ?, ?, ?, ?)' % setup.db_columns(), setup.to_db_tuple()
            )
            cur.execute('SELECT last_insert_rowid() FROM derived_key_setup')
            entry_id = cur.fetchall()[0][0]
        return entry_id

    def get_derived_key_setup(self, entry_id):
        """ Retrieve a key derivation setup from the database.

        :param int entry_id: row ID
        :rtype: DerivedKeySetup
        """
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                'SELECT %s FROM derived_key_setup WHERE derived_key_setup_id = %i' %
                (DerivedKeySetup.db_columns(), entry_id)
            )
            setup = cur.fetchall()[0]
        return DerivedKeySetup.from_db_tuple(setup)


class InventoryLog(SQLiteLog):

    TABLE_DEF = OrderedDict([
        ('request', OrderedDict([
            ('request_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('vault_name', 'TEXT NOT NULL'),
            ('sent_dt', 'TEXT NOT NULL'),
            ('job_id', 'TEXT NOT NULL')
        ])),
        ('response', OrderedDict([
            ('response_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('retrieved_dt', 'TEXT NOT NULL'),
            ('content_type', 'TEXT'),
            ('status', 'INTEGER'),
            ('body', 'BLOB'),
            ('request_id', 'INTEGER')
        ]))
    ])
    TABLE_FK = {
        'response': [ConstraintIdentical('request_id', 'request')]
    }
    DEFAULT_FILENAME = 'inventory_log.sqlite'
    RequestRow = namedtuple('RequestRow', list(TABLE_DEF['request'].keys()))
    ResponseRow = namedtuple('ResponseRow', list(TABLE_DEF['response'].keys()) + ['sent_dt'])

    def __init__(self, vault_name, filename=None):
        self.vault_name = vault_name
        super(InventoryLog, self).__init__(filename=filename)

    def store_request(self, request_dict):
        job_id = request_dict['jobId']
        sent_dt = datetime.datetime.utcnow().isoformat()
        with self.connect() as con:
            con.execute('INSERT INTO request (vault_name, sent_dt, job_id) VALUES (?, ?, ?)',
                        (self.vault_name, sent_dt, job_id))

    def store_response(self, request_id, response_dict):
        retrieved_dt = datetime.datetime.utcnow().isoformat()
        content_type = response_dict['contentType']
        status = response_dict['status']
        body = response_dict['body'].read()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT response_id FROM response WHERE request_id = %i' % request_id)
            result = cur.fetchall()
            # replace already existing responses for this request_id
            if len(result):
                con.execute(
                    'INSERT OR REPLACE INTO response ('
                    'response_id, retrieved_dt, content_type, status, body, request_id) '
                    'VALUES (?, ?, ?, ?, ?, ?)', (
                        result[0][0], retrieved_dt, content_type, status, body, request_id
                    )
                )
            else:
                con.execute(
                    'INSERT INTO response ('
                    'retrieved_dt, content_type, status, body, request_id) '
                    'VALUES (?, ?, ?, ?, ?)', (
                        retrieved_dt, content_type, status, body, request_id
                    )
                )

    def get_open_requests(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT a.* FROM request a LEFT JOIN response b USING(request_id) '
                        'WHERE retrieved_dt IS NULL AND vault_name = "%s"' % self.vault_name)
            result = cur.fetchall()
            open_requests = [self.RequestRow(*row) for row in result]
        return open_requests

    def get_latest_response(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute('CREATE TEMP TABLE tmp_req AS '
                        'SELECT MAX(sent_dt) AS sent_dt FROM request a '
                        'LEFT JOIN response b USING(request_id) '
                        'WHERE vault_name = "%s" AND retrieved_dt IS NOT NULL'
                        % self.vault_name)
            cur.execute('''\
            SELECT 
                b.*, 
                a.sent_dt 
            FROM response b 
            JOIN request a USING(request_id)
            JOIN tmp_req c USING(sent_dt)
            LIMIT 1''')
            responses = cur.fetchall()
            if not len(responses):
                return None
            last_response = self.ResponseRow(*responses[0])
            return last_response


def get_projects_for_file(filename):
    if not os.path.exists(filename):
        return []
    else:
        with sqlite3.Connection(database=filename) as con:
            cur = con.cursor()
            cur.execute('SELECT project.name, '
                        'project.base_path, '
                        'COUNT(file_id) AS num_files '
                        'FROM project LEFT JOIN file USING(project_id) '
                        'GROUP BY project_id')
            return [ProjectInfo(*x) for x in cur.fetchall()]


def get_file_size(filepath):
    return os.stat(filepath).st_size
