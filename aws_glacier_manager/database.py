"""
Content uploaded to amazon Glacier is tar-zipped and encrypted.
For large files, tar a dummy and restore the content in chunks of the original.
"""
import os
import datetime
from abc import ABC
from collections import OrderedDict, namedtuple
from contextlib import contextmanager
import sqlite3
from .datatypes import DerivedKeySetup


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


class BackupLog(SQLiteLog):

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
    DEFAULT_FILENAME = 'backup_log.sqlite'

    def __init__(self, project, base_path='/', filename=None):
        self.project = project
        self.base_path = base_path
        super(BackupLog, self).__init__(filename=filename)
        self.filename = filename
        self.init_db()

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
