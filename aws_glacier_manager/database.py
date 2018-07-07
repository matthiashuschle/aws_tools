"""
Content uploaded to amazon Glacier is tar-zipped and encrypted.
For large files, tar a dummy and restore the content in chunks of the original.
"""
import datetime
from abc import ABC
from collections import OrderedDict, namedtuple
from contextlib import contextmanager
import sqlite3


class ForeignKey:

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


class ForeignKeyIdentical(ForeignKey):

    def __init__(self, column, other_table):
        super(ForeignKeyIdentical, self).__init__(column, other_table, column)


class SQLiteLog(ABC):

    TABLE_DEF = OrderedDict([])
    TABLE_FK = {}
    DEFAULT_FILENAME = None

    def __init__(self, filename=None):
        self.filename = filename or self.DEFAULT_FILENAME
        if self.filename is None:
            raise NotImplementedError('Your derived class can not handle empty filenames!')
        self.init_db()

    def init_db(self):
        with sqlite3.Connection(database=self.filename) as con:
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
            ('name', 'TEXT NOT NULL')
        ])),
        ('file', OrderedDict([
            ('file_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('name', 'TEXT NOT NULL'),
            ('path', 'TEXT NOT NULL'),
            ('size', 'INTEGER NOT NULL'),
            ('outdated', 'INTEGER DEFAULT 0'),
            ('project_id', 'INTEGER')
        ])),
        ('chunk', OrderedDict([
            ('chunk_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('upload_id', 'TEXT'),
            ('checksum', 'TEXT'),
            ('signature', 'TEXT'),
            ('start_offset', 'INTEGER NOT NULL'),
            ('size', 'INTEGER NOT NULL'),
            ('encrypted', 'INTEGER DEFAULT 0'),
            ('file_id', 'INTEGER')
        ])),
    ])
    TABLE_FK = {
        'file': [ForeignKeyIdentical('project_id', 'project')],
        'chunk': [ForeignKeyIdentical('file_id', 'file')]
    }
    DEFAULT_FILENAME = 'backup_log.sqlite'

    def __init__(self, project, filename=None):
        self.project = project
        super(BackupLog, self).__init__(filename=filename)
        self.filename = filename
        self.init_db()

    def add_file(self, recursive=True):
        """ Add a file to the database. If it is a directory, descend. Should not follow symlinks to folders. """
        pass

    def get_files_for_upload(self):
        """ Get list of stored files, for uploading, i.e. verify that they are unmodified and not uploaded yet."""
        pass

    def define_chunks(self, file):
        """ Create chunk information for a file. """
        pass


class InventoryLog(SQLiteLog):

    TABLE_DEF = OrderedDict([
        ('request', OrderedDict([
            ('request_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('vault_name', 'TEXT NOT NULL'),
            ('sent_dt', 'TEXT NOT NULL'),
            ('job_id', 'TEXT NOT NULL'),
            ('retrieved', 'INTEGER DEFAULT 0')
        ])),
        ('response', OrderedDict([
            ('response_id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('request_id', 'INTEGER')
        ]))
    ])
    TABLE_FK = {
        'response': [ForeignKeyIdentical('request_id', 'request')]
    }
    DEFAULT_FILENAME = 'inventory_log.sqlite'
    RequestRow = namedtuple('RequestRow', list(TABLE_DEF['request'].keys()))
    ResponseRow = namedtuple('ResponseRow', list(TABLE_DEF['response'].keys()) + ['sent_dt'])

    def __init__(self, vault_name, filename=None):
        self.vault_name = vault_name
        super(InventoryLog, self).__init__(filename=filename)

    @contextmanager
    def connect(self):
        with sqlite3.Connection(database=self.filename) as con:
            yield con

    def store_request(self, request_dict):
        job_id = request_dict['jobId']
        sent_dt = datetime.datetime.utcnow().isoformat()
        with self.connect() as con:
            con.execute('INSERT INTO request (vault_name, sent_dt, job_id) VALUES (?, ?, ?)',
                        (self.vault_name, sent_dt, job_id))

    def get_open_requests(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute('SELECT * FROM request WHERE NOT retrieved')
            open_requests = [self.RequestRow(*row) for row in cur.fetchall()]
        return open_requests

    def get_latest_response(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute('CREATE TEMP TABLE tmp_req AS '
                        'SELECT MAX(sent_dt) AS sent_dt FROM request WHERE vault_name = "%s" AND retrieved'
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


