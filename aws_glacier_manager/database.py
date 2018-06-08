"""
Content uploaded to amazon Glacier is tar-zipped and encrypted.
For large files, tar a dummy and restore the content in chunks of the original.
"""
import os
from collections import OrderedDict, namedtuple
import sqlite3


class BackupLog:

    TABLE_DEF = OrderedDict([
        ('project', OrderedDict([
            ('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('name', 'TEXT NOT NULL')
        ])),
        ('file', OrderedDict([
            ('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('name', 'TEXT NOT NULL'),
            ('path', 'TEXT NOT NULL'),
            ('size', 'INTEGER NOT NULL'),
            ('outdated', 'INTEGER DEFAULT 0'),
            ('fk_project_id', 'INTEGER')
        ])),
        ('chunk', OrderedDict([
            ('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('upload_id', 'TEXT'),
            ('checksum', 'TEXT'),
            ('signature', 'TEXT'),
            ('start_offset', 'INTEGER NOT NULL'),
            ('size', 'INTEGER NOT NULL'),
            ('encrypted', 'INTEGER DEFAULT 0'),
            ('fk_file_id', 'INTEGER')
        ])),
    ])
    TABLE_FK = {
        'file': ('project', 'id'),
        'chunk': ('file', 'id')
    }

    def __init__(self, project, filename='backup_log.sqlite'):
        self.project = project
        self.filename = filename
        self.init_db()

    def init_db(self):
        with sqlite3.Connection(database=self.filename) as con:
            cur = con.cursor()
            for tablename, coldefs in self.TABLE_DEF:
                elements = ['%s %s' % (x, y) for x, y in coldefs.items()]
                for target_table, target_col in self.TABLE_FK.get(tablename, []):
                    elements.append(
                        'CONSTRAINT fk_{t}_{t_target}_{col_target} '
                        'FOREIGN KEY (fk_{t_target}_{col_target}) '
                        'REFERENCES {t_target} ({col_target})'.format(
                            t=tablename,
                            t_target=target_table,
                            col_target=target_col
                        )
                    )
                cur.execute('CREATE TABLE IF NOT EXISTS %s (%s)' % (
                    tablename,
                    ', '.join(elements)
                ))

    def add_file(self, recursive=True):
        """ Add a file to the database. If it is a directory, descend. Should not follow symlinks to folders. """
        pass

    def get_files_for_upload(self):
        """ Get list of stored files, for uploading, i.e. verify that they are unmodified and not uploaded yet."""
        pass

    def define_chunks(self, file):
        """ Create chunk information for a file. """
        pass
