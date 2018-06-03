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
        ('item', OrderedDict([
            ('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
            ('name', 'TEXT NOT NULL'),
            ('fk_project_id', 'INTEGER')
        ])),
    ])
    TABLE_FK = {
        'item': ('project', 'id')
    }

    def __init__(self, filename='backup_log.sqlite'):
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
