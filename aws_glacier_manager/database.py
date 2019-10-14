""" ORM definitions and other database-handling.

The Definitions are used by classes in datatypes.py
"""
import warnings
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Binary, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from . import local_cfg


class TableUtility:

    id_column = None

    @classmethod
    def id_col_property(cls):
        return getattr(cls, cls.id_column)

    @classmethod
    def from_db(cls, row_id, session):
        return session.query(cls).filter(cls.id_col_property() == row_id).first()


BackupLogBase = declarative_base(cls=TableUtility)


# == File Backup

class TabProject(BackupLogBase):
    __tablename__ = 'project'
    id_column = 'project_id'

    project_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    base_path = Column(String, nullable=False)
    vault = Column(String)


class TabFile(BackupLogBase):
    __tablename__ = 'file'
    id_column = 'file_id'

    file_id = Column(Integer, primary_key=True, autoincrement=True)
    is_folder = Column(Boolean, default=False)
    name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    size = Column(Integer)
    outdated = Column(Boolean, default=False)
    project_id = Column(Integer, ForeignKey('project.project_id'), nullable=False)
    project = relationship("TabProject", back_populates="files")


TabProject.files = relationship('TabFile', order_by=TabFile.file_id, back_populates="project")


class TabDerivedKeySetup(BackupLogBase):
    __tablename__ = 'derived_key_setup'
    id_column = 'derived_key_setup_id'

    derived_key_setup_id = Column(Integer, primary_key=True, autoincrement=True)
    construct = Column(String, nullable=False)
    ops = Column(Integer, nullable=False)
    mem = Column(Integer, nullable=False)
    key_size_enc = Column(Integer, nullable=False)
    key_size_sig = Column(Integer, default=0)
    salt_key_enc = Column(Binary, nullable=False)
    salt_key_sig = Column(Binary, default=b'')


class TabChunk(BackupLogBase):
    __tablename__ = 'chunk'
    id_column = 'chunk_id'

    chunk_id = Column(Integer, primary_key=True, autoincrement=True)
    upload_id = Column(String)
    checksum = Column(String)
    signature_type = Column(String)
    signature = Column(Binary)
    verify_key = Column(Binary)
    start_offset = Column(Integer, nullable=False)
    size = Column(Integer, nullable=False)
    encrypted = Column(Boolean, default=False)
    derived_key_setup_id = Column(Integer, ForeignKey('derived_key_setup.derived_key_setup_id'))
    derived_key_setup = relationship('TabDerivedKeySetup', back_populates='chunks')
    file_id = Column(Integer, ForeignKey('file.file_id'), nullable=False)
    file = relationship('TabFile', back_populates='chunks')


TabDerivedKeySetup.chunks = relationship('TabChunk', order_by=TabChunk.chunk_id, back_populates="derived_key_setup")
TabFile.chunks = relationship('TabChunk', order_by=TabChunk.chunk_id, back_populates="file")


# == Inventory

class TabInventoryRequest(BackupLogBase):
    __tablename__ = 'inventory_request'
    id_column = 'request_id'

    request_id = Column(Integer, primary_key=True, autoincrement=True)
    vault_name = Column(String, nullable=False)
    sent_dt = Column(DateTime, nullable=False)
    job_id = Column(String, nullable=False)


class TabInventoryResponse(BackupLogBase):
    __tablename__ = 'inventory_response'
    id_column = 'response_id'

    response_id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey('inventory_request.request_id'), nullable=False)
    request = relationship('TabInventoryRequest', back_populates='response')
    retrieved_dt = Column(DateTime, nullable=False)
    content_type = Column(String)
    status = Column(Integer)
    body = Column(Binary)


TabInventoryRequest.response = relationship('TabInventoryResponse', order_by=TabInventoryResponse.response_id,
                                            back_populates="request")


# == context handler

class SessionContext:

    def __init__(self, connector_str=None):
        connector_str = connector_str or local_cfg.LocalConfig().remote_db
        self.engine = create_engine(connector_str)
        self.session_fac = sessionmaker(bind=self.engine)

    def set_engine(self, connector_str):
        self.engine = create_engine(connector_str)
        self.session_fac = sessionmaker(bind=self.engine)

    @contextmanager
    def __call__(self, session=None):
        if session is not None:
            yield session
            return
        session = self.session_fac()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


make_session = SessionContext()


def create_tables():
    BackupLogBase.metadata.create_all(make_session.engine)
    warnings.warn('created tables with engine %s' % make_session.engine)


def set_test():
    with local_cfg.LocalConfig.test_mode():
        make_session.set_engine(local_cfg.LocalConfig().remote_db)
        create_tables()
