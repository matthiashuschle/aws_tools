""" ORM definitions and other database-handling.

The Definitions are used by classes in datatypes.py
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Binary, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from . import config

BackupLogBase = declarative_base()


# == File Backup

class TabProject(BackupLogBase):
    __tablename__ = 'project'

    project_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    base_path = Column(String, nullable=False)
    vault = Column(String)

    def __init__(self, name, base_path):
        self.name = name
        self.base_path = base_path


class TabFile(BackupLogBase):
    __tablename__ = 'file'

    file_id = Column(Integer, primary_key=True, autoincrement=True)
    is_folder = Column(Boolean, default=False)
    name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    size = Column(Integer)
    outdated = Column(Boolean, default=False)
    project_id = Column(Integer, ForeignKey('project.project_id'))
    project = relationship("TabProject", back_populates="file")

    def __init__(self, name, path, project_id):
        self.name = name
        self.path = path
        self.project_id = project_id


TabProject.files = relationship('TabFile', order_by=TabFile.file_id, back_populates="project")


class TabDerivedKeySetup(BackupLogBase):
    __tablename__ = 'derived_key_setup'

    derived_key_setup_id = Column(Integer, primary_key=True, autoincrement=True)
    construct = Column(String, nullable=False)
    ops = Column(Integer, nullable=False)
    mem = Column(Integer, nullable=False)
    key_size_enc = Column(Integer, nullable=False)
    key_size_sig = Column(Integer, default=0)
    salt_key_enc = Column(Binary, nullable=False)
    salt_key_sig = Column(Binary, default=b'')

    def __init__(self, construct, ops, mem, key_size_enc, salt_key_enc):
        self.construct = construct
        self.ops = ops
        self.mem = mem
        self.key_size_enc = key_size_enc
        self.salt_key_enc = salt_key_enc


class TabChunk(BackupLogBase):
    __tablename__ = 'chunk'

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
    derived_key_setup = relationship('TabDerivedKeySetup', back_populates='chunk')
    file_id = Column(Integer, ForeignKey('file.file_id'))
    file = relationship('TabFile', back_populates='chunk')

    def __init__(self, file_id, start_offset, size, encrypted, derived_key_setup_id):
        self.file_id = file_id
        self.start_offset = start_offset
        self.size = size
        self.encrypted = encrypted
        self.derived_key_setup_id = derived_key_setup_id


TabDerivedKeySetup.chunks = relationship('TabChunk', order_by=TabChunk.chunk_id, back_populates="derived_key_setup")
TabFile.chunks = relationship('TabChunk', order_by=TabChunk.chunk_id, back_populates="file")


# == Inventory

class TabInventoryRequest(BackupLogBase):
    __tablename__ = 'inventory_request'

    request_id = Column(Integer, primary_key=True, autoincrement=True)
    vault_name = Column(String, nullable=False)
    sent_dt = Column(DateTime, nullable=False)
    job_id = Column(String, nullable=False)

    def __init__(self, name, base_path):
        self.name = name
        self.base_path = base_path


class TabInventoryResponse(BackupLogBase):
    __tablename__ = 'inventory_response'

    response_id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey('inventory_request.request_id'))
    request = relationship('TabInventoryRequest', back_populates='inventory_response')
    retrieved_dt = Column(DateTime, nullable=False)
    content_type = Column(String)
    status = Column(Integer)
    body = Column(Binary)

    def __init__(self, request_id, retrieved_dt):
        self.request_id = request_id
        self.retrieved_dt = retrieved_dt


TabInventoryRequest.response = relationship('TabInventoryResponse', order_by=TabInventoryResponse.response_id,
                                            back_populates="request")


# == context handler

class SessionContext:

    def __init__(self, connector_str=None):
        connector_str = connector_str or config.config['database']['connector']
        self.engine = create_engine(connector_str)
        self.session_fac = sessionmaker(bind=self.engine)

    @contextmanager
    def __call__(self, *args, **kwargs):
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
