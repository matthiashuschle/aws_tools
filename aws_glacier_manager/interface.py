import os
import logging
from typing import Sequence
from cryp_to_go.path_handler import AnyPath
from . import local_cfg
from . import datatypes


class LocalConfigInterface:

    def __init__(self):
        self._local_cfg = None

    @property
    def local_cfg(self):
        if self._local_cfg is None:
            self._local_cfg = local_cfg.LocalConfig()
        return self._local_cfg

    @property
    def db_string(self) -> str:
        return self.local_cfg.database

    @db_string.setter
    def db_string(self, val: str):
        if not len(val):
            raise ValueError('empty string for database connector!')
        self.local_cfg.database = val
        self.local_cfg.write_cfg_file()

    @property
    def vault(self) -> str:
        return self.local_cfg.vault

    @vault.setter
    def vault(self, val: str):
        if not len(val):
            raise ValueError('empty string for vault!')
        self.local_cfg.vault = val
        self.local_cfg.write_cfg_file()

    @property
    def project(self) -> str:
        return self.local_cfg.project

    @project.setter
    def project(self, val: str):
        if not len(val):
            raise ValueError('empty string for project name!')
        self.project = val
        self.local_cfg.write_cfg_file()

    @staticmethod
    def get_remote_project_status(db_string):
        with local_cfg.LocalConfig.temporary_cfg(
            db_string=db_string,
        ):
            remote_projects = {x.name: x.vault for x in datatypes.get_overview()}
        return remote_projects


class ProjectInterface:

    def __init__(self, name: str):
        self.name = name
        self._db_project = None

    @classmethod
    def from_cfg(cls, cfg: LocalConfigInterface):
        if not cfg.db_string:
            raise ValueError('Project operations need a valid DB connector.')
        project = cls(cfg.project)
        # if no remote information (new project), vault is required
        if not project.exists:
            if not cfg.vault:
                raise ValueError('New projects need to have a vault set.')
            project.create_db_project()
        return project

    @property
    def exists(self):
        self.load_db_project()
        return self._db_project is not None

    def load_db_project(self):
        if self._db_project is not None:
            return self._db_project
        self._db_project = datatypes.Project.load_named(self.name)
        return self._db_project

    def create_db_project(self):
        if self._db_project is not None:
            raise RuntimeError('DB project already exists.')
        obj = datatypes.Project(name=self.name, vault=self._local_cfg.vault)
        with datatypes.make_session() as session:
            obj.create_db_entry(session)
        self._db_project = obj
        return obj

    def db_add_files(self, filepaths: Sequence[AnyPath]):
        return self._db_project.add_files(filepaths)

    def db_remove_files(self, filepaths: Sequence[AnyPath]):
        return self._db_project.remove_files(filepaths)
