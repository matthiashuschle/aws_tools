import logging
import pathlib
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
    def db_string(self):
        return self.local_cfg.remote_db

    @db_string.setter
    def db_string(self, val):
        self.local_cfg.remote_db = val
        self.local_cfg.write_cfg_file()

    @property
    def default_vault(self):
        return self.local_cfg.default_vault

    @default_vault.setter
    def default_vault(self, val):
        self.local_cfg.default_vault = val
        self.local_cfg.write_cfg_file()

    def get_project_status(self):
        remote_projects = {x.name: x.vault for x in datatypes.get_overview()}
        return self.local_cfg.local_projects, remote_projects


class ProjectInterface:

    KEY_ROOT = 'local_root'

    def __init__(self, name: str):
        self.name = name
        self._local_cfg = local_cfg.LocalConfig()
        self._project_dict = self._local_cfg.local_projects.get(name, {})
        self._db_project = None

    @property
    def exists(self):
        return bool(self.project_dict)

    def _update(self):
        if not self.project_dict:
            del self._local_cfg.local_projects[self.name]
        else:
            self._local_cfg.local_projects[self.name] = self.project_dict
        self._local_cfg.write_cfg_file()

    @property
    def db_project(self):
        assert self.local_root
        if self._db_project is None:
            obj = datatypes.Project.load_named(self.name)
            if obj is None:
                obj = datatypes.Project(name=self.name, vault=self._local_cfg.default_vault)
                with datatypes.make_session() as session:
                    obj.create_db_entry(session)
            obj.base_path = self.local_root
            self._db_project = obj
        return self._db_project

    @property
    def project_dict(self):
        return self._project_dict

    @property
    def local_root(self):
        return self.project_dict.get(self.KEY_ROOT)

    @local_root.setter
    def local_root(self, val):
        logger = logging.getLogger(__name__)
        if not val:
            if self.exists:
                logger.info('deleting local project %s' % self.name)
                self._project_dict = {}
                self._update()
            else:
                logger.warning('empty local root path for new project -> ignoring')
            return
        if not self.exists:
            logger.info('creating project %s' % self.name)
        else:
            logger.info('previous root path: %s' % str(self.local_root))
        self.project_dict[self.KEY_ROOT] = val
        self._update()

    def db_add_files(self, filepaths):
        self.db_project.add_files([pathlib.Path(x).absolute() for x in filepaths])

    def db_remove_files(self, filepaths):
        self.db_project.remove_files([pathlib.Path(x).absolute() for x in filepaths])
