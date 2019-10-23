import logging
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

    @property
    def exists(self):
        return bool(self.project_dict)

    def _update(self):
        self._local_cfg.local_projects[self.name] = self.project_dict
        self._local_cfg.write_cfg_file()

    @property
    def project_dict(self):
        return self._project_dict

    @property
    def local_root(self):
        return self.project_dict.get(self.KEY_ROOT)

    @local_root.setter
    def local_root(self, val):
        if not self.exists:
            logging.getLogger(__name__).info('creating project %s' % self.name)
        else:
            logging.getLogger(__name__).info('previous root path: %s' % str(self.local_root))
        self.project_dict[self.KEY_ROOT] = val
        self._update()
