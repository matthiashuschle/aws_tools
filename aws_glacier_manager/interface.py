from . import local_cfg
from . import datatypes


class ConfigInterface:

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
        return self.local_cfg.local_projects, datatypes.get_overview()
