""" Handler for local configuration.

Local settings include the connection string for the real database,
and folder mappings for individual projects. Sync state is also a local
attribute, but not part of the configuration, but refreshed on-demand.
"""
import os.path
from pathlib import Path
import yaml
from contextlib import contextmanager


class LocalConfig:

    FILENAME = 'aws_glacier_manager.yaml'
    HOME = Path.home()
    _KEY_REMOTE_DB = 'remote_db'
    _KEY_DEFAULT_VAULT = 'default_vault'
    _KEY_LOCAL_PROJECTS = 'local_projects'
    _DEFAULTS = {
        _KEY_DEFAULT_VAULT: 'myvault',
        _KEY_REMOTE_DB: 'sqlite:///backup_log.sqlite',
    }

    def __init__(self, cfg_path=None):
        self.cfg_path = cfg_path or self.HOME / self.FILENAME
        cfg_dict = self.load_local_cfg(self.cfg_path)
        self.remote_db = cfg_dict.get(self._KEY_REMOTE_DB)
        self.default_vault = cfg_dict.get(self._KEY_DEFAULT_VAULT)
        self.local_projects = cfg_dict.get(self._KEY_LOCAL_PROJECTS)

    def write_cfg_file(self, path: str=None):
        path = path or self.cfg_path
        with open(path, 'w') as f_out:
            yaml.safe_dump(self.as_dict(), f_out)

    def as_dict(self) -> dict:
        return {
            self._KEY_REMOTE_DB: self.remote_db,
            self._KEY_DEFAULT_VAULT: self.default_vault,
            self._KEY_LOCAL_PROJECTS: self.local_projects,
        }

    @classmethod
    def load_local_cfg(cls, path: str=None) -> dict:
        if not os.path.isfile(path):
            return cls._DEFAULTS
        with open(path, 'r') as f_in:
            cfg = yaml.safe_load(f_in)
        return cfg

    @classmethod
    @contextmanager
    def test_mode(cls):
        filename_test = '_unittest_aws_glacier_manager.yaml'
        defaults_test = {
            cls._KEY_DEFAULT_VAULT: 'testvault',
            cls._KEY_REMOTE_DB: 'sqlite:///_unittest.sqlite',
        }
        filename, defaults = cls.FILENAME, cls._DEFAULTS
        cls.FILENAME, cls._DEFAULTS = filename_test, defaults_test
        yield
        cls.FILENAME, cls._DEFAULTS = filename, defaults
