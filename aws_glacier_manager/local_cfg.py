""" Handler for local configuration.

Local settings include the connection string for the real database,
and folder mappings for individual projects. Sync state is also a local
attribute, but not part of the configuration, but refreshed on-demand.
"""
import os.path
from typing import Mapping, Optional, ContextManager
import logging
import yaml
from contextlib import contextmanager


class LocalConfig:

    FILENAME = 'aws_glacier_manager.yaml'
    _KEY_DB = 'remote_db'
    _KEY_VAULT = 'default_vault'
    _KEY_PROJECT = 'project'
    _KEY_LOCAL_INVENTORY = 'local_inventory'
    _DEFAULTS = {
        _KEY_VAULT: 'myvault',
        _KEY_DB: 'sqlite:///backup_log.sqlite',
        _KEY_PROJECT: None,
        _KEY_LOCAL_INVENTORY: None
    }
    _TEST_FILENAME = '_unittest_aws_glacier_manager.yaml'
    _TEST_VAULT = 'testvault'
    _TEST_DB = 'sqlite:///_unittest.sqlite'

    def __init__(self, cfg_path: Optional[str] = None):
        """ Handler for local configuration.

        :param cfg_path: optional path to file, currently not used elsewhere.
        """
        self.cfg_path = cfg_path or self.FILENAME
        cfg_dict = self.load_local_cfg(self.cfg_path)
        self.database = cfg_dict.get(self._KEY_DB)
        self.vault = cfg_dict.get(self._KEY_VAULT)
        self.project = cfg_dict.get(self._KEY_PROJECT)
        self.local_inventory = cfg_dict.get(self._KEY_LOCAL_INVENTORY)

    def write_cfg_file(self, path: Optional[str] = None) -> None:
        path = path or self.cfg_path
        with open(path, 'w') as f_out:
            yaml.safe_dump(self.as_dict(), f_out)

    def as_dict(self) -> Mapping[str, str]:
        return {
            self._KEY_DB: self.database,
            self._KEY_VAULT: self.vault,
            self._KEY_PROJECT: self.project,
            self._KEY_LOCAL_INVENTORY: self.local_inventory,
        }

    @classmethod
    def load_local_cfg(cls, path: Optional[str] = None) -> Mapping[str, str]:
        if not os.path.isfile(path):
            return cls._DEFAULTS
        with open(path, 'r') as f_in:
            cfg = yaml.safe_load(f_in)
        return cfg

    @classmethod
    @contextmanager
    def test_mode(cls, db_string: Optional[str] = None, ) -> ContextManager[None]:
        with cls.temporary_cfg(
            project_name=None,
            db_string=cls._TEST_DB,
            vault=cls._TEST_VAULT,
            filename=cls._TEST_FILENAME,
        ):
            yield

    @classmethod
    @contextmanager
    def temporary_cfg(
            cls,
            project_name: Optional[str] = None,
            db_string: Optional[str] = None,
            vault: Optional[str] = None,
            filename: str = "temporary_cfg.yaml"
    ) -> ContextManager[None]:
        filename_test = cls._TEST_FILENAME
        defaults_test = {
            cls._KEY_VAULT: vault,
            cls._KEY_DB: db_string,
            cls._KEY_PROJECT: project_name,
        }
        filename, defaults = cls.FILENAME, cls._DEFAULTS
        cls.FILENAME, cls._DEFAULTS = filename_test, defaults_test
        try:
            try:
                os.remove(cls.FILENAME)
            except OSError:
                pass
            yield
        finally:
            cls.FILENAME, cls._DEFAULTS = filename, defaults
