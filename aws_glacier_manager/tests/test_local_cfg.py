from unittest import TestCase
import os.path
from ..local_cfg import LocalConfig


class TestLocalConfig(TestCase):

    def test_default(self):
        cfg = LocalConfig()
        assert os.path.basename(cfg.cfg_path) == LocalConfig.FILENAME
        assert cfg.default_vault is not None
        assert cfg.remote_db is not None
        assert cfg.local_projects is not None
        assert cfg.remote_db != LocalConfig._TEST_REMOTE_DB
        assert cfg.default_vault != LocalConfig._TEST_DEFAULT_VAULT

    def test_testmode(self):
        with LocalConfig.test_mode():
            cfg = LocalConfig()
            assert os.path.basename(cfg.cfg_path) == LocalConfig._TEST_FILENAME
            assert cfg.default_vault == LocalConfig._TEST_DEFAULT_VAULT
            assert cfg.remote_db == LocalConfig._TEST_REMOTE_DB
            assert cfg.local_projects is not None
        cfg = LocalConfig()
        assert os.path.basename(cfg.cfg_path) == LocalConfig.FILENAME
        assert cfg.default_vault is not None
        assert cfg.remote_db is not None
        assert cfg.local_projects is not None
        assert cfg.remote_db != LocalConfig._TEST_REMOTE_DB
        assert cfg.default_vault != LocalConfig._TEST_DEFAULT_VAULT
