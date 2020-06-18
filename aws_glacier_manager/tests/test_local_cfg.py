from unittest import TestCase
import os.path
from ..local_cfg import LocalConfig


class TestLocalConfig(TestCase):

    def test_default(self):
        cfg = LocalConfig()
        assert os.path.basename(cfg.cfg_path) == LocalConfig.FILENAME
        assert cfg.vault is not None
        assert cfg.database is not None
        assert cfg.local_projects is not None
        assert cfg.database != LocalConfig._TEST_DB
        assert cfg.vault != LocalConfig._TEST_VAULT

    def test_testmode(self):
        with LocalConfig.test_mode():
            cfg = LocalConfig()
            assert os.path.basename(cfg.cfg_path) == LocalConfig._TEST_FILENAME
            assert cfg.vault == LocalConfig._TEST_VAULT
            assert cfg.database == LocalConfig._TEST_DB
            assert cfg.local_projects is not None
        cfg = LocalConfig()
        assert os.path.basename(cfg.cfg_path) == LocalConfig.FILENAME
        assert cfg.vault is not None
        assert cfg.database is not None
        assert cfg.local_projects is not None
        assert cfg.database != LocalConfig._TEST_DB
        assert cfg.vault != LocalConfig._TEST_VAULT
