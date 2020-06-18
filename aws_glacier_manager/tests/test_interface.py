from unittest import TestCase
from ..local_cfg import LocalConfig
from ..interface import LocalConfigInterface, ProjectInterface


class TestConfigInterface(TestCase):

    @LocalConfig.test_mode()
    def test_db_string(self):
        interface = LocalConfigInterface()
        assert interface.db_string == LocalConfig._TEST_DB
        interface.db_string = 'foo'
        assert interface.db_string == 'foo'
        if2 = LocalConfigInterface()
        assert if2.db_string == 'foo'

    @LocalConfig.test_mode()
    def test_default_vault(self):
        interface = LocalConfigInterface()
        assert interface.vault == LocalConfig._TEST_VAULT
        interface.vault = 'foo'
        assert interface.vault == 'foo'
        if2 = LocalConfigInterface()
        assert if2.vault == 'foo'


class TestProjectInterface(TestCase):

    @LocalConfig.test_mode()
    def test_init(self):
        pi = ProjectInterface('foo')
        assert not pi.exists
        assert pi.project_dict == {}
        assert pi.local_root is None
        pi.local_root = 'bar'
        assert pi.exists
        assert pi.project_dict == {ProjectInterface.KEY_ROOT: 'bar'}
        assert pi.local_root == 'bar'
        del pi
        pi = ProjectInterface('foo')
        assert pi.exists
        assert pi.project_dict == {ProjectInterface.KEY_ROOT: 'bar'}
        assert pi.local_root == 'bar'


