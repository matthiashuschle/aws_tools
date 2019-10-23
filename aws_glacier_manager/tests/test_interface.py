from unittest import TestCase
from ..local_cfg import LocalConfig
from ..interface import LocalConfigInterface, ProjectInterface


class TestConfigInterface(TestCase):

    @LocalConfig.test_mode()
    def test_db_string(self):
        interface = LocalConfigInterface()
        assert interface.db_string == LocalConfig._TEST_REMOTE_DB
        interface.db_string = 'foo'
        assert interface.db_string == 'foo'
        if2 = LocalConfigInterface()
        assert if2.db_string == 'foo'

    @LocalConfig.test_mode()
    def test_default_vault(self):
        interface = LocalConfigInterface()
        assert interface.default_vault == LocalConfig._TEST_DEFAULT_VAULT
        interface.default_vault = 'foo'
        assert interface.default_vault == 'foo'
        if2 = LocalConfigInterface()
        assert if2.default_vault == 'foo'


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


