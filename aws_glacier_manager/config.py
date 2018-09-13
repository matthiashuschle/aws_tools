import os
import configparser
import warnings

CONFIG_FILE = 'config.ini'
_DEFAULT = {
    'vault': {
        'name': 'myvault'
    },
    'database': {
        'connector': 'sqlite:///backup_log.sqlite'
    }
}
config = None


def load(file=CONFIG_FILE):
    global config
    config = configparser.ConfigParser()
    if os.path.isfile(file):
        config.read(file)
        warnings.warn('loaded %s' % file)
    for section, settings in _DEFAULT.items():
        if section not in config:
            config[section] = settings
        else:
            for key, val in settings.items():
                config[section][key] = settings.get(key, val)
    warnings.warn(repr(config))


def load_test():
    global _DEFAULT
    _DEFAULT = {
        'vault': {
            'name': 'testvault'
        },
        'database': {
            'connector': 'sqlite:///_unittest.sqlite'
        }
    }
    load()


load()
