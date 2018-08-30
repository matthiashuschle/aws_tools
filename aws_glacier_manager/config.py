import os
import configparser
import copy

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
    config = copy.deepcopy(_DEFAULT)
    parser = configparser.ConfigParser()
    if os.path.isfile(file):
        parser.read(file)
        print('loaded %s' % file)
    for section, settings in parser.items():
        if section not in config:
            config[section] = settings
        else:
            for key, val in settings.items():
                config[section][key] = settings.get(key, val)


def load2(file=CONFIG_FILE):
    global config
    config = configparser.ConfigParser()
    if os.path.isfile(file):
        config.read(file)
        print('loaded %s' % file)
    for section, settings in _DEFAULT.items():
        if section not in config:
            config[section] = settings
        else:
            for key, val in settings.items():
                config[section][key] = settings.get(key, val)

print('foo')
load2()
print(config)