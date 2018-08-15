from collections import namedtuple
import base64
from nacl import pwhash, utils, secret


class DerivedKeySetup:
    
    _item_names = 'ops', 'mem', 'construct', 'salt_key_enc', 'salt_key_sig', 'key_size_enc', 'key_size_sig'

    def __init__(self, ops, mem, construct, salt_key_enc, salt_key_sig, key_size_enc, key_size_sig):
        self.ops = ops
        self.mem = mem
        self.construct = construct
        self.salt_key_enc = salt_key_enc
        self.salt_key_sig = salt_key_sig
        self.key_size_enc = key_size_enc
        self.key_size_sig = key_size_sig

    def __repr__(self):
        return repr('DerivedKeySetup(%s)' % ','.join('%s=%r' % (x, self[x]) for x in self._item_names))
    
    def __getitem__(self, item):
        return getattr(self, item)

    @classmethod
    def db_columns(cls):
        return ', '.join(cls._item_names)
    
    def to_db_tuple(self):
        return (
            self.ops,
            self.mem,
            self.construct,
            self.key_size_enc,
            self.key_size_sig,
            base64.b64encode(self.salt_key_enc),
            base64.b64encode(self.salt_key_sig)
        )

    @classmethod
    def from_db_tuple(cls, db_tuple):
        return cls(
            ops=db_tuple[0],
            mem=db_tuple[1],
            construct=db_tuple[2],
            key_size_enc=db_tuple[3],
            key_size_sig=db_tuple[4],
            salt_key_enc=base64.b64decode(db_tuple[5]),
            salt_key_sig=base64.b64decode(db_tuple[6])
        )

    @classmethod
    def create_default(cls, enable_auth_key=False):
        """ Create default settings for encryption key derivation from password.

        original source: https://pynacl.readthedocs.io/en/stable/password_hashing/#key-derivation
        :param bool enable_auth_key: generate a key for full data signatures via HMAC
        :rtype: DerivedKeySetup
        """
        return cls(
            ops=pwhash.argon2i.OPSLIMIT_SENSITIVE,
            mem=pwhash.argon2i.MEMLIMIT_SENSITIVE,
            construct='argon2i',
            salt_key_enc=utils.random(pwhash.argon2i.SALTBYTES),
            salt_key_sig=utils.random(pwhash.argon2i.SALTBYTES) if enable_auth_key else b'',
            key_size_enc=secret.SecretBox.KEY_SIZE,
            key_size_sig=64 if enable_auth_key else 0
        )


DerivedKeys = namedtuple('DerivedKeys', ['key_enc', 'key_sig', 'setup'])
