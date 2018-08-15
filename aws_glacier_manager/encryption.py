"""
Handles encryption. Based on the awesome post of Ynon Perek:
https://www.ynonperek.com/2017/12/11/how-to-encrypt-large-files-with-python-and-pynacl/
"""
import os
import contextlib
import binascii
import nacl.secret
import nacl.utils
import nacl.encoding
import nacl.signing
import nacl.pwhash
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.exceptions import InvalidSignature
from .datatypes import DerivedKeySetup, DerivedKeys

SYMKEY = 'symkey.bin'
AUTHKEY = 'authkey.bin'
CHUNK_SIZE = 16 * 1024


def create_keys(out_path, replace=False):
    if out_path:
        os.makedirs(out_path, exist_ok=True)
    # encryption key
    key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    auth_key = nacl.utils.random(size=64)
    if not out_path:
        return key, auth_key
    with open(os.path.join(out_path, SYMKEY), 'wb' if replace else 'xb') as f:
        f.write(key)
    # signature key
    with open(os.path.join(out_path, AUTHKEY), 'wb' if replace else 'xb') as f:
        f.write(auth_key)


def create_keys_from_password(password, setup=None, enable_auth_key=False):
    """ Create encryption and signature keys from a password.

    Uses salt and resilient hashing. Returns the hashing settings, so the keys can be recreated with the same password.
    original source: https://pynacl.readthedocs.io/en/stable/password_hashing/#key-derivation
    :param bytes password: password as bytestring
    :param DerivedKeySetup setup: settings for the hashing
    :param bool enable_auth_key: generate a key for full data signatures via HMAC. Usually not necessary, as each block
        is automatically signed. The only danger is block loss and block order manipulation.
    :rtype: DerivedKeys
    """
    setup = setup or DerivedKeySetup.create_default(enable_auth_key=enable_auth_key)
    kdf = None
    if setup.construct == 'argon2i':
        kdf = nacl.pwhash.argon2i.kdf
    if kdf is None:
        raise AttributeError('construct %s is not implemented' % setup.construct)
    key_enc = kdf(setup.key_size_enc, password, setup.salt_key_enc,
                  opslimit=setup.ops, memlimit=setup.mem)
    key_sig = kdf(setup.key_size_sig, password, setup.salt_key_sig,
                  opslimit=setup.ops, memlimit=setup.mem) if setup.key_size_sig else b''
    return DerivedKeys(
        key_enc=key_enc,
        key_sig=key_sig,
        setup=setup)


def _chunk_nonce(base, index):
    size = nacl.secret.SecretBox.NONCE_SIZE
    return int.to_bytes(int.from_bytes(base, byteorder='big') + index, length=size, byteorder='big')


class CryptoHandler:

    def __init__(self, secret_key, auth_key=None):
        self._secret_key = secret_key
        self.secret_box = get_secret_box_from_key(secret_key)  # for encryption
        self.auth_key = auth_key  # for signing
        self._last_signature = None

    @property
    def last_signature(self):
        return self._last_signature

    @last_signature.setter
    def last_signature(self, val):
        if self.auth_key:
            self._last_signature = binascii.hexlify(val)
        else:
            self._last_signature = None

    @property
    def secret_key(self):
        return self._secret_key

    @secret_key.setter
    def secret_key(self, val):
        self._secret_key = val
        self.secret_box = get_secret_box_from_key(val)

    @contextlib.contextmanager
    def init_hmac(self, force=False):
        """ Creates a new HMAC instance if possible.

        :param bool force: must return an HMAC handler, fails if not possible
        :rtype: hmac.HMAC
        :raises RuntimeError if force is True, but no auth_key available
        """
        if self.auth_key:
            yield get_auth_hmac_from_key(self.auth_key)
        else:
            if force:
                raise RuntimeError('no signature key given, but HMAC requested!')

            class HMACDummy:
                """ A dummy that ignores the applied actions. """
                update = staticmethod(lambda data: None)
                finalize = staticmethod(lambda: None)
            yield HMACDummy

    def get_auth_hmac(self):
        if self.auth_key is None:
            return None
        return get_auth_hmac_from_key(self.auth_key)

    def encrypt_stream(self, plain_file_object, read_total=None):
        with self.init_hmac() as auth_hmac:
            for chunk in encrypt_stream(self.secret_box, plain_file_object, read_total=read_total):
                auth_hmac.update(chunk)
                yield chunk
            self.last_signature = auth_hmac.finalize()

    def sign_stream(self, enc_file_object, read_total=None):
        with self.init_hmac(force=True) as auth_hmac:
            return sign_stream(auth_hmac, enc_file_object, read_total=read_total)

    def verify_stream(self, enc_file_object, signature, read_total=None):
        with self.init_hmac(force=True) as auth_hmac:
            return verify_stream(auth_hmac, enc_file_object, signature, read_total=read_total)

    def decrypt_stream(self, enc_file_object, read_total=None, signature=None):
        if signature:
            sig_bytes = binascii.unhexlify(signature)
            with self.init_hmac(force=True) as auth_hmac:
                for chunk, index in _read_in_chunks(enc_file_object, read_total=read_total):
                    auth_hmac.update(chunk)
                    dec = self.secret_box.decrypt(chunk)
                    yield dec
                auth_hmac.verify(sig_bytes)
        else:
            for dec in decrypt_stream(self.secret_box, enc_file_object, read_total=None):
                yield dec

    @staticmethod
    def get_unenc_block_size(enc_block_size):
        """ Calculate how many unencrypted bytes amount to the desired encrypted amount.

        :param enc_block_size: desired encrypted number of bytes
        :return: size of unencrypted data
        :rtype: int
        :raises ValueError: if the target block size can not be created from the encryption chunk size.
        """
        if enc_block_size % CHUNK_SIZE:
            raise ValueError('can not divide %i by %i!' % (enc_block_size, CHUNK_SIZE))
        n_chunks = enc_block_size // CHUNK_SIZE
        return n_chunks * (CHUNK_SIZE - 40)


def _read_in_chunks(file_object, chunk_size=None, read_total=None):
    """ Generator to read a stream piece by piece with a given chunk size.
    Total read size may be given. Only read() is used on the stream. """
    chunk_size = chunk_size or CHUNK_SIZE
    read_size = chunk_size
    index = 0
    read_yet = 0
    while True:
        if read_total is not None:
            read_size = min(read_total - read_yet, chunk_size)
        data = file_object.read(read_size)
        if not data:
            break
        yield (data, index)
        read_yet += read_size
        index += 1


def get_secret_box_from_key(key):
    return nacl.secret.SecretBox(key)


def get_secret_box_from_file(keyfile=SYMKEY):
    with open(keyfile, 'rb') as f:
        key = f.read()
    return get_secret_box_from_key(key)


def get_auth_hmac_from_key(auth_key):
    return hmac.HMAC(auth_key, hashes.SHA512(), backend=default_backend())


def get_auth_hmac_from_file(keyfile=AUTHKEY):
    with open(keyfile, 'rb') as f:
        auth_key = f.read()
    return get_auth_hmac_from_key(auth_key)


def encrypt_stream(secret_box, plain_file_object, read_total=None):
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
    for chunk, index in _read_in_chunks(plain_file_object, chunk_size=CHUNK_SIZE - 40, read_total=read_total):
        enc = secret_box.encrypt(chunk, _chunk_nonce(nonce, index))
        yield enc


def sign_stream(auth_hmac, enc_file_object, read_total=None):
    for chunk, _ in _read_in_chunks(enc_file_object, read_total=read_total):
        auth_hmac.update(chunk)
    return binascii.hexlify(auth_hmac.finalize())


def verify_stream(auth_hmac, enc_file_object, signature, read_total=None):
    sig_bytes = binascii.unhexlify(signature)
    for chunk, _ in _read_in_chunks(enc_file_object, read_total=read_total):
        auth_hmac.update(chunk)
    try:
        auth_hmac.verify(sig_bytes)
        return True
    except InvalidSignature:
        return False


def decrypt_stream(secret_box, enc_file_object, read_total=None):
    for chunk, index in _read_in_chunks(enc_file_object, read_total=read_total):
        dec = secret_box.decrypt(chunk)
        yield dec
