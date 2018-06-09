"""
Handles encryption. Based on the awesome post of Ynon Perek:
https://www.ynonperek.com/2017/12/11/how-to-encrypt-large-files-with-python-and-pynacl/
"""
import os
import binascii
import nacl.secret
import nacl.utils
import nacl.encoding
import nacl.signing
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.exceptions import InvalidSignature

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


def _chunk_nonce(base, index):
    size = nacl.secret.SecretBox.NONCE_SIZE
    return int.to_bytes(int.from_bytes(base, byteorder='big') + index, length=size, byteorder='big')


class CryptoHandler:

    def __init__(self, secret_key, auth_key):
        self._secret_key = secret_key
        self.secret_box = get_secret_box_from_key(secret_key)
        self.auth_key = auth_key
        self.last_signature = None

    @property
    def secret_key(self):
        return self._secret_key

    @secret_key.setter
    def secret_key(self, val):
        self._secret_key = val
        self.secret_box = get_secret_box_from_key(val)

    def get_auth_hmac(self):
        return get_auth_hmac_from_key(self.auth_key)

    def encrypt_stream(self, plain_file_object, read_total=None):
        auth_hmac = self.get_auth_hmac()
        for chunk in encrypt_stream(self.secret_box, plain_file_object, read_total=read_total):
            auth_hmac.update(chunk)
            yield chunk
        self.last_signature = binascii.hexlify(auth_hmac.finalize())

    def sign_stream(self, enc_file_object, read_total=None):
        return sign_stream(self.get_auth_hmac(), enc_file_object, read_total=read_total)

    def verify_stream(self, enc_file_object, signature, read_total=None):
        return verify_stream(self.get_auth_hmac(), enc_file_object, signature, read_total=read_total)

    def decrypt_stream(self, enc_file_object, read_total=None, signature=None):
        if signature:
            sig_bytes = binascii.unhexlify(signature)
            auth_hmac = self.get_auth_hmac()
            for chunk, index in _read_in_chunks(enc_file_object, read_total=read_total):
                auth_hmac.update(chunk)
                dec = self.secret_box.decrypt(chunk)
                yield dec
            auth_hmac.verify(sig_bytes)
        else:
            for dec in decrypt_stream(self.secret_box, enc_file_object, read_total=None):
                yield dec


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
