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

SYMKEY = 'symkey.bin'
AUTHKEY = 'authkey.bin'


def create_keys(out_path, replace=False):
    os.makedirs(out_path, exist_ok=True)
    # encryption key
    key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    with open(os.path.join(out_path, SYMKEY), 'wb' if replace else 'xb') as f:
        f.write(key)
    # signature key
    auth_key = nacl.utils.random(size=64)
    with open(os.path.join(out_path, AUTHKEY), 'wb' if replace else 'xb') as f:
        f.write(auth_key)


def _chunk_nonce(base, index):
    size = nacl.secret.SecretBox.NONCE_SIZE
    return int.to_bytes(int.from_bytes(base, byteorder='big') + index, length=size, byteorder='big')


def _read_in_chunks(file_object, chunk_size=16 * 1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 16k."""
    index = 0
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield (data, index)
        index += 1


def get_secret_box_from_file(keyfile=SYMKEY):
    with open(keyfile, 'rb') as f:
        key = f.read()
    return nacl.secret.SecretBox(key)


def get_auth_hmac_from_file(keyfile=AUTHKEY):
    with open(keyfile, 'rb') as f:
        auth_key = f.read()
    return hmac.HMAC(auth_key, hashes.SHA512(), backend=default_backend())


def encrypt_stream(secret_box, input_file_object):
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
    for chunk, index in _read_in_chunks(input_file_object, chunk_size=16 * 1024 - 40):
        enc = secret_box.encrypt(chunk, _chunk_nonce(nonce, index))
        yield enc


def sign_output(auth_hmac, input_file_object):
    for chunk, _ in _read_in_chunks(input_file_object):
        auth_hmac.update(chunk)
    return binascii.hexlify(auth_hmac.finalize())
