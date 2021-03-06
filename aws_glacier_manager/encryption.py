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
import nacl.pwhash
from nacl.exceptions import BadSignatureError
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.exceptions import InvalidSignature
from .datatypes import DerivedKeySetup, DerivedKeys

SYMKEY = 'symkey.bin'
AUTHKEY = 'authkey.bin'
CHUNK_SIZE = 16 * 1024


def create_keys(out_path, replace=False):
    """ Simple random key generation. They are highly random, but only of use
    if you can store them safely. Try :meth:`create_keys_from_password` otherwise.

    :param str out_path: path to use for file output. If None, keys are returned.
    :param bool replace: replace existing files.
    :rtype: tuple
    :returns: encryption key, signing key
    """
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
    """ Creates incrementing nonces. Make sure that the base is different for each reset of index!

    :param bytes base: random base for the nonces
    :param int index: offset for the nonce
    :rtype: bytes
    """
    size = nacl.secret.SecretBox.NONCE_SIZE
    return int.to_bytes(int.from_bytes(base, byteorder='big') + index, length=size, byteorder='big')


class CryptoHandler:

    def __init__(self, secret_key, auth_key=None):
        """ Handle symmetric encryption of data of any size.

        :param bytes secret_key: encryption key
        :param bytes auth_key: optional key for signing output with HMAC
        """
        self.secret_box = None
        self._secret_key = None
        self.secret_key = secret_key
        self.auth_key = auth_key  # for signing
        self._last_signature = None

    @property
    def last_signature(self):
        """ After finalizing encryption, holds a signature, if auth_key is available.

        :rtype: bytes
        """
        return self._last_signature

    @last_signature.setter
    def last_signature(self, val):
        """ Set signature, ignore if signing via HMAC is disabled.

        :param bytes val: new signature
        """
        if self.auth_key:
            self._last_signature = binascii.hexlify(val)
        else:
            self._last_signature = None

    @property
    def secret_key(self):
        """ Secret encryption key.

        :rtype: bytes
        """
        return self._secret_key

    @secret_key.setter
    def secret_key(self, val):
        """ Set encryption key. Also changes the SecretBox for crypto-operations.

        :param bytes val: new encryption key
        """
        self._secret_key = val
        self.secret_box = nacl.secret.SecretBox(val)

    def init_hmac(self, force=False, dummy=False):
        """ Creates a new HMAC instance if possible.

        :param bool force: must return an HMAC handler, fails if not possible
        :param bool dummy: force return of dummy handler
        :rtype: hmac.HMAC
        :raises RuntimeError if force is True, but no auth_key available
        """
        if force and dummy:
            raise AttributeError('must not set both, force and dummy')
        if self.auth_key and not dummy:
            return get_auth_hmac_from_key(self.auth_key)
        else:
            if force:
                raise RuntimeError('no signature key given, but HMAC requested!')

            class HMACDummy:
                """ A dummy that ignores the applied actions. """
                update = staticmethod(lambda data: None)
                finalize = staticmethod(lambda: None)
                verify = staticmethod(lambda data: True)
            return HMACDummy

    def encrypt_stream(self, plain_file_object, read_total=None):
        """ Here the encryption happens in chunks (generator).

        The output size is the CHUNK SIZE, the chunks read are 40 bytes smaller to add nonce and chunk
        signature. HMAC signing of the full encrypted data is only done, if an auth_key is provided.

        :param BytesIO plain_file_object: input file
        :param int read_total: maximum bytes to read
        :return: encrypted chunks
        :rtype: bytes
        """
        nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)  # default way of creating a nonce in nacl
        auth_hmac = self.init_hmac()
        # nacl adds nonce (24bytes) and signature (16 bytes), so read 40 bytes less than desired output size
        for index, chunk in enumerate(_read_in_chunks(
                plain_file_object, chunk_size=CHUNK_SIZE - 40, read_total=read_total)
        ):
            enc = self.secret_box.encrypt(chunk, _chunk_nonce(nonce, index))
            auth_hmac.update(enc)
            yield enc
        self.last_signature = auth_hmac.finalize()

    def sign_stream(self, enc_file_object, read_total=None):
        """ Create HMAC for existing encrypted data stream.

        :param BytesIO enc_file_object: encrypted data
        :param int read_total: maximum bytes to read
        :return: signature, hex-serialized
        :rtype: bytes
        """
        auth_hmac = self.init_hmac(force=True)
        return sign_stream(auth_hmac, enc_file_object, read_total=read_total)

    def verify_stream(self, enc_file_object, signature, read_total=None):
        """ Verify HMAC signature for encrypted data stream.

        :param BytesIO enc_file_object: encrypted data
        :param bytes signature: hex-serialized signature
        :param int read_total: maximum bytes to read
        :return: validity
        :rtype: bool
        """
        auth_hmac = self.init_hmac(force=True)
        return verify_stream(auth_hmac, enc_file_object, signature, read_total=read_total)

    def decrypt_stream(self, enc_file_object, read_total=None, signature=None):
        """ Decrypt encrypted stream. (generator)

        If auth_key and signature is provided, HMAC verification is done automatically.

        :param BytesIO enc_file_object: encrypted data stream
        :param int read_total: maximum bytes to read
        :param signature: hex-serialized signature
        :return: plain data in chunks
        :rtype: bytes
        """
        sig_bytes = binascii.unhexlify(signature) if signature else None
        auth_hmac = self.init_hmac(force=signature is not None, dummy=signature is None)
        for chunk in _read_in_chunks(enc_file_object, read_total=read_total):
            auth_hmac.update(chunk)
            yield self.secret_box.decrypt(chunk)
        auth_hmac.verify(sig_bytes)

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
    Total read size may be given. Only read() is used on the stream.

    :param BytesIO file_object: readable stream
    :param int chunk_size: chunk read size
    :param int read_total: maximum amount to read in total
    :rtype: tuple
    :returns: data as bytes and index as int
    """
    chunk_size = chunk_size or CHUNK_SIZE
    read_size = chunk_size
    read_yet = 0
    while True:
        if read_total is not None:
            read_size = min(read_total - read_yet, chunk_size)
        data = file_object.read(read_size)
        if not data:
            break
        yield data
        read_yet += read_size


def get_auth_hmac_from_key(auth_key):
    """ Default instanciation of HMAC

    :param bytes auth_key: secret key for signing data
    :rtype: hmac.HMAC
    """
    return hmac.HMAC(auth_key, hashes.SHA512(), backend=default_backend())


def sign_stream(auth_hmac, enc_file_object, read_total=None):
    """ Sign a stream with a given HMAC handler. Suitable for large amounts of data.

    :param hmac.HMAC auth_hmac: HMAC handler
    :param BytesIO enc_file_object: encrypted stream
    :param int read_total: optional size limit for read().
    :returns: hex-serialized signature
    :rtype: bytes
    """
    for chunk in _read_in_chunks(enc_file_object, read_total=read_total):
        auth_hmac.update(chunk)
    return binascii.hexlify(auth_hmac.finalize())


def verify_stream(auth_hmac, enc_file_object, signature, read_total=None):
    """ Verify signed encrypted stream. Suitable for large amounts of data.

    :param hmac.HMAC auth_hmac: HMAC handler
    :param BytesIO enc_file_object: encrypted byte stream
    :param bytes signature: hex-serialized signature
    :param int read_total: maximum bytes to read
    :return: whether signature is valid
    :rtype: bool
    """
    sig_bytes = binascii.unhexlify(signature)
    for chunk in _read_in_chunks(enc_file_object, read_total=read_total):
        auth_hmac.update(chunk)
    try:
        auth_hmac.verify(sig_bytes)
        return True
    except InvalidSignature:
        return False


def sign_bytesio(enc_message):
    """ Sign small and medium sized objects.

    Uses public/private keys, but the private keys is thrown away, as it is cheap to produce and
    there is no need to reuse.

    :param bytes enc_message: encrypted data
    :returns: hex-serialized verification key, signature
    :rtype: tuple
    """
    signing_key = nacl.signing.SigningKey.generate()  # throwaway, I verify_keys are stored locally
    signature = signing_key.sign(enc_message).signature
    verify_key_hex = signing_key.verify_key.encode(encoder=nacl.encoding.HexEncoder)
    return verify_key_hex, signature


def verify_bytesio(enc_message, verify_key_hex, signature):
    """ Verify asymmetrically signed bytesreams.

    :param bytes enc_message: encrypted data
    :param bytes verify_key_hex: serialized verification key
    :param bytes signature: signature
    """
    verify_key = nacl.signing.VerifyKey(verify_key_hex, encoder=nacl.encoding.HexEncoder)
    try:
        verify_key.verify(enc_message, signature)
    except BadSignatureError:
        return False
    return True
