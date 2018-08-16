from unittest import TestCase
from hashlib import md5
from .. import encryption, datatypes
from io import BytesIO
from nacl import pwhash
import os


class TestEncryptionGeneral(TestCase):

    def test_general(self):
        # create temporary keys
        secret_key, auth_key = encryption.create_keys(None)
        # secret_box = encryption.get_secret_box_from_key(secret_key)
        # create temporary data
        data = BytesIO(os.urandom(80000))
        # prepare checksum for input data
        checksum_in = md5()
        checksum_in.update(data.read())
        # encrypt
        data.seek(0)
        data_out = BytesIO()
        for chunk in encryption.CryptoHandler(secret_key).encrypt_stream(data):
            data_out.write(chunk)
        # create signature
        data_out.seek(0)
        sig = encryption.sign_stream(encryption.get_auth_hmac_from_key(auth_key), data_out)
        # verify by signature
        data_out.seek(0)
        self.assertTrue(encryption.verify_stream(encryption.get_auth_hmac_from_key(auth_key), data_out, sig))
        # decrypt
        data_out.seek(0)
        checksum = md5()
        for chunk in encryption.CryptoHandler(secret_key).decrypt_stream(data_out):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())

    def test_create_keys_from_password(self):
        password = b'supersecret'
        settings = datatypes.DerivedKeySetup.create_default(enable_auth_key=True)
        settings.ops = pwhash.argon2i.OPSLIMIT_MIN
        settings.mem = pwhash.argon2i.MEMLIMIT_MIN
        keys = encryption.create_keys_from_password(password, settings)
        recreated_keys = encryption.create_keys_from_password(password, keys.setup)
        self.assertIs(keys.setup, recreated_keys.setup)
        self.assertEqual(keys.key_sig, recreated_keys.key_sig)
        self.assertEqual(keys.key_enc, recreated_keys.key_enc)
        self.assertTrue(len(keys.key_enc))
        self.assertTrue(len(keys.key_sig))

    def test_class(self):
        secret_key, auth_key = encryption.create_keys(None)
        handler = encryption.CryptoHandler(secret_key, auth_key)
        # create temporary data
        data = BytesIO(os.urandom(80000))
        # prepare checksum for input data
        checksum_in = md5()
        checksum_in.update(data.read())
        # encrypt
        data.seek(0)
        data_out = BytesIO()
        for chunk in handler.encrypt_stream(data):
            data_out.write(chunk)
        # create signature
        sig = handler.last_signature
        # verify by signature
        data_out.seek(0)
        self.assertTrue(handler.verify_stream(data_out, sig))
        # decrypt
        data_out.seek(0)
        checksum = md5()
        for chunk in handler.decrypt_stream(data_out):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())
        # decrypt with verification
        data_out.seek(0)
        checksum = md5()
        for chunk in handler.decrypt_stream(data_out, signature=sig):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())

    def test_class_partial(self):
        secret_key, auth_key = encryption.create_keys(None)
        handler = encryption.CryptoHandler(secret_key, auth_key)
        # create temporary data
        data = BytesIO(os.urandom(80000))
        # prepare checksum for input data
        checksum_in = md5()
        checksum_in.update(data.read(70000))
        # encrypt
        data.seek(0)
        data_out = BytesIO()
        for chunk in handler.encrypt_stream(data, read_total=70000):
            data_out.write(chunk)
        # create signature
        sig = handler.last_signature
        # verify by signature
        data_out.seek(0)
        self.assertTrue(handler.verify_stream(data_out, sig))
        # decrypt
        data_out.seek(0)
        checksum = md5()
        for chunk in handler.decrypt_stream(data_out):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())
        # decrypt with verification
        data_out.seek(0)
        checksum = md5()
        for chunk in handler.decrypt_stream(data_out, signature=sig):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())

    def test_class_partial_wo_signature(self):
        secret_key, _ = encryption.create_keys(None)
        handler = encryption.CryptoHandler(secret_key)
        # create temporary data
        data = BytesIO(os.urandom(80000))
        # prepare checksum for input data
        checksum_in = md5()
        checksum_in.update(data.read(70000))
        # encrypt
        data.seek(0)
        data_out = BytesIO()
        for chunk in handler.encrypt_stream(data, read_total=70000):
            data_out.write(chunk)
        # create signature
        self.assertIsNone(handler.last_signature)
        # verify by signature should fail
        data_out.seek(0)
        with self.assertRaises(RuntimeError):
            handler.verify_stream(data_out, 'abcd')
        # decrypt
        data_out.seek(0)
        checksum = md5()
        for chunk in handler.decrypt_stream(data_out):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())
        # decrypt with verification should fail
        data_out.seek(0)
        with self.assertRaises(RuntimeError):
            for _ in handler.decrypt_stream(data_out, signature='abcd'):
                pass
        # decrypt without full verification
        data_out.seek(0)
        checksum = md5()
        for chunk in handler.decrypt_stream(data_out):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())
