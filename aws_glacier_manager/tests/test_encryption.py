from unittest import TestCase
from hashlib import md5
from .. import encryption
from io import BytesIO
import os


class TestEncryptionGeneral(TestCase):

    def test_general(self):
        # create temporary keys
        secret_key, auth_key = encryption.create_keys(None)
        secret_box = encryption.get_secret_box_from_key(secret_key)
        # create temporary data
        data = BytesIO(os.urandom(80000))
        # prepare checksum for input data
        checksum_in = md5()
        checksum_in.update(data.read())
        # encrypt
        data.seek(0)
        data_out = BytesIO()
        for chunk in encryption.encrypt_stream(secret_box, data):
            data_out.write(chunk)
        # create signature
        data_out.seek(0)
        sig = encryption.sign_output(encryption.get_auth_hmac_from_key(auth_key), data_out)
        # verify by signature
        data_out.seek(0)
        self.assertTrue(encryption.verify_stream(encryption.get_auth_hmac_from_key(auth_key), data_out, sig))
        # decrypt
        data_out.seek(0)
        checksum = md5()
        for chunk in encryption.decrypt_stream(secret_box, data_out):
            checksum.update(chunk)
        self.assertEqual(checksum.digest(), checksum_in.digest())


