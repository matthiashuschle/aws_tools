""" Requirements:
- have ~/.aws/credentials
- and region in ~/.aws/config

see https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration

for boto3 Documentation on Glacier, see https://boto3.readthedocs.io/en/latest/reference/services/glacier.html
"""
import os
from io import BytesIO
import logging
import base64
import json
import boto3
from botocore.utils import calculate_tree_hash
from .encryption import CryptoHandler


class MultipartChunk:

    classname = 'MultipartChunk'

    def __init__(self, filepath, block_size, start_loc, end_loc, crypto_handler=None):
        """ Handles generation of Data chunks as atomic parts of the Glacier upload.

        :param str filepath: path of the file to upload
        :param int block_size: size of the part to be uploaded. Must not match end_loc - start_loc + 1,
            as encryption increases the volume.
        :param int start_loc: start offset within unencrypted file
        :param int end_loc: end offset within unencrypted file
        :param CryptoHandler crypto_handler: manages data encryption, optional
        """
        self.filepath = filepath
        assert end_loc - start_loc < block_size
        self.block_size = block_size
        self.start_loc = start_loc
        self.end_loc = end_loc
        self.completed = False
        self._crypto_handler = crypto_handler
        self._encrypted = crypto_handler is not None
        self._checksum = None
        self._data = None

    @property
    def encrypted(self):
        """ As encryption determines the relation of end_pos and block size, this should not be changed.

        :rtype: bool
        """
        return self._encrypted

    @property
    def crypto_handler(self):
        """ If using encryption, this must be set before usage.

        :rtype: CryptoHandler
        :raises RuntimeError: on illegal use
        """
        if self.encrypted and self.crypto_handler is None:
            raise RuntimeError('crypto_handler is undefined for encrypted Chunk!')
        return self._crypto_handler

    @crypto_handler.setter
    def crypto_handler(self, value):
        """ Allow setting only if using encryption.

        :param CryptoHandler value:
        :raises RuntimeError: on illegal use
        """
        if not self.encrypted:
            raise RuntimeError('You can not set a crypto_handler, if the chunk is not created as encrypted!')
        self._crypto_handler = value

    def __str__(self):
        return '%s(%r, %i, %i, %i, %s)' % (
            self.classname, self.filepath,
            self.block_size, self.start_loc, self.end_loc,
            'encrypted' if self.encrypted else 'unencrypted'
        )

    def to_dict(self):
        return {x: getattr(self, x) for x in [
            'filepath',
            'block_size',
            'start_loc',
            'end_loc',
            'completed',
            '_encrypted',
            '_checksum'
        ]}

    @classmethod
    def from_dict(cls, val_dict, crypto_handler=None):
        instance = cls(**{x: y for x, y in val_dict.items() if x in [
            'filepath',
            'block_size',
            'start_loc',
            'end_loc']})
        for key in ['completed', '_checksum', '_encrypted']:
            setattr(instance, key, val_dict[key])
        if crypto_handler is not None:
            instance.crypto_handler = crypto_handler
        return instance

    @property
    def data(self):
        """ Fetch the data to be sent. Handles encryption internally.

        :rtype: bytearray
        """
        if self._data is None:
            with open(self.filepath, 'rb',) as f_in:
                f_in.seek(self.start_loc)
                if self.encrypted:
                    # we pipe the data through the encryptor, inflating due to signatures
                    data = BytesIO()
                    unenc_block_size = self.crypto_handler.get_unenc_block_size(self.block_size)
                    for chunk in self.crypto_handler.encrypt_stream(f_in, unenc_block_size):
                        data.write(chunk)
                    data.seek(0)
                    self._data = data.getvalue()
                else:
                    self._data = f_in.read(self.block_size)
        return self._data

    @property
    def pos_range(self):
        """ string representation of file position range.

        :rtype: str
        """
        return 'bytes %i-%i/*' % (self.start_loc, self.end_loc)

    def finalize(self):
        """ Free memory after upload completion.

        :return: None
        """
        self.completed = True
        self._data = None

    @property
    def checksum(self):
        """ Calculate the checksum for the upload, as needed by Glacier API.

        :rtype: str
        """
        if self._checksum is None:
            self._checksum = calculate_tree_hash(self.data)
        if self.completed:
            self._data = None
        return self._checksum

    @classmethod
    def split_file(cls, filepath, block_size, crypto_handler=None):
        """ Create a Chunk array from a file.

        :param str filepath: path of the file.
        :param int block_size: size per upload chunk. must be 2**k.
        :param CryptoHandler crypto_handler: optional encryption handler.
        """
        chunks = []
        total_size = get_file_size(filepath)
        cur_position = 0
        # if encryption is used, the unencrypted block size must be used for input chunks
        unenc_block_size = block_size if crypto_handler is None else crypto_handler.get_unenc_block_size(block_size)
        while cur_position <= total_size:
            end_pos = min(cur_position + unenc_block_size, total_size)
            chunks.append(cls(filepath, block_size, cur_position, end_pos - 1, crypto_handler=crypto_handler))
            cur_position = end_pos
        return chunks


class MultipartUpload:

    DEFAULT_BLOCK = 2 * 1024 * 1024

    def __init__(self, vault_name, filepath, block_size=None, description='', crypto_handler=None):
        self.vault_name = vault_name
        self.filepath = filepath
        self.block_size = block_size or self.DEFAULT_BLOCK
        self.description = description
        self.crypto_handler = crypto_handler
        self.upload_id = None
        self.archive_id = None
        self._chunks = []
        self.responses = {}

    def to_dict(self):
        dict_out = {x: getattr(self, x) for x in [
            'vault_name',
            'filepath',
            'block_size',
            'description',
            'upload_id',
            'archive_id',
            'responses'
        ]}
        dict_out['_chunks'] = [x.to_dict() for x in self._chunks]
        return dict_out

    @classmethod
    def from_dict(cls, val_dict, crypto_handler=None):
        instance = cls(crypto_handler=crypto_handler, **{x: y for x, y in val_dict.items() if x in [
            'vault_name',
            'filepath',
            'block_size',
            'description']})
        for key in ['upload_id', 'archive_id', 'responses']:
            setattr(instance, key, val_dict[key])
        instance._chunks = [MultipartChunk.from_dict(x, crypto_handler=crypto_handler) for x in val_dict['_chunks']]
        return instance

    def initialize(self):
        step = 'initialize'
        logging.info('creating multipart upload for file %s' % self.filepath)
        self.responses[step] = get_client().initiate_multipart_upload(
            partSize=str(self.block_size),
            archiveDescription=base64.standard_b64encode(self.description),
            vaultName=self.vault_name
        )
        self.upload_id = self.responses[step].get('uploadId', None)
        if self.upload_id is None:
            logging.warning('%s failed: %r' % (step, self.responses[step]))
            raise RuntimeError('could not initiate upload!')
        logging.info('created multipart upload for file %s with ID %s' % (self.filepath, self.upload_id))

    @property
    def chunks(self):
        if not self._chunks:
            self._chunks = MultipartChunk.split_file(self.filepath, self.block_size, crypto_handler=self.crypto_handler)
        return self._chunks

    def upload(self):
        if not self.upload_id:
            raise RuntimeError('must initialize first!')
        step = 'upload'
        if step not in self.responses:
            self.responses[step] = []
        for chunk in self.chunks:
            logging.info('uploading chunk %s' % chunk)
            if chunk.completed:
                continue
            response = get_client().upload_multipart_part(
                body=chunk.data,
                checksum=chunk.checksum,
                range=chunk.pos_range,
                uploadId=self.upload_id,
                vaultName=self.vault_name,
            )
            if response.get('checksum', None) == chunk.checksum:
                logging.info('uploading chunk %s finished' % chunk)
                chunk.finalize()
            else:
                logging.info('uploading chunk %s failed' % chunk)
            self.responses[step].append(response)

    def upload_loop(self, n_tries=3):
        loop_count = 0
        while not all(x.completed for x in self.chunks):
            if loop_count >= n_tries:
                raise RuntimeError('failed after %i tries' % loop_count)
            logging.info('upload loop round %i for %s' % (loop_count, self.filepath))
            self.upload()
            loop_count += 1

    def finalize(self):
        step = 'finalize'
        total_checksum = calculate_tree_hash(open(self.filepath, 'rb'))
        self.responses[step] = get_client().complete_multipart_upload(
            archiveSize=str(get_file_size(self.filepath)),
            checksum=total_checksum,
            uploadId=self.upload_id,
            vaultName=self.vault_name,
        )
        if self.responses[step].get('checksum', None) == total_checksum:
            self.archive_id = self.responses[step]['archiveId']
            logging.info('multipart upload %s finished with archive ID %s' % (self.filepath, self.archive_id))
        else:
            logging.info('multipart upload %s failed' % self.filepath)


class VaultStorage:

    def __init__(self, vault_name):
        self.vault_name = vault_name

    def dump_uploader(self, uploader):
        print(json.dumps(uploader))

    def upload_file(self, filepath, block_size=None, description_dict=None):
        if not description_dict:
            description_dict = {'path': filepath}
        assert get_file_size(filepath)
        uploader = MultipartUpload(self.vault_name, filepath, block_size=block_size,
                                   description=json.dumps(description_dict))
        return self._upload_from_uploader(uploader)

    def _upload_from_uploader(self, uploader):
        try:
            if not uploader.upload_id:
                uploader.initialize()
            if not uploader.archive_id:
                uploader.upload_loop()
                uploader.finalize()
            return uploader.archive_id
        except:
            self.dump_uploader(uploader.to_dict())
            raise

    def request_inventory(self):
        return get_client().initiate_job(
            vaultName=self.vault_name,
            jobParameters={"Description": "inventory-job", "Type": "inventory-retrieval", "Format": "JSON"}
        )

    def retrieve_inventory(self, job_id):
        running = self.list_running_jobs()
        for job in running['JobList']:
            if job['JobId'] == job_id:
                return None
        return get_client().get_job_output(vaultName=self.vault_name, jobId=job_id)

    def list_running_jobs(self):
        return get_client().list_jobs(vaultName=self.vault_name, completed='false')


def get_file_size(filepath):
    return os.stat(filepath).st_size


def get_client():
    return boto3.client('glacier')
