import subprocess
import os
from hashlib import md5
from io import BytesIO, FileIO
import tarfile

root_in = '/tmp/test_tar_in'
root_out = '/tmp/test_tar_out'
testfile = 'test_tarfile.0'
subprocess.check_call('dd count=1 ibs=30000 if=/dev/urandom of=' + os.path.join(root_in, testfile), shell=True)
with open(os.path.join(root_in, testfile), 'rb') as f_in:
    hasher = md5()
    hasher.update(f_in.read())
    checksum = hasher.digest()
buffer = BytesIO()
tar_write = tarfile.TarFile(fileobj=buffer, mode='w')
tar_write.add(os.path.join(root_in, testfile), arcname=testfile)
tar_write.close()
buffer.seek(0)
tar_read = tarfile.TarFile(fileobj=buffer)
print(tar_read.getmembers())
print(tar_read.getnames())
tar_read.extractall(path=root_out)
with open(os.path.join(root_out, testfile), 'rb') as f_in:
    hasher = md5()
    hasher.update(f_in.read())
    assert hasher.digest() == checksum


