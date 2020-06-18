import os
import datetime
from typing import Optional, Mapping, Any
from cryp_to_go.core import hexlify, unhexlify


class File:

    def __init__(self, path):
        self.path = path
        self.exists = os.path.exists(path)
        self.file_hash: Optional[bytes] = None
        self.file_hash_updated: Optional[datetime.datetime] = None
        self.size: Optional[int] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "path": self.path,
            "file_hash": None if not self.file_hash else hexlify(self.file_hash),
            "file_hash_updated": None if not self.file_hash_updated else self.file_hash_updated.isoformat(),
            "size": self.size,
        }

    @classmethod
    def from_dict(cls, data_dict: Mapping[str, Any]) -> "File":
        inst = cls(data_dict['path'])
        if inst.exists:
            inst.file_hash = file_hash \
                if (file_hash := data_dict["file_hash"]) is None \
                else unhexlify(file_hash)
            inst.file_hash_updated = file_hash_updated \
                if (file_hash_updated := data_dict["file_hash_updated"]) is None \
                else datetime.datetime.fromisoformat(file_hash_updated)
            inst.size = data_dict['size']

    def update(self, with_hash: bool = False) -> None:
        """ check if exists, load size, calc optional hash """
        self.exists = os.path.exists(self.path)
        if not self.exists:
            self.file_hash = None
            self.file_hash_updated = None
            return
        size = os.stat(self.path).st_size
        if size != self.size:
            # if the size changed, the hash must be removed
            self.file_hash = None
            self.file_hash_updated = None
            self.size = size
        if with_hash:
            self.file_hash = None  # ToDo: calculate hash
            self.file_hash_updated = datetime.datetime.utcnow()
