from pathlib import Path


class Ledger:
    FILENAME = ".glacier_sync.sqlite"

    def __init__(self, local_path: Path):
        self.local_path = local_path

    @classmethod
    def from_cwd(cls):
        current_dir = Path.cwd()
        while True:
            target = current_dir / cls.FILENAME
            if target.exists():
                return cls(local_path=current_dir)
            if current_dir.parent == current_dir:
                break
            current_dir = current_dir.parent

    def create_config_file(self):
        pass
