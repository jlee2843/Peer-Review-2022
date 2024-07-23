from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

from modules.building_block import Singleton


@dataclass
class PathManager(metaclass=Singleton):
    def __init__(self):
        self._base_path_name: Union[str, None] = None
        self._path: Union[str, None] = None
        self._filename: Union[str, None] = None

    def set_base_path_name(self, base_path: str) -> None:
        with PathManager.lock:
            if not base_path.endswith('/'):
                base_path = base_path + '/'
            self._base_path_name = base_path
            p = Path(base_path)
            p.mkdir(parents=True, exist_ok=True)

    def get_base_path_name(self) -> str:
        return self._base_path_name

    def set_path(self, path: str) -> None:
        with PathManager.lock:
            if not path.endswith('/'):
                path = path + '/'
            self._path = path

    def get_path(self, base_path: str = '') -> str:
        if self._path is None:
            self.set_path(f'{datetime.now()}/')
        p = Path(f'{base_path}{self._path}')
        p.mkdir(parents=True, exist_ok=True)
        return f'{base_path}{self._path}'

    def set_filename(self, filename: str) -> None:
        with PathManager.lock:
            self._filename = filename

    def get_filename(self, path: str = '', ext: str = '.parquet') -> str:
        if self._filename is None:
            self.set_filename(f'{datetime.utcnow().timestamp()}{ext}')
        return f'{path}{self._filename}'
