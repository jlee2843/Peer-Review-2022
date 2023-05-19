import datetime

from pathlib import Path
from typing import Union

_basePathName: Union[str, None] = None
_path: Union[str, None] = None
_filename: Union[str, None] = None


def set_base_path_name(base_path: str) -> None:
    global _basePathName

    if not base_path.endswith('/'):
        base_path = base_path + '/'
    _basePathName = base_path

    p = Path(base_path)
    p.mkdir(parents=True, exist_ok=True)


def get_base_path_name() -> str:
    global _basePathName

    return _basePathName


def set_path(path: str) -> None:
    global _path

    if not path.endswith('/'):
        path = path + '/'
    _path = path


def get_path(basePath: str = '') -> str:
    global _path

    if _path is None:
        set_path(f'{datetime.now()}/')

    p = Path(f'{basePath}{_path}')
    p.mkdir(parents=True, exist_ok=True)

    return f'{basePath}{_path}'


def set_filename(filename: str) -> None:
    global _filename

    _filename = filename


def get_filename(path: str = '', ext: str = '.parquet') -> str:
    from datetime import datetime

    global _filename

    if _filename is None:
        set_filename(f'{datetime.utcnow().timestamp()}{ext}')

    return f'{path}{_filename}'

# from configparser import ConfigParser
# from itertools import chain

# parser = ConfigParser()
# with open("foo.conf") as lines:
#    lines = chain(("[top]",), lines)  # This line does the trick.
#    parser.read_file(lines)
