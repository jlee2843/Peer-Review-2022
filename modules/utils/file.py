from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Union, Dict

import numpy as np
from configobj import ConfigObj

_base_path_name: Union[str, None] = None
_path: Union[str, None] = None
_filename: Union[str, None] = None


def set_base_path_name(base_path: str) -> None:
    global _base_path_name

    if not base_path.endswith('/'):
        base_path = base_path + '/'
    _base_path_name = base_path

    p = Path(base_path)
    p.mkdir(parents=True, exist_ok=True)


def get_base_path_name() -> str:
    global _base_path_name

    return _base_path_name


def set_path(path: str) -> None:
    global _path

    if not path.endswith('/'):
        path = path + '/'
    _path = path


def get_path(base_path: str = '') -> str:
    global _path

    if _path is None:
        set_path(f'{datetime.now()}/')

    p = Path(f'{base_path}{_path}')
    p.mkdir(parents=True, exist_ok=True)

    return f'{base_path}{_path}'


def set_filename(filename: str) -> None:
    global _filename

    _filename = filename


def get_filename(path: str = '', ext: str = '.parquet') -> str:
    from datetime import datetime

    global _filename

    if _filename is None:
        set_filename(f'{datetime.utcnow().timestamp()}{ext}')

    return f'{path}{_filename}'


def get_dir_list(directory: Path, pattern: str = '') -> np.ndarray:
    listing: np.ndarray
    if pattern == '':
        listing = np.array(filter(lambda x: x.is_file(), directory.rglob('*')))
    else:
        listing = np.array(directory.rglob(pattern))

    return listing


def save_stringio(file: Path, iostream: StringIO):
    save_text(file, iostream.getvalue())


def save_text(file: Path, text: str):
    with open(file, "a") as output_stream:
        output_stream.write(text)
        output_stream.flush()
        output_stream.close()


def assign_values(config: ConfigObj, params: dict) -> ConfigObj
    for param in params.keys():
        config[param] = params.get(param)

    return config


def create_config_file(file: Path, params: Dict[str, dict], comments: Dict[str, dict]):
    config: ConfigObj = ConfigObj()
    config.filename = file
    config = assign_values(config, params.pop(''))
    config = assign_values(config, params)
    config.write()


def read_config_file(file: Path)
# from configparser import ConfigParser
# from itertools import chain

# parser = ConfigParser()
# with open("foo.conf") as lines:
#    lines = chain(("[top]",), lines)  # This line does the trick.
#    parser.read_file(lines)
