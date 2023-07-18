from datetime import datetime
from io import StringIO
from pathlib import Path
from threading import Lock
from typing import Union, Dict, Optional

import numpy as np
from configobj import ConfigObj

from modules.building_block import Singleton

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


def assign_values(config: ConfigObj, params: dict) -> ConfigObj:
    for param in params.keys():
        config[param] = params.get(param)

    return config


def create_config_file(file: str, params: Dict[str, dict], comments: Dict[str, dict]) -> ConfigObj:
    """
    The structure of the comments dictionary is <attribute, <'' or section_name, <keyword , comment>

    the structure of parameter is <'' or section_name, <keyword, value>
    :param file:
    :param params:
    :param comments: a dictionary that contain <command to execute, dictionary of <section, comment> or comments>
    :return:
    """

    config: ConfigObj = ConfigObj()
    config.filename = file
    keywords = params.pop('', None)
    if keywords is not None:
        config = assign_values(config, keywords)

    config = assign_values(config, params)

    config = process_comments(config, comments)
    config.write()

    return config


def read_config_file(file: str):
    Settings.set_config_object(ConfigObj(file))


def output_comments(config: ConfigObj, attribute: str, key: str, data: dict) -> ConfigObj:
    value = data.get(key)
    if type(data.get(key)) is dict:
        for inner_key in value.keys():
            output_comments(config[key], attribute, inner_key, value)
    else:
        getattr(config, attribute)[key] = value

    return config


def process_comments(config: ConfigObj, comments: dict) -> ConfigObj:
    """
    The structure of the comments dictionary is <attribute, <'' or section_name, [comment or <keyword , comment]>

    :param config:
    :param comments:
    :return:
    """

    for attribute in comments.keys():
        comment_info: Dict[str, dict] = comments.get(attribute)
        keywords = comment_info.pop('', None)
        if keywords is not None:
            for keyword in keywords.keys():
                config = output_comments(config, attribute, keyword, keywords)

        for section in comment_info.keys():
            config = output_comments(config, attribute, section, comment_info)

    return config


# from configparser import ConfigParser
# from itertools import chain

# parser = ConfigParser()
# with open("foo.conf") as lines:
#    lines = chain(("[top]",), lines)  # This line does the trick.
#    parser.read_file(lines)

class Settings(metaclass=Singleton):
    _config_object: ConfigObj = ConfigObj()
    _lock: Lock = Lock()

    def __init(self, config: ConfigObj):
        self.set_config_object(config)

    def set_config_object(self, config: ConfigObj):
        with self._lock:
            self._config_object = config

    def get_value(self, key: str, section: Optional[str] = None, subsection: Optional[str] = None) -> Union[str, None]:
        value: Union[str, None] = None

        with self._lock:
            if section is not None:
                if subsection is not None:
                    value = self._config_object[section][subsection][key]
                else:
                    value = self._config_object[section][key]
            else:
                value = self._config_object[key]

        return value


if __name__ == "__main__":
    pass
