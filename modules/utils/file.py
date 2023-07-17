from datetime import datetime
from io import StringIO
from pathlib import Path
from pprint import pprint
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


def assign_values(config: ConfigObj, params: dict) -> ConfigObj:
    for param in params.keys():
        config[param] = params.get(param)

    return config


def create_config_file(file: Path, params: Dict[str, dict], comments: Dict[str, dict]) -> dict:
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


def read_config_file(file: Path):
    pass


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


if __name__ == "__main__":
    config1 = ConfigObj()
    config1['test'] = {'subsection': {'test1': 'test_1', 'test2': 'test_2'}, 'testing': 'testing_1'}
    config1['global'] = {'testing_A': 'A'}
    # config.inline_comments = {'global', 'this is a test'}
    getattr(config1, 'comments')['test'] = ['this is a test for test', 'testing out the comment feature of ConfigObj']
    getattr(config1['test']['subsection'], 'inline_comments')['test1'] = 'testing test_1'
    tmp = config1['test']['subsection']
    getattr(tmp, 'inline_comments')['test2'] = 'testing test_2'
    getattr(config1['global'], 'inline_comments')['testing_A'] = 'testing testing_1'
    config1.filename = '../../data/testing.ini'
    config1.write()
    pprint(f'comment\n{config1.comments}')
    pprint(f'inline\n{config1.inline_comments}')
    print(f'struct\n{config1}')

    test_config = ConfigObj('../../data/testing.ini')
    pprint(f'read in\n{test_config}')
    pprint(test_config.comments)
