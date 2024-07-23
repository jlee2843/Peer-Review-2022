from threading import Lock
from typing import Optional, Union

from configobj import ConfigObj

from modules.building_block import Singleton


class Settings(metaclass=Singleton):
    _config_object: ConfigObj = None

    def __init__(self, config: ConfigObj):
        with Settings.lock:
            self._config_object = config

    def get_value(self, key: str, section: Optional[str] = None, subsection: Optional[str] = None) -> Union[str, None]:
        value: Union[str, None] = None

        with Singleton.lock:
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
