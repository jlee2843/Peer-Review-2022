import pytest
from pathlib import Path
from datetime import datetime
from modules.utils.file.PathManager import PathManager


class TestPathManager:
    def setup_method(self):
        self.pm = PathManager()

    def test_set_base_path_name(self):
        self.pm.set_base_path_name('/test/path')
        assert self.pm._base_path_name == '/test/path/'

    def test_get_base_path_name(self):
        self.pm._base_path_name = '/test/path/'
        assert self.pm.get_base_path_name() == '/test/path/'

    def test_set_path(self):
        self.pm.set_path('/test/path')
        assert self.pm._path == '/test/path/'

    def test_get_path(self):
        self.pm._path = None
        assert self.pm.get_path('/base/path') == '/base/path/'
        assert Path('/base/path').exists()

        self.pm._path = '/test/path/'
        assert self.pm.get_path('/base/path') == '/base/path/test/path/'
        assert Path('/base/path/test/path').exists()

    def test_set_filename(self):
        self.pm.set_filename('file_name')
        assert self.pm._filename == 'file_name'

    def test_get_filename(self):
        timestamp = datetime.utcnow().timestamp()
        self.pm._filename = None
        assert self.pm.get_filename('/base/path', '.ext').startswith('/base/path')
        assert self.pm.get_filename('/base/path', '.ext').endswith('.ext')

        self.pm._filename = 'file_name'
        assert self.pm.get_filename('/base/path', '.ext') == '/base/pathfile_name'

    def test_singleton(self):
        assert PathManager() is self.pm
