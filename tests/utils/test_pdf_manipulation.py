from pathlib import Path

import pytest


@pytest.fixture()
def pdf():
    return Path('../../data/example.pdf')


def test_extract_text(pdf):
    assert pdf.exists() is True  # able to fine test pdf
