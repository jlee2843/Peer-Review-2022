from pathlib import Path

import pytest


@pytest.fixture()
def pdf():
    return Path('../../data/example.pdf')


def test_extract_fulltext(pdf):
    from modules.utils.pdf_manipulation import extract_fulltext

    assert pdf.exists() is True  # able to fine test pdf
    text: str = extract_fulltext(pdf)
    assert text.__contains__('object categorization') is True
    text = ''
    assert text.__contains__('Cats and Dogs') is False


def test_extract_elements(pdf):
    from modules.utils.pdf_manipulation import extract_elements

    elements = extract_elements(pdf)

    assert elements is not None


