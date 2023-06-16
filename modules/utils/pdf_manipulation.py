import os
from io import StringIO
import re
from pathlib import Path

from pdfminer.high_level import extract_text
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.layout import *


def extract_fulltext(pdf: Path, layout: LAParams = None):
    return extract_text(pdf, layout)


def pdf_to_text(path:Path, layout: LAParams = None):
    '''Extract text from pdf documents
    '''

    manager = PDFResourceManager()
    retstr = StringIO()
    device = TextConverter(manager, retstr, laparams=layout)
    interpreter = PDFPageInterpreter(manager, device)
    with open(path, 'rb') as filepath:
        for page in PDFPage.get_pages(filepath, check_extractable=True):
            interpreter.process_page(page)
    text = retstr.getvalue()
    device.close()
    retstr.close()
    return text


def extraction(split_path, text_path):
    '''Extract and save text files to output dir
    '''

    # entries names
    entries = os.listdir(split_path)

    # repeat the process for each entry
    for entry in entries:

        # define a custom list cotain entries files paths
        custom_list = os.listdir(os.path.join(split_path, entry))

        # list must be sorted
        custom_list.sort(key=lambda f: int(re.sub(r'\D', '', f)))

        # repeat the process for each file path
        for file_path in custom_list:

            text_output = pdf_to_text(
                os.path.join(split_path, entry, file_path))

            # save text file of each entry
            with open(os.path.join(text_path, f"{entry}.txt"),
                      "a",
                      encoding="utf-8") as text_file:
                text_file.write(text_output)


def extract_elements(pdf: Path, layout: LAParams = None):
    from pdfminer.high_level import extract_pages

    return extract_pages(pdf, laparams=layout)