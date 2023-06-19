import os
import re
from io import StringIO
from pathlib import Path
from pprint import pprint

from pdfminer.converter import TextConverter
from pdfminer.high_level import extract_text_to_fp, extract_text
from pdfminer.layout import *
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage


def extract_fulltext(pdf: Path, layout: LAParams = None, file: Path = None) -> str:
    from modules.utils.file import save_stringio

    doc: StringIO = StringIO()
    with open(str(pdf), 'rb') as original_file:
        extract_text_to_fp(inf=original_file, outfp=doc, laparams=layout)

    if file is not None:
        save_stringio(str(file), doc)
    return doc.getvalue()


def pdf_to_text(path: Path, layout: LAParams = None):
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

        # define a custom list contain entries files paths
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


def get_element_list(pdf: Path, layout: LAParams = None,
                     tag: Union[LTTextBox, LTFigure, LTLine, LTRect, LTImage, None] = LTTextBox) -> List[
    LTLayoutContainer]:
    element_list = []
    for pages in extract_elements(pdf, layout):
        for element in pages:
            if (tag is None) or isinstance(element, tag):
                element_list.append(element)

    return element_list


def get_string_occurrence(element_list: Iterator[LTTextContainer], string: str, sep: str = '.') -> int:
    occurrence_list = []
    for element in element_list:
        txt = element.get_text().strip().lower()
        if txt.startswith(string):
            occurrence_list.append(txt.split(sep)[0])

    return len(set(occurrence_list))


if __name__ == "__main__":
    test = Path('../../data/example.pdf')
    setting = LAParams(boxes_flow=0.0, detect_vertical=True)
    # tmp = extract_fulltext(test)
    # pprint(tmp)
    pprint(extract_text(test, laparams=setting))
    pprint(list(get_element_list(pdf=test, layout=setting, tag=LTTextBox)))
'''
    pprint(list(get_element_list(test)))
    lst = []
    for g_pages in extract_elements(test):
        for el in g_pages:
            if isinstance(el, LTTextBox):
                tmp = el.get_text()
                if tmp.strip().lower().startswith('figure '):
                    # pprint(tmp)
                    pprint(tmp.split('.')[0])
                    lst.append(tmp.split('.')[0])

                # pprint(element)

    pprint(f'{len(set(lst))}\n{set(lst)}')
'''
