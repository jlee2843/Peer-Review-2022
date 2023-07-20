import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from modules.creational.factory_design_pattern import *
from modules.utils.biorxiv_api import process_data, create_article, create_prepublish_df
from modules.utils.query import create_df


@pytest.fixture
def prepub_test_file():
    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    filename = Path('../utils/prepub-test.json').absolute()
    assert filename.exists()
    json_data = json.load(filename.open())
    result = np.array(process_data(json_data, 'collection', keys, 0))

    return result


def load_article_factory_dataframe(result: np.ndarray,
                                   col_names=("DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Date",
                                              "Version", "Type",
                                              "Category", "Xml", "Published")) -> pd.DataFrame:
    df: pd.DataFrame = create_prepublish_df(create_df(result, col_names))
    for row in range(len(df)):
        # doi = df.loc[str(row), 'DOI']
        create_article(doi=df.loc[str(row), 'DOI'],
                       title=df.loc[str(row), 'Title'],
                       authors=df.loc[str(row), 'Authors'],
                       corr_authors=df.loc[str(row), 'Corresponding_Authors'],
                       institution=df.loc[str(row), 'Institution'],
                       date=df.loc[str(row), 'Date'],
                       version=df.loc[str(row), 'Version'],
                       type=df.loc[str(row), 'Type'],
                       category=df.loc[str(row), 'Category'],
                       xml=df.loc[str(row), 'Xml'],
                       pub_doi=df.loc[str(row), 'Published'])

    return df


def test_add_publication_list(prepub_test_file):
    df: pd.DataFrame = load_article_factory_dataframe(prepub_test_file)
    assert (sorted(ArticleFactory().get_publication_list())) == \
           sorted(list(df[df.Published.apply(lambda x: x.upper()) != 'NA'].Published.unique()))
