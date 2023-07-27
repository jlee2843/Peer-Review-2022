"""
Standard Unit Testing in Python

import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
"""
from typing import Tuple, List

import numpy as np
import pandas as pd
import pytest

from modules.behavioural.mediator_design_pattern import PublishedPrepubArticleMediator
from modules.building_block import Article, Journal
from modules.creational.factory_design_pattern import ArticleFactory, JournalFactory, PublicationFactory
from modules.utils.biorxiv_api import process_data, create_article, create_prepublish_df, create_journal, \
    get_journal_name, create_publication, receive_missing_initial_version_list
from modules.utils.query import Query, get_web_data, get_json_data, create_df


@pytest.fixture()
def prepub_query():
    # url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    # url = 'https://api.biorxiv.org/details/biorxiv/10.1101/339747'
    url = 'https://api.biorxiv.org/details/medrxiv/10.1101/2021.04.29.21256344'
    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    col_names = ["DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Date", "Version", "Type",
                 "Category", "Xml", "Published"]
    query = Query(url, keys, col_names)
    result = get_json_data(0, 0, query)
    return result[1]


@pytest.fixture()
def pub_query():
    url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    keys: Tuple = ('published_journal',)
    col_names: List[str] = ['Journal']
    query: Query = Query(url, keys, col_names)
    return get_json_data(0, 0, query)[1]


@pytest.fixture()
def query():
    url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    keys: Tuple = (
        "preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
        "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
        "published_date")
    col_names = ["DOI", "pub_DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Category", "Journal",
                 "Preprint_Date", "Published_Date"]
    query = Query(url, keys, col_names)
    return query


def test_query(query):
    assert isinstance(query, Query)


def test_get_keys(query):
    keys = ("preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
            "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
            "published_date")
    assert query.get_keys() is not None
    assert query.get_keys() == keys


def test_get_col_names(query):
    col_names = ["DOI", "pub_DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Category", "Journal",
                 "Preprint_Date", "Published_Date"]
    assert query.get_col_names() is not None
    assert query.get_col_names() == col_names


def test_get_query_result(pub_query, query):
    assert query.get_url() == 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    query.set_result(get_web_data(0, query.get_url(), 'json'))
    assert query.get_result() is not None
    assert pub_query.get_result() is not None
    # assert pub_query.get_result() == query.get_result()


def test_get_web_data():
    # url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    url = 'https://api.biorxiv.org/details/biorxiv/10.1101/339747'
    result = get_web_data(0, url, 'json')
    assert isinstance(result, dict)
    result = get_web_data(0, url)
    assert isinstance(result, str)
    result = get_web_data(0, url, 'content')
    assert isinstance(result, bytes)


def test_invalid_attr():
    with pytest.raises(ValueError):
        get_web_data(counter=0, url='hello', attr='texTs')
        get_web_data(counter=0, url='hello', attr=' ')
        get_web_data(counter=0, url='hello', attr='hello')
        get_web_data(counter=0, url='hello', attr='.')


def test_valid_attr(query):
    try:
        get_web_data(0, query.get_url(), 'TEXT')
        get_web_data(0, query.get_url(), 'text ')
        get_web_data(0, query.get_url(), ' text')
        get_web_data(0, query.get_url(), ' text ')
        get_web_data(0, query.get_url(), '\n\ttext')
    except ValueError as exc:
        assert False, f'get_web_data: {exc}'


def test_process_biorxiv_query(prepub_query):
    result = process_data(prepub_query.get_result(), 'collection', prepub_query.get_keys(), 0)
    assert result[0][1] is not None
    result = process_data(prepub_query.get_result(), 'collection', prepub_query.get_keys(), 0)
    assert len(result[0]) == len(prepub_query.get_keys()) + 1


def test_get_value(prepub_query):
    result: dict = prepub_query.get_result()['collection'][0]
    collection = list(result.keys())
    try:
        _ = [result[key] for key in collection]
    except KeyError as exc:
        assert False, f'raised an exception {exc}'


def test_create_article(prepub_query):
    with pytest.raises(RuntimeError, match='Please instantiate class through the corresponding Factory'):
        Article()
    result = np.array(process_data(prepub_query.get_result(), 'collection', prepub_query.get_keys(), 0))
    assert type(result) is np.ndarray
    assert type(prepub_query.get_col_names()) is list
    df = create_prepublish_df(create_df(result, prepub_query.get_col_names()))
    for row in range(len(df)):
        row = str(row)
        doi = df.loc[row, 'DOI']
        create_article(doi=df.loc[row, 'DOI'], title=df.loc[row, 'Title'],
                       authors=df.loc[row, 'Authors'], corr_authors=df.loc[row, 'Corresponding_Authors'],
                       institution=df.loc[row, 'Institution'], date=df.loc[row, 'Date'],
                       version=df.loc[row, 'Version'], type=df.loc[row, 'Type'],
                       category=df.loc[row, 'Category'], xml=df.loc[row, 'Xml'],
                       pub_doi=df.loc[row, 'Published'])

        assert doi == '10.1101/2021.04.29.21256344'
        assert ArticleFactory().get_object(identifier=doi) is not None


def test_create_journal(pub_query):
    journal = create_journal(get_journal_name(pub_query))
    assert pub_query.get_keys() == ('published_journal',)
    assert get_journal_name(pub_query) == 'PLOS ONE'
    assert type(journal) is Journal
    assert journal is JournalFactory().get_object(journal.get_title())
    assert journal.get_prefix() == ''
    assert journal.get_issn() == ''
    assert journal.get_impact_factor() == 0.0


def test_create_publication(prepub_query: Query, pub_query):
    """
    url = 'https://api.biorxiv.org/details/medrxiv/10.1101/2021.04.29.21256344'
    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    col_names = ["DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Date", "Version", "Type",
                 "Category", "Xml", "Published"]
    query = Query(url, keys, col_names)
    _, prepub_query = get_json_data(0, 0, query)
    """

    result = np.array(process_data(prepub_query.get_result(), 'collection', prepub_query.get_keys(), 0))
    df = create_prepublish_df(create_df(result, prepub_query.get_col_names()))
    article = create_article(doi=df.loc['0', 'DOI'],
                             title=df.loc['0', 'Title'],
                             authors=df.loc['0', 'Authors'],
                             corr_authors=df.loc['0', 'Corresponding_Authors'],
                             institution=df.loc['0', 'Institution'],
                             date=df.loc['0', 'Date'],
                             version=df.loc['0', 'Version'],
                             type=df.loc['0', 'Type'],
                             category=df.loc['0', 'Category'],
                             xml=df.loc['0', 'Xml'],
                             pub_doi=df.loc['0', 'Published'])

    assert article is not None
    journal = create_journal(get_journal_name(pub_query))
    publication = create_publication(journal, article)
    assert publication is PublicationFactory().get_object(article.get_pub_doi())


@pytest.fixture
def prepub_test_file():
    from pathlib import Path
    import json

    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    filename = '../../data/prepub-test.json'
    assert Path(filename).exists()
    json_data = json.load(Path(filename).open())
    result = np.array(process_data(json_data, 'collection', keys, 0))

    return result


def test_receive_initial_version(prepub_test_file, prepub_query):
    from tests.creational.test_factory_design_pattern import load_articles

    df: pd.DataFrame = create_prepublish_df(create_df(prepub_test_file, prepub_query.get_col_names()))
    load_articles(df)
    missing_items: List[str] = receive_missing_initial_version_list()
    url = 'https://api.biorxiv.org/details/biorxiv/'
    articles: List[Article] = []
    for missing in missing_items:
        query = Query(url + missing, prepub_query.get_keys(), prepub_query.get_col_names())
        result = get_json_data(0, 0, query)[1]
        result = np.array(process_data(result.get_result(), 'collection', prepub_query.get_keys(), 0))
        df = create_prepublish_df(create_df(result, prepub_query.get_col_names()))
        for line in range(len(df)):
            line = str(line)
            articles.append(create_article(doi=df.loc[line, 'DOI'], title=df.loc[line, 'Title'],
                                           authors=df.loc[line, 'Authors'],
                                           corr_authors=df.loc[line, 'Corresponding_Authors'],
                                           institution=df.loc[line, 'Institution'], date=df.loc[line, 'Date'],
                                           version=df.loc[line, 'Version'], type=df.loc[line, 'Type'],
                                           category=df.loc[line, 'Category'], xml=df.loc[line, 'Xml'],
                                           pub_doi=df.loc[line, 'Published']))

        doi: str = articles[0].get_pub_doi()
        article = PublishedPrepubArticleMediator().get_object(doi)
        assert article.get_version() == 1
