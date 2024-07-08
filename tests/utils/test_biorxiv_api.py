"""
The provided Python code is used for testing different parts of a larger application, specifically parts
related to retrieving and processing publication data from the BioRxiv API. It consists of multiple parts:

1. Importing Required Libraries: Various libraries and modules are imported, such as pytest (for testing) and various
   custom modules of the project.

2. PyTest Fixtures: These functions prepub_query(), pubs_query(), and query() are set up as fixtures using pytest's
   @pytest.fixture() decorator. These fixtures are functions that create and return some sort of object that's needed
   in a test. Pytest ensures that fixtures are run before the tests that request them.

3. Unit Test Functions (test_xxx): These are a series of test blocks that validate different aspects of the project,
   which include but are not limited to verifying:
   * The validity of various query parameters (test_query, test_get_keys, test_get_col_names, test_get_query_result)
   * The correct return types of some web data fetching function (test_get_web_data)
   * Processing of data returned from the BioRxiv API (test_process_biorxiv_query, test_get_value)
   * Creation of various objects (test_create_article, test_create_journal) This involves a factory pattern,
     where objects are constructed via factory classes, rather than directly.

In general, when called, these test functions will execute their assertions and if any assertion fails, pytest will
consider that test to fail and report it accordingly. This code is crucial for maintaining the proper functioning and
reliability of the software as it evolves.
"""

from typing import Tuple, List, Union, Any

import numpy as np
import pandas as pd
import pytest

from modules.behavioural.mediator_design_pattern import PublishedPrepubArticleMediator
from modules.building_block import Article, Journal, Publication, Department, Institution, Category
from modules.creational.factory_design_pattern import ArticleFactory, JournalFactory, PublicationFactory
from modules.utils.biorxiv_api import process_data, create_article, create_prepublish_df, create_journal, \
    get_journal_name, create_publication, receive_missing_initial_version_list
from modules.utils.query import Query, get_web_data, get_json_data, create_df, get_value


@pytest.fixture()
def query():
    """
    Returns an instance of the Query class initialized with a specific URL, keys, and column names.

    :return: An instance of the Query class.
    """

    url = 'https://api.biorxiv.org/details/medrxiv/10.1101/2021.04.29.21256344'
    keys: Tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    col_names: List = ["DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Date", "Version", "Type",
                       "Category", "Xml", "Published"]
    query = Query(url, keys, col_names)

    return query


@pytest.fixture()
def prepub_query(query: Query) -> Query:
    """
    Executes a query to retrieve prepublication data from the BioRxiv API.

    :return: A dictionary containing the prepublication data.
    """

    # url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    # url = 'https://api.biorxiv.org/details/biorxiv/10.1101/339747'
    # url = 'https://api.biorxiv.org/details/medrxiv/10.1101/2021.04.29.21256344'
    result = get_json_data(0, 0, query)
    return result[1]


@pytest.fixture()
def pubs_query() -> Query:
    url: str = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    keys: Tuple = (
        "preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
        "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
        "published_date")
    col_names: List = ["DOI", "pub_DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Category",
                       "Journal", "Preprint_Date", "Published_Date"]

    return Query(url, keys, col_names)


@pytest.fixture()
def pub_query(pubs_query) -> Union[Query, Any]:
    """
    Retrieve publication data from the BioRxiv API.

    :return: The publication data, specifically the journal name.
    """

    # url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    # keys: Tuple = ('published_journal',)
    # col_names: List[str] = ['Journal']
    # query: Query = Query(url, keys, col_names)
    return get_json_data(0, 0, pubs_query)[1]


def test_get_query_result(query: Query) -> None:
    """
    Test the result of the query object using the get_web_data method. Assert that the result of the query
    is not None.

    :param query: The query object containing the URL to retrieve data from.
    :return: None
    :raises: AssertionError: If the query object isn't able to retrieve prepublication data using the BioRixiv API

    """

    result = get_web_data(0, query.get_url(), 'json')
    assert result is not None


def test_get_pubs_query_result(pubs_query: Query) -> None:
    """
    Test the result of the pubs_query object using the get_web_data method. Assert that the result of the pubs_query
    is not None.

    :param pubs_query: An instance of the Query class containing the query information.
    :return: None.
    """
    pubs_query.set_result(get_web_data(0, pubs_query.get_url(), 'json'))
    assert pubs_query.get_result() is not None


def test_get_web_data(query: Query) -> None:
    """
    This method `test_get_web_data` is used to test the `get_web_data` function by providing different parameters and
    checking the type of the returned result.

    :param query: An instance of the Query class representing the query parameters.
    :return: None
    :raises: AssertionError: result is not an instance of dict when the attr parameter to get_web_data is 'json'
                             result is not an instance of str when the attr parameter to get_web_data is blank or 'text'
                             result is not an instead of bytes when the attr parameter to get_web_data is 'content'
    """

    url: str = query.get_url()
    result = get_web_data(0, url, 'json')
    assert isinstance(result, dict)
    result = get_web_data(0, url)
    assert isinstance(result, str)
    result = get_web_data(0, url, 'content')
    assert isinstance(result, bytes)


def test_valid_attr(query):
    """
    This method tests the validity of the attribute in the given query by calling the `get_web_data` function with
    different attribute values. If any of the calls raise a `ValueError`, an assertion error is raised.

    :param query: The query object containing the URL to be tested.
    :return: None.

    Example usage:

        query = Query(url='https://example.com')
        test_valid_attr(query)

    """

    try:
        get_web_data(0, query.get_url(), 'TEXT')
        get_web_data(0, query.get_url(), 'text ')
        get_web_data(0, query.get_url(), ' text')
        get_web_data(0, query.get_url(), ' text ')
        get_web_data(0, query.get_url(), '\n\ttext')
    except ValueError as exc:
        assert False, f'get_web_data: {exc}'


def test_key_values(prepub_query, query):
    keys: Tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')

    assert prepub_query.get_keys() == query.get_keys()
    assert keys == prepub_query.get_keys()


def test_key_values_postprint(pub_query, pubs_query):
    keys: Tuple = (
        "preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
        "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
        "published_date")
    assert pub_query.get_keys() == pubs_query.get_keys()
    assert keys == pub_query.get_keys()


def test_process_data_preprint(prepub_query: Query) -> None:
    """
    Test the process_data method for the preprint query

    :param prepub_query: The preprint query object.
    :type prepub_query: Query
    :return: None
    :raises AssertError: If the prepub_query keys do not match the expected;
                         if the returned data does not contain the key 'doi'; or
                         if the returned value for the key 'doi' does not match the expected
    """

    process_biorxiv_query(prepub_query, 'doi')


def test_process_data_postprint(pub_query: Query) -> None:
    """
    Test the process_data method for the postprint query.

    :param pub_query: The Query object to be processed.
    :type pub_query: Query
    :return: None
    :raises AssertError: If the pub_query keys do not match the expected;
                         if the returned data does not contain the key 'preprint_doi'; or
                         if the returned value for the key 'preprint_doi' does not match the expected
    """
    process_biorxiv_query(pub_query, 'preprint_doi')


def process_biorxiv_query(query: Query, attr: str) -> None:
    result: dict = query.get_result()['collection'][0]
    collection: List = list(result.keys())
    try:
        _ = [get_value(result, key) for key in collection]
    except KeyError as exc:
        assert False, f'raised an exception {exc}'

    tmp = process_data(query.get_result(), 'collection', query.get_keys(), 0)
    assert tmp[0][1] == query.get_result()['collection'][0].get(attr)


def test_instantiation_factory():
    with pytest.raises(RuntimeError, match='Please instantiate class through the corresponding Factory'):
        Article()
        Journal()
        Publication()
        Department()
        Institution()
        Category()


def test_create_article(prepub_query):
    """

    This method is used to test the creation of article object(s).

    :param prepub_query: An instance of the PrepublishQuery class representing the prepublication query.
    :return: None

    """
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


def test_create_journal(pubs_query: Query) -> None:
    """
    The function test_create_journal is a test function which takes in one argument, pubs_query, a Query object
    representing a query used to retrieve journal information from the BioRxiv database.

    :param pubs_query: A Query object representing the query used to retrieve journal information from a database.
    :return: None
    """

    journal = create_journal(get_journal_name(pubs_query))
    assert get_journal_name(pubs_query) == 'PLOS ONE'
    assert type(journal) is Journal
    assert journal is JournalFactory().get_object(journal.get_title())
    assert journal.get_prefix() == ''
    assert journal.get_issn() == ''
    assert journal.get_impact_factor() == 0.0


def test_create_publication(prepub_query: Query, pubs_query: Query) -> None:
    """
    :param prepub_query: A Query object representing the prepublication data.
    :param pubs_query: A query representing the publication data.
    :return: The created Publication object.

    This method takes a prepublication query and a publication query as input. It processes the prepublication query to
    obtain a result, creates a prepublication DataFrame, and then creates
    * an Article object using the data from the DataFrame. The Article object is then used to create a Journal object
      and a Publication object. The method asserts that the created Article and Publication objects are not None.

    Example usage:
    prepub_query = Query(...)
    pubs_query = ...
    result = test_create_publication(prepub_query, pubs_query)
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
    journal = create_journal(get_journal_name(pubs_query))
    publication = create_publication(journal, article)
    assert publication is PublicationFactory().get_object(article.get_pub_doi())


@pytest.fixture
def prepub_test_file() -> np.ndarray:
    from pathlib import Path
    import json

    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    filename = '../../data/prepub-test.json'
    assert Path(filename).exists()
    json_data = json.load(Path(filename).open())
    result = np.ndarray(process_data(json_data, 'collection', keys, 0))

    return result


def test_receive_initial_version(prepub_test_file: np.ndarray, prepub_query: Query) -> None:
    """
    Test method to receive the initial version of articles.

    :param prepub_test_file: The file containing the prepublication data.
    :param prepub_query: The query object containing column names.
    :return: None
    """
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
