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

import pytest

from modules.behavioural.database.query import Query, BioRvixQuery
from modules.utils.database.biorxiv_api import process_data
from modules.utils.database.process_query_results import get_web_data, get_json_data, get_value


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
    query = BioRvixQuery(url, keys, col_names)

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

    return BioRvixQuery(url, keys, col_names)


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

    result = query.execute()
    assert result is not None


def test_get_pubs_query_result(pubs_query: Query) -> None:
    """
    Test the result of the pubs_query object using the get_web_data method. Assert that the result of the pubs_query
    is not None.

    :param pubs_query: An instance of the Query class containing the query information.
    :return: None.
    """
    pubs_query.execute()
    assert pubs_query.result is not None


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

    url: str = query.url
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
        get_web_data(0, query.url, 'TEXT')
        get_web_data(0, query.url, 'text ')
        get_web_data(0, query.url, ' text')
        get_web_data(0, query.url, ' text ')
        get_web_data(0, query.url, '\n\ttext')
    except ValueError as exc:
        assert False, f'get_web_data: {exc}'


def test_key_values(prepub_query, query):
    keys: Tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')

    assert prepub_query.keys == query.keys
    assert keys == prepub_query.keys


def test_key_values_published(pub_query, pubs_query):
    keys: Tuple = (
        "preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
        "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
        "published_date")
    assert pub_query.keys == pubs_query.keys
    assert keys == pub_query.keys


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


def test_process_data_published(pub_query: Query) -> None:
    """
    Test the process_data method for the publishedt query.

    :param pub_query: The Query object to be processed.
    :type pub_query: Query
    :return: None
    :raises AssertError: If the pub_query keys do not match the expected;
                         if the returned data does not contain the key 'preprint_doi'; or
                         if the returned value for the key 'preprint_doi' does not match the expected
    """
    process_biorxiv_query(pub_query, 'preprint_doi')


def process_biorxiv_query(query: Query, attr: str) -> None:
    result: dict = query.result['collection'][0]
    collection: List = list(result.keys)
    try:
        _ = [get_value(result, key) for key in collection]
    except KeyError as exc:
        assert False, f'raised an exception {exc}'

    tmp = process_data(query.result, 'collection', query.keys, 0)
    assert tmp[0][1] == query.result['collection'][0].get(attr)
