"""
The biorvix_api module contains functions that interacts with the Biorxiv database through its API and processes the
information that is retreived from the Biorxiv database.

"""
import time
from typing import List, Tuple

import numpy as np
import pandas as pd

from modules.behavioural.mediator_design_pattern import PublishedPrepubArticleMediator
from modules.building_block import Journal, Article, Publication
from modules.creational.factory_design_pattern import JournalFactory, PublicationFactory, ArticleFactory
from modules.utils.query import get_value, convert_date, get_json_data, Query


def process_data(json_info: dict, section: str, keys: Tuple[str], cursor: int, disable: bool = True) -> List:
    """
    The process_data function processes the data and prepares a journal list.

    :param json_info: A dictionary containing the JSON data.
    :param section: A string representing the section of the JSON data to process.
    :param keys: A tuple of strings representing the keys to extract from each entry in the specified section.
    :param cursor: An integer representing the desired cursor value to add to each entry.
    :param disable: A boolean indicating whether to disable a certain action if True. Defaults to True.
    :return: A list containing processed journal entries.

    """

    # a list comprehension is created with a nested list comprehension inside.
    # The outer list comprehension enumerates over entries in the section of the input json_info.
    # Each enumerated entry is incremented by cursor and each journal entry is processed to determine its key's value
    # using the get_value(journal, key) function call.
    journal_list = [[entry + cursor] + [get_value(journal, key) for key in keys] for entry, journal in
                    enumerate(json_info[section])]
    if disable is False:
        time.sleep(60)

    # The function returns a list of lists(journal_list), where each inner list represents processed data from a journal
    # entry.
    return journal_list


# The create_article function uses the Factory and Mediator Design Patterns.
def create_article(doi: str, *args: object, **kwargs: object) -> Article:
    """
    It's purpose is to create an article object using a digital object identifier (DOI) and other optional parameters,
    potentially returning an Article object.

    :param doi: The DOI (Digital Object Identifier) of the article.
    :param args: Variable length argument list.
    :param kwargs: Arbitrary keyword arguments.
    :return: The created article object.

    Examples::

        >>> create_article("10.1234/abcd", author="John Doe", title="Sample Article")
        <Article object at 0x12345678>

    """
    from modules.creational.factory_design_pattern import ArticleFactory

    # The following line is importing ArticleFactoryfrom modules.creational.factory_design_pattern.ArticleFactory is a
    # factory for creating Article objects in the context of the factory design pattern.
    article = ArticleFactory().create_object(identifier=doi, *args, **kwargs)
    pub_doi = article.get_pub_doi()
    # if created article object has a published DOI then added it to Publication list
    if pub_doi.upper() != 'NA':
        # the newly created article object is added to the ArticleFactory's publication list and
        # a mediator object PublishedPrepubArticleMediator is used to add the article object.
        ArticleFactory().add_publication_list(article)
        PublishedPrepubArticleMediator().add_object(pub_doi, article)

    return article


def create_prepublish_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    The function create_prepublish_df, which transforms a Pandas DataFrame, df, to get it ready for future processing
    related to publishing. The function takes in a DataFrame and returns a processed DataFrame.

    :param df: a pandas DataFrame containing the data to be preprocessed
    :return: a pandas DataFrame with the preprocessed data

    The `create_prepublish_df` method takes a pandas DataFrame `df` as input and performs a series of preprocessing
    steps on the data. It modifies the DataFrame by applying various transformations
    * to different columns. The method then returns the modified DataFrame with the preprocessed data.

    Here are the steps performed by the method:

    1. The `Num_of_Authors` column is added to the DataFrame by splitting the values in the `Authors` column using ';'
       separator and counting the number of authors for each row.

    2. The `DOI` column is converted to string type.

    3. The `Title` column is converted to string type and leading/trailing white spaces are removed.

    4. The `Authors` column is converted to string type and leading/trailing white spaces are removed.

    5. The `Corresponding_Authors` column is converted to string type and leading/trailing white spaces are removed.

    6. The `Institution` column is converted to uppercase and leading/trailing white spaces are removed.
       It is then converted to a categorical data type.

    7. The `Date` column is converted to a date format using the `convert_date` function. The function must be defined
       elsewhere in the code.

    8. The `Version` column is converted to integer type.

    9. The `Type` column is converted to lowercase and leading/trailing white spaces are removed. It is then converted
       to a categorical data type.

    10. The `Category` column is converted to title case and leading/trailing white spaces are removed. It is then
        converted to a categorical data type.

    11. The `Xml` column is converted to string type.

    12. The `Published` column is converted to string type.

    If any errors occur during the preprocessing steps, an error message is printed along with the exception details.

    Finally, the modified DataFrame is returned.

    Example usage:

    ```python
    import pandas as pd

    # Assuming 'df' is a pandas DataFrame containing the data to be processed

    preprocessed_df = create_prepublish_df(df)

    ```
    """
    try:
        # adds a new column to the DataFrame, Num_of_Authors, by counting the number of authors.
        # It assumes that author names in the Authors column are separated by semicolon (;)
        df['Num_of_Authors'] = df.Authors.apply(lambda x: len(x.split(';')))
        # converts the DOI column to string. DOI stands for Digital Object Identifier, a unique alphanumeric string
        # assigned to digital content.
        df.DOI = df.DOI.astype('str')
        #  remove leading and trailing whitespaces in the Title, Authors, Corresponding_Authors and Institution
        #  columns and ensure they are string type.
        df.Title = df.Title.astype('str').map(lambda x: x.strip())
        df.Authors = df.Authors.astype('str').map(lambda x: x.strip())
        df.Corresponding_Authors = df.Corresponding_Authors.astype('str').map(lambda x: x.strip())
        df.Institution = df.Institution.map(lambda x: x.strip().upper()).astype('category')
        # applies the convert_date function to every entry in the Date column and then converts it to
        # pandas datetime64[ns] format.
        df.Date = df.Date.map(lambda x: convert_date(x)).astype('datetime64[ns]')
        df.Version = df.Version.astype('int32')
        df.Type = df.Type.map(lambda x: x.strip().lower()).astype('category')
        df.Category = df.Category.map(lambda x: x.strip().title()).astype('category')
        df.Xml = df.Xml.astype('str')
        df.Published = df.Published.astype('str')
    # any error occurs during these transformations, an exception will be caught.
    except Exception as e:
        print(f'Error in data format:{e.args}\n')
        # the error and traceback will be printed out.
        print(e.with_traceback)

    return df


def receive_missing_initial_version_list() -> List[str]:
    """
    Fetches a list of DOIs for articles that have a version number other than 1.

    :return: A list of DOIs for articles with non-initial versions.
    :rtype: List[str]

    """
    result: List[str] = []
    dois: List[str] = ArticleFactory().get_publication_list()
    for doi in dois:
        article = PublishedPrepubArticleMediator().get_object(doi)
        if article.get_version() != 1:
            result.append(article.get_doi())

    return result


def get_journal_name(query: Query) -> str:
    """
    The purpose of the function is to return the name of the journal based on the results of the provided query.

    :param query: A Query object representing the search query.
    :return: A string representing the name of the journal found in the search results.

    """
    _, result = get_json_data(0, 0, query)
    return np.array(process_data(result.get_result(), 'collection', result.get_keys(), 0))[0, 1]


def create_journal(name: str) -> Journal:
    """
    Creates a journal object.

    :param name: The name of the journal.
    :return: An instance of the Journal class.

    """
    return JournalFactory().create_object(identifier=name)


# This code is the implementation of the Factory design pattern, where a Factory class PublicationFactory is
# responsible for the creation of Publication instances. The Factory pattern promotes loose-coupling by eliminating
# the need for the class (that requires the Publication object) to be involved in instantiation
def create_publication(journal: Journal, article: Article) -> Publication:
    """
    This function creates a new Publication object, which represents an academic publication.

    :param journal: A Journal object representing the journal in which the publication will be created.
    :param article: An Article object representing the article that will be published.
    :return: A Publication object representing the created publication.

    """

    # The new Publication object uses the Digital Object Identifier (DOI) returned by the get_pub_doi method of the
    # Article instance as the identifier.
    return PublicationFactory().create_object(article.get_pub_doi(), journal=journal, article=article)
