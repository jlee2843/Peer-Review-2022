import time
from typing import List, Tuple

import numpy as np
import pandas as pd

from modules.behavioural.mediator_design_pattern import PublishedPrepubArticleMediator
from modules.building_block import Journal, Article, Publication
from modules.creational.factory_design_pattern import JournalFactory, PublicationFactory, ArticleFactory
from modules.utils.query import get_value, convert_date, get_json_data, Query


def process_data(json_info: dict, section: str, keys: Tuple[str], cursor: int, disable: bool = True) -> List:
    journal_list = [[entry + cursor] + [get_value(journal, key) for key in keys] for entry, journal in
                    enumerate(json_info[section])]
    if disable is False:
        time.sleep(60)

    return journal_list


def create_article(doi: str, *args: object, **kwargs: object) -> Article:
    from modules.creational.factory_design_pattern import ArticleFactory

    article = ArticleFactory().create_object(identifier=doi, *args, **kwargs)
    pub_doi = article.get_pub_doi()
    if pub_doi.upper() != 'NA':
        ArticleFactory().add_publication_list(article)
        PublishedPrepubArticleMediator().add_object(pub_doi, article)

    return article


def create_prepublish_df(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df['Num_of_Authors'] = df.Authors.apply(lambda x: len(x.split(';')))
        df.DOI = df.DOI.astype('str')
        df.Title = df.Title.astype('str').map(lambda x: x.strip())
        df.Authors = df.Authors.astype('str').map(lambda x: x.strip())
        df.Corresponding_Authors = df.Corresponding_Authors.astype('str').map(lambda x: x.strip())
        df.Institution = df.Institution.map(lambda x: x.strip().upper()).astype('category')
        df.Date = df.Date.map(lambda x: convert_date(x)).astype('datetime64')
        df.Version = df.Version.astype('int32')
        df.Type = df.Type.map(lambda x: x.strip().lower()).astype('category')
        df.Category = df.Category.map(lambda x: x.strip().title()).astype('category')
        df.Xml = df.Xml.astype('str')
        df.Published = df.Published.astype('str')
    except Exception as e:
        print(f'Error in data format:{e.args}\n')
        print(e.with_traceback)

    return df


def receive_missing_initial_version_list() -> List[str]:
    result: List[str] = []
    dois: List[str] = ArticleFactory().get_publication_list()
    for doi in dois:
        article = PublishedPrepubArticleMediator().get_object(doi)
        if article.get_version() != 1:
            result.append(article.get_doi())

    return result


def get_journal_name(query: Query):
    _, result = get_json_data(0, 0, query)
    return np.array(process_data(result.get_result(), 'collection', result.get_keys(), 0))[0, 1]


def create_journal(name: str) -> Journal:
    return JournalFactory().create_object(identifier=name)


def create_publication(journal: Journal, article: Article) -> Publication:
    return PublicationFactory().create_object(article.get_pub_doi(), journal=journal, article=article)
