import time
from typing import List, Tuple

import pandas as pd

from modules.utils.query import get_value, convert_date


def process_data(json_info: dict, section: str, keys: Tuple[str], cursor: int, disable: bool = True) -> List:
    journal_list = [[entry + cursor] + [get_value(journal, key) for key in keys] for entry, journal in
                    enumerate(json_info[section])]
    if disable is False:
        time.sleep(60)

    return journal_list


def create_article(doi, *args, **kwargs):
    from modules.creational.factory_design_pattern import ArticleFactory

    article = ArticleFactory().create_object(identifier=doi, *args, **kwargs)
    pub_doi = article.get_pub_doi()
    if pub_doi.upper() != 'NA':
        ArticleFactory().add_publication_list(article)


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
