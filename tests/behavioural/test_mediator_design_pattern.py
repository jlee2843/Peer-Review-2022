import pytest

from modules.behavioural.mediator_design_pattern import PublishedPrepubArticleMediator
from modules.creational.factory_design_pattern import ArticleFactory
from tests.creational.test_factory_design_pattern import load_article_factory_dataframe


def test_published_prepub_mediator(prepub_test_file):
    load_article_factory_dataframe(prepub_test_file)
    test_doi = '10.7554/eLife.72498'
    article = PublishedPrepubArticleMediator().get_object(test_doi)
    expected = ArticleFactory().get_object(article.get_doi())
    assert article.get_version() == expected.get_version()

# TODO: need to implement a function that receives the initial version of the published article
@pytest.fixture
def prepub_test_file():
    from pathlib import Path
    import json

    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    filename = './prepub-test.json'
    assert Path(filename).exists()

    json_data = json.load(Path(filename).open())
    import numpy as np
    from modules.utils.biorxiv_api import process_data
    result = np.array(process_data(json_data, 'collection', keys, 0))

    return result
