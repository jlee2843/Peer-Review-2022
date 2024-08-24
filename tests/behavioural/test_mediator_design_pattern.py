import pytest

from modules.behavioural.mediator_design_pattern import PublishedPrepubArticleMediator
from modules.building_block import Article
from modules.creational.factory_design_pattern import ArticleFactory
from tests.creational.test_factory_design_pattern import load_article_factory_dataframe


def test_published_prepub_mediator(prepub_test_file):
    load_article_factory_dataframe(prepub_test_file)
    test_doi = '10.7554/eLife.72498'
    article: Article = PublishedPrepubArticleMediator().get_first_stored_article_version(test_doi)
    expected: Article = ArticleFactory().get_base_object(article.doi)
    assert article.version == expected.version


def test_get_first_stored_article_version(prepub_test_file):
    load_article_factory_dataframe(prepub_test_file)
    test_doi = '10.7554/eLife.72498'
    article: Article = PublishedPrepubArticleMediator().get_first_stored_article_version(test_doi)
    assert article.version == 4
    assert test_doi in PublishedPrepubArticleMediator().get_missing_initial_prepub_articles_list()


def test_complete_article_info(completed_test_file):
    assert completed_test_file.size == 2
    load_article_factory_dataframe(completed_test_file)
    test_doi = '10.1101/001768'
    article: Article = PublishedPrepubArticleMediator().get_first_stored_article_version(test_doi)
    assert article.version == 1
    assert test_doi not in PublishedPrepubArticleMediator().get_missing_initial_prepub_articles_list()



# TODO: need to implement a function that receives the initial version of the published article
@pytest.fixture
def prepub_test_file():
    from pathlib import Path
    import json

    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    filename = '../../data/prepub-test.json'
    assert Path(filename).exists()

    json_data = json.load(Path(filename).open())
    import numpy as np
    from modules.utils.database.biorxiv_api import process_data
    result = np.array(process_data(json_data, 'collection', keys, 0))

    return result


@pytest.fixture
def completed_test_file():
    from pathlib import Path
    import json

    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'license', 'category', 'jatsxml', 'abstract', 'published', 'server')
    filename = '../../data/completed-test.json'
    assert Path(filename).exists()

    json_data = json.load(Path(filename).open())
    import numpy as np
    from modules.utils.database.biorxiv_api import process_data
    result = np.array(process_data(json_data, 'collection', keys, 0))

    return result
