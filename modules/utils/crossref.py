from typing import List, Dict, Union

from modules.behavioural.mediator_design_pattern import ArticleLinkTypeMediator
from modules.creational.factory_design_pattern import ArticleFactory

"""
Unlike conventional source code comments, the docstring should describe what the function does, not how.

What should a docstring look like?

The doc string line should begin with a capital letter and end with a period.
The first line should be a short description.
If there are more lines in the documentation string, the second line should be blank, visually separating the summary from the rest of the description.
The following lines should be one or more paragraphs describing the object’s calling conventions, its side effects, etc.
"""

"""
This is a class for mathematical operations on complex numbers.

Attributes:
    real (int): The real part of complex number.
    imag (int): The imaginary part of complex number.
"""


def get_publication_info(doi: str) -> Dict:
    import crossref_commons.retrieval as xref

    import json

    result = xref.get_publication_as_json(doi)
    print(type(result))
    print(json.dumps(result))
    print(result['link'])
    return result


def process_publication_info(doi: str, result: Dict):
    """
    Summary line.

    Extended description of function.

    Parameters:
    arg1 (int): Description of arg1

    Returns:
    int: Description of return value

    """
    # update publication related objects
    from modules.creational.factory_design_pattern import ArticleFactory

    from modules.building_block import Article
    article: Article = ArticleFactory.get_object(doi)
    # get publication list
    tmp = process_link(result['link'], 'content-type', 'application/xml')
    if tmp is None or valid_link(tmp) is False:
        tmp = process_link(result['link'], 'content-type', 'application/pdf')
        if tmp is None or valid_link(tmp) is False:
            ArticleLinkTypeMediator.add_object('web', article)
        else:
            # add url to article
            article.set_publication_link(tmp)
            ArticleLinkTypeMediator.add_object('pdf', article)
    else:
        # add url to article
        article.set_publication_link(tmp)
        ArticleLinkTypeMediator.add_object('xml', article)


def process_link(links: List[Dict], key: str, value: str) -> Union[str, None]:
    for link in links:
        tmp = link.get(key)
        if tmp is None:
            continue
        elif tmp == value:
            return link.get('url')

    return None


def valid_link(url: str) -> bool:
    from modules.utils.query import connect_url

    from requests import HTTPError

    try:
        result = connect_url(0, url)
        print(result)
        return True
    except HTTPError:
        return False


if __name__ == '__main__':
    # get_publication_info('10.5555/515151')
    # get_publication_info("10.1101/104778")
    factory = ArticleFactory()
    article = factory.create_object(identifier="10.1038/s41598-017-04402-4")
    print(article)
    get_publication_info("10.1038/s41598-017-04402-4")
    # get_publication_info("10.1109/MM.2019.2910009")
    # valid_link('https://www.nature.com/articles/s41598-017-04402-4.pdf')
    # valid_link('http://xplorestaging.ieee.org/ielx7/40/8709856/08686049.pdf?arnumber=8686049')
