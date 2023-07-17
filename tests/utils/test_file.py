from configobj import ConfigObj

from modules.utils.file import create_config_file


def test_comment():
    config1 = ConfigObj()
    config1['test'] = {'subsection': {'test1': 'test_1', 'test2': 'test_2'}, 'testing': 'testing_1'}
    config1['global'] = {'testing_A': 'A'}
    # config.inline_comments = {'global', 'this is a test'}
    getattr(config1, 'comments')['test'] = ['this is a test for test', 'testing out the comment feature of ConfigObj']
    getattr(config1['test']['subsection'], 'inline_comments')['test1'] = 'testing test_1'
    tmp = config1['test']['subsection']
    getattr(tmp, 'inline_comments')['test2'] = 'testing test_2'
    getattr(config1['global'], 'inline_comments')['testing_A'] = 'testing testing_1'
    data = {'test', {'subsection': {'test1': 'test_1', 'test2': 'test_2'}, 'testing': 'testing_1'},
            'global', {'testing_A': 'A'}}

    config = create_config_file(file="../../data/tmp.ini", params=data, comments={})
    assert config == config1
    # config1.filename = '../../data/testing.ini'
    # config1.write()
    # pprint(f'comment\n{config1.comments}')
    # pprint(f'inline\n{config1.inline_comments}')
    # print(f'struct\n{config1}')

    # test_config = ConfigObj('../../data/testing.ini')
    # pprint(f'read in\n{test_config}')
    # pprint(test_config.comments)
    pass
