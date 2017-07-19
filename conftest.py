
def pytest_addoption(parser):
    parser.addoption('--host', action='store',
                     default='http://localhost:8000', help='s3 host')
    parser.addoption('--access', action='store',
                     default='access1', help='access key')
    parser.addoption('--secret', action='store',
                     default='secret1', help='secret key')
    parser.addoption('--bucket', action='store',
                     default='tagbucket', help='tag bucket')
