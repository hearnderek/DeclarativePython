import requests
import context
from declarative import *

@ignore
def test_requests():
    Run(return_dataframe=False)

@io_bound
def wikipedia():
    print('getting wikipedia')
    return requests.get('https://www.wikipedia.org/')

@io_bound
def microsoft():
    print('getting microsoft')
    return requests.get('https://www.microsoft.com/')

@io_bound
def cnn():
    print('getting cnn')
    return requests.get('https://edition.cnn.com/')

@io_bound
def benz():
    print('getting benz')
    return requests.get('https://www.mercedes-benz.com/')

@io_bound
def yahoo():
    print('getting yahoo')
    return requests.get('https://www.yahoo.co.jp/')

def setup():
    print('nothing to setup')

def finialize(wikipedia, microsoft, cnn, benz, yahoo):
    print('have all', 
        wikipedia.status_code, 
        microsoft.status_code, 
        cnn.status_code, 
        benz.status_code,
        yahoo.status_code)


if __name__ == '__main__':
    assert wikipedia.is_io_bound
    assert microsoft.is_io_bound
    assert cnn.is_io_bound
    assert benz.is_io_bound
    assert yahoo.is_io_bound
    assert function_markers.is_io_bound(setup) == False

    #test_requests()
    Run(return_dataframe=False)