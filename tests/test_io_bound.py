import requests
import context
import declarative

def test_requests():
    declarative.Run(return_dataframe=False)
declarative.ignore(test_requests)

def wikipedia():
    print('getting wikipedia')
    return requests.get('https://www.wikipedia.org/')
declarative.io_bound(wikipedia)

def microsoft():
    print('getting microsoft')
    return requests.get('https://www.microsoft.com/')
declarative.io_bound(microsoft)

def cnn():
    print('getting cnn')
    return requests.get('https://edition.cnn.com/')
declarative.io_bound(cnn)

def benz():
    print('getting benz')
    return requests.get('https://www.mercedes-benz.com/')
declarative.io_bound(benz)

def yahoo():
    print('getting yahoo')
    return requests.get('https://www.yahoo.co.jp/')
declarative.io_bound(yahoo)

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
    assert declarative.function_markers.is_io_bound(setup) == False

    #test_requests()
    declarative.Run(return_dataframe=False)