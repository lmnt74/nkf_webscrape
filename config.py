'''
Should be where all the configurations needed for the repo should lie
'''

# todo eventually, all of these should be environment variables
def headers (key):
    headers = {
        'User-Agent': '***',
        'From': '***@gmail.com',
        'data-sitekey': '{}'.format(key)
    }
    return headers
