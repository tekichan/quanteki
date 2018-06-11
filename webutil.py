'''
Utility module of Web operation
'''
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random

def get_proxy_list():
    '''
    Get Proxy Server List
    '''
    ua = UserAgent() # From here we generate a random user agent
    proxies = [] # Will contain proxies [ip, port]

    # Retrieve latest proxies
    proxies_req = Request('https://www.us-proxy.org/')
    proxies_req.add_header('User-Agent', ua.random)
    proxies_doc = urlopen(proxies_req).read().decode('utf8')

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    # Save proxies in the array
    for row in proxies_table.tbody.find_all('tr'):
        if row.find_all('td')[6].string.lower() == 'yes':
            proxies.append({
                'ip':   row.find_all('td')[0].string
                , 'port': row.find_all('td')[1].string
            })
    
    return proxies

def get_random_proxy(
    proxy_list = get_proxy_list()
):
    '''
    Get a Proxy Server randomly from a proxy list
    '''
    return proxy_list[random.randint(0, len(proxy_list) - 1)]

# Create Request of Web Crawler
def create_web_request(
    url
    , user_agent = UserAgent().random
    , referer = 'http://www.google.com'
    , proxy_server = None
):
    web_req = Request(url)
    if proxy_server is not None:
        web_req.set_proxy(proxy_server['ip'] + ':' + proxy_server['port'], 'http')
    web_req.add_header('User-Agent', user_agent)
    web_req.add_header('referer', referer)
    return web_req
