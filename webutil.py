'''
Utility module of Web operation
'''
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random
import requests

def get_proxy_list():
    '''
    Get Proxy Server List
    '''
    ua = UserAgent() # From here we generate a random user agent
    proxies = [] # Will contain proxies [ip, port]

    # Retrieve latest proxies
    proxies_req = Request('https://www.sslproxies.org/')
    proxies_req.add_header('User-Agent', ua.random)
    proxies_doc = urlopen(proxies_req).read().decode('utf8')

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    # Save proxies in the array
    for row in proxies_table.tbody.find_all('tr'):
        if row.find_all('td')[1].string.lower() == '8080' and\
            row.find_all('td')[4].string.lower() == 'anonymous':
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

def create_default_request(url, proxy_server = None):
    USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'
    REFERER = 'http://www.google.com'
    headers = {
        'User-Agent': USER_AGENT
        , 'referer': REFERER
    }
    web_req = Request(url = url, headers = headers)
    if proxy_server is not None:
        web_req.set_proxy(proxy_server['ip'] + ':' + proxy_server['port'], 'http')
    return web_req

def create_get_request(
    url
    , user_agent = UserAgent().random
    , referer = 'http://www.google.com'
    , cookies = None
    , proxy_server = None    
    , timeout = None
):
    sess = requests.Session()
    if proxy_server is not None:
        sess.proxies = {"http": "http://" + proxy_server['ip'] + ':' + proxy_server['port']}

    headers = {
        'User-Agent': user_agent
        , 'referer': referer
    }

    if cookies is not None:
        return sess.get(url, cookies=cookies, headers=headers, timeout=timeout)
    else:
        return sess.get(url, headers=headers, timeout=timeout)