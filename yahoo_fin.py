'''
Module for retriving Yahoo! Finance Data
'''
# core modules
import re
import logutil
import sys
import io

# modules for downloading and URL
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout
import webutil

# modules for Data Science
import pandas as pd
import excelutil

# modules for concurrency
import concurrent.futures
import time
from datetime import datetime

### Section of constant values applicable to the following functions ###
# Define label strings
LABEL_DATE = 'Date'
LABEL_OPEN = 'Open'
LABEL_HIGH = 'High'
LABEL_LOW = 'Low'
LABEL_CLOSE = 'Close'
LABEL_ADJCLOSE = 'Adj Close'
LABEL_VOLUME = 'Volume'

YAHOO_DATE_FORMAT = '%Y-%m-%d'

PAGE_TIMEOUT = 5.0

'''
Functions of Handling Yahoo! Finance Site Cookies
'''
def get_cookie_value(res):
    '''
    This function is to get value from cookie
    Parameters
    ----------
    res : Respones Object
          Response Object
    Returns
    -------
    Dictionary of Cookie Key and its Value
    '''
    return {'B': res.cookies['B']}

def get_page_data(stock_id, proxy_server=None, timeout=None):
    '''
    This function is to get page data include cookie and content lines from given stock page
    '''
    WELCOME_FORMAT = 'https://hk.finance.yahoo.com/quote/{0}/history?p={0}'
    welcome_url = WELCOME_FORMAT.format(stock_id)
    response = webutil.create_get_request(url = welcome_url, proxy_server=proxy_server, timeout=timeout)
    cookie = get_cookie_value(response)
    
    ENCODING = 'unicode-escape'
    lines = response.content.decode(ENCODING).strip(). replace('}', '\n')
    return cookie, lines.split('\n')

def find_crumb_store(lines):
    '''
    This function is to retrieve crumb store data from content lines
    Looking for
    ,"CrumbStore":{"crumb":"9q.A4D1c.b9
    '''
    logger = logutil.getLogger(__name__)
    for l in lines:
        if re.findall(r'CrumbStore', l):
            return l
    logger.error("Did not find CrumbStore")

def split_crumb_store(value):
    '''
    split crumb store by given value
    '''
    return value.split(':')[2].strip('"')

def get_cookie_crumb(stock_id, proxy_server=None, timeout=None):
    '''
    This function is to retrieve cookie and crumb values
    '''
    cookie, lines = get_page_data(stock_id, proxy_server=proxy_server, timeout=timeout)
    crumb = split_crumb_store(find_crumb_store(lines))
    return cookie, crumb

# Download Yahoo! Finance Historical Prices
# For HK only
def download_yahoo_hist(
    stock_code
    , from_date='2000-01-01'
    , to_date=datetime.now().strftime(YAHOO_DATE_FORMAT)
    , proxy_flag=False
    , retry_time=3
    , retry_delay=10
    , timeout=PAGE_TIMEOUT
):
    '''
    This function is to download historical stock prices from Yahoo! Finance.
    Parameters
    ----------
    stock_code : string
               Stock ID, usually a 4 digit number
    from_date: string
               Starting Date in yyyy-mm-dd format
    to_date: string
               Ending Date in yyyy-mm-dd format
    retry_time : int
                 number of time to retry if each connection fails
    Returns
    -------
    Pandas DataFrame
    '''
    logger = logutil.getLogger(__name__)
    df = None

    from_timestamp = int(round(datetime.strptime(from_date, YAHOO_DATE_FORMAT).timestamp()))
    to_timestamp = int(round(datetime.strptime(to_date, YAHOO_DATE_FORMAT).timestamp()))
    if from_timestamp >= to_timestamp:
        # invalid time range
        return None
    
    CSV_FORMAT = 'https://query1.finance.yahoo.com/v7/finance/download/{}?period1={}&period2={}&interval=1d&events=history&crumb={}'
    for _ in range(retry_time):
        try:
            proxy_server = None
            if proxy_flag:
                proxy_server = webutil.get_random_proxy()
                logger.info('download via a proxy server: %s', proxy_server['ip'] + ':' + proxy_server['port'])

            cookie, crumb = get_cookie_crumb(stock_code, proxy_server=proxy_server, timeout=timeout)
            csv_url = CSV_FORMAT.format(stock_code, from_timestamp, to_timestamp, crumb)

            response = webutil.create_get_request(url=csv_url, cookies=cookie, proxy_server=proxy_server, timeout=timeout)            
            if response.status_code != 200:
                response.raise_for_status()
            else:
                df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))    
            break
        except Timeout:
            logger.error('socket timed out - URL %s', csv_url)
            time.sleep(retry_delay)
        except RequestException as error:
            logger.error('Data not retrieved because %s\nURL: %s', error, csv_url)
            time.sleep(retry_delay)
        except Exception as error:
            logger.error('Unexpected error of %s\nURL: %s', error, csv_url)
            time.sleep(retry_delay)

    else:
        logger.error('No historical data after %d retry.' % retry_time)

    if df is None:
        return df

    # Change correct Data Type
    df[LABEL_DATE] = pd.to_datetime(df[LABEL_DATE], format=YAHOO_DATE_FORMAT)    
    df[LABEL_OPEN] = pd.to_numeric(df[LABEL_OPEN], errors='ignore', downcast='float')
    df[LABEL_HIGH] = pd.to_numeric(df[LABEL_HIGH], errors='ignore', downcast='float')
    df[LABEL_LOW] = pd.to_numeric(df[LABEL_LOW], errors='ignore', downcast='float')
    df[LABEL_CLOSE] = pd.to_numeric(df[LABEL_CLOSE], errors='ignore', downcast='float')
    df[LABEL_ADJCLOSE] = pd.to_numeric(df[LABEL_ADJCLOSE], errors='ignore', downcast='float')
    df[LABEL_VOLUME] = pd.to_numeric(df[LABEL_VOLUME], errors='ignore', downcast='integer')
    return df.set_index(LABEL_DATE, append=False)

def get_hk_yahoo_code(stock_number):
    '''
    This function is to convert HKex Stock ID in Yahoo! Finance format
    '''
    return '{:04}.HK'.format(stock_number)

# Get Stock Quote (list of dict) by Stock ID
def get_stock_quote(
    stock_code
    , proxy_flag=False
    , retry_time=3
    , retry_delay=10
    , timeout=PAGE_TIMEOUT
    ):
    '''
    Get stock quote in format of dictionary by a given stock Code
    Parameters
    ----------
    stock_code : string
                 stock Code
    proxy_flag : boolean
                 Whether retrieval uses a random Proxy Server
    retry_time : int
                 number of time to retry if each connection fails
    retry_delay : int
                  How long does it wait if retry fails to get the next
    Returns
    -------
    stock quote in format of dictionary 
    '''
    logger = logutil.getLogger(__name__)

    stock_url = 'https://hk.finance.yahoo.com/quote/' + stock_code
    td_class = "C(black) W(51%)"
    for _ in range(retry_time):
        try:
            td_list = []

            proxy_server = None
            if proxy_flag:
                proxy_server = webutil.get_random_proxy()
                logger.info('download via a proxy server: %s', proxy_server['ip'] + ':' + proxy_server['port'])

            response = webutil.create_get_request(url=stock_url, proxy_server=proxy_server, timeout=timeout)
            if response.status_code != 200:
                response.raise_for_status()
            stock_page = response.content.decode('utf-8', 'ignore')
            stock_soup = BeautifulSoup(stock_page, 'html.parser')
            td_list = stock_soup.findAll('td', {"class": td_class})

            logger.info('Result of %s has %d records', stock_code, len(td_list))
            pair_list = {
                td.get_text():
                td.findNext('td').get_text()
                for td in td_list
            }
            break
        except Timeout:
            logger.error('socket timed out - URL %s', stock_url)
            time.sleep(retry_delay)
        except RequestException as error:
            logger.error('Data not retrieved because %s\nURL: %s', error, stock_url)
            time.sleep(retry_delay)
        except Exception as error:
            logger.error('Unexpected error of %s\nURL: %s', error, stock_url)
            time.sleep(retry_delay)
    else:
        logger.error('No response after %d retry.' % retry_time)
        pair_list = {}
    pair_list.update({'stock_code': stock_code})
    return pair_list

# Get Stock Quote Data Frame by Stock List
def get_stock_quote_df(
    stock_code_list
    , max_workers=10
    , proxy_flag=False
    ):
    logger = logutil.getLogger(__name__)

    stock_quote_list = []   
    # Threads start and it takes quite a long time due to multiple network I/O
    logger.info('It starts to download stock quotes. Please wait.')
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_stock_code = {
            executor.submit(get_stock_quote, stock_code, proxy_flag): 
            stock_code for stock_code in stock_code_list
        }

    # Update thread status for monitoring
    for future in concurrent.futures.as_completed(future_to_stock_code):
        stock_quote_list.append(future.result())
    
    logger.info('Downloading stock quotes completed.')
    return pd.DataFrame(
        data=stock_quote_list
        ).set_index('stock_code', append=False)

### Run as a main program ###
if __name__ == '__main__':
    print(get_stock_quote_df(stock_code_list=[stock_code for stock_code in sys.argv[1:]], max_workers=1, proxy_flag=True).to_csv(index=True, sep='\t'))    
    # print(download_yahoo_hist(stock_code=sys.argv[1], proxy_flag=True).to_csv(index=True, sep='\t'))
