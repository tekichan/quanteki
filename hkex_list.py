'''
Module to retrieve a list of company information
'''
# core modules
import re
from logutil import getLogger

# modules for downloading and URL
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
from socket import timeout
from webutil import create_web_request, get_random_proxy

# modules for Data Science
import pandas as pd
from excelutil import save_excel_base64, save_excel_file

# modules for concurrency
import time

### Constant Values ###
DEFAULT_EXCEL_FILENAME = 'stock_list.xlsx'
HKEXNEWS_URL_CHI = 'http://www.hkexnews.hk/hyperlink/hyperlist_c.HTM'

# Get Stock Dict from TD tag object crawled
def get_stock_dict(td_list):
    '''
    Get a Dict of stock information 
    from the list of Table TD
    Parameters
    ----------
    td_list : list
              list of Table TD objects
    Returns
    -------
    a Dict of stock information (Stock ID, Stock Name, Company URL) 
    '''
    stock_name = re.sub(r"\r?\n", " ", td_list[1].get_text().strip())
    stock_name = " ".join(filter(lambda x: not x.startswith('http'), stock_name.split()))
    return {
        'stock_id': int(td_list[0].get_text().strip())
        , 'chi_name': stock_name
        , 'url': td_list[2].get_text().strip()
    }

# Get Stock List from HKex website and output the list of given processer format    
def download_stock_list(
        td_processor=get_stock_dict
        , proxy_flag=False
        , retry_time=3
        , retry_delay=10
    ):
    # Define the destination URL
    hkex_list_url = HKEXNEWS_URL_CHI
    hkex_list_tr_class_list = ["ms-rteTableOddRow-BlueTable_CHI", "ms-rteTableEvenRow-BlueTable_CHI"]
    
    # Section of downloading stock list
    logger = getLogger(__name__)
    logger.info('It starts to download stock list. Please wait.')
    tr_list = []
    for _ in range(retry_time):
        try:
            proxy_server = None
            if proxy_flag:
                proxy_server = get_random_proxy()
                logger.info('download via a proxy server: %s', proxy_server['ip'] + ':' + proxy_server['port'])
            with urlopen(create_web_request(url=hkex_list_url, proxy_server=proxy_server)) as page:
                # Create a BeautifulSoup object
                soup = BeautifulSoup(page.read().decode('utf-8', 'ignore'), 'html.parser')
                # Search by CSS Selector
                tr_list = soup.findAll("tr", {"class": hkex_list_tr_class_list})
            break
        except (HTTPError, URLError) as error:
            logger.error('Data not retrieved because %s\nURL: %s', error, hkex_list_url)
            time.sleep(retry_delay)
        except timeout:
            logger.error('socket timed out - URL %s', hkex_list_url)
            time.sleep(retry_delay)
    else:
        logger.error('No response after %d retry.' % retry_time)
    logger.info('Downloading stock list completed with length of %d.', len(tr_list))

    # Prepare list of stock triple from list of table TR objects
    return [td_processor(tr.findAll("td")) for tr in tr_list ]

# Get Stock List from HKex website and output DataFrame format    
def download_stocks_df(proxy_flag=False):
    return pd.DataFrame(data=download_stock_list(proxy_flag=proxy_flag)).set_index('stock_id', append=False)

# Convert DataFrame to Excel in Base64 encoding
def save_excel_base64(
    df
    , sheetname='hkex_stocks'):
    return save_excel_base64(df, sheetname)

# Convert DataFrame to Excel in File
def save_excel_file(
    df
    , filepath=DEFAULT_EXCEL_FILENAME
    , sheetname='hkex_stocks'):
    save_excel_file(df, filepath, sheetname)

# init stock list database by downloading
def init_stock_list(filepath=DEFAULT_EXCEL_FILENAME, proxy_flag=False):
    # Prepare DataFrame object from the list of stock dict
    save_excel_file(download_stocks_df(proxy_flag=proxy_flag), filepath)

# read stock list database by local Excel
def read_stock_list(filepath=DEFAULT_EXCEL_FILENAME, sheetname='hkex_stocks'):
    _filepath = filepath
    if filepath is None:
        _filepath = DEFAULT_EXCEL_FILENAME
    return pd.read_excel(filepath, sheet_name=sheetname).set_index('stock_id', append=False)

### Run as a main program ###
if __name__ == '__main__':
    print(download_stocks_df(proxy_flag=False).to_csv(columns=['stock_id', 'chi_name', 'url'], index=True, sep='\t'))
