'''
Module to retrieve a list of company information
'''
# core modules
import re
import logutil

# modules for downloading and URL
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout
import webutil

# modules for Data Science
import pandas as pd
import excelutil

# modules for concurrency
import time

### Constant Values ###
DEFAULT_EXCEL_FILENAME = 'stock_list.xlsx'
HKEXNEWS_URL_CHI = 'http://www.hkexnews.hk/hyperlink/hyperlist_c.HTM'
PAGE_TIMEOUT = 5.0

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
        , timeout=PAGE_TIMEOUT
    ):
    # Define the destination URL
    hkex_list_url = HKEXNEWS_URL_CHI
    hkex_list_tr_class_list = ["ms-rteTableOddRow-BlueTable_CHI", "ms-rteTableEvenRow-BlueTable_CHI"]
    
    # Section of downloading stock list
    logger = logutil.getLogger(__name__)
    logger.info('It starts to download stock list. Please wait.')
    tr_list = []
    for _ in range(retry_time):
        try:
            proxy_server = None
            if proxy_flag:
                proxy_server = webutil.get_random_proxy()
                logger.info('download via a proxy server: %s', proxy_server['ip'] + ':' + proxy_server['port'])

            response = webutil.create_get_request(url=hkex_list_url, proxy_server=proxy_server, timeout=timeout)
            if response.status_code != 200:
                response.raise_for_status()
            decoded_result = response.content.decode('utf-8', 'ignore')
            if len(decoded_result) <= 0:
                logger.error('Failed to retrieve content.')
                continue                
            # Create a BeautifulSoup object
            soup = BeautifulSoup(decoded_result, 'html.parser')
            # Search by CSS Selector
            tr_list = soup.findAll("tr", {"class": hkex_list_tr_class_list})
            break
        except Timeout:
            logger.error('socket timed out - URL %s', hkex_list_url)
            time.sleep(retry_delay)
        except RequestException as error:
            logger.error('Data not retrieved because %s\nURL: %s', error, hkex_list_url)
            time.sleep(retry_delay)
        except Exception as error:
            logger.error('Unexpected error of %s\nURL: %s', error, hkex_list_url)
            time.sleep(retry_delay)
    else:
        logger.error('No response after %d retry.' % retry_time)
    logger.info('Downloading stock list completed with length of %d.', len(tr_list))

    # Prepare list of stock triple from list of table TR objects
    return [td_processor(tr.findAll("td")) for tr in tr_list ]

# Get Stock List from HKex website and output DataFrame format    
def download_stocks_df(
    proxy_flag=False
    , retry_time=3
    , retry_delay=10
    , timeout=PAGE_TIMEOUT    
):
    return pd.DataFrame(
        data=download_stock_list(
            proxy_flag=proxy_flag
            , retry_time=retry_time
            , retry_delay=retry_delay
            , timeout=timeout
        )
    ).set_index('stock_id', append=False)

# Convert DataFrame to Excel in Base64 encoding
def save_excel_base64(
    df
    , sheetname='hkex_stocks'):
    return excelutil.save_excel_base64(df, sheetname)

# Convert DataFrame to Excel in File
def save_excel_file(
    df
    , filepath=DEFAULT_EXCEL_FILENAME
    , sheetname='hkex_stocks'):
    excelutil.save_excel_file(df, filepath, sheetname)

# init stock list database by downloading
def init_stock_list(
    filepath=DEFAULT_EXCEL_FILENAME
    , proxy_flag=False
    , retry_time=3
    , retry_delay=10
    , timeout=PAGE_TIMEOUT    
):
    # Prepare DataFrame object from the list of stock dict
    save_excel_file(
        download_stocks_df(
            proxy_flag=proxy_flag
            , retry_time=retry_time
            , retry_delay=retry_delay
            , timeout=timeout
        )
        , filepath
    )

# read stock list database by local Excel
def read_stock_list(filepath=DEFAULT_EXCEL_FILENAME, sheetname='hkex_stocks'):
    _filepath = filepath
    if filepath is None:
        _filepath = DEFAULT_EXCEL_FILENAME
    return pd.read_excel(filepath, sheet_name=sheetname).set_index('stock_id', append=False)

### Run as a main program ###
if __name__ == '__main__':
    print(download_stocks_df(proxy_flag=True).to_csv(columns=['stock_id', 'chi_name', 'url'], index=True, sep='\t'))
