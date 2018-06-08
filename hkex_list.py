'''
Module to retrieve a list of company information
'''
# core modules
import logging
import re

# modules for downloading and URL
import urllib
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
from socket import timeout

# modules for Data Science
import pandas as pd

# modules for handling files
import base64
import io

### Constant Values ###
DEFAULT_EXCEL_FILENAME = 'stock_list.xlsx'
HKEXNEWS_URL_CHI = 'http://www.hkexnews.hk/hyperlink/hyperlist_c.HTM'

# Define Logging attributes
def getLogger(
    logger_name
    , logger_level = 'INFO'
    ):
    FORMAT = '%(asctime)-15s,%(levelname)s:%(name)s %(message)s'
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_level)
    return logger

# Create Response of Web Crawler
def create_web_response(url):
    USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'
    REFERER = 'http://www.google.com'
    headers = {
        'User-Agent': USER_AGENT
        , 'referer': REFERER
    }
    return urllib.request.Request(url = url, headers = headers)

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

# Get Stock List from HKex website and output Triple format    
def download_stock_list(
        td_processor=get_stock_dict
    ):
    # Define the destination URL
    hkex_list_url = HKEXNEWS_URL_CHI
    hkex_list_tr_class_list = ["ms-rteTableOddRow-BlueTable_CHI", "ms-rteTableEvenRow-BlueTable_CHI"]
    
    # Section of downloading stock list
    logger = getLogger(__name__)
    logger.info('It starts to download stock list. Please wait.')
    try:
        with urllib.request.urlopen(create_web_response(hkex_list_url)) as page:
            # Create a BeautifulSoup object
            soup = BeautifulSoup(page.read().decode('utf-8', 'ignore'), 'html.parser')
            # Search by CSS Selector
            tr_list = soup.findAll("tr", {"class": hkex_list_tr_class_list})
    except (HTTPError, URLError) as error:
        logger.error('Data not retrieved because %s\nURL: %s', error, hkex_list_url)
    except timeout:
        logger.error('socket timed out - URL %s', hkex_list_url)
    logger.info('Downloading stock list completed.')

    # Prepare list of stock triple from list of table TR objects
    return [td_processor(tr.findAll("td")) for tr in tr_list ]

# Convert DataFrame to Excel in Base64 encoding
def save_excel_base64(
    df
    , sheetname='hkex_stocks'):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, encoding='utf-8', sheet_name=sheetname, index=True)
        writer.save()
        xlsx_data = output.getvalue()
    b64 = base64.b64encode(xlsx_data)
    return b64.decode('utf-8')

# Convert DataFrame to Excel in File
def save_excel_file(
    df
    , filepath=DEFAULT_EXCEL_FILENAME
    , sheetname='hkex_stocks'):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        df.to_excel(writer, encoding='utf-8', sheet_name=sheetname, index=True)
        writer.save()

# init stock list database by downloading
def init_stock_list(filepath=DEFAULT_EXCEL_FILENAME):
    # Prepare DataFrame object from the list of stock dict
    save_excel_file(
        pd.DataFrame(
            data=download_stock_list()
        ).set_index('stock_id', append=False)
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
    print(read_stock_list().to_csv(columns=['stock_id', 'chi_name', 'url'], index=True, sep='\t'))
