'''
Module to retrieve company information from AASTOCKS
'''
# core modules
import re
import logutil
import sys

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

### Constant Values ###
PAGE_TIMEOUT = 5.0

def get_hk_aastocks_code(stock_number):
    '''
    This function is to convert HKex Stock ID in Bloomberg format
    '''
    return '{:05}'.format(stock_number)

# Get List of tr elements about Dividend 
def get_dividend_trlist(soup_object):
    searchtext = re.compile(r'^Dividend\sHistory$')
    div_title_list = soup_object.find_all('div',text=searchtext)   
    table_list = [div_title.find_next('table', {"class": 'cnhk-cf'}) for div_title in div_title_list if div_title is not None]
    tr_list_list = [table_item.find_all('tr') for table_item in table_list if table_item is not None]
    for tr_list in tr_list_list:
        for tr in tr_list:
            searchtext = re.compile(r'^Announce\sDate$')
            td_list = tr.find_all('td', text=searchtext)
            if len(td_list) > 0:
                return tr_list
    return None

def get_dividend_list(tr_list, stock_code):
    dividend_list = []
    ENDED_PATTERN = r'\d{4}/\d{2}'
    ended_re = re.compile(ENDED_PATTERN)
    DATE_PATTERN = r'\d{4}/\d{2}/\d{2}'
    date_re = re.compile(DATE_PATTERN)
    DATERANGE_PATTERN = r'\d{4}/\d{2}/\d{2}-\d{4}/\d{2}/\d{2}'
    daterange_re = re.compile(DATERANGE_PATTERN)
    HKD_PATTERN = r'HKD\s(\d*\.\d+|\d+)'
    hkd_re = re.compile(HKD_PATTERN)
    FX_PATTERN = r'(\w{3})\s(\d*\.\d+|\d+)'
    fx_re = re.compile(FX_PATTERN)
    for tr in tr_list or []:
        td_list = tr.find_all('td', {'class': 'mcFont'})
        if len(td_list) > 0:
            hkd_match = hkd_re.search(td_list[3].text)
            if hkd_match:
                amount_figure = float(hkd_match.group(1))
            else:
                fx_match = fx_re.search(td_list[3].text)
                if fx_match:
                    amount_figure = float(fx_match.group(2))
                    if fx_match.group(1) == 'USD':
                        amount_figure = amount_figure * 7.8
                    elif fx_match.group(1) == 'GBP':
                        amount_figure = amount_figure * 12
                    elif fx_match.group(1) == 'RMB':
                        amount_figure = amount_figure * 1.24
                else:
                    amount_figure = 0.0
            announce_date = datetime.strptime(td_list[0].text, '%Y/%m/%d')
            dividend_list.append(
                {
                    'stock_code': stock_code
                    , 'announce_date': announce_date
                    , 'ended_year': int(td_list[1].text[:4] if ended_re.match(td_list[1].text) else td_list[0].text[:4])
                    , 'ended_month': int(td_list[1].text[-2:] if ended_re.match(td_list[1].text) else td_list[0].text[5:7])
                    , 'dividend_event': td_list[2].text
                    , 'dividend_type': td_list[4].text
                    , 'dividend_amount': amount_figure
                    , 'dividend_value': td_list[3].text
                    , 'ex_date': datetime.strptime(td_list[5].text, '%Y/%m/%d') if date_re.match(td_list[5].text) else announce_date
                    , 'book_start_date': datetime.strptime(td_list[6].text[:10], '%Y/%m/%d') if daterange_re.match(td_list[6].text) else announce_date
                    , 'book_close_date': datetime.strptime(td_list[6].text[-10:], '%Y/%m/%d') if daterange_re.match(td_list[6].text) else announce_date
                    , 'payable_date': datetime.strptime(td_list[7].text, '%Y/%m/%d') if date_re.match(td_list[7].text) else announce_date
                }
            )
    return dividend_list

def download_dividend_hist(
    stock_code
    , proxy_flag=False
    , retry_time=3
    , retry_delay=10
    , timeout=PAGE_TIMEOUT
    ):
    logger = logutil.getLogger(__name__)

    dividend_list = []
    data_url = 'http://www.aastocks.com/en/stocks/analysis/dividend.aspx?symbol={}'.format(stock_code)
    for _ in range(retry_time):
        try:
            proxy_server = None
            if proxy_flag:
                proxy_server = webutil.get_random_proxy()
                logger.info('download via a proxy server: %s', proxy_server['ip'] + ':' + proxy_server['port'])

            response = webutil.create_get_request(url=data_url, proxy_server=proxy_server, timeout=timeout)
            if response.status_code != 200:
                response.raise_for_status()
            decoded_result = response.content.decode('utf-8', 'ignore')
            if len(decoded_result) <= 0:
                logger.error('Failed to retrieve content.')
                continue
            stock_soup = BeautifulSoup(decoded_result, 'html.parser')

            tr_list = get_dividend_trlist(stock_soup)
            dividend_list = get_dividend_list(tr_list, stock_code)
            break
        except Timeout:
            logger.error('socket timed out - URL %s', data_url)
            time.sleep(retry_delay)
        except RequestException as error:
            logger.error('Data not retrieved because %s\nURL: %s', error, data_url)
            time.sleep(retry_delay)
        except Exception as error:
            logger.error('Unexpected error of %s\nURL: %s', error, data_url)
            time.sleep(retry_delay)            
    else:
        logger.error('No response after %d retry.' % retry_time)
    return dividend_list

def download_dividend_hist_df(
    stock_code_list
    , max_workers=10
    , proxy_flag=False
    ):
    logger = logutil.getLogger(__name__)

    stock_dividend_hist_list = []   
    # Threads start and it takes quite a long time due to multiple network I/O
    logger.info('It starts to download stock dividend list. Please wait.')
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_stock_code = {
            executor.submit(download_dividend_hist, stock_code, proxy_flag): 
            stock_code for stock_code in stock_code_list
        }

    # Update thread status for monitoring
    for future in concurrent.futures.as_completed(future_to_stock_code):
        stock_dividend_hist_list.extend(future.result())
    
    logger.info('Downloading stock dividend list completed.')
    return pd.DataFrame(
        data=stock_dividend_hist_list
        )

### Run as a main program ###
if __name__ == '__main__':
    print(download_dividend_hist_df(stock_code_list=[stock_code for stock_code in sys.argv[1:]], max_workers=10, proxy_flag=True).to_csv(index=True, sep='\t'))
