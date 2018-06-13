'''
Module to retrieve company information from Bloomberg
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

def get_hk_bloomberg_code(stock_number):
    '''
    This function is to convert HKex Stock ID in Bloomberg format
    '''
    return '{}:HK'.format(stock_number)

def download_bloomberg_quote(
    stock_code
    , proxy_flag=False
    , retry_time=3
    , retry_delay=10
    , timeout=PAGE_TIMEOUT
    ):
    logger = logutil.getLogger(__name__)

    data_dict = {'stock_code':stock_code}
    data_url = 'https://www.bloomberg.com/quote/{}'.format(stock_code)
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

            # Open Price
            open_price_section = stock_soup.find('section', {"class": 'dataBox openprice numeric'})
            open_price = ''
            if open_price_section is not None:
                open_price_div = open_price_section.findNext('div')
                if open_price_div is not None:
                    open_price = open_price_div.get_text()
            # Previous Close
            prev_close_section = stock_soup.find('section', {"class": 'dataBox previousclosingpriceonetradingdayago numeric'})
            prev_close = ''
            if prev_close_section is not None:
                prev_close_div = prev_close_section.findNext('div')
                if prev_close_div is not None:
                    prev_close = prev_close_div.get_text()
            # Volume
            volume_section = stock_soup.find('section', {"class": 'dataBox volume numeric'})
            volume = ''
            if volume_section is not None:
                volume_div = volume_section.findNext('div')
                if volume_div is not None:
                    volume = volume_div.get_text()                
            # Market Cap
            marketcap_section = stock_soup.find('section', {"class": 'dataBox marketcap numeric'})
            marketcap = ''
            if marketcap_section is not None:
                marketcap_div = marketcap_section.findNext('div')
                if marketcap_div is not None:
                    marketcap = marketcap_div.get_text()                
            # Range one day
            rangeoneday_section = stock_soup.find('section', {"class": 'dataBox rangeoneday'})
            rangeoneday = ''
            if rangeoneday_section is not None:
                rangeoneday_div = rangeoneday_section.findNext('div')
                if rangeoneday_div is not None:
                    rangeoneday = rangeoneday_div.get_text()
            # Range 52 weeks
            range52weeks_section = stock_soup.find('section', {"class": 'dataBox range52weeks'})
            range52weeks = ''
            if range52weeks_section is not None:
                range52weeks_div = range52weeks_section.findNext('div')
                if range52weeks_div is not None:
                    range52weeks = range52weeks_div.get_text()

            # Industry Category
            industry_div = stock_soup.find('div', {"class": 'industry labelText__6f58d7c0'})
            industry = ''
            if industry_div is not None:
                industry = industry_div.get_text()
            # Sector Category
            sector_div = stock_soup.find('div', {"class": 'sector labelText__6f58d7c0'})
            sector = ''
            if sector_div is not None:
                sector = sector_div.get_text()
            # Nominal Price
            nominal_price_div = stock_soup.find('span', {"class": 'priceText__1853e8a5'})
            nominal_price = ''
            if nominal_price_div is not None:
                nominal_price = nominal_price_div.get_text()

            data_dict.update({
                'prev_close': prev_close
                , 'open_price': open_price
                , 'nominal_price': nominal_price
                , 'volume': volume
                , 'marketcap': marketcap
                , 'rangeoneday': rangeoneday
                , 'range52weeks': range52weeks
                , 'industry': industry
                , 'sector': sector
            })

            div_list = stock_soup.findAll('div', {"class": 'rowListItemWrap__4121c877'})
            for div in div_list or []:
                key_str = div.findNext('span').get_text()
                val_str = div.find('span', {"class": 'fieldValue__2d582aa7'}).get_text()
                data_dict.update(
                    {
                        key_str: val_str
                    }
                )

            next_announce_date_span = stock_soup.find('span', {'class': 'nextAnnouncementDate__0dd98bb1'})
            if next_announce_date_span is not None:
                next_announce_date = next_announce_date_span.get_text()
                data_dict.update({'next_announce_date': next_announce_date})
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
    return data_dict

def download_bloomberg_df(
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
            executor.submit(download_bloomberg_quote, stock_code, proxy_flag): 
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
    print(download_bloomberg_df(stock_code_list=[stock_code for stock_code in sys.argv[1:]], max_workers=1, proxy_flag=True).to_csv(index=True, sep='\t'))