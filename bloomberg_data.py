'''
Module to retrieve company information from Bloomberg
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

# modules for concurrency
import concurrent.futures
import time
from datetime import datetime

# modules for handling files
import base64
import io

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

def download_bloomberg_quote(stock_id, retry_time=3):
    logger = getLogger(__name__)

    data_dict = {'stock_id':stock_id}
    data_url = 'https://www.bloomberg.com/quote/{}:HK'.format(stock_id)
    for _ in range(retry_time):
        try:
            with urllib.request.urlopen(create_web_response(data_url)) as stock_page:
                stock_soup = BeautifulSoup(stock_page.read().decode('utf-8', 'ignore'), 'html.parser')

                open_price = stock_soup.find('section', {"class": 'dataBox openprice numeric'}).findNext('div').get_text()
                prev_close = stock_soup.find('section', {"class": 'dataBox previousclosingpriceonetradingdayago numeric'}).findNext('div').get_text()
                volume = stock_soup.find('section', {"class": 'dataBox volume numeric'}).findNext('div').get_text()
                marketcap = stock_soup.find('section', {"class": 'dataBox marketcap numeric'}).findNext('div').get_text()
                rangeoneday = stock_soup.find('section', {"class": 'dataBox rangeoneday'}).findNext('div').get_text()
                range52weeks = stock_soup.find('section', {"class": 'dataBox range52weeks'}).findNext('div').get_text()
                industry = stock_soup.find('div', {"class": 'industry labelText__6f58d7c0'}).get_text()
                sector = stock_soup.find('div', {"class": 'sector labelText__6f58d7c0'}).get_text()
                nominal_price = stock_soup.find('span', {"class": 'priceText__1853e8a5'}).get_text()

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
                for div in div_list:
                    key_str = div.findNext('span').get_text()
                    val_str = div.find('span', {"class": 'fieldValue__2d582aa7'}).get_text()
                    data_dict.update(
                        {
                            key_str: val_str
                        }
                    )

                next_announce_date = stock_soup.find('span', {'class': 'nextAnnouncementDate__0dd98bb1'}).get_text()
                data_dict.update({'next_announce_date': next_announce_date})
                break
        except (HTTPError, URLError) as error:
            logger.error('Data not retrieved because %s\nURL: %s', error, data_url)
            time.sleep(3 * retry_time)
        except timeout:
            logger.error('socket timed out - URL %s', data_url)
            time.sleep(3 * retry_time)
    else:
        logger.error('No response after %d retry.' % retry_time)
    return data_dict

def download_bloomberg_df(stock_id_list):
    logger = getLogger(__name__)

    # Concurrent download
    MAX_WORKDERS = 10
    stock_quote_list = []
    
    # Threads start and it takes quite a long time due to multiple network I/O
    logger.info('It starts to download stock quotes. Please wait.')
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKDERS) as executor:
        future_to_stock_id = {
            executor.submit(download_bloomberg_quote, stock_id): 
            stock_id for stock_id in stock_id_list
        }

    # Update thread status for monitoring
    for future in concurrent.futures.as_completed(future_to_stock_id):
        stock_quote_list.append(future.result())
    
    logger.info('Downloading stock quotes completed.')
    return pd.DataFrame(
        data=stock_quote_list
        ).set_index('stock_id', append=False)
