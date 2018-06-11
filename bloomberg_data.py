'''
Module to retrieve company information from Bloomberg
'''
# core modules
import re
import logutil
import sys

# modules for downloading and URL
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
from socket import timeout
import webutil

# modules for Data Science
import pandas as pd
import excelutil

# modules for concurrency
import concurrent.futures
import time
from datetime import datetime

def download_bloomberg_quote(
    stock_id
    , proxy_flag=False
    , retry_time=3
    , retry_delay=10
    ):
    logger = logutil.getLogger(__name__)

    data_dict = {'stock_id':stock_id}
    data_url = 'https://www.bloomberg.com/quote/{}:HK'.format(stock_id)
    for _ in range(retry_time):
        try:
            proxy_server = None
            if proxy_flag:
                proxy_server = webutil.get_random_proxy()
                logger.info('download via a proxy server: %s', proxy_server['ip'] + ':' + proxy_server['port'])

            web_request = webutil.create_web_request(url=data_url, proxy_server=proxy_server)
            print(web_request.header_items())
            with urlopen(web_request) as stock_page:
                decoded_result = stock_page.read().decode('utf-8', 'ignore')
                if len(decoded_result) <= 0:
                    logger.error('Failed to retrieve content.')
                    continue
                stock_soup = BeautifulSoup(decoded_result, 'html.parser')

                open_price = stock_soup.find('section', {"class": 'dataBox openprice numeric'}).findNext('div').get_text()
                prev_close = stock_soup.find('section', {"class": 'dataBox previousclosingpriceonetradingdayago numeric'}).findNext('div').get_text()
                volume = stock_soup.find('section', {"class": 'dataBox volume numeric'}).findNext('div').get_text()
                marketcap = stock_soup.find('section', {"class": 'dataBox marketcap numeric'}).findNext('div').get_text()
                rangeoneday = stock_soup.find('section', {"class": 'dataBox rangeoneday'}).findNext('div').get_text()
                range52weeks = stock_soup.find('section', {"class": 'dataBox range52weeks'}).findNext('div').get_text()

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
            time.sleep(retry_delay)
        except timeout:
            logger.error('socket timed out - URL %s', data_url)
            time.sleep(retry_delay)
    else:
        logger.error('No response after %d retry.' % retry_time)
    return data_dict

def download_bloomberg_df(
    stock_id_list
    , max_workers=10
    , proxy_flag=False
    ):
    logger = logutil.getLogger(__name__)

    stock_quote_list = []   
    # Threads start and it takes quite a long time due to multiple network I/O
    logger.info('It starts to download stock quotes. Please wait.')
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_stock_id = {
            executor.submit(download_bloomberg_quote, stock_id, proxy_flag): 
            stock_id for stock_id in stock_id_list
        }

    # Update thread status for monitoring
    for future in concurrent.futures.as_completed(future_to_stock_id):
        stock_quote_list.append(future.result())
    
    logger.info('Downloading stock quotes completed.')
    return pd.DataFrame(
        data=stock_quote_list
        ).set_index('stock_id', append=False)

### Run as a main program ###
if __name__ == '__main__':
    print(download_bloomberg_df(stock_id_list=[stock_id for stock_id in sys.argv[1:]], max_workers=1, proxy_flag=False).to_csv(index=True, sep='\t'))