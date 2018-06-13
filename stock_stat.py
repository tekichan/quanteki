'''
A standalone program to collect and generate the result of Stock Statistics
'''
# modules of Data Source
import hkex_list
import bloomberg_data
import yahoo_fin

# modules for Data Science
import pandas as pd
import excelutil

# modules for date time
from datetime import datetime

### Run as a main program ###
if __name__ == '__main__':
    # Get Stock List, key is Stock ID (int)
    stock_df = hkex_list.download_stocks_df(proxy_flag=True)
    # Get Stock Statistics from Bloomberg, key is Stock Code (string)
    # stock_bb = bloomberg_data.download_bloomberg_df(
    #    stock_code_list=[bloomberg_data.get_hk_bloomberg_code(stock_id) for stock_id in stock_df.index]
    #    , max_workers=20
    #    , proxy_flag=True
    #    )
    # stock_bb['stock_id'] = stock_bb.index.str.split(':').str.get(0).astype(int).values
    # stock_bb = stock_bb.set_index('stock_id', append=False)
    # Get Stock Statistics from Yahoo! Finance, key is Stock Code (string)
    stock_yf = yahoo_fin.get_stock_quote_df(
        stock_code_list=[yahoo_fin.get_hk_yahoo_code(stock_id) for stock_id in stock_df.index]
        , max_workers=20
        , proxy_flag=True
        )
    stock_yf['stock_id'] = stock_yf.index.str.split('.').str.get(0).astype(int).values
    stock_yf = stock_yf.set_index('stock_id', append=False)
    # Concatenate three DataFrame objects
    # stock_stat = pd.concat([stock_df, stock_bb, stock_yf], axis=1)
    stock_stat = pd.concat([stock_df, stock_yf], axis=1)
    # Save the resulted DataFrame into a local Excel file
    excelutil.save_excel_file(
        stock_stat
        , 'stock_stat.' + datetime.now().strftime(yahoo_fin.YAHOO_DATE_FORMAT) + '.xlsx'
        , 'stock_stat'
        )
