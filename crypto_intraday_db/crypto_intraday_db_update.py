import os
import sys
from pathlib import Path
import warnings
import datetime as dt
import pandas as pd
import requests

# import DB_Base from other directory
proj_path = Path(__file__).absolute().parent.parent
sys.path.insert(1, os.path.join(str(proj_path))) # path to the entire project
from db.base import DB_Base, parse_args

from config import SYMBOL_NAME_FILE_PATH, DATA_DIR_PATH, LOG_FILE_PATH

class CryptoIntradayDB(DB_Base):
    __MAX_LOOKBACK_MINUTES = 2000 # determined by the API's constraints
    
    def __get_request_data(symbol, comparison_symbol, limit, aggregate):
        assert limit <= CryptoIntradayDB.__MAX_LOOKBACK_MINUTES, 'Limit was too large for the API constraints.'
        url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}'\
                    .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
        page = requests.get(url)
        data = pd.DataFrame(page.json()['Data'])
        return data

    def get_data(self, symbol: str, start: dt.datetime, end: dt.datetime, new_data: bool) -> pd.DataFrame:
        
        if new_data: # go back as far in time as possible
            limit = CryptoIntradayDB.__MAX_LOOKBACK_MINUTES
        else: # go back in time only to the extent necessary
            assert start < end, 'Invalid start and end arguments; try setting new_data to True if the intention is to get as much data as possible.'

            buffer = dt.timedelta(seconds=10)
            min_time = (dt.datetime.now() - dt.timedelta(CryptoIntradayDB.__MAX_LOOKBACK_MINUTES)) + buffer
            
            if start <= min_time:
                warnings.warn('Start time is out of bounds with the minimum start time. Fetching data up to maximum lookback days.')
            
            
            minutes = lambda td: (td.seconds)//60 # convert from timedelta to minutes
            limit = min(CryptoIntradayDB.__MAX_LOOKBACK_MINUTES, minutes((dt.datetime.now()-start)) )
            
        # Get basic data from the request
        data = CryptoIntradayDB.__get_request_data(symbol, 'USD', limit, 1)
        
        if len(data) == 0:
            return data # don't worry about formatting, since there's no data to format

        # Format data
        data.index = pd.Series([dt.datetime.fromtimestamp(t) for t in data['time']], name='datetime')
        
        data.drop(columns=['time'], inplace=True)
        data = data[['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']]
        
        # If specific time range is in mind, filter to only include desired datetime
        if not new_data: # filter to only include upto the end date
            inrange = lambda dt_series: (dt_series >= start) & (dt_series <= end)
            data = data[inrange(data.index)]
            
        return data


if __name__ == "__main__":
    '''
    Arguments (in order):
        -s [symbol_file_path]
        -d [data_file_path]
        -l [log_file_path]
    '''
    args_dict = parse_args(SYMBOL_NAME_FILE_PATH, DATA_DIR_PATH, LOG_FILE_PATH)
    CryptoIntradayDB(**args_dict).update()