import os
import sys
from pathlib import Path
import datetime as dt
import pandas as pd
proj_path = Path(__file__).absolute().parent.parent
sys.path.insert(1, os.path.join(str(proj_path))) # path to the entire project
from db.base import DB_Base, parse_args
from ameritrade_utils import PriceHistory
from config import api_key, STOCK_NAME_FILE_PATH, DATA_FILE_PATH, LOG_FILE_PATH


class StockIntradayDB(DB_Base):
    # Implement method for getting data
    def get_data(self, symbol: str, start: dt.datetime, end: dt.datetime, new_data: bool) -> pd.DataFrame:
        ph = PriceHistory(api_key)
        if new_data: # do a max-data-pull
            return ph.max_minute_data(symbol)
        else: # only get necessary data
            return ph.minute_data(symbol, start, end)
        return

if __name__ == '__main__':
    '''
    Arguments (in order):
        -s [symbol_file_path]
        -d [data_file_path]
        -l [log_file_path]
    '''
    args_dict = parse_args(STOCK_NAME_FILE_PATH, DATA_FILE_PATH, LOG_FILE_PATH)
    StockIntradayDB(**args_dict).update()

