"""
Abstract class that handles the lower-level interaction with
the database.
"""

from abc import ABC, abstractmethod
import pandas as pd
import datetime as dt
from typing import Dict, List
import asyncio
import os
import sys
from pathlib import Path
db_path = Path(__file__).absolute().parent
sys.path.insert(1, str(db_path)) # path to the entire project

from db_config import __DTIME_FORMAT as DTIME_FORMAT

class DB_Base(ABC):

    def __init__(self, symbol_file_path: str, data_file_path: str, log_file_path: str):
        """
        Initialize database with the absolute paths to different files

        Arguments:
            symbol_file_path {str} -- Absolute path to file containing symbols.
            data_file_path {str} -- Absolute path to folder in which data files are to be stored.
            log_file_path {str} -- Absolute path to file containing runtime logs.
        """

        # Make all file paths relative to the current working directory
        self.__symb_file_path = DB_Base.__clean_path(symbol_file_path)
        self.__data_file_path = DB_Base.__clean_path(data_file_path)
        self.__log_file_path = DB_Base.__clean_path(log_file_path)

        return

    @staticmethod
    def __clean_path(abs_path: str) -> str:
        """
        Summary
            1. Make the full absolute path to the file
            2. Return the relative path from the current working directory to the file
        
        Arguments:
            abs_path {str} -- Absolute path of the file or dir.

        Returns:
            str -- Path to the file or dir, relative to the current 
                working directory (not necessarily the same as this 
                file's directory). This allows the path to be used in
                python File I/O methods.
        """
        
        start = str(Path(os.getcwd()).absolute())
        file_path_rel_cwd = os.path.relpath(abs_path, start) 

        return file_path_rel_cwd

    def __get_symbols(self) -> List[str]:
        stock_names = []
        with open(self.__symb_file_path, 'r') as f:
            line = f.readline().strip('\n')
            while line != '':
                stock_names.append(line)
                line = f.readline().strip('\n')

        return stock_names

    @staticmethod
    def __read_last_line(file_path: str) -> str:
        with open(file_path, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            
            val = f.read(1)
            while val == b'\n':
                f.seek(-2, os.SEEK_CUR)
                val = f.read(1)

            while val != b'\n':
                f.seek(-2, os.SEEK_CUR)
                val = f.read(1)

            return f.readline().decode().strip('\n')

    @staticmethod
    def __parse_date_from_line(line: str) -> dt.datetime:
        date_str = line.split(',')[0]
        return dt.datetime.strptime(date_str, DTIME_FORMAT)

    @staticmethod
    def __parse_last_dtime(file_path: str) -> dt.datetime:
        last_line = DB_Base.__read_last_line(file_path)
        return DB_Base.__parse_date_from_line(last_line)

    def log(self, msg: str):
        
        with open(self.__log_file_path, 'a') as lf:
            lf.write('{} {}\n'.format(dt.datetime.now().strftime(DTIME_FORMAT), msg))
        return

    async def update_async(self):
        """
        Summary:
            + Parse the current universe of symbols
            + Iterate over all symbols in universe
                + Parse the last line of the file to get the last date we have symbol data for
                + Make an API call using the get_data function
                + Append new data to the end of the file
        """

        # Update each of the relevant symbols
        self.log('STARTED UPDATE.')
        symbols = self.__get_symbols()
        for symbol in symbols:
            # Set up a minimum iteration time, so as to not overwhelm the api
            hourglass = asyncio.sleep(.5)

            # If the file already exists, update it; otherwise make a new file
            file_path = os.path.join(self.__data_file_path, '{}.csv'.format(symbol))
            if os.path.exists(file_path):
                 last_dtime = DB_Base.__parse_last_dtime(file_path)
                 start_dtime = last_dtime + dt.timedelta(microseconds=1) # barely nudge forward in time to avoid pulling data that's already in the csv
                 data = self.get_data(symbol, start=start_dtime, end=dt.datetime.now(), new_data=False)
                 data.to_csv(file_path, mode='a', header=False)
                 self.log('APPENDED {} LINES TO {}.'.format(len(data), symbol))
            else:
                data = self.get_data(symbol, start=None, end=None, new_data=True)
                data.to_csv(file_path, mode='w', header=True)
                self.log('CREATED: {}'.format(symbol))
            

            # ph = tda.PriceHistory(api_key)

            # try: # try appending to an already existing csv
            #     last_dtime = __parse_last_dtime(file_path) # will raise FileNotFoundError if the stock is not already in the database
            #     start_dtime = last_dtime + dt.timedelta(microseconds=1) # barely nudge forward in time to avoid pulling data that's already in the csv
            #     recent_pricing: pd.DataFrame = ph.minute_data(symbol, start_dtime, need_extended_hours=True)
            #     recent_pricing.to_csv(file_path, mode='a', header=False, sep=DATA_DELIMITER)
            #     log('APPEND {} LINES TO {}'.format(len(recent_pricing), symbol), self.__log_file_path)
            # except FileNotFoundError: # no csv exists for this symbol yet, so write a new one
            #     recent_pricing: pd.DataFrame = ph.max_minute_data(symbol)
            #     recent_pricing.to_csv(file_path, mode='w', header=True, sep=DATA_DELIMITER)
            #     log('CREATE: {}'.format(symbol), self.__log_file_path)
            # except tda.EmptyApiRequest:
            #     log('NO CHANGE: {}'.format(symbol), self.__log_file_path)
            # except Exception as ex:
            #     log('ERROR ON {}:\n{}'.format(symbol, ex), self.__log_file_path)

            await hourglass

        self.log('FINISHED UPDATE.')
        return

    def update(self):
        """See 'update_async' for details.
        """
        asyncio.run(self.update_async())
        return

    @abstractmethod
    def get_data(self, symbol: str, start: dt.datetime, end: dt.datetime, new_data: bool) -> pd.DataFrame:
        """This function will perform some action to get data. This function must return
        a pandas DataFrame, with a datetime index in ascending order (old-to-new), and data in the columns.

        Arguments:
            symbol {str}
            start {dt.datetime} -- Start time for the data to be received; if None, then assume new_data=True
            end {dt.datetime} -- End time for the data to be received; if None, then assume new_data=True
            new_data {bool} -- True if start is unknown, but the goal is to get as much historical data as possible
                                If True, then this will overwrite the currently available data. This flag is mandatory,
                                so as to avoid accidentally overwriting files if start and end are None.

        Returns:
            pd.DataFrame -- Data for the given symbol, with datetime index
        """
        
        pass


def parse_args(symbol_file_path, data_file_path_default, log_file_path_default):
    S = 'symbol_file_path'
    D = 'data_file_path'
    L = 'log_file_path'

    default_args = {S:symbol_file_path, D:data_file_path_default, L:log_file_path_default}

    args = sys.argv[1:]
    if len(args) == 0:
        return default_args
    elif True not in [arg[0]=='-' for arg in args]: # there are no keyword arguments
        keywords = S, D, L # keywords in the expected order
        for i in range(len(args)):
            default_args[keywords[i]] = args[i] # match each arg with its keyword
        return default_args 
    else:
        assert len(args)%2 == 0, 'Bad input arguments; must be even number of arguments if using keywords.'
        
        pairs = [[args[i], args[i+1]] for i in range(0, len(args), 2)]
        assert (False not in ['-' == pair[0][0] for pair in pairs]), 'There must either be 0 keyword args, or all args must be keyword args.'

        kw_dict = {'-s': S, '-d': D, '-l': L}

        for pair in pairs:
            assert (pair[0] in kw_dict.keys()), 'Keyword "{}" is not valid.'.format(pair[0])

        for pair in pairs:
            default_args[kw_dict[pair[0]]] = pair[1]

        return default_args

    