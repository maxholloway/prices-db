import warnings
import datetime
from config import api_key
from typing import Dict, Union, List, Callable, Any
import requests
import pandas as pd
import asyncio

def datetime_to_ms_epoch(dtime: datetime.datetime) -> str:
    return str(int(dtime.timestamp())*1000)

def ms_epoch_to_datetime(epoch: str) -> datetime.datetime:
    epoch = float(epoch)/1000
    return datetime.datetime.fromtimestamp(epoch)


class PriceHistoryRequest:
    def __init__(self):
        self.api_key = -1
        self.start_date = ''
        self.end_date = ''
        self.period_type = '' # not necessary if using start and end date
        self.period = '' # not necessary if using start and end date
        self.frequency_type = -1
        self.frequency = -1
        self.need_extended_hours = 'true'
        self.symbol = ''
    
    @property
    def api_key(self):
        if self.__api_key == -1:
            warnings.warn('Property "{}" not defined yet.'.format(self.__api_key))
        return self.__api_key
    @api_key.setter
    def api_key(self, val: str):
        self.__api_key = val
    
    @property
    def start_date(self):
        if self.__start_date == -1:
            warnings.warn('Property "{}" not defined yet.'.format(self.__start_date))
        return self.__start_date
    @start_date.setter
    def start_date(self, start_date: Union[datetime.datetime, str]):
        if start_date == '':
            self.__start_date = start_date
        elif type(start_date) == datetime.datetime:
            # next_market_date = next_market_day(start_date)
            self.__start_date = datetime_to_ms_epoch(start_date)#next_market_date)
        else:
            raise Exception('Invalid input.')
        
        
    
    @property
    def end_date(self):
        if self.__end_date == -1:
            warnings.warn('Property "{}" not defined yet.'.format(self.__end_date))
        return self.__end_date
    @end_date.setter
    def end_date(self, end_date: datetime.datetime):
        if end_date == '':
            self.__end_date = end_date
        elif type(end_date) == datetime.datetime:
            # next_market_date = next_market_day(end_date)
            self.__end_date = datetime_to_ms_epoch(end_date)#next_market_date)
        else:
            raise Exception('Invalid input.')

    @property
    def period_type(self):
        return self.__period_type
    @period_type.setter
    def period_type(self, period_type):
        self.__period_type = period_type
    
    @property
    def period(self):
        return self.__period
    @period.setter
    def period(self, period):
        self.__period = period 

    @property
    def frequency_type(self):
        return self.__frequency_type
    @frequency_type.setter
    def frequency_type(self, frequency_type):
        self.__frequency_type = frequency_type

    @property
    def frequency(self):
        return self.__frequency
    @frequency.setter
    def frequency(self, frequency):
        self.__frequency = frequency

    @property
    def need_extended_hours(self):
        return self.__need_extended_hours
    @need_extended_hours.setter
    def need_extended_hours(self, need_extended_hours):
        self.__need_extended_hours = need_extended_hours
    
    @property
    def symbol(self):
        return self.__symbol
    @symbol.setter
    def symbol(self, symbol):
        self.__symbol = symbol


class BadApiRequest(Exception):
    pass


class EmptyApiRequest(Exception):
    pass


class PriceHistory:
    __MAX_INTRADAY_BACKWARD_DAYS = 30 # number of calendar days that the api lets us look back for intraday data
    __REQ_PER_SEC_CAP = 100 # maximum number of requests the API can process per second

    def __init__(self, api_key):
        self.api_key = api_key
        return
    
    def make_api_call(self, phr: PriceHistoryRequest) -> Dict:
        endpoint = r'https://api.tdameritrade.com/v1/marketdata/{}/pricehistory'.format(phr.symbol)

        payload = {
            'apikey': phr.api_key,
            'frequencyType': phr.frequency_type,
            'frequency': phr.frequency,
            'needExtendedHoursData': phr.need_extended_hours,
        }
        
        # Since startDate, endDate, periodType, and period are not necessarily in all of the messages,
        # we only add the non-null ones.

        if phr.start_date != '':
            payload['startDate'] = phr.start_date
        if phr.end_date != '':
            payload['endDate'] = phr.end_date

        if phr.period_type != '': 
            payload['periodType'] = phr.period_type
        if phr.period != '': 
            payload['period'] = phr.period

        content = requests.get(url=endpoint, params=payload).json()
        if ('empty' in content.keys() and content['empty'] == True):
            raise EmptyApiRequest()

        elif ('error' in content.keys()):
            print(content)
            raise BadApiRequest()
        return content
    
    def __json_dict_to_df(self, json_dict: Dict) -> pd.DataFrame:
        data_df = pd.DataFrame(json_dict['candles']).set_index('datetime')
        data_df.index = pd.Series([ms_epoch_to_datetime(ep) for ep in data_df.index], name='datetime')
        return data_df
    
    async def __get_api_call_tasks(self, symbols: List[str], async_func: Callable, params: Dict):
        tasks = []
        for i, symbol in enumerate(symbols):
            if (i > 0) and  (i % PriceHistory.__REQ_PER_SEC_CAP == 0): # slow down so as to not break the API limits
                await asyncio.sleep(1)

            tasks.append(asyncio.create_task(async_func(symbol=symbol, **params)))
            # tasks.append(async_func(symbol=symbol, **params))
            
        all_api_tasks = asyncio.gather(*tasks)
        return all_api_tasks

    def multi_security_pull_async(self, symbols: List[str], async_func: Callable, params: Dict) -> Dict[str, Any]:
        all_api_tasks = self.__get_api_call_tasks(symbols, async_func, params)

        # block until the api requests have finished processing
        results = asyncio.run(all_api_tasks).result()

        mapped_results = {symbols[i] : results[i] for i in range(len(symbols))}

        return mapped_results

    def multi_security_pull(self, symbols: List[str], func: Callable, params: Dict) -> Dict[str, Any]:
        """Use specific parameters to call a function on different symbols. Intended for use on *_data methods.

        Arguments:
            symbols {List[str]} -- List of symbols to look up
            func {Callable} -- The function to be called with params and each separate symbol
            params {Dict} -- The parameters to be used on each function call

        Returns:
            List -- Dict of all output from 
        """

        return {symbol: func(symbol=symbol, **params) for symbol in symbols}
    
    def _minute_data_all(self, symbol, start_date: datetime.datetime, end_date: datetime.datetime = None, need_extended_hours: bool=False) -> Dict:
        phr = PriceHistoryRequest()
        phr.symbol = symbol
        phr.api_key = self.api_key
        phr.frequency_type = 'minute'
        phr.frequency = '1'
        phr.start_date = start_date
        phr.end_date = end_date or datetime.datetime.now()
        phr.need_extended_hours = 'true' if need_extended_hours else 'false'
        
        data = self.make_api_call(phr)
        return data
    
    async def _minute_data_all_async(self, symbol, start_date: datetime.datetime, end_date: datetime.datetime = None, need_extended_hours: bool=False):
        return self._minute_data_all(symbol, start_date, end_date, need_extended_hours)
    
    def minute_data(self, symbol, start_date: datetime.datetime, end_date: datetime.datetime = None, need_extended_hours: bool=False) -> pd.DataFrame:
        json_dict_data = self._minute_data_all(symbol, start_date, end_date, need_extended_hours)
        price_data = self.__json_dict_to_df(json_dict_data)

        
        return price_data[price_data.index >= start_date]

    async def minute_data_async(self, symbol, start_date: datetime.datetime, end_date: datetime.datetime = None, need_extended_hours: bool=False):
        return self.minute_data(symbol, start_date, end_date, need_extended_hours)

    def max_minute_data(self, symbol):
        # go back beyond the API's allowed start
        pre_max_period = datetime.timedelta(days=60)
        start = datetime.datetime.now() - pre_max_period

        return self.minute_data(symbol, start, need_extended_hours=True)

    async def max_minute_data_async(self, symbol):
        return self.max_minute_data(symbol)
    
    def _day_data_all(self, symbol, start_date: datetime.datetime, end_date: datetime.datetime = None, need_extended_hours: bool=False) -> Dict:
        phr = PriceHistoryRequest()
        phr.symbol = symbol
        phr.api_key = self.api_key
        phr.period_type = 'month'
        # phr.period = '6'
        phr.frequency_type = 'daily'
        phr.frequency = '1'
        phr.start_date = start_date
        phr.end_date = end_date or datetime.datetime.now()
        phr.need_extended_hours = 'true' if need_extended_hours else 'false'
        data = self.make_api_call(phr)
        return data

    def day_data(self, symbol, start_date: datetime.datetime, end_date: datetime.datetime = None, need_extended_hours: bool=False) -> pd.DataFrame:
        json_dict_data = self._day_data_all(symbol, start_date, end_date, need_extended_hours)
        return self.__json_dict_to_df(json_dict_data)

if __name__ == '__main__':
    start = datetime.datetime(year=2020, month=2, day=2)
    end = None#datetime.datetime(year=2020, month=3, day=1)
    price_hist = PriceHistory(api_key)
    print(price_hist.minute_data('AAPL', start, end))
    # print(price_hist.day_data('LEH', start, end))
    print(price_hist.max_minute_data('AAPL'))

    '''
    # Sync. vs. Async. Time Test
    tech_symbs = ['WORK', 'FB', 'GOOGL', 'MSFT']#, 'AAPL', 'AMZN', 'NFLX', 'GOOG', 'BABA', 'INTC', 'NVDA', 'CSCO', 'CRM', 'ACN', 'IBM', 'TXN']
    params = {}

    # Run synchronously
    start_sync = datetime.datetime.now()
    print('STARTING SYNCHRONOUS TEST AT: {}'.format(start_sync))
    sync_res = price_hist.multi_security_pull(tech_symbs, price_hist.max_minute_data, params)
    end_sync = datetime.datetime.now()
    num_sync_test_seconds = (end_sync-start_sync).seconds
    print('ENDING SYNCHRONOUS TEST AT: {}'.format(end_sync))
    print('SYNCHRONOUS TEST TOOK A TOTAL OF {} SECONDS, AVERAGING {} SECONDS PER STOCK'.format(num_sync_test_seconds, num_sync_test_seconds/len(tech_symbs)))
    
    # Run asynchronously
    start_async = datetime.datetime.now()
    print('STARTING ASYNC TEST AT {}'.format(start))
    async_res = price_hist.multi_security_pull_async(tech_symbs, price_hist.max_minute_data_async, params)
    end_async = datetime.datetime.now()
    num_async_test_seconds = (end_async-start_async).seconds
    print('ENDING ASYNC TEST AT {}'.format(end_async))
    print('ASYNC TEST TOOK A TOTAL OF {} SECONDS, AVERAGING {} SECONDS PER STOCK'.format(num_async_test_seconds, num_async_test_seconds/len(tech_symbs)))

    print('IT TOOK {} SECONDS FEWER WITH ASYNC THAN WITH SYNC DATA PULL METHOD.'.format(num_sync_test_seconds - num_async_test_seconds))
    '''
