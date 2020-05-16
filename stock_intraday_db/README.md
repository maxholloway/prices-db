This database tool allows us to make and maintain a database of stocks with their intraday prices. It works by pulling down data from TD Ameritrade's API, which allows us to pull up to 30 trading days of data into the past. Then we can schedule the file stock_intraday_db_update.py to run at intervals (with the interval length < 30 trading days) in order to pull down data to update the database. The following are required:

1. File called ```config.py``` that has the following lines:
    ```
    DATA_FILE_PATH = '' # absolute path to an already existing directory where stock files should be stored.
    LOG_FILE_PATH = '' # absolute to file that should store logs; this file does not need to exist before initializing the database
    STOCK_NAME_FILE_PATH = '' # absolute path to a file with names of stocks that are in the universe; must be formatted in the same way as SP500.txt
    api_key = '' # your TD Ameritrade API key 
    ```
    ```config.py```must be stored in this directory (namely stock_intraday_db).
2. Stock data directory. See (1) DATA_FILE_PATH for more details.
3. Stock name file. See (1) STOCK_NAME_FILE_PATH for more details.
