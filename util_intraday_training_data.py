# UTIL_INTRADAY_TRAINING_DATA:  Downloads minute-by-minute intraday
# data from Alpaca, calculates the technical indicators, and writes it to CosmosDB
# Written by Dave Andrus on May 3, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import datetime                         # used for stock timestamps
import json                             # for manipulating array data
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from dateutil.parser import parse       # used to create date/time objects from strings
from datetime import timedelta
import datetime
import time                             # for manipulating time data
import pandas as pd                     # in-memory database capabilities
import talib as ta                      # lib to calcualted technical indicators
import alpaca_trade_api as tradeapi     # required for interaction with Alpaca
from pandas.io.json import json_normalize
import ouro_lib as ol
import argparse
from progress.bar import Bar

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('UTIL_INTRADAY_TRAINING_DATA logging enabled.')

# Setup command line arguements
parser = argparse.ArgumentParser(description="UTIL_INTRADAY_TRAINING_DATA:  Ingest intraday minute-by-minute trading data.")
parser.add_argument("--test", action="store_true", default=False, help="Script runs in test mode.  FALSE (Default) = update the entire universe of stock; TRUE = update a small subset of stocks to make testing quicker")
parser.add_argument("--start", help="Specify the first date that intraday data is gathered from.  The default is the last date/time stored in the training data database for that stock.")
parser.add_argument("--source", default='db', help="Whether to get data from Yahoo Finance or cached data in SQL Server.  (Default) = db; (Alternate) = yahoo")

cmdline = parser.parse_args()

# Connect to the daily_indicators container
dbconn = ol.cosdb('stockdata', 'training_data', '/ticker')

try:
    startdate = parse(cmdline.start)
    enddate = datetime.datetime.now()
except:
    startdate = datetime.datetime.now() - timedelta(days=90)
    enddate = datetime.datetime.now()

# Initialize the Alpaca API
alpaca = tradeapi.REST()

if not cmdline.test:
    # Calculate universe of stocks
    assets = alpaca.list_assets()
    slist = []
    for x in assets:
        if x.exchange == 'NASDAQ' or x.exchange=='NYSE':
            if x.tradable is True and x.status == 'active':
                slist.append(x.symbol)
                logging.debug('Adding ' + x.symbol + ' to the universe of stocks.')
            else:
                logging.debug('Skipping ' + x.symbol + ' because it is not tradable or active.')
        else:
            logging.debug('Skipping ' + x.symbol + ' because it is not in NYSE or NASDAQ.')
else:
    # Test with a small diverse set of stocks
    slist = ['CVS', 'DRI', 'EVR','FDEF', 'IBM', 'MPW', 'PPBI', 'PPL', 'PXD', 'QIWI', 'RL', 'TX', 'VZ']

logging.info('Universe of stocks created; ' + str(len(slist)) + ' stocks in the list.')

firsttime = 0
reqctr = 0
while startdate <= enddate:
    # set the start/end dates for this trade date
    s = str(startdate.strftime("%Y-%m-%d")) + ' 09:30'
    e = str(startdate.strftime("%Y-%m-%d")) + ' 16:30'

    retry = 0
    prgbar = Bar('  ' + startdate.strftime("%Y-%m-%d"), max=len(slist))

    for stock in slist:
        # get the minute-by-minute stock data for the current stock
        barset = None
        while barset == None:
            try:
                barset = alpaca.get_barset(stock, timeframe='1Min', limit=1000, start=pd.Timestamp(s, tz='America/New_York').isoformat(), end=pd.Timestamp(e, tz='America/New_York').isoformat())
                reqctr = reqctr + 1
            except Exception as ex:
                retry = retry + 1
                logging.warning('Could not get barset data; retry #' + str(retry) + ' in 10 seconds')
                print (ex)
                time.sleep(10)

        df = {}
        raw = {}

        # Pause 3 second every 20 stocks to avoid hitting the request limit on Alpaca
        if (reqctr/20)==int(reqctr/20):
            time.sleep(3)

        # Convert barset to usable dataframe
        # Note:  There is typically only one stock in the barset, but I guess there could be more
        for stock in barset.keys():
            bars = barset[stock]

            logging.debug('Converting bar data for ' + stock)
            data = {'id': str(uuid.uuid1(uuid.getnode())),
                    'ticker': [stock for bar in bars],
                    't': [bar.t for bar in bars],
                    'h': [bar.h for bar in bars],
                    'l': [bar.l for bar in bars],
                    'o': [bar.o for bar in bars],
                    'c': [bar.c for bar in bars],
                    'v': [bar.v for bar in bars]}

            # copy the raw data befoire doing anything else to it
            raw[stock] = pd.DataFrame(data)

            # Calculate technical indicators
            logging.debug('Calculating technical indicators ' + stock)
            df[stock] = ol.calcind(pd.DataFrame(data))

            # Calculate training parameters
            if isinstance(df[stock], pd.DataFrame) :
                if not df[stock].empty:
                    # find the highest and lowest price for this stock in this day
                    highidx = -1
                    lowidx = -1
                    high = -1
                    low = 99999999
                    for x in df[stock].index:
                        if df[stock].loc[x, 'h'] > high:
                            high = df[stock].loc[x, 'h']
                            highidx = x
                        if df[stock].loc[x, 'l'] < low:
                            low = df[stock].loc[x, 'l']
                            lowidx = x

                    # set the action for training
                    df[stock].loc[:, 'ACTION'] = 'None'
                    if lowidx < highidx:
                        # These are already in time series order; no need to parse the time
                        df[stock].loc[lowidx, 'ACTION'] = 'Buy'
                        df[stock].loc[highidx, 'ACTION'] = 'Sell'

                    # Calculate the best profit margin for the day
                    df[stock].loc[:, 'DAYMARGIN'] = (high-low)/low

                    # for x in json.loads(df[stock].to_json(orient='records')):
                    #     # write the data
                    #     try:
                    #         dbconn.create_item(body=x)
                    #     except Exception as ex:
                    #         logging.debug ('Problem creating document for ' + stock)
                    #         print(ex)

                    # Write it to a CSV file; I'll figure out the best place to host it later
                    if firsttime == 1:
                        try:
                            df[stock].to_csv('D:\OneDrive\Dev\SQL\ouro-training-data.csv', mode='a', header=False, index=False)
                            raw[stock].to_csv('D:\OneDrive\Dev\SQL\ouro-raw-training-data.csv', mode='a', header=False, index=False)
                        except Exception as ex:
                            logging.error('Unable to write data for ' + stock + ' on ' + s)
                            print(ex)
                    else:
                        try:
                            df[stock].to_csv('D:\OneDrive\Dev\SQL\ouro-training-data.csv', mode='w', header=True, index=False)
                            raw[stock].to_csv('D:\OneDrive\Dev\SQL\ouro-raw-training-data.csv', mode='a', header=True, index=False)
                            firsttime = 1
                        except Exception as ex:
                            logging.error('Unable to write data for ' + stock + ' on ' + s)
                            print(ex)
        prgbar.next()
    prgbar.finish()
    startdate = startdate + timedelta(days=1)



