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

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('UTIL_INTRADAY_TRAINING_DATA logging enabled.')

# Setup command line arguements
parser = argparse.ArgumentParser(description="UTIL_INTRADAY_TRAINING_DATA:  Ingest intraday minute-by-minute trading data.")
parser.add_argument("--test", action="store_true", default=False, help="Script runs in test mode.  FALSE (Default) = update the entire universe of stock; TRUE = update a small subset of stocks to make testing quicker")
parser.add_argument("--start", help="Specify the first date that intraday data is gathered from.  The default is the last date/time stored in the training data database for that stock.")
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

while startdate <= enddate:
    # set the start/end dates for this trade date
    s = str(startdate.strftime("%Y-%m-%d")) + ' 09:30'
    e = str(startdate.strftime("%Y-%m-%d")) + ' 16:30'

    logging.info ('Getting training data on ' + s)

    for stock in slist:
        # Get the last 30 minutes of data for all the stocks in the list
        barset = alpaca.get_barset(stock, timeframe='1Min', limit=1000, start=pd.Timestamp(s, tz='America/New_York').isoformat(), end=pd.Timestamp(e, tz='America/New_York').isoformat())
        df = {}

        # Convert barset to usable dataframe
        for stock in barset.keys():
            logging.info('Starting training data for ' + stock)
            bars = barset[stock]

            logging.debug('Converting bar data for ' + stock)
            data = {'t': [bar.t for bar in bars],
                    'h': [bar.h for bar in bars],
                    'l': [bar.l for bar in bars],
                    'o': [bar.o for bar in bars],
                    'c': [bar.c for bar in bars],
                    'v': [bar.v for bar in bars]}

            logging.debug('Calculating technical indicators ' + stock)
            df[stock] = ol.calcind(pd.DataFrame(data))
            if isinstance(df[stock], pd.DataFrame) :
                if not df[stock].empty:
                    df[stock].loc[:, 'ticker'] = stock

                    # find the highest and lowest price for this stock in this day
                    highidx = -1
                    lowidx = -1
                    high = -1
                    low = 99999999
                    for x in df[stock].index:
                        df[stock].loc[x, 'id'] = str(uuid.uuid1(uuid.getnode()))
                        if df[stock].loc[x, 'h'] > high:
                            high = df[stock].loc[x, 'h']
                            highidx = x
                        if df[stock].loc[x, 'l'] < low:
                            low = df[stock].loc[x, 'l']
                            lowidx = x

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
                            df[stock].to_csv('D:\OneDrive\Dev\SQL\ouro-training-data.csv', mode='a', header=False)
                        except:
                            logging.error('Unable to write data for ' + stock + ' on ' + startdate)
                    else:
                        try:
                            df[stock].to_csv('D:\OneDrive\Dev\SQL\ouro-training-data.csv', mode='w', header=True)
                            firsttime = 1
                        except:
                            logging.error('Unable to write data for ' + stock + ' on ' + startdate)

    logging.info('Completed training data for ' + stock)
    startdate = startdate + timedelta(days=1)



