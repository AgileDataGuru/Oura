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

while startdate <= enddate:
    s = str(startdate.strftime("%Y-%m-%d")) + ' 09:30'
    e = str(startdate.strftime("%Y-%m-%d")) + ' 16:30'

    print(s, e)

    for stock in slist:
        # Get the last 30 minutes of data for all the stocks in the list
        barset = alpaca.get_barset(stock, timeframe='1Min', limit=1000, start=pd.Timestamp(s, tz='America/New_York').isoformat(), end=pd.Timestamp(e, tz='America/New_York').isoformat())
        df = {}

        # Convert barset to usable dataframe
        for stock in barset.keys():
            bars = barset[stock]
            data = {'t': [bar.t for bar in bars],
                    'h': [bar.h for bar in bars],
                    'l': [bar.l for bar in bars],
                    'o': [bar.o for bar in bars],
                    'c': [bar.c for bar in bars],
                    'v': [bar.v for bar in bars]}
            df[stock] = ol.calcind(pd.DataFrame(data))
            print(df[stock])

startdate = startdate + timedelta(days=1)



