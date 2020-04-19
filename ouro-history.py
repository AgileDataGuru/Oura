# OURO-HISTORY:  Gets daily and 5m history data from Yahoo Finance
#   and stores it in a project database
# Written by Dave Andrus and Dwight Brookland on April 13, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import csv                              # for reading static data
import yfinance as yf                   # for stock list
import alpaca_trade_api as tradeapi     # required for trading
import datetime                         # used for stock timestamps

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

# Setup the API
api = tradeapi.REST()

# Calculate universe of stocks
assets = api.list_assets()
slist = []
for x in assets:
    if x.exchange == 'NASDAQ' or x.exchange=='NYSE':
        if x.tradable is True and x.status == 'active':
            slist.append(x.symbol)


for x in slist:
    # Get daily data for last 60 days
    startdate = datetime.datetime.now() + datetime.timedelta(days=-60)
    startdate_str = startdate.strftime('%Y-%m-%d')
    data = yf.download(x, startdate_str, interval='1d', prepost='False')

    # Get intra-day data for last 24 hours
    startdate = datetime.datetime.now() + datetime.timedelta(hours=-72)
    startdate_str = startdate.strftime('%Y-%m-%d')
    data = yf.download(x, startdate_str, interval='5m', prepost='False')
    print(x)
    print(data)


