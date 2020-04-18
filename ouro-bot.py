# OURO:  An automated day-trading bot
# Written by Dave Andrus and Dwight Brookland on April 13, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import csv                              # for reading static data
import alpaca_trade_api as tradeapi     # required for trading
import json                             # for converting to a usable data format
import datetime                         # used for stock timestamps
from dateutil.parser import parse       # used to create date/time objects from strings
import time                             # used for pausing
import yfinance                         # for stock list

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

def GetLastTrx(stocks, orders):
    # Searches the stocks list and finds the last sell order in the orders
    # If there are no orders, uses one minute after midnight.
    # This is used to limit how far back to search for a buy signal.
    # We never want to make buys today based on yesterday's data
    # and we probably never want to make buys based ond signals before
    # the most recent sell order.
    tmpdct = {}
    for stock in stocks:
        # we don't actually have an orders object with data
        # Do this part later
        tmpdct[stock] = parse('2020-04-17')
    return tmpdct

def RsiSignals(lasttx, rsi):
    # For the given stock, find the buy and sell signals since the last transaction
    buy = 0     # Assume there is no buy signal
    sell = 0    # Assume there is no sell signal
    for t in rsi:
        if parse(t) > lasttx and float(rsi[t]['RSI']) <= 33.33 and buy == 0:
            # This is an RSI buy signal
            buy = 1
        if parse(t) > lasttx and float(rsi[t]['RSI']) >= 66.66 and sell == 0:
            # This is an RSI sell signal
            sell = 1
    return {'buy':buy, 'sell':sell}

def GetRsi(s):
    # tries to get the RSI for the specified stock
    rsiraw = api.alpha_vantage.techindicators('RSI', output_format='JSON', symbol=s, interval='1min', time_period='500',
                                              series_type='close')
    rsi = json.loads(json.dumps(rsiraw))

    try:
        retval = rsi["Technical Analysis: RSI"]
    except:
        # pause for a minute and retry
        time.sleep(60)
        rsiraw = api.alpha_vantage.techindicators('RSI', output_format='JSON', symbol=s, interval='1min',
                                                  time_period='500',
                                                  series_type='close')
        rsi = json.loads(json.dumps(rsiraw))
        retval = rsi["Technical Analysis: RSI"]

    return (retval)

# Create list of tickers in the S&P 500 from the CSV file
with open('sp500.csv', mode='r', encoding='utf-8-sig') as infile:
    reader = csv.reader(infile)
    splist = []
    for rows in reader:
        splist.append(rows[0])

# Setup the API
api = tradeapi.REST()

# Get key pieces of information
account = api.get_account()
orders = api.list_orders()
positions = api.list_positions()

lastorder = GetLastTrx(splist, orders)

# rsiraw = api.alpha_vantage.techindicators('RSI', output_format='JSON', symbol='CVS', interval='1min', time_period='30', series_type='close')
# rsi = json.loads(json.dumps(rsiraw))
# print(RsiSignals(lastorder['CVS'], rsi["Technical Analysis: RSI"]))



for s in splist:
    print(s, RsiSignals(lastorder[s], GetRsi(s)))








