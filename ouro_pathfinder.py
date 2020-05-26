# OURO_PATHFINDER:  Finds stocks to trade with intraday data
# Written by Dave Andrus on April 30, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import datetime                         # used for stock timestamps
import json                             # for manipulating array data
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from dateutil.parser import parse       # used to create date/time objects from stringsec
import time                             # for manipulating time data
import pandas as pd                     # in-memory database capabilities
import talib as ta                      # lib to calcualted technical indicators
import alpaca_trade_api as tradeapi     # required for interaction with Alpaca
from pandas.io.json import json_normalize
import ouro_lib as ol
from progress.bar import Bar
import threading
import math
import csv
import argparse

# Get Quorum path from environment
quorumroot = os.environ.get("OURO_QUORUM", "C:\\TEMP")
actionpath = quorumroot + '\\broker-actions.json'
quorumpath = quorumroot + '\\pathfinder-status.csv'
logpath = quorumroot + '\\pathfinder.log'
installpath = os.environ.get("OURO_INSTALL", "D:\\OneDrive\\Dev\\Python\\Oura")

# Setup Logging
logging.basicConfig(
    filename=logpath,
    filemode='a',
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('OURO-PATHFINDER logging enabled.')

# setup command line
parser = argparse.ArgumentParser(description="OURO-HISTORY:  Daily stock data ingestion.")
parser.add_argument("--test", action="store_true", default=False, help="Script runs in test mode.  FALSE (Default) = ignore if the market is closed; TRUE = only run while the market is open")
cmdline = parser.parse_args()
logging.info('Command line arguement; test mode is ' + str(cmdline.test))

# initialize files
with open (quorumpath, 'w', newline='\n', encoding='utf-8') as outfile:
    outfile.write('')
with open (actionpath, 'w', newline='\n', encoding='utf-8') as outfile:
    outfile.write('{}')
logging.info('Files initialized')

logging.info('Quorum path set to ' + quorumpath)

# Initialize the Alpaca API
alpaca = tradeapi.REST()

# Connect to the daily_indicators container
indicators = ol.cosdb('stockdata', 'daily_indicators', '/ticker')

# Read the buy and sell strategies
buy = pd.read_csv(installpath + '\\buy_strategies.csv')
buylist = buy['strategy_id'].values.tolist()
buyfam = buy['Family'].unique()

# Initialize the actions set
actions = {}

# I don't think I actually care about this because I'm selling based on a risk management profile.
# sell = pd.read_csv('D:\\OneDrive\\Dev\\Python\\Oura\\sell_strategies.csv')
# selllist = sell['strategy_id'].values.tolist()
# sellfam = sell['Family'].unique()

# build a simple index between the strategy_id and the family
famref = {}
for x in buy['strategy_id'].keys():
    famref[buy.at[x, 'strategy_id']] = buy.at[x, 'Family']

# build simple index between family and average signals
famavg = {}
for x in buy['Family'].keys():
    famavg[buy.at[x, 'Family']] = buy.at[x, 'AvgBuyWarning']

# Create a buy string
f = 0
buystr = ''
for x in buy['strategy_id']:
    if f != 0:
        buystr = buystr + ", "
    buystr = buystr + "'" + x + "'"
    f = 1

# Get the last date in the daily table
ddate = ol.qrycosdb(indicators, 'SELECT value max(d.tradedate) from daily d')[0]

# get list of stocks in the universe; find stocks potentially worth trading
# NOTE:  With $1000 starting capital, stocks that cost over $100 are over my money management budget
query = "select distinct d.ticker, d.STRATEGY_ID, d.tradedate, d.v, d.h-d.l Change from daily_indicators d where d.l < 100 and d.o > 5 and d.l > 5 and d.tradedate = '" + ddate + "' and d.STRATEGY_ID in (" + buystr + ") order by d.v desc"
tmpraw = ol.qrycosdb(indicators, query)
logging.info ('Found ' + str(len(tmpraw)) + ' worth monitoring today.')

# Put the stock list into a dataframe ordered by their buy vote
# A higher vote number, the more concensus; the lower, the less concensus
stockraw = pd.DataFrame(tmpraw)  #.sort_values(by=['Vote'], ascending=False)
if len(stockraw) > 750:
    stocklist = stockraw.nlargest(750, 'v')  # This is about all I can deal with
    logging.info ('More than 750 stocks detected.')
else:
    stocklist = stockraw
    logging.info ('Less than 750 stocks detected.')

# Free up memory
tmpraw = None
stockraw = None

# setup counters for chunking stocks into sets
set = 0
stockctr = 0
setctr = 0
stockset = {}
sl = []

# Get a list of closing prices from yesterday
today = datetime.datetime.utcnow()
yesterday = today - datetime.timedelta(days=1)
ystr = ol.GetLastOpenMarket()

query = "select d.ticker, d.c from daily d where d.tradedate = '" + ystr + "' order by d.tradedate desc"
closingraw  = ol.qrycosdb(indicators, query)
closing = {}
for x in closingraw:
    closing.update({x.get('ticker'):x.get('c')})

# Chunk stocks into groups of 200, the limit that can be requested at once
while stockctr < len(stocklist['ticker']):
    try:
        sl.append(stocklist.loc[stockctr, 'ticker'])
    except:
        logging.debug('Problem with stocklist index ' + str(stockctr))

    stockctr = stockctr + 1
    setctr = setctr + 1
    if setctr == 200:
        # Save the set and rest the counter for the next one
        stockset.update({set : sl})
        set = set + 1
        setctr = 0
        sl = []

# Add the last group of stocks to the sets
stockset.update({set : sl})

# Wait for the market to open unless it's a test
while not ol.IsOpen() and not cmdline.test:
    ol.WaitForMinute()

# Initialize MarketOpen
marketopen = ol.IsOpen()
eod = ol.IsEOD()

# initialize the counting array
sgnl = ol.InitSignal(stocklist['ticker'], buyfam)

# set the first time flag
firsttime = True

# Main processing loop to look for stocks to buy
# NOTE:  Pathfinder doesn't cancel orders before the market closes so it can run the whole day
while (marketopen) or cmdline.test is True:

    # reset the path finder status
    pf = {}

    # Check if the market is open
    marketopen = ol.IsOpen()

    # update screen
    print (datetime.datetime.now())
    print('The market is {}'.format('open.' if marketopen else 'closed.'))

    # Get the last 42 (TRIX + 12 extra minutes) minutes of data for all the stocks in the list
    logging.info('Getting stock sets.')
    barset = {}
    for x in stockset:
        logging.debug('Getting set ' + str(x))
        barset[x] = alpaca.get_barset(stockset[x], '1Min', limit=42)

    # process each stock
    df = {}
    for x in stockset:
        prgbar = Bar('  Set ' + str(x), max=len(barset[x]))
        # process each stock in the barset
        for stock in barset[x].keys():
            # convert bars from Alpaca into something more usable
            bars = barset[x][stock]
            logging.debug('Converting bar data for ' + stock)
            data = {'ticker': [stock for bar in bars],
                    't': [bar.t for bar in bars],
                    'h': [bar.h for bar in bars],
                    'l': [bar.l for bar in bars],
                    'o': [bar.o for bar in bars],
                    'c': [bar.c for bar in bars],
                    'v': [bar.v for bar in bars]}

            # Calculate technical indicators
            logging.debug('Calculating technical indicators ' + stock)
            df[stock] = ol.calcind(pd.DataFrame(data))

            # Check if there is a significant difference between yesterday's open and today's
            opendiff = 1 # this will fail by default
            try:
                opendiff = ( df[stock].at[df[stock].index[-1], 'o']-closing[stock])/closing[stock]
                logging.debug('Difference calculated.')
            except Exception:
                logging.error('Could not calculate the opening difference', exc_info=True)

            skip = False
            if firsttime and opendiff > .02: # guess threshold
                skip = True

            # Check if the last strategy is in the buy strategy list
            try:
                tmpstrat = df[stock].at[df[stock].index[-1], 'STRATEGY_ID']
                #print(stock, df[stock].at[df[stock].index[-1], 'o'], closing[stock], opendiff, (tmpstrat in buylist and not skip))
                # Add to the number of times this family has been seen for this stock
                if tmpstrat in buylist and not skip:
                    tmpfam = famref.get(tmpstrat)
                    v = sgnl[stock].get(tmpfam)
                    v += 1
                    sgnl[stock].update({tmpfam:v})

                    # check if the value exceeds the average buy warning
                    tavg = famavg.get(tmpfam)
                    if not math.isnan(tavg):
                        # print (stock, v > tavg, stock not in actions.keys(), df[stock].at[df[stock].index[-1], 'c'])
                        if v > tavg and stock not in actions.keys():
                            logging.debug('Buy signal triggered for ' + stock + ' with ' + str(v) + ' signals in ' + tmpfam)
                            # record the buy trigger
                            actions[stock] = {
                                'triggertime': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M'),
                                'strategyfamily': tmpfam,
                                'strategies': sgnl[stock],
                                'price': df[stock].at[df[stock].index[-1], 'c']
                            }

                        else:
                            logging.debug('Buy signal ' + str(v) + ' less than the threshold ' + str(tavg) + ' for family ' + tmpfam)

            except Exception as ex:
                logging.debug ('Could not check signal for ' + stock)
            prgbar.next()
        prgbar.finish()

    # update path finder status
    curtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open (quorumpath, 'w', newline='\n', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        # write the header
        writer.writerow(['datetime', 'signal', 'ticker', 'family', 'signals', 'threshold'])
        for x in sgnl:
            for y in sgnl[x]:
                if sgnl[x].get(y) > 0:
                    tavg = famavg.get(y)
                    if math.isnan(tavg):
                        tavg = 0
                    writer.writerow([curtime, 'buy', x, y, sgnl[x].get(y), tavg])

    # write actions
    with open (actionpath, 'w', newline='\n', encoding='utf-8') as outfile:
        actionstr = json.dumps(actions, indent=4)
        outfile.write(actionstr)

    # wait until the next minute before checking again
    ol.WaitForMinute()


