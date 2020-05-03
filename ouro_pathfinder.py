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

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('OURO-PATHFINDER logging enabled.')

# Initialize the Alpaca API
alpaca = tradeapi.REST()

# Connect to the daily_indicators container
indicators = ol.cosdb('stockdata', 'daily_indicators', '/ticker')

# get list of stocks in the universe; find stocks potentially worth trading
query = "select * from (select i.ticker, i.adjclose, i.RSIVOTE + i.MACDVOTE + i.AROONVOTE + i.CCIVOTE as Vote from daily_indicators i where i.tradedate = '2020-04-29') x where x.Vote > 0"
stocksraw = ol.qrycosdb(indicators, query)

# Put the stock list into a dataframe ordered by their buy vote
# A higher vote number, the more concensus; the lower, the less concensus
stocklist = pd.DataFrame(stocksraw).sort_values(by=['Vote'], ascending=False)

# Get the last 30 minutes of data for all the stocks in the list
barset = alpaca.get_barset(stocklist['ticker'], '1Min', limit=30)
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


#
#
#         # Get the max date that indicator data exists for this stock
#         # Note: 4.88 RU per 100 rows
#         query = "SELECT VALUE max(d.tradedate) from daily d where d.ticker = '" + stock + "'"
#         dt = parse('1971-01-01')
#         dt_str = dt.strftime('%Y-%m-%d')
#         try:
#             dt_list = list(indicators.query_items(
#                 query=query,
#                 enable_cross_partition_query=True
#             ))
#             logging.debug('Last indicator seen for ' + stock + ' on ' + dt_list[0])
#             dt = parse(dt_list[0])
#             dt_str = dt.strftime('%Y-%m-%d')
#         except:
#             logging.debug('No indicators found for ' + stock + ', creating them from scratch.')
#         rc=0
#         for x in json.loads(df.to_json(orient='records')):
#             if parse(x['tradedate']) > dt:
#                 # write the data
#                 try:
#                     indicators.create_item(body=x)
#                     logging.debug('Created document for ' + x['ticker'] + ' on ' + x['tradedate'])
#                     rc = rc + 1
#                 except:
#                     logging.error('Could not create document for ' + x['ticker'] + ' on ' + x['tradedate'])
#         logging.info('Wrote ' + str(rc) + ' records for ' + stock)