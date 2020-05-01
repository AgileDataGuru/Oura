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

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))
logging.info('OURO-PATHFINDER logging enabled.')

# Initialize the Alpaca API
alpaca = tradeapi.REST()

# Initialize the Cosmos client
endpoint = os.environ.get("OURO_DOCUMENTS_ENDPOINT", "SET OURO_DOCUMENTS_ENDPOINT IN ENVIRONMENT")
key = os.environ.get("OURO_DOCUMENTS_KEY", "SET OURO_DOCUMENTS_KEY IN ENVIRONMENT")
client = CosmosClient(endpoint, key)
database_name = 'stockdata'
database = client.create_database_if_not_exists(id=database_name)

# Connect to the daily_indicators container
indicators = database.create_container_if_not_exists(
    id="daily_indicators",
    partition_key=PartitionKey(path="/ticker"),
    offer_throughput=400
)
logging.info ('Azure-Cosmos client initialized; connected to ' + endpoint)

# get list of stocks in the universe; find stocks potentially worth trading
query = "select * from (select i.ticker, i.adjclose, i.RSIVOTE + i.MACDVOTE + i.AROONVOTE + i.CCIVOTE as Vote from daily_indicators i where i.tradedate = '2020-04-29') x where x.Vote > 0"
try:
    stocksraw = list(indicators.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    logging.debug('Retrieve daily data.')
except:
    logging.debug('No daily date available.')

# Put the stock list into a dataframe ordered by their buy vote
# A higher vote number, the more concensus; the lower, the less concensus
stocklist = pd.DataFrame(stocksraw).sort_values(by=['Vote'], ascending=False)

# Get the last 30 minutes of data for all the stocks in the list
barset = alpaca.get_barset(stocklist['ticker'], '1Min', limit=30)
df = {}

# Convert barset to usable dataframe
for stock in barset.keys():
    bars = barset[stock]
    data = {'adjclose': [bar.c for bar in bars],
            'high': [bar.h for bar in bars],
            'low': [bar.l for bar in bars],
            'open': [bar.o for bar in bars],
            'time': [bar.t for bar in bars],
            'volume': [bar.v for bar in bars]}
    df[stock] = pd.DataFrame(data)

    if not df[stock].empty:
        # calculate the technical indicators if there is data to do so
        df[stock]['RSI14'] = ta.RSI(df[stock]['adjclose'], timeperiod=14)
        df[stock]['SMA14'] = ta.SMA(df[stock]['adjclose'], timeperiod=14)
        df[stock]['EMA14'] = ta.EMA(df[stock]['adjclose'], timeperiod=14)
        df[stock]['MACD0'] = ta.MACD(df[stock]['adjclose'], fastperiod=12, slowperiod=26, signalperiod=9)[0]
        df[stock]['MACD1'] = ta.MACD(df[stock]['adjclose'], fastperiod=12, slowperiod=26, signalperiod=9)[1]
        df[stock]['MACD2'] = ta.MACD(df[stock]['adjclose'], fastperiod=12, slowperiod=26, signalperiod=9)[2]
        df[stock]['ADX14'] = ta.ADX(df[stock]['high'], df[stock]['low'], df[stock]['adjclose'])
        df[stock]['CCI14'] = ta.CCI(df[stock]['high'], df[stock]['low'], df[stock]['adjclose'], timeperiod=10)
        df[stock]['AROONUP'], df[stock]['AROONDN'] = ta.AROON(df[stock]['high'], df[stock]['low'], timeperiod=14)
        df[stock]['BBANDS14-0'] = ta.BBANDS(df[stock]['adjclose'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)[0]
        df[stock]['BBANDS14-1'] = ta.BBANDS(df[stock]['adjclose'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)[1]
        df[stock]['BBANDS14-2'] = ta.BBANDS(df[stock]['adjclose'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)[2]
        df[stock]['AD14'] = ta.AD(df[stock]['high'], df[stock]['low'], df[stock]['adjclose'], df[stock]['volume'])
        df[stock]['OBV14'] = ta.OBV(df[stock]['adjclose'], df[stock]['volume'])

        df[stock]['BBANDSIND'] = 'Not Interpretted'
        df[stock]['ADX14IND'] = 'Absent'
        df[stock]['AD14IND'] = 'Not Interpretted'
        df[stock]['OBV14IND']= 'Not Inperpretted'

        # RSI Votes
        df[stock]['RSIVOTE'] = 0
        df[stock].loc[df[stock]['RSI14'] >= 70, 'RSIVOTE'] = 1
        df[stock].loc[df[stock]['RSI14'] <= 30, 'RSIVOTE'] = -1

        # need to compare a state change from pos-to-neg or neg-to-pos for the indicator
        df[stock]['MACDVOTE'] = 0
        # print (df[['MACD0','MACD1','MACD2' ]])

        # ADX Trend Strength
        df[stock].loc[df[stock]['ADX14'] >= 25, 'RSIIND'] = 'Strong'
        df[stock].loc[df[stock]['ADX14'] >= 50, 'RSIIND'] = 'Very Strong'
        df[stock].loc[df[stock]['ADX14'] >= 75, 'RSIIND'] = 'Extremely Strong'

        # CCI Vote
        df[stock]['CCIVOTE'] = 0
        df[stock].loc[df[stock]['CCI14'] >= 100, 'CCIVOTE'] = 1
        df[stock].loc[df[stock]['CCI14'] <= -100, 'CCIVOTE'] = -1

        # AROON Oscillator
        df[stock]['AROONOSC'] = df[stock]['AROONDN'] - df[stock]['AROONUP']
        df[stock]['AROONVOTE'] = 0
        df[stock].loc[df[stock]['AROONOSC'] >= 25, 'AROONVOTE'] = 1    # This threshold is a guess
        df[stock].loc[df[stock]['AROONOSC'] <= -25, 'AROONVOTE'] = -1  # This threshold is a guess

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