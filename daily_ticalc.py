# DAILY_TICALC:  Calculates technical indicators
# Written by Dave Andrus and Dwight Brookland on April 13, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import datetime                         # used for stock timestamps
import json
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from dateutil.parser import parse       # used to create date/time objects from stringsec
import time
import pandas as pd
import talib as ta


# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))
logging.info('OURO-TICALC logging enabled.')

# Initialize the Cosmos client
endpoint = os.environ.get("OURO_DOCUMENTS_ENDPOINT", "SET OURO_DOCUMENTS_ENDPOINT IN ENVIRONMENT")
key = os.environ.get("OURO_DOCUMENTS_KEY", "SET OURO_DOCUMENTS_KEY IN ENVIRONMENT")
client = CosmosClient(endpoint, key)
database_name = 'stockdata'
database = client.create_database_if_not_exists(id=database_name)
dhistory = database.create_container_if_not_exists(
    id="daily",
    partition_key=PartitionKey(path="/ticker"),
    offer_throughput=400
)
indicators = database.create_container_if_not_exists(
    id="daily_indicators",
    partition_key=PartitionKey(path="/ticker"),
    offer_throughput=400
)
logging.info ('Azure-Cosmos client initialized; connected to ' + endpoint)

# get list of stocks in the universe
query = "select distinct value d.ticker from daily d"
try:
    stocklist = list(dhistory.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    logging.debug('Retrieve daily data.')
except:
    logging.debug('No daily date available.')

# process the daily stocks
for stock in stocklist:

# Get all the daily data for the past 60 days for the given stock
# Note: 4.88 RU per 100 rows
    query = "SELECT d.id, d.ticker, d.tradedate, d.high, d.low, d.open, d.adjclose, d.volume FROM daily d where d.ticker = '" + stock + "'"
    df = pd.DataFrame()
    try:
        data = list(dhistory.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        logging.debug('Retrieve daily data for ' + stock)
        df = pd.DataFrame(data)
    except:
        logging.debug('No daily date available for ' + stock)

    if not df.empty:
        # calculate the technical indicators if there is data to do so
        df['RSI14'] = ta.RSI(df['adjclose'], timeperiod=14)
        df['SMA14'] = ta.SMA(df['adjclose'], timeperiod=14)
        df['EMA14'] = ta.EMA(df['adjclose'], timeperiod=14)
        df['MACD0'] = ta.MACD(df['adjclose'], fastperiod=12, slowperiod=26, signalperiod=9)[0]
        df['MACD1'] = ta.MACD(df['adjclose'], fastperiod=12, slowperiod=26, signalperiod=9)[1]
        df['MACD2'] = ta.MACD(df['adjclose'], fastperiod=12, slowperiod=26, signalperiod=9)[2]
        df['ADX14'] = ta.ADX(df['high'], df['low'], df['adjclose'])
        df['CCI14'] = ta.CCI(df['high'], df['low'], df['adjclose'], timeperiod=10)
        df['AROONUP'], df['AROONDN'] = ta.AROON(df['high'], df['low'], timeperiod=14)
        df['BBANDS14-0'] = ta.BBANDS(df['adjclose'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)[0]
        df['BBANDS14-1'] = ta.BBANDS(df['adjclose'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)[1]
        df['BBANDS14-2'] = ta.BBANDS(df['adjclose'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)[2]
        df['AD14'] = ta.AD(df['high'], df['low'], df['adjclose'], df['volume'])
        df['OBV14'] = ta.OBV(df['adjclose'], df['volume'])

        df['BBANDSIND'] = 'Not Interpretted'
        df['ADX14IND'] = 'Absent'
        df['AD14IND'] = 'Not Interpretted'
        df['OBV14IND']= 'Not Inperpretted'

        # RSI Votes
        df['RSIVOTE'] = 0
        df.loc[df['RSI14'] >= 70, 'RSIVOTE'] = 1
        df.loc[df['RSI14'] <= 30, 'RSIVOTE'] = -1

        # need to compare a state change from pos-to-neg or neg-to-pos for the indicator
        df['MACDVOTE'] = 0
        # print (df[['MACD0','MACD1','MACD2' ]])

        # ADX Trend Strength
        df.loc[df['ADX14'] >= 25, 'RSIIND'] = 'Strong'
        df.loc[df['ADX14'] >= 50, 'RSIIND'] = 'Very Strong'
        df.loc[df['ADX14'] >= 75, 'RSIIND'] = 'Extremely Strong'

        # CCI Vote
        df['CCIVOTE'] = 0
        df.loc[df['CCI14'] >= 100, 'CCIVOTE'] = 1
        df.loc[df['CCI14'] <= -100, 'CCIVOTE'] = -1

        # AROON Oscillator
        df['AROONOSC'] = df['AROONDN'] - df['AROONUP']
        df['AROONVOTE'] = 0
        df.loc[df['AROONOSC'] >= 25, 'AROONVOTE'] = 1    # This threshold is a guess
        df.loc[df['AROONOSC'] <= -25, 'AROONVOTE'] = -1  # This threshold is a guess


        # Get the max date that indicator data exists for this stock
        # Note: 4.88 RU per 100 rows
        query = "SELECT VALUE max(d.tradedate) from daily d where d.ticker = '" + stock + "'"
        dt = parse('1971-01-01')
        dt_str = dt.strftime('%Y-%m-%d')
        try:
            dt_list = list(indicators.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            logging.debug('Last indicator seen for ' + stock + ' on ' + dt_list[0])
            dt = parse(dt_list[0])
            dt_str = dt.strftime('%Y-%m-%d')
        except:
            logging.debug('No indicators found for ' + stock + ', creating them from scratch.')
        rc=0
        for x in json.loads(df.to_json(orient='records')):
            if parse(x['tradedate']) > dt:
                # write the data
                try:
                    indicators.create_item(body=x)
                    logging.debug('Created document for ' + x['ticker'] + ' on ' + x['tradedate'])
                    rc = rc + 1
                except:
                    logging.error('Could not create document for ' + x['ticker'] + ' on ' + x['tradedate'])
        logging.info('Wrote ' + str(rc) + ' records for ' + stock)