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
container = database.create_container_if_not_exists(
    id="daily",
    partition_key=PartitionKey(path="/ticker"),
    offer_throughput=400
)
logging.info ('Azure-Cosmos client initialized; connected to ' + endpoint)

# get list of stocks in the universe
query = "select distinct value d.ticker from daily d"
try:
    stocklist = list(container.query_items(
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
        data = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        logging.debug('Retrieve daily data.')
        df = pd.DataFrame(data)
    except:
        logging.debug('No daily date available.')

    if not df.empty:
        # calculate the technical indicators if there is data to do so
        df['RSI14'] = ta.RSI(df['adjclose'], timeperiod=14)
        df['SMA14'] = ta.SMA(df['adjclose'], timeperiod=14)
        df['EMA14'] = ta.EMA(df['adjclose'], timeperiod=14)
        # ValueError: Length of values does not match length of index
        # df['MACD'] = ta.MACD(df['adjclose'], fastperiod=12, slowperiod=26, signalperiod=9)
        df['ADX14'] = ta.ADX(df['high'], df['low'], df['adjclose'])
        df['CCI14'] = ta.CCI(df['high'], df['low'], df['adjclose'], timeperiod=10)
        df['AROONUP'], df['AROONDN'] = ta.AROON(df['high'], df['low'], timeperiod=14)
        # ValueError: Length of values does not match length of index
        # df['BBANDS14'] = ta.BBANDS(df['adjclose'], timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
        df['AD14'] = ta.AD(df['high'], df['low'], df['adjclose'], df['volume'])
        df['OBV14'] = ta.OBV(df['adjclose'], df['volume'])
