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
import ouro_lib as ol

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))
logging.info('OURO-TICALC logging enabled.')

# Initialize the Cosmos client
dhistory = ol.cosdb('stockdata', 'daily', '/ticker')
indicators = ol.cosdb('stockdata', 'daily_indicators', '/ticker')

# get list of stocks in the universe
query = "select distinct value d.ticker from daily d"
stocklist = ol.qrycosdb(dhistory, query)

# process the daily stocks
for stock in stocklist:

# Get all the daily data for the past 60 days for the given stock
# Note: 4.88 RU per 100 rows
    query = "SELECT d.id, d.ticker, d.tradedate, d.high as h, d.low as l, d.open as o, d.adjclose as c, d.volume as v FROM daily d where d.ticker = '" + stock + "'"
    df=pd.DataFrame(ol.qrycosdb(dhistory, query))


    if not df.empty:
        # Get the max date that indicator data exists for this stock
        # Note: 4.88 RU per 100 rows
        query = "SELECT VALUE max(d.tradedate) from daily d where d.ticker = '" + stock + "'"
        dt = parse('1971-01-01')
        dt_str = dt.strftime('%Y-%m-%d')
        dt_list = ol.qrycosdb(indicators, query)
        df = ol.calcind(df)

        rc=0
        for x in json.loads(df.to_json(orient='records')):
            if parse(x['tradedate']) > dt:
                # write the data
                try:
                    indicators.create_item(body=x)
                    logging.debug('Created document for ' + x['ticker'] + ' on ' + x['tradedate'])
                    rc = rc + 1
                except:
                    logging.debug('Could not create document for ' + x['ticker'] + ' on ' + x['tradedate'])
        logging.info('Wrote ' + str(rc) + ' records for ' + stock)