# DAILY_HISTORY:  Gets daily history data from Yahoo Finance and stores it in a project database
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
from datetime import timedelta
import json
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from dateutil.parser import parse       # used to create date/time objects from stringsec
import time

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('OURO-HISTORY logging enabled.')

# Setup the API
api = tradeapi.REST()
logging.info('Trade API configured.')

# Calculate universe of stocks
assets = api.list_assets()
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
logging.info('Universe of stocks created; ' + str(len(slist)) + ' stocks in the list.')

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

# configure common dates
today = datetime.datetime.utcnow()
today_str = today.strftime('%Y-%m-%d')
yesterday = today - datetime.timedelta(days=1)
earliest = today - datetime.timedelta(days=60)

counter = -1    # the number of items processed
rucounter = 0     # the number of requests made
procstart = datetime.datetime.now()

for x in slist:
    # count which item I'm on
    counter = counter + 1

    # check the throttle; limit this process to 100RU/sec
    throttle = ((datetime.datetime.now()-procstart).total_seconds())*400
    if rucounter > throttle:
        logging.debug('Sleeping ' + str((rucounter - throttle) / 400) + ' seconds to throttle the process.')
        time.sleep((rucounter-throttle)/400)

    # Get the last time daily data for this stock was cached
    # Note: 4.24 RU
    query = "select value max(d.tradedate) from daily d where d.ticker = '" + x + "'"
    rucounter = rucounter + 1
    startdate = today
    try:
        dt = list(container.query_items(
            query=query,
            enable_cross_partition_query=False
        ))
        startdate = parse(dt[0]) + timedelta(days=1)
        startdate_str = startdate.strftime('%Y-%m-%d')
        logging.debug ('Last daily date for ' + x + ' is ' + startdate_str)
    except:
        startdate = earliest
        startdate_str = str(earliest.strftime('%Y-%m-%d'))
        logging.debug('No date found for ' + x + '; getting data between ' + startdate_str + ' and ' + today_str)

    # If the last date was in the past, get new data; otherwise, skip it
    if startdate < today:

        data = yf.download(x, start=startdate_str, end=today_str, interval='1d', prepost='False', group_by='ticker')
        try:
            logging.info(
                '(' + str(counter) + ' of ' + str(len(slist)) + ') ' + x + ':  Getting data between ' + startdate_str + ' and ' + today_str)
            jsondata = json.loads(data.to_json(orient='index'))
        except:
            jsondata = {}
            logging.warning ('Data for ' + x + ' has problems; it is being skipped.')

        # Transform the json into a format easier to analyze and write it to Cosmos DB
        for r in jsondata:
            tradedate = datetime.datetime.utcfromtimestamp(int(r) / 1e3).strftime('%Y-%m-%d')
            id = uuid.uuid1(uuid.getnode())
            row = {
                'id': str(id),
                'ticker': x,
                'tradedate': tradedate,
                'open': jsondata[r]['Open'],
                'high': jsondata[r]['High'],
                'low': jsondata[r]['Low'],
                'close': jsondata[r]['Close'],
                'adjclose': jsondata[r]['Adj Close'],
                'volume': jsondata[r]['Volume']
            }
            # write the data
            # Note:  Approximately 4.2 RU
            rucounter = rucounter + 1
            try:
                container.create_item(body=row)
                logging.debug('Created document for ' + x + ' on ' + tradedate)
            except:
                logging.error('Could not create document for ' + x + ' on ' + tradedate)
        logging.info(
            '(' + str(counter) + ' of ' + str(len(slist)) + ') Finished processing ' + x)








    # Get intra-day data for last 24 hours
    #startdate = datetime.datetime.now() + datetime.timedelta(hours=-72)
    #startdate_str = startdate.strftime('%Y-%m-%d')
    #data = yf.download(x, startdate_str, interval='5m', prepost='False')



