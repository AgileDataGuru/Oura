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
endpoint = "https://ouro.documents.azure.com:443"
key = 'xrEm8rxq1R7wjxUZjVYP1cxX8clN4LTpOqmKUmiPc5GOOtI1GjyzjhC2VeAl0xboJqZ1Kx3la5jAB4fPxNtoCQ=='
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
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
earliest = today - datetime.timedelta(days=60)

counter = 0
procstart = datetime.datetime.now()

for x in slist:
    # count which item I'm on
    counter = counter + 1

    # check the throttle; limit this to 3 request per second
    throttle = ((datetime.datetime.now()-procstart).total_seconds())/3
    if counter > throttle:
        time.sleep(counter-throttle)
        logging.debug ('Sleeping ' + str(counter-throttle) + ' seconds to throttle the process.')

    # Get the last time daily data for this stock was cached
    query = "select value max(d.tradedate) from daily d where d.ticker = '" + x + "'"
    try:
        dt = list(container.query_items(
            query=query,
            enable_cross_partition_query=False
        ))
        startdate_str = str(parse(dt[0]).strftime('%Y-%m-%d'))
        logging.debug ('Last daily date for ' + x + ' is ' + startdate_str)
    except:
        startdate_str = str(earliest)
        logging.debug('No date found for ' + x + '; getting data since ' + startdate_str)

    # If the last date was in the past, get new data; otherwise, skip it
    if startdate_str != str(yesterday) and startdate_str != str(today):

        data = yf.download(x, startdate_str, interval='1d', prepost='False', group_by='ticker')
        try:
            jsondata = json.loads(data.to_json(orient='index'))
            logging.info('(' + str(counter) + ' of ' + str(len(slist)) + ') Getting data for ' + x + ' since ' + startdate_str)
        except:
            jsondata = {}
            logging.warning ('Data for ' + x + ' has problems; it is being skipped.')

        # Transform the json into a format easier to analyze and write it to Cosmos DB
        for r in jsondata:
            tradedate = datetime.datetime.fromtimestamp(int(r) / 1e3)
            id = uuid.uuid1(uuid.getnode())
            row = {
                'id': str(id),
                'ticker': x,
                'tradedate': tradedate.strftime('%Y-%m-%d'),
                'open': jsondata[r]['Open'],
                'high': jsondata[r]['High'],
                'low': jsondata[r]['Low'],
                'close': jsondata[r]['Close'],
                'adjclose': jsondata[r]['Adj Close'],
                'volume': jsondata[r]['Volume']
            }
            # write the data
            try:
                container.create_item(body=row)
                logging.debug('Created document for ' + x + ' on ' + tradedate.strftime('%Y-%m-%d'))
            except:
                logging.error('Could not create document for ' + x + ' on ' + tradedate.strftime('%Y-%m-%d'))









    # Get intra-day data for last 24 hours
    #startdate = datetime.datetime.now() + datetime.timedelta(hours=-72)
    #startdate_str = startdate.strftime('%Y-%m-%d')
    #data = yf.download(x, startdate_str, interval='5m', prepost='False')



