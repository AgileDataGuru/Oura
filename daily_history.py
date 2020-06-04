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
import argparse
import ouro_lib as ol

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('OURO-HISTORY logging enabled.')

# Setup command line arguements
parser = argparse.ArgumentParser(description="OURO-HISTORY:  Daily stock data ingestion.")
parser.add_argument("--test", action="store_true", default=False, help="Script runs in test mode.  FALSE (Default) = update the entire universe of stock; TRUE = update a small subset of stocks to make testing quicker")
cmdline = parser.parse_args()

# Setup the API
api = tradeapi.REST()
logging.info('Trade API configured.')
logging.info('Test mode is:  ' + str(cmdline.test))

if not cmdline.test:
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
else:
    # Test with a small diverse set of stocks
    slist = ['CVS', 'DRI', 'EVR','FDEF', 'IBM', 'MPW', 'PPBI', 'PPL', 'PXD', 'QIWI', 'RL', 'TX', 'VZ']

# Initialize the Cosmos client
# endpoint = os.environ.get("OURO_DOCUMENTS_ENDPOINT", "SET OURO_DOCUMENTS_ENDPOINT IN ENVIRONMENT")
# key = os.environ.get("OURO_DOCUMENTS_KEY", "SET OURO_DOCUMENTS_KEY IN ENVIRONMENT")
# client = CosmosClient(endpoint, key)
# database_name = 'stockdata'
# database = client.create_database_if_not_exists(id=database_name)
# container = database.create_container_if_not_exists(
#     id="daily",
#     partition_key=PartitionKey(path="/ticker"),
#     offer_throughput=400
# )
# logging.info ('Azure-Cosmos client initialized; connected to ' + endpoint)

# Initialize SQL connection
sqlc = ol.sqldbcursor()

# configure common dates
today = datetime.datetime.utcnow()
today_str = today.strftime('%Y-%m-%d')
yesterday = today - datetime.timedelta(days=1)
earliest = today - datetime.timedelta(days=180)

counter = -1    # the number of items processed
# rucounter = 0     # the number of requests made
procstart = datetime.datetime.now()

for x in slist:
    # count which item I'm on
    counter = counter + 1

    # check the throttle; limit this process to 100RU/sec
    # throttle = ((datetime.datetime.now()-procstart).total_seconds())*400
    # if rucounter > throttle:
    #     logging.debug('Sleeping ' + str((rucounter - throttle) / 400) + ' seconds to throttle the process.')
    #     time.sleep((rucounter-throttle)/400)

    # Get the last time daily data for this stock was cached
    # Note: 4.24 RU
    # query = "select value max(d.tradedate) from daily d where d.ticker = '" + x + "'"
    # rucounter = rucounter + 1

    # SQL Query
    query = "SELECT MAX(tradedate) FROM stockdata..ohlcv_day WHERE ticker = '" + x + "';"
    startdate = today
    try:
        # dt = list(container.query_items(
        #     query=query,
        #     enable_cross_partition_query=False
        # ))
        dt = ol.qrysqldb(sqlc, query)
        dts = dt.fetchone()[0]
        startdate = earliest
        if dts is not None:
            startdate = parse(dts) - timedelta(days=1)
    except Exception as ex:
        logging.error('Setting start date to earliest date. ', exc_info=True)
        startdate = earliest

    # If the last date was in the past, get new data; otherwise, skip it
    startdate_str = startdate.strftime('%Y-%m-%d')
    if startdate < today:
        try:
            logging.info(
                '(' + str(counter) + ' of ' + str(len(slist)) + ') ' + x + ':  Getting data between ' + startdate_str + ' and ' + today_str)
            #data = yf.download(x, start=startdate_str, end=today_str, interval='1d', prepost='False', group_by='ticker')
            data = yf.download(x, start=startdate_str, interval='1d', prepost='False', group_by='ticker')
            jsondata = json.loads(data.to_json(orient='index'))
        except Exception as ex:
            jsondata = {}
            logging.warning ('Data for ' + x + ' has problems; it is being skipped.', exc_info=True)

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
            # rucounter = rucounter + 1
            # # Try to write this to cosmos db
            # try:
            #     container.create_item(body=row)
            #     logging.debug('Created document for ' + x + ' on ' + tradedate)
            # except:
            #     logging.error('Could not create document for ' + x + ' on ' + tradedate)

            # Try writing this to SQL
            query = "INSERT INTO stockdata..ohlcv_day (ticker, tradedate, o, h, l, c, v) VALUES (" \
                    "'" + x + "', " \
                    "'" + tradedate + "', "\
                    "'" + str(jsondata[r]['Open']) + "', "\
                    "'" + str(jsondata[r]['High']) + "', "\
                    "'" + str(jsondata[r]['Low']) + "', " \
                    "'" + str(jsondata[r]['Close']) + "', " \
                    "'" + str(jsondata[r]['Volume']) + "'"\
                    ")"
            ol.qrysqldb(sqlc, query)
        logging.info(
            '(' + str(counter) + ' of ' + str(len(slist)) + ') Finished processing ' + x)




