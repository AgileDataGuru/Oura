# OURO-HISTORY:  Gets daily and 5m history data from Yahoo Finance
#   and stores it in a project database
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
from azure.cosmos import exceptions, CosmosClient, PartitionKey

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

# Setup the API
api = tradeapi.REST()

# Calculate universe of stocks
assets = api.list_assets()
slist = []
for x in assets:
    if x.exchange == 'NASDAQ' or x.exchange=='NYSE':
        if x.tradable is True and x.status == 'active':
            slist.append(x.symbol)


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

#query = "SELECT * FROM c WHERE c.lastName IN ('Wakefield', 'Andersen')"

#items = list(container.query_items(
#    query=query,
#    enable_cross_partition_query=True
#))

#request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
#print('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))

for x in slist:
    # Get daily data for last 60 days
    startdate = datetime.datetime.now() + datetime.timedelta(days=-60)
    startdate_str = startdate.strftime('%Y-%m-%d')
    data = yf.download(x, startdate_str, interval='1d', prepost='False', group_by='ticker')
    jsondata = json.loads(data.to_json(orient='index'))

    # Transform the json into a format easier to analyze
    for r in jsondata:
        tradedate = datetime.datetime.fromtimestamp(int(r) / 1e3)
        row = {
            'ticker': x,
            'tradedate': tradedate.strftime('%Y-%m-%d'),
            'open': jsondata[r]['Open'],
            'high': jsondata[r]['High'],
            'low': jsondata[r]['Low'],
            'close': jsondata[r]['Close'],
            'adjclose': jsondata[r]['Adj Close'],
            'volume': jsondata[r]['Volume']
        }
        print (row)









    # Get intra-day data for last 24 hours
    #startdate = datetime.datetime.now() + datetime.timedelta(hours=-72)
    #startdate_str = startdate.strftime('%Y-%m-%d')
    #data = yf.download(x, startdate_str, interval='5m', prepost='False')



