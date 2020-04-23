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
    query = "SELECT d.id, d.ticker, d.tradedate, d.open, d.adjclose, d.volume FROM daily d where d.ticker = '" + stock + "'"
    #try:
    data = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    logging.debug('Retrieve daily data.')
    #except:
    #    logging.debug('No daily date available.')

    print(data)
    df = pd.DataFrame(data)
    print (df)