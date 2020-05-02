# OURO-LIB:  Common functions used across the Ouro project
# Written by Dave Andrus on May 2, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for operating system specific functions
import logging                          # for application logging
import json                             # for manipulating array data
import pandas as pd                     # in-memory database capabilities
import talib as ta                      # lib to calcualted technical indicators
from azure.cosmos import exceptions, CosmosClient, PartitionKey

def cosdb (db, ctr, prtn):
    # Connect to a CosmosDB database and container

    # Setup Logging
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))

    # Initialize the Cosmos client
    endpoint = os.environ.get("OURO_DOCUMENTS_ENDPOINT", "SET OURO_DOCUMENTS_ENDPOINT IN ENVIRONMENT")
    key = os.environ.get("OURO_DOCUMENTS_KEY", "SET OURO_DOCUMENTS_KEY IN ENVIRONMENT")
    client = CosmosClient(endpoint, key)
    database = client.create_database_if_not_exists(id=db)

    # Connect to the daily_indicators container
    try:
        container = database.create_container_if_not_exists(
            id=ctr,
            partition_key=PartitionKey(path=prtn),
            offer_throughput=400
        )
        logging.info('Azure-Cosmos client initialized; connected to ' + db + '.' + ctr + ' at ' + endpoint)
        return container
    except Exception as ex:
        logging.critical('Unable to create connection to ' + db + '.' + ctr + ' at ' + endpoint)
        logging.critical(ex)
        quit(-1)

def qrycosdb(ctr, query):
    # Execute a query against a connection to a CosmosDB container
    try:
        ds = list(ctr.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        logging.debug('Running query:  ' + query)
        return ds
    except Exception as ex:
        logging.debug('Query did not return results:  ' + query)
        logging.debug(ex)
        return None
