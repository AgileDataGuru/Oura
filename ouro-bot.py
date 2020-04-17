# OURO:  An automated day-trading bot
# Written by Dave Andrus and Dwight Brookland on April 13, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import csv                              # for reading static data
import alpaca_trade_api as tradeapi     # required for trading


# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

# Create dictionary from S&P 500 CSV file
with open('sp500.csv', mode='r', encoding='utf-8-sig') as infile:
    reader = csv.reader(infile)
    splist = {rows[0]:rows[2] for rows in reader}

# Setup the API
api = tradeapi.REST()

# Get key pieces of information
account = api.get_account()
orders = api.list_orders()
positions = api.list_positions()

print (account)
print (orders)
print(positions)



