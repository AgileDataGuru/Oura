# UTIL_CREATE_STRAT_INDEX:  Create a list of all possible trade strategies
# based on the types of votes I have availble in the dataset.
# Written by Dave Andrus on May 3, 2020
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

# AROON Oscillator
# BOP Signal
# CCI Vote
# CMO Votes
# MACD Vote; based on when the histogram crosses the zero line
# PPO Votes; cousin of MACD
# RSI Votes
# STOCH Votes
# STOCHRSI Votes
# TRIX Votes
# ADOSC Votes

# Strategies with votes in OURO_LIB
strats = ['AROON', 'BOP', 'CCI', 'CMO', 'MACD', 'PPO', 'RSI', 'STOCH', 'STOCHRSI', 'TRIX', 'ADOSC', ]

# Possible votes
# A: Vote = -1
# B: Vote =  0
# C: Vote =  1

aroon = ['A', 'B', 'C']
bop = ['A', 'B', 'C']
cci = ['A', 'B', 'C']
cmo = ['A', 'B', 'C']
macd = ['A', 'B', 'C']
ppo = ['A', 'B', 'C']
rsi = ['A', 'B', 'C']
stoch = ['A', 'B', 'C']
stochrsi = ['A', 'B', 'C']
trix = ['A', 'B', 'C']
adosc = ['A', 'B', 'C']

# create an empty container for the codes
codes = []

# generate every possible codes; there should be 177,147
for a in aroon:
    for b in bop:
        for c in cci:
            for d in cmo:
                for e in macd:
                    for f in ppo:
                        for g in rsi:
                            for h in stoch:
                                for i in stochrsi:
                                    for j in trix:
                                        for l in adosc:
                                            code = a + b + c + d + e + f + g + h + i + j + l
                                            codes.append(code)
# Create an empty container for the strat list

stratDict = {}

for code in codes:
    family = ''
    sname = ''
    for x in range(len(code)):
        if code[x] == 'C':
            family = family + '+' + strats[x]
            sname = sname + '+' + strats[x]
        if code[x] == 'A':
            sname = sname + '-' + strats[x]
    stratDict[code] = {'family':family, 'name':sname}



# Connect to the daily_indicators container
dbconn = ol.cosdb('stockdata', 'strategies', '/family')

# loop through the stratgies and write them to the database
for x in stratDict:
    row = {
        'id':x,
        'family':stratDict[x]['family'],
        'name':stratDict[x]['name']
    }
    try:
        dbconn.create_item(body=row)
        logging.debug('Created document for ' + x )
    except:
        logging.error('Could not create document for ' + x)






