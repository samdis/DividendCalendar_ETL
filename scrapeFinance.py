# https://www.thestreet.com/util/divs.jsp?date=09_10_2020

# !/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import timedelta, date

import requests
import time
import json

from pymongo import MongoClient
client = MongoClient() #get client
db = client.development #get db
collection = db.finance #get collection


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

print('hello')
start_date = date(2020, 7, 1)
end_date = date(2020, 12, 31)
for single_date in daterange(start_date, end_date):
    next_date = single_date.strftime("%m_%d_%Y")
    url = "http://www.thestreet.com/util/divs.jsp?date=" + next_date
    p = requests.get(url)

    cleanText = p.text

    if not cleanText:
        continue
    cleanText = cleanText.replace('results', '\"results\"')
    cleanText = cleanText.replace("symbol", '\"symbol\"')
    cleanText = cleanText.replace("name", '\"name\"')
    cleanText = cleanText.replace("amount", '\"amount\"')
    cleanText = cleanText.replace("yield", '\"yield\"')
    cleanText = cleanText.replace("exdate", '\"exdate\"')

    finance_data = json.loads(cleanText)

    for results in finance_data['results']:
        print(results['symbol'])
        print(results['name'])
        print(results['amount'])
