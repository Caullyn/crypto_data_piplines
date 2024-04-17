import os
import psycopg2
import pgpasslib
import requests
import pandas as pd
import psycopg2
import json
import time
from io import StringIO
from datetime import datetime, timedelta

# Ensure you have Environment Variables set
host =                  os.environ['GLINT_HOST']
dbname =                os.environ['GLINT_DBNAME']
user =                  os.environ['GLINT_USER']
port =                  os.environ['GLINT_PORT']
coingecko_api_key =     os.environ['COINGECKO_API_KEY']

# Connect to Redshift
passw = pgpasslib.getpass(host,
    5439,
    dbname,
    user)
connection = psycopg2.connect(
    host=host,
    user=user,
    dbname=dbname,
    password=passw,
    port=port,
    connect_timeout=500)
cursor = connection.cursor()

# Function to fetch historical prices for a given symbol
def fetch_historical_prices(id, start_date):
    url = f"https://api.coingecko.com/api/v3/coins/bitcoin/history"
    params = {
        'vs_currency': 'usd',
        'date': '17-11-2023',
        'api_key': 'CG-JuUQSuHgVZZ3TjQXrvi8BokL'
    }
    response = requests.get(url, params=params)
    data = response.json()

    filtered_prices = []
    print(data)
    if 'prices' in data:
        prices = data['prices']

        filtered_prices = [(datetime.utcfromtimestamp(price[0] / 1000).date(), price[1])
                        for price in prices ]
    else:
        print(data)
    return filtered_prices

counter = 0
start_date = datetime(2023, 10, 1)  # Default start date if no data exists

prices = fetch_historical_prices('bitcoin', start_date)
print(prices)
cursor.close()
connection.close()


print("Price data backfill complete.")
