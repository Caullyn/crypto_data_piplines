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
host = os.environ['GLINT_HOST']
dbname = os.environ['GLINT_DBNAME']
user = os.environ['GLINT_USER']
port = os.environ['GLINT_PORT']

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

# Fetch the list of top coins from your Redshift table
cursor.execute("SELECT id, symbol FROM coingecko.top_coins limit 1")
coins = cursor.fetchall()

# Function to fetch historical prices for a given symbol
def fetch_historical_prices(id):
    url = f"https://api.coingecko.com/api/v3/coins/{id}/market_chart"
    params = {
        'vs_currency': 'usd',
        'days': '1',  # '1' fetches historical data for the last day
        'api_key': 'CG-JuUQSuHgVZZ3TjQXrvi8BokL'
    }
    response = requests.get(url, params=params)
    data = response.json()

    prices = data['prices']

    filtered_prices = [(datetime.utcfromtimestamp(price[0] / 1000).replace(tzinfo=None), price[1])
                       for price in prices ]

    print(filtered_prices)
    return filtered_prices

# Iterate through each coin, fetch its historical prices, and upsert them into the prices table
for coin_id, symbol in coins:
    print(coin_id)
    print(symbol)
    prices = fetch_historical_prices(coin_id)
    last_price_date = None
    last_price = None
    second_last_price_date = None
    second_last_price = None

    # Fetch the last few rows from the prices table
    cursor.execute("SELECT price_date::timestamp price_date, price FROM coingecko.prices WHERE coin_id=%s ORDER BY price_date DESC LIMIT 1",
                   (coin_id,))
    rows = cursor.fetchone()
    connection.commit()
    print(rows)
    for price_date, price in prices:
        print(price)
        print(price_date)

        if price_date > rows[0]:
            # The price is not in the database, so we can insert it
            cursor.execute("INSERT INTO coingecko.prices (coin_id, price_date, price) VALUES (%s, %s, %s)",
                            (coin_id, price_date, price))
            connection.commit()
            print('row inserted')
    # Commit the transactions and close the connection
    connection.commit()
    time.sleep(2)
cursor.close()
connection.close()

print("Price data update complete.")
