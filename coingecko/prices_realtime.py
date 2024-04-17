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
cursor.execute("SELECT id, symbol FROM coingecko.top_coins")
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
    return filtered_prices

# Iterate through each coin, fetch its historical prices, and upsert them into the prices table
for coin_id, symbol in coins:
    prices = fetch_historical_prices(coin_id)
    values = ""
    for price_date, price in prices:
        # Check if there's already a price for today
        cursor.execute("SELECT EXISTS(SELECT 1 FROM coingecko.prices WHERE coin_id=%s AND price_date=%s)",
                       (coin_id, price_date.date()))
        exists = cursor.fetchone()[0]
        if not exists:
            # Insert if not exists
            cursor.execute("INSERT INTO coingecko.prices (coin_id, price_date, price) VALUES (%s, %s, %s)",
                           (coin_id, price_date.date(), price))
        else:
            # Update if exists
            cursor.execute("UPDATE coingecko.prices SET price=%s WHERE coin_id=%s AND price_date=%s",
                           (price, coin_id, price_date.date()))
    # Commit the transactions and close the connection
    connection.commit()
    time.sleep(30)
cursor.close()
connection.close()

print("Price data update complete.")
