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
# cg = CoinGeckoAPI(api_key='CG-JuUQSuHgVZZ3TjQXrvi8BokL')

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
def fetch_historical_prices(id):
    url = f"https://api.coingecko.com/api/v3/coins/{id}/market_chart"
    params = {
        'vs_currency': 'usd',
        'days': 'max',  # 'max' fetches historical data up to the coin's inception
        'api_key': coingecko_api_key
    }
    response = requests.get(url, params=params)
    data = response.json()
    print(data)

    prices = data['prices']
    
    filtered_prices = [(datetime.utcfromtimestamp(price[0] / 1000).date(), price[1])
                       for price in prices ]
    return filtered_prices


# Fetch the list of top coins from your Redshift table
cursor.execute("SELECT id, symbol FROM coingecko.top_coins limit 1")
coins = cursor.fetchall()
connection.commit()

# Iterate through each coin, fetch its historical prices, and insert them into the prices table
for coin_id, symbol in coins:
    prices = fetch_historical_prices(coin_id)

    # Fetch the last few rows from the prices table
    cursor.execute("SELECT price_date::timestamp price_date, price FROM coingecko.prices WHERE coin_id=%s ORDER BY price_date DESC LIMIT 1",
                    (coin_id,))
    rows = cursor.fetchone()
    connection.commit()
    values = ""
    for price_date, price in prices:
        # cursor.execute("INSERT INTO coingecko.prices (coin_id, price_date, price) VALUES (%s, %s, %s)",
        #                (coin_id, price_date, price))
        if price_date > rows[0]:
            values += ",('%(n)s','%(d)s', %(p)s)" % {"n": coin_id, "d": price_date, "p": price}
    insert_cmd = "INSERT INTO coingecko.prices (coin_id, price_date, price) VALUES %(n)s" % {"n": values[1:]}
    print(insert_cmd)
    cursor.execute(insert_cmd)
    # Commit the transactions and close the connection
    connection.commit()
    time.sleep(30)
cursor.close()
connection.close()

print("Price data backfill complete.")
