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
    url = f"https://api.coingecko.com/api/v3/coins/{id}/market_chart"
    params = {
        'vs_currency': 'usd',
        'days': 90,
        'api_key': 'CG-JuUQSuHgVZZ3TjQXrvi8BokL'
    }
    response = requests.get(url, params=params)
    data = response.json()

    filtered_prices = []

    if 'prices' in data:
        prices = data['prices']

        filtered_prices = [(datetime.utcfromtimestamp(price[0] / 1000).date(), price[1])
                        for price in prices ]
    else:
        print(data)
    return filtered_prices

# Fetch the list of top coins from your Redshift table
cursor.execute("SELECT coin_id, MAX(price_date) FROM coingecko.prices GROUP BY coin_id HAVING max(price_date) < sysdate - '1 week'::interval")
coins = cursor.fetchall()
connection.commit()

counter = 0
# Iterate through each coin, fetch its historical prices, and insert them into the prices table
for coin_id, latest_date in coins:
    print(coin_id)
    if latest_date is None:
        start_date = datetime(2015, 1, 1)  # Default start date if no data exists
    else:
        start_date = latest_date 

    prices = fetch_historical_prices(coin_id, start_date)
    values = ""
    prev_price = None
    for price_date, price in prices:
        if price_date == (latest_date+timedelta(days=1)).date():
            if prev_price is not None:
                values += ",('%(n)s','%(d)s', %(p)s)" % {"n": coin_id, "d": (latest_date+timedelta(days=1)).date(), "p": prev_price}
            latest_date+= timedelta(days=1)
        prev_price = price
    insert_cmd = "INSERT INTO coingecko.prices (coin_id, price_date, price) VALUES %(n)s" % {"n": values[1:]}
    if len(values) > 6:
        cursor.execute(insert_cmd)
        # Commit the transactions and close the connection
        connection.commit()
    counter+=1
    time.sleep(1)
    if counter%3 == 0:
        time.sleep(60)
    elif counter%10 == 0:
        time.sleep(360)
cursor.close()
connection.close()

print("Price data backfill complete.")
