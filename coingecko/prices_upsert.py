import os
import psycopg2
import pgpasslib
import requests
import time
from datetime import datetime, timedelta

# Ensure you have Environment Variables set
host = os.environ['GLINT_HOST']
dbname = os.environ['GLINT_DBNAME']
user = os.environ['GLINT_USER']
port = os.environ['GLINT_PORT']

# Connect to Redshift/PostgreSQL
passw = pgpasslib.getpass(host, port, dbname, user)
connection = psycopg2.connect(
    host=host,
    user=user,
    dbname=dbname,
    password=passw,
    port=port,
    connect_timeout=500)
cursor = connection.cursor()

# Function to fetch the last price date
def get_last_price_date(coin_id):
    cursor.execute("SELECT MAX(price_date)::timestamp FROM coingecko.prices WHERE coin_id = %s", (coin_id,))
    result = cursor.fetchone()
    return result[0] if result[0] else datetime(2000, 1, 1).date()  # Use a very early date if no data exists

# Function to fetch historical prices for a given symbol, since the last recorded date
def fetch_historical_prices(id, start_date):
    days = (datetime.utcnow().date() - start_date.date()).days
    url = f"https://api.coingecko.com/api/v3/coins/{id}/market_chart"
    params = {
        'vs_currency': 'usd',
        'days': days,
        'api_key': 'CG-JuUQSuHgVZZ3TjQXrvi8BokL'
    }
    response = requests.get(url, params=params)
    data = response.json()

    prices = data['prices']
    filtered_prices = [(datetime.utcfromtimestamp(price[0] / 1000).date(), price[1])
                       for price in prices if datetime.utcfromtimestamp(price[0] / 1000) > start_date]
    return filtered_prices

# Fetch the list of top coins from your Redshift table
cursor.execute("SELECT id, symbol FROM coingecko.top_coins")
coins = cursor.fetchall()

# Iterate through each coin, fetch its historical prices since last recorded date, and upsert them into the prices table
for coin_id, symbol in coins:
    last_price_date = get_last_price_date(coin_id)
    prices = fetch_historical_prices(coin_id, last_price_date)
    for price_date, price in prices:
        cursor.execute("""
        INSERT INTO coingecko.prices (coin_id, price_date, price) 
        VALUES (%s, %s, %s) 
        ON CONFLICT (coin_id, price_date) DO UPDATE 
        SET price = EXCLUDED.price
        """, (coin_id, price_date, price))
    time.sleep(5)

# Commit the transactions and close the connection
connection.commit()
cursor.close()
connection.close()

print("Price data backfill complete.")
