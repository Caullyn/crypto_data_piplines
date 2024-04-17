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
api_key = os.environ['COINGECKO_API_KEY']

# Connect to Redshift
passw = pgpasslib.getpass(host, port, dbname, user)
connection = psycopg2.connect(
    host=host,
    user=user,
    dbname=dbname,
    password=passw,
    port=port,
    connect_timeout=500)
cursor = connection.cursor()

# Function to create a temporary staging table
def create_staging_table():
    cursor.execute("DROP TABLE IF EXISTS coingecko_staging;")
    connection.commit()
    time.sleep(5)
    cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS coingecko_staging (LIKE coingecko.prices);
    """)
    connection.commit()

# Function to fetch the last price date for each coin
def get_last_price_date(coin_id):
    cursor.execute("SELECT MAX(price_date)::timestamp FROM coingecko.prices WHERE coin_id = %s", (coin_id,))
    result = cursor.fetchone()
    return result[0] if result[0] else datetime(2000, 1, 1).date()

# Function to fetch historical prices for a given symbol, since the last recorded date
def fetch_historical_prices(id, start_date):
    days = (datetime.utcnow().date() - start_date.date()).days
    if days == 0:
      days = 1
    url = f"https://api.coingecko.com/api/v3/coins/{id}/market_chart"
    params = {
        'vs_currency': 'usd',
        'days': days,
        'api_key': api_key  
    }
    response = requests.get(url, params=params)
    data = response.json()

    print(data)
    prices = data['prices']
    # Use a dictionary to store the last price of each date
    last_prices_by_date = {}
    for price in prices:
        price_date = datetime.utcfromtimestamp(price[0] / 1000).date()
        # Ensure the price date is after the start date
        if price_date > start_date.date():
            last_prices_by_date[price_date] = price[1]

    # Convert the dictionary back to a list of tuples and sort by date
    filtered_prices = sorted([(date, price) for date, price in last_prices_by_date.items()])
    print(id)
    print(filtered_prices)
    return filtered_prices

# Upsert data
def upsert_data(coin_id, prices):
    # Insert data into the staging table
    for price_date, price in prices:
        cursor.execute("INSERT INTO coingecko_staging (coin_id, price_date, price) VALUES (%s, %s, %s)",
                       (coin_id, price_date, price))
        connection.commit()

    # Delete existing records that overlap with the staging data
    cursor.execute("""
        DELETE FROM coingecko.prices
        USING coingecko_staging
        WHERE coingecko.prices.coin_id = coingecko_staging.coin_id
        AND coingecko.prices.price_date::date = coingecko_staging.price_date::date;
    """)
    connection.commit()
    
    # Insert new data from staging into the target table
    cursor.execute("""
        INSERT INTO coingecko.prices (coin_id, price_date, price)
        SELECT coin_id, price_date, price
        FROM coingecko_staging
        WHERE coin_id = '%(c)s';
    """ % {"c": coin_id})
    connection.commit()

create_staging_table()  # Prepare the staging table

# Fetch the list of top coins from your Redshift table
cursor.execute("SELECT id, symbol FROM coingecko.top_coins ")
coins = cursor.fetchall()

sleeper = 1
for coin_id, symbol in coins:
    last_price_date = get_last_price_date(coin_id)
    prices = fetch_historical_prices(coin_id, last_price_date)
    upsert_data(coin_id, prices)
    sleeper+=1
    if sleeper>2:
      time.sleep(61)
      print('sleeping to avoid rate limits....')
      sleeper=1

# Commit the transactions and close the connection
connection.commit()
cursor.close()
connection.close()

print("Price data backfill complete.")
