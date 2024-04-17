import os
import psycopg2
import pgpasslib
import requests
import pandas as pd
import psycopg2
import json
import time
import random, string
from io import StringIO
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

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

def fetch_coins(connection):
    cursor = connection.cursor()
    # Fetch the list of top coins from your Redshift table
    cursor.execute("SELECT DISTINCT coin_id FROM coingecko.prices")
    coins = cursor.fetchall()
    connection.commit()
    cursor.close()  ``

    return coins 

# Function to fetch historical prices for a given symbol
def fetch_prices(id):
    url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=USD&per_page=250&page=1&locale=en&precision=11&ids=" + ids
    params = {
        'x-cg-demo-api-key': 'CG-vQCWhk3Vt1rZ9MvjNcTSyWz7'
    }
    response = requests.get(url, params=params)
    data = response.json()

    return data

def put_latest(connection, values):
    cursor = connection.cursor()
    table_random_string = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    create_table_cmd = """
    CREATE TABLE coingecko.prices_latest_%(r)s (
        coin_id VARCHAR(128),
        name VARCHAR(128),
        current_price NUMERIC(18,11),
        market_cap NUMERIC(22,8),
        market_cap_rank INT
    )""" % {"r": table_random_string}
    cursor.execute(create_table_cmd)
    connection.commit()

    insert_cmd = """INSERT INTO coingecko.prices_latest_%(r)s (    coin_id,
        name,
        current_price,
        market_cap,
        market_cap_rank) 
        VALUES %(n)s""" % {"r": table_random_string, "n": values[1:]}
    cursor.execute(insert_cmd)
    connection.commit()

    drop_tables_cmd = """
    DROP TABLE IF EXISTS coingecko.prices_latest;"""
    cursor.execute(drop_tables_cmd)

    change_tables_cmd = """
    ALTER TABLE coingecko.prices_latest_%(r)s RENAME TO prices_latest;""" % {"r": table_random_string}
    cursor.execute(change_tables_cmd)
    connection.commit()

    cursor.close()
    
    return True

def put_prices(connection, prices):
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT coin_id, MAX(price_date)::date price_date FROM coingecko.prices GROUP BY coin_id")
    coins = cursor.fetchall()
    connection.commit()
    price_values = ""
    for coin in coins:
        for info in prices:
            if coin['coin_id'] == info[0]:
                if coin['price_date'] == info[1]:
                    update_cmd = """
                    UPDATE coingecko.prices set price = %(p)s
                     WHERE price_date = '%(d)s'
                       AND coin_id = '%(c)s'
                    """ % {"p": info[2],
                    "d": info[1],
                    "c": info[0]}
                    cursor.execute(update_cmd)
                    connection.commit()
                else:
                    price_values += ",('%(n)s','%(d)s', %(p)s)" % {"n": info[0], "d": info[1], "p": info[2]}

    if len(price_values) > 5:
        insert_cmd = "INSERT INTO coingecko.prices (coin_id, price_date, price) VALUES %(n)s" % {"n": price_values[1:]}
        cursor.execute(insert_cmd)
        connection.commit()
    cursor.close()

    return True


coins = fetch_coins(connection)
current_time = datetime.now()
counter = 0
values = ""
prices = []
ids = ','.join(map(str, coins)) 
ids = ids.replace(',),(',',').replace('(','').replace(',)','').replace("'","")
price_data = fetch_prices(ids)

for info in price_data:
    coin_id = info['id']
    name = info['name']
    price = info['current_price']
    market_cap = info['market_cap']
    market_cap_rank = info['market_cap_rank']
    values += ",('%(i)s','%(n)s', %(c)s, %(m)s, %(r)s)" % {"i": coin_id, "n": name, "c": price, "m": market_cap, "r": market_cap_rank}
    prices.append([coin_id, current_time.date(),  price])

latest_success = put_latest(connection, values)
price_success = put_prices(connection, prices)
connection.close()
print("latest_success %(s)s" % {"s": latest_success})
print("price_success %(s)s" % {"s": price_success})