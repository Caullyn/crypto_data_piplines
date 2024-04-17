import os
import psycopg2
import pgpasslib
import requests
import pandas as pd
import psycopg2
import json
from io import StringIO

# cg = CoinGeckoAPI(api_key='CG-JuUQSuHgVZZ3TjQXrvi8BokL')

# Ensure you have Environment Variables set
host =                  os.environ['GLINT_HOST']
dbname =                os.environ['GLINT_DBNAME']
user =                  os.environ['GLINT_USER']
port =                  os.environ['GLINT_PORT']

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

def insert_into_redshift(df, table_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    columns = ', '.join(df.columns)
    cursor.copy_from(csv_buffer, f'coingecko.{table_name}', columns=columns, sep=',')
    connection.commit()

def get_latest_date_from_table():
    query = "SELECT MAX(last_updated) FROM coingecko.prices"
    cursor.execute(query)
    latest_date = cursor.fetchone()[0]
    return latest_date

def insert_latest_prices():
    # latest_date = get_latest_date_from_table()
    latest_date = '2020-01-01'
    url = f'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=500&page=1&sparkline=false&last_updated_at={latest_date}'
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    insert_into_redshift(df, 'prices')

def get_top_500_crypto_prices():
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=500&page=1&sparkline=false'
    response = requests.get(url)
    data = response.json()

    values = ""

    for elem in data:
        values += ",('%(n)s','%(d)s')" % {"n": elem['id'], "d": elem["symbol"]}

    cursor.execute("INSERT INTO coingecko.top_coins VALUES %(n)s" % {"n": values[1:]})
    connection.commit()

    # Insert into prices table
    # insert_into_redshift(df, 'prices')

    # Insert into top_coins table
    # top_coins_df = df[['id', 'name', 'symbol', 'market_cap_rank', 'image']]
    # insert_into_redshift(top_coins_df, 'top_coins')

    # return df

if __name__ == '__main__':
    get_top_500_crypto_prices()
    
    # To insert only the latest prices
    # insert_latest_prices()

cursor.close()
connection.close()
