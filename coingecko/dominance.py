import os
import psycopg2
import pgpasslib
from pycoingecko import CoinGeckoAPI
from datetime import timedelta
from datetime import datetime

cg = CoinGeckoAPI(api_key='CG-JuUQSuHgVZZ3TjQXrvi8BokL')

# Ensure you have Environment Variables set
host =                  os.environ['GLINT_HOST']
dbname =                os.environ['GLINT_DBNAME']
user =                  os.environ['GLINT_USER']
port =                  os.environ['GLINT_PORT']

def fetch_and_insert_newer_btc_dominance_data():
    # Connect to the database
    passw = pgpasslib.getpass(host,
        5439,
        dbname,
        user)
    conn = psycopg2.connect(
        host=host,
        user=user,
        dbname=dbname,
        password=passw,
        port=port,
        connect_timeout=500)
    cursor = conn.cursor()

    # Fetch the latest date from the table
    query = "SELECT MAX(bitcoin_dominance_timestamp::timestamp) FROM bitcoin_schema.dominance"
    cursor.execute(query)
    result = cursor.fetchone()
    last_date = result[0] if result else None

    # Close the connection
    cursor.close()
    conn.close()

    # If there's no data in the table, start from 2013
    if not last_date:
        start_date = datetime(2013, 1, 1)
    else:
        start_date = last_date + timedelta(days=1)

    end_date = datetime.now()

    while start_date < end_date:
        # Calculate the end of the month
        next_month_start = (start_date + timedelta(days=32)).replace(day=1)

        # Get market dominance for each month
        market_data = cg.get_coin_market_chart_range_by_id(
            id='bitcoin',
            vs_currency='usd',
            from_timestamp=start_date.timestamp(),
            to_timestamp=min(next_month_start, end_date).timestamp(),
            localization=False
        )

        market_caps = market_data['market_caps']

        # Insert data into database right away
        formatted_data = [(datetime.utcfromtimestamp(item[0] / 1000), item[1]) for item in market_caps]

        insert_into_postgres(formatted_data)

        # Move to the next month
        start_date = next_month_start

if __name__ == "__main__":
    fetch_and_insert_newer_btc_dominance_data()
