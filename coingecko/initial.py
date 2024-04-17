from pycoingecko import CoinGeckoAPI
import psycopg2
import pgpasslib
from datetime import timedelta
from datetime import datetime

cg = CoinGeckoAPI(api_key='CG-JuUQSuHgVZZ3TjQXrvi8BokL')


DB_HOST = 'blockscope-etl.ckbpm1pslz3a.eu-central-1.rds.amazonaws.com'
DB_NAME = 'blockscope'
DB_USER = 'master'
DB_PASS = pgpasslib.getpass(DB_HOST, 5432,DB_NAME, DB_USER)

# Fetch BTC dominance data from CoinGecko using pycoingecko

def fetch_and_insert_btc_dominance_since_2013():
    start_date = datetime(2013, 1, 1)
    end_date = datetime.now()

    while start_date < end_date:
        # Calculate the end of the month
        next_month_start = (start_date + timedelta(days=32)).replace(day=1)

        print(start_date)
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


# Insert data into PostgreSQL
def insert_into_postgres(data):
    # Connect to the database
    connection = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = connection.cursor()

    print('inserting..')
    # Insert data
    query = "INSERT INTO btc_dominance (timestamp, dominance) VALUES (%s, %s)"
    cursor.executemany(query, data)

    # Commit and close
    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    fetch_and_insert_btc_dominance_since_2013()
