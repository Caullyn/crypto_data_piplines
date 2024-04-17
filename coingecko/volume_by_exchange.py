import requests
import psycopg2
import pgpasslib
from datetime import datetime
from psycopg2.extras import RealDictCursor

API_BASE_URL = "https://pro-api.coingecko.com/api/v3/exchanges"
API_FREE_URL = "https://api.coingecko.com/api/v3/exchanges"
API_KEY = "CG-JuUQSuHgVZZ3TjQXrvi8BokL"  # Replace with your API key

def get_all_exchange_volumes_since_start():
    passw = pgpasslib.getpass('blockscope-redshift-01.744103681161.eu-central-1.redshift-serverless.amazonaws.com',
    5439,
    'dev',
    'uadmin'
    )
    conn = psycopg2.connect(
    host='blockscope-redshift-01.744103681161.eu-central-1.redshift-serverless.amazonaws.com',
    user='uadmin',
    dbname='dev',
    password=passw,
    port=5439,
    connect_timeout=500)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Get all exchange ids
    response = requests.get(f"{API_FREE_URL}/list")
    response.raise_for_status()
    exchanges = response.json()
    exchange_ids = [exchange['id'] for exchange in exchanges]
    # exchange_ids = ['binance']
    # Define the start and end timestamps (assuming UNIX timestamp in seconds)
    start_timestamp = int(datetime(2013, 1, 1).timestamp())
    end_timestamp = start_timestamp + 2000000

    volume_data = {}
    insert_clause = "INSERT INTO volume_by_exchange_historical_test (exchange, volume_by_exchange_historical_time, volume) VALUES "
    values = ''
    last_dates_cmd = """
    SELECT exchange, max(volume_by_exchange_historical_time) volume_by_exchange_historical_time
      FROM volume_by_exchange_historical
     GROUP BY exchange;"""
    cur.execute(last_dates_cmd)
    last_dates = cur.fetchall()
    conn.commit()
    start_timestamp = int(datetime(2013, 1, 1).timestamp())
    end_timestamp = start_timestamp + 2000000
    last_dates_length = len(last_dates)
    print(last_dates_length)
    for last_date in last_dates:
        start_timestamp = int(last_date['volume_by_exchange_historical_time'].timestamp())
        end_timestamp = start_timestamp + 2000000
        exchange_id = last_date['exchange']
        print('start_timestamp set')
        while start_timestamp < int(datetime.now().timestamp()):
            try:
                # Build the URL for the API call
                volume_url = f"{API_BASE_URL}/{exchange_id}/volume_chart/range?from={start_timestamp}&to={end_timestamp}&x_cg_pro_api_key={API_KEY}"
                print(volume_url)
                
                # Fetch the volume data
                response = requests.get(volume_url)
                response.raise_for_status()
                volume_info = response.json()
                for data in volume_info:
                    values += prev_value
                    prev_value = """('%(a)s',timestamp 'epoch' + left('%(q)s'::TEXT, 10)::INT * interval '1 second',%(w)s),""" % {
                        "a": exchange_id,
                        "q": data[0],
                        "w": data[1]
                        }
                    last_date = data[1]
                # Sleep to avoid rate limits
                # time.sleep(1)
            except Exception as e:
                print(f"Error fetching data for {exchange_id}: {e}")
                continue
            start_timestamp = end_timestamp + 1
            end_timestamp = start_timestamp + 2000000
        if len(values) > 3:
            print(insert_clause + values[:-1])
            cur.execute(insert_clause + values[:-1])
            conn.commit()
        values = ''
    return volume_data

if __name__ == "__main__":
    volumes = get_all_exchange_volumes_since_start()
    for exchange, volume_list in volumes.items():
        print(f"Exchange: {exchange}")
        for timestamp, volume in volume_list:
            date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
            print(f"Date: {date}, Volume: {volume}")
