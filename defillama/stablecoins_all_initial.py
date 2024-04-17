import requests
import json
import psycopg2
import pgpasslib
import traceback 
import sys
from datetime import datetime
from psycopg2.extras import RealDictCursor

passw = pgpasslib.getpass('blockscope-redshift-01.744103681161.eu-central-1.redshift-serverless.amazonaws.com',
  5439,
  'dev',
  'uadmin')
conn = psycopg2.connect(
  host='blockscope-redshift-01.744103681161.eu-central-1.redshift-serverless.amazonaws.com',
  user='uadmin',
  dbname='dev',
  password=passw,
  port=5439,
  connect_timeout=500)
cur = conn.cursor(cursor_factory=RealDictCursor)

# Define the API endpoint for DeFiLlama
defillama_api_url = "https://stablecoins.llama.fi/"
counter = 1
insert_clause = """INSERT INTO defillama.stablecoincharts_all (
  coin_id,
  stablecoincharts_all_time ,
  totalCirculating ,
  totalUnreleased ,
  totalCirculatingUSD ,
  totalMintedUSD ,
  totalBridgedToUSD ) SELECT """
values = ''
headers = {'Content-Type': 'application/json'}

# Get the latest date in the historical_tvl table
# cur.execute("SELECT MAX(stablecoincharts_all_time)::timestamp stablecoincharts_all_time, chain FROM defillama.stablecoincharts_all group by chain order by chain")
# last_chain_dates = cur.fetchall()
# conn.commit()
coin_id = 1
first_date = datetime(2000, 3, 1, 9, 30)
while coin_id < 139:
  try:
    coin_name_cmd = """
      SELECT name FROM defillama.stablecoins WHERE id = %(c)s""" % {"c": coin_id}
    cur.execute(coin_name_cmd)
    coin_name = cur.fetchone()
    conn.commit()

    response = requests.get(f"{defillama_api_url}stablecoincharts/all?stablecoin={coin_id}", headers=headers)
    data = response.json()
    error_text = ""
    if 'statusCode' in data:
      print('ERROR: %(e)s' % {"e": data['body']})
      url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=historical_tvl_auto " + data['body']
      print(url_req)
      # results = requests.post(url_req)
      # print(results.json())
    else:
      for elem in data:
        # print(elem)
        stablecoin_date = datetime.fromtimestamp(int(elem['date']))
        if stablecoin_date > first_date:
          totalCirculating  = elem['totalCirculating']['peggedUSD'] 
          totalUnreleased  = elem['totalUnreleased']['peggedUSD']
          totalCirculatingUSD  = elem['totalCirculatingUSD']['peggedUSD']
          totalMintedUSD  = elem['totalMintedUSD']['peggedUSD']
          totalBridgedToUSD = elem['totalBridgedToUSD']['peggedUSD']
          values = values + """,
          %(g)s,
          '%(h)s',
          '%(a)s',
          %(b)s,
          %(c)s,
          %(d)s,
          %(e)s,
          %(f)s
          """ % {
              "g": coin_id,
              "h": coin_name,
              "a": stablecoin_date,
              "b": totalCirculating,
              "c": totalUnreleased,
              "d": totalCirculatingUSD,
              "e": totalMintedUSD,
              "f": totalBridgedToUSD
          }
          counter+=1
      if len(values) > 7:
        cur.execute(insert_clause + values[1:])
        conn.commit()
  except Exception as e:
    error_text = f"Unexpected database connection error: {str(e)}"
    stack_trace = "".join(traceback.format_exception(*sys.exc_info()))
    # url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=historical_tvl_auto " + error_text + " " + stack_trace
    print(error_text)
    print(stack_trace)
    # results = requests.post(url_req)
    # print(results.json())
  finally:
    conn.close()
    conn = psycopg2.connect(
      host='blockscope-redshift-01.744103681161.eu-central-1.redshift-serverless.amazonaws.com',
      user='uadmin',
      dbname='dev',
      password=passw,
      port=5439,
      connect_timeout=500)
    cur = conn.cursor(cursor_factory=RealDictCursor)
  values = ""
  coin_id += 1
