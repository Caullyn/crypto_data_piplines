import os
import sys
import requests
import traceback
import psycopg2
import pgpasslib
import json
from datetime import datetime
from psycopg2.extras import RealDictCursor


host = os.environ['GLINT_HOST']
dbname = os.environ['GLINT_DBNAME']
user = os.environ['GLINT_USER']
port = os.environ['GLINT_PORT']
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
cur = conn.cursor(cursor_factory=RealDictCursor)

# Define the API endpoint for DeFiLlama
defillama_api_url = "https://stablecoins.llama.fi/"
counter = 1
insert_clause = """INSERT INTO defillama.stablecoincharts_all (
  stablecoincharts_all_time ,
  coin_id,
  name,
  totalCirculating ,
  totalUnreleased ,
  totalCirculatingUSD ,
  totalMintedUSD ,
  totalBridgedToUSD ) VALUES """
values = ''
headers = {'Content-Type': 'application/json'}

# Get the latest date in the stablecoincharts_all table
cur.execute("""
SELECT MAX(stablecoincharts_all_time)::timestamp stablecoincharts_all_time, coin_id, name
  FROM defillama.stablecoincharts_all 
 GROUP BY coin_id, name
 ORDER BY coin_id""")
last_dates = cur.fetchall()
conn.commit()
for last_date in last_dates:
  try:
    response = requests.get(f"{defillama_api_url}stablecoincharts/all?stablecoin={last_date['coin_id']}", headers=headers)
    data = response.json()
    error_text = ""
    if 'statusCode' in data:
      print('ERROR: %(e)s' % {"e": data['body']})
      url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=stablecoins_all " + data['body']
      print(url_req)
      results = requests.post(url_req)
      print(results.json())
    else:
      for elem in data:
        stablecoin_time = datetime.fromtimestamp(int(elem['date']))
        if stablecoin_time > last_date['stablecoincharts_all_time'] and \
        'peggedUSD' in elem['totalCirculating']:
          totalCirculating  = elem['totalCirculating']['peggedUSD'] 
          totalUnreleased  = elem['totalUnreleased']['peggedUSD']
          totalCirculatingUSD  = elem['totalCirculatingUSD']['peggedUSD']
          totalMintedUSD  = elem['totalMintedUSD']['peggedUSD']
          totalBridgedToUSD = elem['totalBridgedToUSD']['peggedUSD']
          values = values + """(
          '%(a)s'::timestamptz,
          %(g)s,
          '%(h)s',
          %(b)s,
          %(c)s,
          %(d)s,
          %(e)s,
          %(f)s),""" % {
              "a": stablecoin_time,
              "g": last_date['coin_id'],
              "h": last_date['name'],
              "b": totalCirculating,
              "c": totalUnreleased,
              "d": totalCirculatingUSD,
              "e": totalMintedUSD,
              "f": totalBridgedToUSD
          }
          counter+=1
          if len(values) > 3000:
            cur.execute(insert_clause + values[:-1])
            conn.commit()
            values = ""
      if len(values) > 7:
        cur.execute(insert_clause + values[:-1])
        conn.commit()
  except Exception as e:
    error_text = f"Unexpected database connection error: {str(e)}"
    stack_trace = "".join(traceback.format_exception(*sys.exc_info()))
    url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=stablecoins_all " + error_text + " " + stack_trace
    print(error_text)
    print(stack_trace)
    results = requests.post(url_req)
    print(results.json())
  finally:
    conn.close()
    conn = psycopg2.connect(
      host=host,
      user=user,
      dbname=dbname,
      password=passw,
      port=port,
      connect_timeout=500)
    cur = conn.cursor(cursor_factory=RealDictCursor)
  values = ""

print("Job completed %(t)s" % {"t": datetime.now()})