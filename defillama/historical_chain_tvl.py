import os
import sys
import requests
import psycopg2
import pgpasslib
import traceback 
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
defillama_api_url = "https://api.llama.fi/v2/"

# Create insert clauses
insert_chain_clause ="INSERT INTO defillama.historical_chain_tvl (chain, historical_chain_tvl_time, tvl) VALUES "
# Get the latest date in the historical_tvl table
cur.execute("SELECT MAX(historical_chain_tvl_time)::timestamp historical_chain_tvl_time, chain FROM defillama.historical_chain_tvl group by chain order by chain")
last_chain_dates = cur.fetchall()
conn.commit()

counter = 0
chain_values = ""
headers = {'Content-Type': 'application/json'}

for chain in last_chain_dates:
  try:
    response = requests.get(f"{defillama_api_url}historicalChainTvl/{chain['chain']}", headers=headers)
    data = response.json()
    error_text = ""
    if 'statusCode' in data:
      if data['body'] ==  '{"message":"There is no chain with that name"}':
        error_text = "There is no chain with that name"
      else:
        print('ERROR: %(e)s' % {"e": data['body']})
        url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=historical_tvl_auto " + data['body']
        print(url_req)
        results = requests.post(url_req)
        print(results.json())
    else:
      for elem in data:
        tvl_date = datetime.fromtimestamp(elem['date'])
        if tvl_date > chain['historical_chain_tvl_time']:
          chain_values += ",('%(n)s','%(d)s', %(t)s)" % {"n": chain['chain'], "d": tvl_date, "t": elem['tvl']}
          counter+=1
      if len(chain_values) > 7:
        cur.execute(insert_chain_clause + chain_values[1:])
        conn.commit()
  except Exception as e:
    error_text = f"Unexpected database connection error: {str(e)}"
    stack_trace = "".join(traceback.format_exception(*sys.exc_info()))
    url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=historical_tvl_auto " + error_text + " " + stack_trace
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
  chain_values = ""
conn.close()
print('%(s)s rows inserted' % {"s": counter})