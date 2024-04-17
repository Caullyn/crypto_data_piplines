import os
import sys
import requests
import traceback
import psycopg2
import pgpasslib
import json
import time
import random, string
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
defillama_api_url = "https://yields.llama.fi/"
counter = 1
headers = {'Content-Type': 'application/json'}

# Get the latest date in the apycharts_all table
cur.execute("""
select * from (SELECT DISTINCT chain,project,symbol,pool, ROW_NUMBER() OVER ( ORDER BY pool) AS row 
  FROM defillama.annual_percentage_yield_latest 
  ORDER BY pool) f where row > 1023 order by pool""")
pools = cur.fetchall()
conn.commit()
insert_clause = 'INSERT INTO defillama.annual_percentage_yield (chain, project, symbol, pool, tvlusd, apy, apybase, apyreward, il7d, apybase7d, annual_percentage_yield_time) VALUES '
for pool in pools:
# try:
    # response = requests.get(f"{defillama_api_url}pools/all?apy={last_date['coin_id']}", headers=headers)
    response = requests.get(f"{defillama_api_url}chart/" + pool['pool'])
    data = response.json()
    error_text = ""
    values = ''

    if 'statusCode' in data:
        print('ERROR: %(e)s' % {"e": data['body']})
        # url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=apy_latest " + data['body']
        # print(url_req)
        # results = requests.post(url_req)
    else:
        # print(str(data)[0:300])
        for name, data in data.items():
            timestamp = ""
            tvlUsd = ""
            apy = ""
            apyBase = ""
            apyReward = ""
            il7d = ""
            apyBase7d = ""
            # apy_time = datetime.fromtimestamp(int(value['date']))
            # if apy_time > last_date['apycharts_all_time'] and \
            # peggedUSD' in value['totalCirculating']:
            if name == 'data':
                for elem in data:
                    inserted = False
                    for key, value in elem.items():
                        if key == 'timestamp':
                            timestamp = value
                        if key == 'tvlUsd':
                            tvlUsd = value
                        if key == 'apy':
                            apy = value
                        if key == 'apyBase':
                            apyBase = value
                        if key == 'apyReward':
                            apyReward = value
                        if key == 'il7d':
                            il7d = value
                        if key == 'apyBase7d':
                            apyBase7d = value
                    if len(timestamp) > 1 and \
                        len(str(tvlUsd)) > 1 and \
                        len(str(apy)) > 1 and \
                        len(str(apyBase)) > 1 and \
                        len(str(apyReward)) > 1 and \
                        len(str(il7d)) > 1 and \
                        len(str(apyBase7d)) > 1 and \
                        apy is not None and \
                        not inserted:
                        values = values + """(
                            '%(y)s',
                            '%(z)s',
                            '%(a)s',
                            '%(c)s',
                            %(d)s,
                            %(e)s,
                            %(f)s,
                            %(g)s,
                            %(h)s,
                            %(i)s,
                            '%(j)s'),""" % {
                            "y": pool['chain'],
                            "z": pool['project'],
                            "a": pool['symbol'],
                            "c": pool['pool'],
                            "d": tvlUsd,
                            "e": apy,
                            "f": str(apyBase).replace('None','NULL'),
                            "g": str(apyReward).replace('None','NULL'),
                            "h": str(il7d).replace('None','NULL'),
                            "i": str(apyBase7d).replace('None','NULL'),
                            "j": timestamp,
                        }
                        counter+=1
                        inserted = True
            if len(values) > 3000:
                print(insert_clause + values[:-1])
                cur.execute(insert_clause + values[:-1])
                conn.commit()
                if counter % 9 == 0:
                    time.sleep(61)
                if counter % 19 == 0:
                    time.sleep(321)
                if counter % 99 == 0:
                    time.sleep(3000)
                values = ""
        if len(values) > 7:
            print(insert_clause + values[:-1])
            cur.execute(insert_clause + values[:-1])
            conn.commit()
            if counter % 9 == 0:
                time.sleep(61)
            if counter % 19 == 0:
                time.sleep(321)
            if counter % 99 == 0:
                time.sleep(3000)
            values = ""
# except Exception as e:
#     error_text = f"Unexpected database connection error: {str(e)}"
#     stack_trace = "".join(traceback.format_exception(*sys.exc_info()))
#     url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=apys_all " + error_text + " " + stack_trace
#     print(error_text)
#     print(stack_trace)
#     results = requests.post(url_req)
#     print(results.json())
# finally:
#     conn.close()
#     conn = psycopg2.connect(
#       host=host,
#       user=user,
#       dbname=dbname,
#       password=passw,
#       port=port,
#       connect_timeout=500)
#     cur = conn.cursor(cursor_factory=RealDictCursor)
#     values = ""
cur.close()
conn.close()
print("Job completed %(t)s" % {"t": datetime.now()})