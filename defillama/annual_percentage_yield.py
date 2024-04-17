import os
import sys
import requests
import traceback
import psycopg2
import pgpasslib
import json
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
# cur.execute("""
# SELECT MAX(apycharts_all_time)::timestamp apycharts_all_time, coin_id, name
#   FROM defillama.apycharts_all 
#  GROUP BY coin_id, name
#  ORDER BY coin_id""")
# last_dates = cur.fetchall()
# conn.commit()
# for last_date in last_dates:
# try:
    # response = requests.get(f"{defillama_api_url}pools/all?apy={last_date['coin_id']}", headers=headers)
response = requests.get(f"{defillama_api_url}pools")
data = response.json()
error_text = ""
table_random_string = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
create_table_cmd = """
CREATE TABLE defillama.annual_percentage_yield_latest_%(r)s (
 chain                               character varying(128)  ,
 project                             character varying(128)  ,
 symbol                              character varying(128)   ,
 pool                                character varying(128)  ,
 apy                                 numeric(22,8)           ,
 annual_percentage_yield_latest_time timestamp with time zone
)""" % {"r": table_random_string}
cur.execute(create_table_cmd)
conn.commit()
insert_clause = """INSERT INTO defillama.annual_percentage_yield_latest_%(r)s (
    chain ,
    project ,
    symbol ,
    pool ,
    apy ,
    annual_percentage_yield_latest_time 
    ) VALUES """ % {"r": table_random_string}
values = ''

if 'statusCode' in data:
    print('ERROR: %(e)s' % {"e": data['body']})
    url_req = "https://api.telegram.org/bot6429530408:AAFJaLq4xK1Tic5yKMDeeP92-bYJJ268FmQ/sendMessage?chat_id=-1002053620728&text=apy_latest " + data['body']
    print(url_req)
    results = requests.post(url_req)
else:
    # print(str(data)[0:300])
    for name, data in data.items():
        chain = ""
        project = ""
        symbol = ""
        pool = ""
        apy = ""
        # apy_time = datetime.fromtimestamp(int(value['date']))
        # if apy_time > last_date['apycharts_all_time'] and \
        # peggedUSD' in value['totalCirculating']:
        if name == 'data':
            for elem in data:
                inserted = False
                # print(elem)
                for key, value in elem.items():
                    if key == 'chain':
                        chain = value
                    if key == 'project':
                        project = value
                    if key == 'symbol':
                        symbol = value
                    if key == 'pool':
                        pool = value
                    if key == 'apy':
                        apy = value
                if len(chain) > 1 and \
                    len(project) > 1 and \
                    len(symbol) > 1 and \
                    len(pool) > 1 and \
                    apy is not None and \
                    not inserted:
                    values = values + """(
                        '%(g)s',
                        '%(h)s',
                        '%(b)s',
                        '%(c)s',
                        %(d)s,
                        sysdate),""" % {
                        "g": chain,
                        "h": project,
                        "b": symbol,
                        "c": pool,
                        "d": apy
                    }
                    counter+=1
                    inserted = True
        if len(values) > 3000:
            cur.execute(insert_clause + values[:-1])
            conn.commit()
            values = ""
    if len(values) > 7:
        cur.execute(insert_clause + values[:-1])
        conn.commit()
        values = ""
    if counter > 1:
        drop_tables_cmd = """
        DROP TABLE IF EXISTS defillama.annual_percentage_yield_latest;"""
        cur.execute(drop_tables_cmd)

        change_tables_cmd = """
        ALTER TABLE defillama.annual_percentage_yield_latest_%(r)s RENAME TO annual_percentage_yield_latest;""" % {"r": table_random_string}
        cur.execute(change_tables_cmd)
        conn.commit()
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