import os
import psycopg2
import pgpasslib
import numbers
import json
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
file = open('pools_fact.json')
elems = json.load(file)
counter = 1
insert_clause = """INSERT INTO defillama.pools SELECT """
values = ''
for elem in elems:
    print(elem)
    if 'data' in elem:
      chain = elem['name'] 
      project = elem['project']
      symbol = elem['symbol']
      values = values + """,
        %(a)s,
        '%(b)s',
        '%(c)s';
      """ % {
          "a": name,
          "b": project,
          "c": symbol
      }
      counter+=1
      print(insert_clause + values[1:])
      cur.execute(insert_clause + values[1:])
      conn.commit()
      values = ''
values = ''
file.close()
print('%(s)s rows inserted' % {"s": counter})