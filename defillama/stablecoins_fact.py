import json
import psycopg2
import pgpasslib
import numbers
import json
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

file = open('stablecoins_fact.json')
data = json.load(file)
counter = 1
insert_clause = """INSERT INTO defillama.stablecoins (
id, name, symbol, gecko_id, pegtype, pricesource, pegmechanism ) SELECT """
values = ''
for elem in data['peggedAssets']:
    print(elem)
    id = elem['id'] 
    name = elem['name'] 
    symbol = elem['symbol']
    gecko_id = elem['gecko_id']
    pegtype = elem['pegType']
    pricesource = elem['priceSource']
    pegmechanism = elem['pegMechanism']
    values = values + """,
      %(a)s,
      '%(b)s',
      '%(c)s',
      '%(d)s',
      '%(e)s',
      '%(f)s',
      '%(g)s';
    """ % {
        "a": id,
        "b": name,
        "c": symbol,
        "d": gecko_id,
        "e": pegtype,
        "f": pricesource,
        "g": pegmechanism
    }
    counter+=1
    print(insert_clause + values[1:])
    cur.execute(insert_clause + values[1:])
    conn.commit()
    values = ''
values = ''
file.close()
print('%(s)s rows inserted' % {"s": counter})