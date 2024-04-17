import os 
import psycopg2
import pgpasslib
import time
import requests
from pycoingecko import CoinGeckoAPI
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from random import randrange

# Ensure you have Environment Variables set
host =                                    os.environ['GLINT_HOST']
dbname =                                os.environ['GLINT_DBNAME']
user =                                    os.environ['GLINT_USER']
port =                                    os.environ['GLINT_PORT']
coingecko_api_key =         os.environ['COINGECKO_API_KEY']

# Connect to Redshift
passw = pgpasslib.getpass(host,
    5439,
    dbname,
    user)
connection = psycopg2.connect(
    host=host,
    user=user,
    dbname=dbname,
    password=passw,
    port=port,
    connect_timeout=500)
cursor = connection.cursor(cursor_factory=RealDictCursor)
        
coins_cmd = """
SELECT token_id, max(market_cap_time)::timestamp market_cap_time FROM coingecko.market_cap GROUP BY token_id havinG max(market_cap_time)::timestamp < sysdate - '2 days'::interval
    """
cursor.execute(coins_cmd)
next_coins = cursor.fetchall()
connection.commit()
counter = 0
for coin in next_coins:
    print(coin)
    cur_date = coin['market_cap_time']
    continue_calls = True
    while cur_date < datetime.today():
        print(cur_date)
        # try:
        url = "https://api.coingecko.com/api/v3/coins/%(t)s/history" % {"t": coin['token_id']}
        params = {
            'x-cg-demo-api-key': 'CG-vQCWhk3Vt1rZ9MvjNcTSyWz7',
            'date': datetime.strftime(cur_date,'%d-%m-%Y')
        }
        print(params)
        history_data = requests.get(url, params=params)
        # history_data = cg.get_coin_history_by_id(coin['id'],datetime.strftime(cur_date,'%d-%m-%Y'))
        print(history_data)
        print(history_data.text)
        rand_sleep = randrange(10)
        if 'market_data' in history_data:
            mcap_insert_cmd = """
            INSERT INTO coingecko.market_cap (
                    token_id, token_symbol, current_price, market_cap, total_volume, facebook_likes, twitter_followers, reddit_average_posts_48h, reddit_average_comments_48h, reddit_subscribers, reddit_accounts_active_48h, forks, stars, subscribers, total_issues, closed_issues, pull_requests_merged, pull_request_contributors, commit_count_4_weeks, alexa_rank, bing_matches, mcap_date)
                    SELECT 
                        '%(a)s',
            '%(q)s',
            '%(w)s',
            %(e)s,
            %(r)s,
            %(t)s,
            %(y)s,
            %(u)s,
            %(i)s,
            %(o)s,
            %(p)s,
            %(s)s,
            %(d)s,
            %(f)s,
            %(g)s,
            %(h)s,
            %(i)s,
            %(j)s,
            %(k)s,
            %(l)s,
            %(m)s,
            '%(n)s';
                    """ % {
                    "a": coin['token_id'],
            "q": coin['symbol'],
            "w": history_data['market_data']['current_price']['usd'],
            "e": history_data['market_data']['market_cap']['usd'],
            "r": history_data['market_data']['total_volume']['usd'],
            "t": int(history_data['community_data']['facebook_likes'] or 0),
            "y": int(history_data['community_data']['twitter_followers'] or 0),
            "u": int(history_data['community_data']['reddit_average_posts_48h'] or 0),
            "i": int(history_data['community_data']['reddit_average_comments_48h'] or 0),
            "o": int(history_data['community_data']['reddit_subscribers'] or 0),
            "p": float(history_data['community_data']['reddit_accounts_active_48h'] or 0),
            "s": int(history_data['developer_data']['forks'] or 0),
            "d": int(history_data['developer_data']['stars'] or 0),
            "f": int(history_data['developer_data']['subscribers'] or 0),
            "g": int(history_data['developer_data']['total_issues'] or 0),
            "h": int(history_data['developer_data']['closed_issues'] or 0),
            "i": int(history_data['developer_data']['pull_requests_merged'] or 0),
            "j": int(history_data['developer_data']['pull_request_contributors'] or 0),
            "k": int(history_data['developer_data']['commit_count_4_weeks'] or 0),
            "l": int(history_data['public_interest_stats']['alexa_rank'] or 0),
            "m": int(history_data['public_interest_stats']['bing_matches'] or 0),
            "n": cur_date
                    }
            # print(mcap_insert_cmd)
            cursor.execute(mcap_insert_cmd)
            connection.commit()

        counter+=1
        time.sleep(1)
        if counter%3 == 0:
            time.sleep(60)
        elif counter%10 == 0:
            time.sleep(360)
        # except Exception as e:
        #     print('error')
        #     print(e)
        #     continue_calls = False
        cur_date = cur_date + relativedelta(days=+1)

