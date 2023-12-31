from sqlalchemy import create_engine
import finpy_tse as fpy
import psycopg2 as pg
from psycopg2 import sql

engin = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/stocks_info")

stock_tuple = []
for _ in range(20):
    try:
        stock_tuple = fpy.Get_MarketWatch()
        break
    except Exception as e:
        print(e)
        continue
else:
    print("NOT Succeeded")

df = stock_tuple[0]
df.columns = df.columns.str.lower()
df.index.name = "ticker"

df.to_sql("market_watch_data", con=engin, if_exists="replace")

conn = None
try:
    conn = pg.connect(database="stocks_info", user="postgres", password="1234", host="localhost")
except Exception as e:
    print(e)

cursor = conn.cursor()
cursor.execute("""DELETE FROM market_watch_data 
                  WHERE "trade type" IN ('بلوکی', 'عمده')
                  OR market IN ('صندوق قابل معامله', 'حق تقدم بورس' ,'حق تقدم فرابورس', 'حق تقدم پایه');
                  """)

cursor.execute("""SELECT ticker FROM market_watch_data
                WHERE (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0) >= 1.5
                AND ((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000 > 25
                AND ((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000 > 5
                AND open < day_ul
                AND close = day_ul
                ORDER BY (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0)""")

filtered_stocks = [stock[0] for stock in cursor.fetchall()]
print(filtered_stocks)

cursor.close()
conn.commit()
conn.close()
