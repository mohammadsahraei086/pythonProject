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
                WHERE (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0) >= 1
                AND ((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000 > 15
                AND ((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000 > 5
                AND low < day_ul
                AND close = day_ul
                ORDER BY (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0)""")

filtered_stocks = [stock[0] for stock in cursor.fetchall()]
print(filtered_stocks)
selected_stocks = []
for stock in filtered_stocks:
    cursor.execute(sql.SQL("""WITH temp1 AS
                                    (SELECT timestamp::date, max(i_buy_per_capita) as High,
                                    min(i_buy_per_capita) as Low
                                    FROM {0} 
                                    GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 1)

                                ,temp2 AS
                                    (WITH temp_first AS 
                                        (SELECT timestamp::date, i_buy_per_capita
                                        FROM {0}
                                        WHERE i_buy_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, first_value(i_buy_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Open
                                    FROM temp_first ORDER BY timestamp::date DESC LIMIT 1)

                                ,temp3 AS
                                    (WITH temp_last AS 
                                        (SELECT timestamp::date, i_buy_per_capita
                                        FROM {0}
                                        WHERE i_buy_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, last_value(i_buy_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Close
                                    FROM temp_last ORDER BY timestamp::date DESC LIMIT 1)

                                SELECT temp1.High, temp1.Low, temp2.Open, temp3.Close
                                FROM temp1
                                INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                                INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                                ;""").format(sql.Identifier(stock.strip() + '-سرانه')))

    output1 = cursor.fetchall()[0]

    #########################################################################################

    cursor.execute(sql.SQL("""WITH temp1 AS
                                    (SELECT timestamp::date, max(i_sell_per_capita) as High,
                                    min(i_sell_per_capita) as Low
                                    FROM {0} 
                                    GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 1)

                                ,temp2 AS
                                    (WITH temp_first AS 
                                        (SELECT timestamp::date, i_sell_per_capita
                                        FROM {0}
                                        WHERE i_sell_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, first_value(i_sell_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Open
                                    FROM temp_first ORDER BY timestamp::date DESC LIMIT 1)

                                ,temp3 AS
                                    (WITH temp_last AS 
                                        (SELECT timestamp::date, i_sell_per_capita
                                        FROM {0}
                                        WHERE i_sell_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, last_value(i_sell_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Close
                                    FROM temp_last ORDER BY timestamp::date DESC LIMIT 1)

                                SELECT temp1.High, temp1.Low, temp2.Open, temp3.Close
                                FROM temp1
                                INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                                INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                                ;""").format(sql.Identifier(stock.strip() + '-سرانه')))

    output2 = cursor.fetchall()[0]

    if output1[3] > 1.3*output1[2] and output2[2] > 1.2*output2[3]\
            and output1[3]-output1[2] > 2 * (output1[0]-output1[3])\
            and output2[2]-output2[3] > 2 * (output2[3]-output2[1]):
        selected_stocks.append(stock)

print(selected_stocks)

cursor.close()
conn.commit()
conn.close()
