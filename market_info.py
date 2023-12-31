import psycopg2 as pg
from psycopg2 import sql
import pandas as pd
import plotly.graph_objects as go
import jdatetime
from plotly.subplots import make_subplots
import plotly.io as io

io.renderers.default = "firefox"

conn = None
try:
    conn = pg.connect(database="stocks_info", user="postgres", password="1234", host="localhost")
except Exception as e:
    print(e)

cursor = conn.cursor()

market = ['market_sarane', 'large_sarane']


for table in market:
    cursor.execute(sql.SQL("""WITH temp1 AS
                                (SELECT timestamp::date, max(i_buy_pow) as High, min(i_buy_pow) as Low
                                FROM {0} 
                                GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 60)

                            ,temp2 AS
                                (WITH temp_first AS 
                                    (SELECT timestamp::date, i_buy_pow
                                    FROM {0}
                                    WHERE i_buy_pow IS NOT NULL)
                                SELECT DISTINCT timestamp::date, first_value(i_buy_pow)
                                over(PARTITION BY timestamp::date ORDER BY timestamp) AS Open
                                FROM temp_first ORDER BY timestamp::date DESC LIMIT 60)

                            ,temp3 AS
                                (WITH temp_last AS 
                                    (SELECT timestamp::date, i_buy_pow
                                    FROM {0}
                                    WHERE i_buy_pow IS NOT NULL)
                                SELECT DISTINCT timestamp::date, last_value(i_buy_pow)
                                over(PARTITION BY timestamp::date ORDER BY timestamp) AS Close
                                FROM temp_last ORDER BY timestamp::date DESC LIMIT 60)

                            SELECT temp1.timestamp::date, temp1.High, temp1.Low, temp2.Open, temp3.Close
                            FROM temp1
                            INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                            INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                            ;""").format(sql.Identifier(table)))

    output1 = cursor.fetchall()

    #########################################################################################

    cursor.execute(sql.SQL("""WITH temp1 AS
                                    (SELECT timestamp::date, max(i_buy_per_capita) as High,
                                    min(i_buy_per_capita) as Low
                                    FROM {0} 
                                    GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 60)

                                ,temp2 AS
                                    (WITH temp_first AS 
                                        (SELECT timestamp::date, i_buy_per_capita
                                        FROM {0}
                                        WHERE i_buy_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, first_value(i_buy_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Open
                                    FROM temp_first ORDER BY timestamp::date DESC LIMIT 60)

                                ,temp3 AS
                                    (WITH temp_last AS 
                                        (SELECT timestamp::date, i_buy_per_capita
                                        FROM {0}
                                        WHERE i_buy_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, last_value(i_buy_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Close
                                    FROM temp_last ORDER BY timestamp::date DESC LIMIT 60)

                                SELECT temp1.timestamp::date, temp1.High, temp1.Low, temp2.Open, temp3.Close
                                FROM temp1
                                INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                                INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                                ;""").format(sql.Identifier(table)))

    output3 = cursor.fetchall()

    #########################################################################################

    cursor.execute(sql.SQL("""WITH temp1 AS
                                    (SELECT timestamp::date, max(i_sell_per_capita) as High,
                                    min(i_sell_per_capita) as Low
                                    FROM {0} 
                                    GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 60)

                                ,temp2 AS
                                    (WITH temp_first AS 
                                        (SELECT timestamp::date, i_sell_per_capita
                                        FROM {0}
                                        WHERE i_sell_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, first_value(i_sell_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Open
                                    FROM temp_first ORDER BY timestamp::date DESC LIMIT 60)

                                ,temp3 AS
                                    (WITH temp_last AS 
                                        (SELECT timestamp::date, i_sell_per_capita
                                        FROM {0}
                                        WHERE i_sell_per_capita IS NOT NULL)
                                    SELECT DISTINCT timestamp::date, last_value(i_sell_per_capita)
                                    over(PARTITION BY timestamp::date ORDER BY timestamp) AS Close
                                    FROM temp_last ORDER BY timestamp::date DESC LIMIT 60)

                                SELECT temp1.timestamp::date, temp1.High, temp1.Low, temp2.Open, temp3.Close
                                FROM temp1
                                INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                                INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                                ;""").format(sql.Identifier(table)))

    output4 = cursor.fetchall()

    #########################################################################################

    cursor.execute(sql.SQL("""SELECT timestamp, i_buy_pow, i_buy_per_capita, i_sell_per_capita, i_entered_money
                                FROM {0} WHERE 
                                timestamp::date = (SELECT DISTINCT timestamp::date
                                FROM {0} ORDER BY timestamp::date DESC LIMIT 1)
                                ;""").format(sql.Identifier(table)))

    output5 = cursor.fetchall()

    #########################################################################################

    dataframe1 = pd.DataFrame(output1, columns=['date', 'High', 'Low', 'Open', 'Close'])
    dataframe3 = pd.DataFrame(output3, columns=['date', 'High', 'Low', 'Open', 'Close'])
    dataframe4 = pd.DataFrame(output4, columns=['date', 'High', 'Low', 'Open', 'Close'])
    dataframe5 = pd.DataFrame(output5, columns=['time', 'i_buy_pow', 'i_buy_per_capita',
                                                'i_sell_per_capita', 'i_entered_money'])

    dataframe1['date'] = dataframe1['date'].apply(lambda x: jdatetime.date(x.year, x.month, x.day).togregorian())
    dataframe3['date'] = dataframe3['date'].apply(lambda x: jdatetime.date(x.year, x.month, x.day).togregorian())
    dataframe4['date'] = dataframe4['date'].apply(lambda x: jdatetime.date(x.year, x.month, x.day).togregorian())
    dataframe5['time'] = dataframe5['time'].apply(lambda x:
                                                  jdatetime.datetime(x.year, x.month, x.day, x.hour, x.minute)
                                                  .togregorian())

    dataframe1['date'] = pd.to_datetime(dataframe1['date'])
    dataframe3['date'] = pd.to_datetime(dataframe3['date'])
    dataframe4['date'] = pd.to_datetime(dataframe4['date'])

    date_all1 = pd.date_range(start=dataframe1['date'].iloc[-1], end=dataframe1['date'].iloc[0], freq='1D')
    date_break1 = [jdatetime.date.fromgregorian(date=d) for d in date_all1 if d not in dataframe1['date'].to_list()]
    date_all3 = pd.date_range(start=dataframe3['date'].iloc[0], end=dataframe3['date'].iloc[-1], freq='1D')
    date_break3 = [jdatetime.date.fromgregorian(date=d) for d in date_all3 if d not in dataframe3['date'].to_list()]
    date_all4 = pd.date_range(start=dataframe4['date'].iloc[0], end=dataframe4['date'].iloc[-1], freq='1D')
    date_break4 = [jdatetime.date.fromgregorian(date=d) for d in date_all4 if d not in dataframe4['date'].to_list()]

    ##############################################################################################

    fig1 = go.Figure(data=[go.Candlestick(x=dataframe1['date'],
                                          open=dataframe1['Open'],
                                          high=dataframe1['High'],
                                          low=dataframe1['Low'],
                                          close=dataframe1['Close'])])

    fig1.update_xaxes(calendar='jalali', rangeslider_visible=False,
                      rangebreaks=[dict(dvalue=24 * 60 * 60 * 1000, values=date_break1)])
    fig1.update_layout(title=table, xaxis=dict(tickmode='linear', tick0=dataframe1['date'].iloc[0], dtick=86400000))

    #################################################################################################

    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
    fig3.add_trace(go.Candlestick(x=dataframe3['date'],
                                  open=dataframe3['Open'],
                                  high=dataframe3['High'],
                                  low=dataframe3['Low'],
                                  close=dataframe3['Close']))

    fig3.add_trace(go.Candlestick(x=dataframe4['date'],
                                  open=dataframe4['Open'],
                                  high=dataframe4['High'],
                                  low=dataframe4['Low'],
                                  close=dataframe4['Close']))

    fig3.update_xaxes(calendar='jalali', rangeslider_visible=False,
                      rangebreaks=[dict(dvalue=24 * 60 * 60 * 1000, values=date_break1)])
    fig3.update_layout(title=table, xaxis=dict(tickmode='linear', tick0=dataframe3['date'].iloc[0], dtick=86400000))

    fig3.data[0].increasing.fillcolor = 'green'
    fig3.data[0].decreasing.fillcolor = 'red'
    fig3.data[1].increasing.fillcolor = 'blue'
    fig3.data[1].decreasing.fillcolor = 'orange'
    fig3.data[0].increasing.line.color = 'green'
    fig3.data[0].decreasing.line.color = 'red'
    fig3.data[1].increasing.line.color = 'blue'
    fig3.data[1].decreasing.line.color = 'orange'
    fig3.data[0].opacity = 0.5
    fig3.data[1].opacity = 0.5

    ################################################################################################

    fig4 = make_subplots(specs=[[{"secondary_y": True}]])
    fig4.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_entered_money'], name="i_entered_money"), secondary_y=True)
    fig4.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_buy_pow'],  name="i_buy_pow", line_color='green'))

    fig5 = make_subplots(specs=[[{"secondary_y": True}]])
    fig5.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_buy_per_capita'],
                              name="i_buy_per_capita", line_color='green'))
    fig5.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_sell_per_capita'],
                              name="i_entered_money", line_color='red'))
    fig5.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_entered_money'],
                              name="i_sell_per_capita", line_color='blue'), secondary_y=True)

    fig4.update_xaxes(calendar='jalali')
    fig5.update_xaxes(calendar='jalali')
    fig4.update_layout(title=table)
    fig5.update_layout(title=table)

    ################################################################################################

    fig1.show()
    fig3.show()
    fig4.show()
    fig5.show()

    breakpoint_test = 0

cursor.close()
conn.commit()
conn.close()
