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

watch = ['آواپارس' 'وجامی' 'وحکمت' 'باران' 'دپارس' 'شفا' 'رمپنا' 'دتوزیع' 'پکویر' 'بتهران' 'لپیام' 'فافق' 
         'ولیز' 'سمایه' 'بمیلا' 'فوکا' 'وپسا' 'پلاست' 'فافزا' 'سنوین' 'کیسون' 'درهآور']
watch4 = ['غشوکو' 'پلاست' 'تاپیکو' 'حپارسا' 'ومهان' 'تاپکیش' 'وبهمن' 'غالبر' ]

cursor = conn.cursor()

aali = ['وتجارت' 'کتوکا' 'ونوین' 'دفارا' 'ستران' 'کگاز' 'سیلام' 'ساوه' 'سیمرغ' 'تیپیکو' 'بمپنا' 'شپارس' 'کلوند' 
        'حگهر' 'کلر']
ali_manfi = ['غالبر' 'سیدکو' 'البرز' 'سرچشمه' 'مادیرا' 'زملارد' 'سدور' 'تبرک' 'سشرق' 'چافست' 'بزاگرس' 'حکشتی' 
             'دسانکو' 'آپ' 'ثنوسا' 'سکرما' 'ساینا']
khob = ['وایران' 'واعتبار' 'زگلدشت' 'بخاور' 'حتوکا' 'کنور' 'خفنر' 'خموتور' 'وآوا' 'غگل' 'شکلر' 'سدبیر' 'زکوثر'
        'غشهداب']

filtered = []

selected_stocks = [ 'دفرا' ]

for stock in selected_stocks:
    cursor.execute(sql.SQL("""SELECT eps, last::FLOAT4/NULLIF(eps,0),
                                volume*close::FLOAT4/10000000000, average_volume*close::FLOAT4/10000000000
                                FROM {}
                                ORDER BY date DESC LIMIT 1
                                ;""").format(sql.Identifier(stock.strip() + '-روزانه')))
    info = cursor.fetchall()[0]
    # cursor.execute("""SELECT ("market cap"/10000000000)::INT8 FROM market_watch
    #                                 WHERE ticker=%s
    #                                 ;""", (stock,))
    market_cap = [1]

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

                            SELECT temp1.timestamp::date, temp1.High, temp1.Low, temp2.Open, temp3.Close,
                            {1}.i_entered_money
                            AS Volume FROM temp1
                            INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                            INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                            INNER JOIN {1} on {1}.date=temp3.timestamp::date
                            ;""").format(sql.Identifier(stock.strip() + '-سرانه'),
                                         sql.Identifier(stock.strip() + '-روزانه')))

    output1 = cursor.fetchall()

    dataframe1 = pd.DataFrame(output1, columns=['date', 'High', 'Low', 'Open', 'Close', 'i_entered_money'])
    dataframe1['date'] = dataframe1['date'].apply(lambda x: jdatetime.date(x.year, x.month, x.day).togregorian())
    dataframe1['date'] = pd.to_datetime(dataframe1['date'])
    date_all1 = pd.date_range(start=dataframe1['date'].iloc[0], end=dataframe1['date'].iloc[-1], freq='1D')
    date_break1 = [jdatetime.date.fromgregorian(date=d) for d in date_all1 if d not in dataframe1['date'].to_list()]

    fig1 = make_subplots(specs=[[{"secondary_y": True}]])
    fig1.add_trace(go.Candlestick(x=dataframe1['date'],
                                  open=dataframe1['Open'],
                                  high=dataframe1['High'],
                                  low=dataframe1['Low'],
                                  close=dataframe1['Close']))
    fig1.add_trace(go.Scatter(x=dataframe1['date'], y=dataframe1['i_entered_money'],
                              name="i_entered_money", line_color='blue'), secondary_y=True)

    fig1.update_xaxes(calendar='jalali', rangeslider_visible=False,
                      rangebreaks=[dict(dvalue=24 * 60 * 60 * 1000, values=date_break1)])
    fig1.update_layout(title=stock, xaxis=dict(tickmode='linear', tick0=dataframe1['date'].iloc[0], dtick=86400000))

    fig1.show()

    ###################################################################################################

    cursor.execute(sql.SQL("""SELECT date, high, low, open, last, volume FROM {}
                            ORDER BY date DESC LIMIT 59
                            ;""").format(sql.Identifier(stock.strip() + '-روزانه')))

    output2 = cursor.fetchall()

    dataframe2 = pd.DataFrame(output2, columns=['date', 'High', 'Low', 'Open', 'Close', 'Volume'])
    dataframe2['date'] = dataframe2['date'].apply(lambda x: jdatetime.date(x.year, x.month, x.day).togregorian())
    dataframe2['date'] = pd.to_datetime(dataframe2['date'])
    date_all2 = pd.date_range(start=dataframe2['date'].iloc[-1], end=dataframe2['date'].iloc[0], freq='1D')
    date_break2 = [jdatetime.date.fromgregorian(date=d) for d in date_all2 if d not in dataframe2['date'].to_list()]

    fig2 = make_subplots(specs=[[{"secondary_y": True}]])
    fig2.add_trace(go.Candlestick(x=dataframe2['date'],
                                  open=dataframe2['Open'],
                                  high=dataframe2['High'],
                                  low=dataframe2['Low'],
                                  close=dataframe2['Close']))

    fig2.add_trace(go.Bar(x=dataframe2['date'],
                          y=dataframe2['Volume'],
                          marker={
                              "color": "rgba(128,128,128,0.5)",
                          }
                          ), secondary_y=True)

    fig2.update_xaxes(calendar='jalali', rangeslider_visible=False,
                      rangebreaks=[dict(dvalue=24 * 60 * 60 * 1000, values=date_break2)])
    fig2.update_layout(title=stock, xaxis=dict(tickmode='linear', tick0=dataframe2['date'].iloc[0], dtick=86400000))

    fig2.show()

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

                                SELECT temp1.timestamp::date, temp1.High, temp1.Low, temp2.Open, temp3.Close,
                                {1}.volume AS Volume FROM temp1
                                INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                                INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                                INNER JOIN {1} on {1}.date=temp3.timestamp::date
                                ;""").format(sql.Identifier(stock.strip() + '-سرانه'),
                                             sql.Identifier(stock.strip() + '-روزانه')))

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

                                SELECT temp1.timestamp::date, temp1.High, temp1.Low, temp2.Open, temp3.Close,
                                {1}.volume AS Volume FROM temp1
                                INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                                INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                                INNER JOIN {1} on {1}.date=temp3.timestamp::date
                                ;""").format(sql.Identifier(stock.strip() + '-سرانه'),
                                             sql.Identifier(stock.strip() + '-روزانه')))

    output4 = cursor.fetchall()

    dataframe3 = pd.DataFrame(output3, columns=['date', 'High', 'Low', 'Open', 'Close', 'Volume'])
    dataframe4 = pd.DataFrame(output4, columns=['date', 'High', 'Low', 'Open', 'Close', 'Volume'])
    dataframe3['date'] = dataframe3['date'].apply(lambda x: jdatetime.date(x.year, x.month, x.day).togregorian())
    dataframe4['date'] = dataframe4['date'].apply(lambda x: jdatetime.date(x.year, x.month, x.day).togregorian())
    dataframe4['date'] = pd.to_datetime(dataframe4['date'])
    dataframe3['date'] = pd.to_datetime(dataframe3['date'])

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
    fig3.add_trace(go.Scatter(x=dataframe1['date'], y=dataframe1['i_entered_money'],
                              name="i_entered_money", line_color='blue'), secondary_y=True)

    fig3.update_xaxes(calendar='jalali', rangeslider_visible=False,
                      rangebreaks=[dict(dvalue=24 * 60 * 60 * 1000, values=date_break1)])
    fig3.update_layout(title=stock, xaxis=dict(tickmode='linear', tick0=dataframe3['date'].iloc[0], dtick=86400000))

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

    fig3.add_annotation(x=0.15, y=1.1, xref="paper", yref="paper", text=info[0], showarrow=False,
                        font=dict(family="Courier New, monospace", size=16),
                        bordercolor="#c7c7c7", borderwidth=2, bgcolor="#ff7f0e", hovertext="EPS")
    fig3.add_annotation(x=0.3, y=1.1, xref="paper", yref="paper",
                        text=round(info[1], 2) if info[1] is not None else "Null", showarrow=False,
                        font=dict(family="Courier New, monospace", size=16),
                        bordercolor="#c7c7c7", borderwidth=2, bgcolor="#ff7f0e", hovertext="P/E")
    fig3.add_annotation(x=0.45, y=1.1, xref="paper", yref="paper", text=round(info[2], 2), showarrow=False,
                        font=dict(family="Courier New, monospace", size=16),
                        bordercolor="#c7c7c7", borderwidth=2, bgcolor="#ff7f0e", hovertext="Transaction Value")
    fig3.add_annotation(x=0.6, y=1.1, xref="paper", yref="paper", text=round(info[3], 2), showarrow=False,
                        font=dict(family="Courier New, monospace", size=16),
                        bordercolor="#c7c7c7", borderwidth=2, bgcolor="#ff7f0e", hovertext="Average Transaction Value")
    fig3.add_annotation(x=0.75, y=1.1, xref="paper", yref="paper", text=market_cap[0], showarrow=False,
                        font=dict(family="Courier New, monospace", size=16),
                        bordercolor="#c7c7c7", borderwidth=2, bgcolor="#ff7f0e", hovertext="Market Cap")

    fig3.show()

    #########################################################################################

    cursor.execute(sql.SQL("""SELECT timestamp, last_percent,
                                i_buy_pow, i_buy_per_capita, i_sell_per_capita, i_entered_money
                                FROM {0} WHERE 
                                timestamp::date = (SELECT DISTINCT timestamp::date
                                FROM {0} ORDER BY timestamp::date DESC LIMIT 1)
                                ;""").format(sql.Identifier(stock.strip() + '-سرانه')))

    output5 = cursor.fetchall()

    dataframe5 = pd.DataFrame(output5, columns=['time', 'last_percent', 'i_buy_pow', 'i_buy_per_capita',
                                                'i_sell_per_capita', 'i_entered_money'])
    dataframe5['time'] = dataframe5['time'].apply(lambda x:
                                                  jdatetime.datetime(x.year, x.month, x.day, x.hour, x.minute)
                                                  .togregorian())

    fig4 = make_subplots(specs=[[{"secondary_y": True}]])
    fig4.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['last_percent'],
                              name="last_percent"), secondary_y=True)
    fig4.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_buy_pow'],  name="i_buy_pow", line_color='green'))

    fig5 = make_subplots(specs=[[{"secondary_y": True}]])
    fig5.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_buy_per_capita'],
                              name="i_buy_per_capita", line_color='green'))
    fig5.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_sell_per_capita'],
                              name="i_sell_per_capita", line_color='red'))
    fig5.add_trace(go.Scatter(x=dataframe5['time'], y=dataframe5['i_entered_money'],
                              name="i_entered_money", line_color='blue'), secondary_y=True)

    fig4.update_xaxes(calendar='jalali')
    fig5.update_xaxes(calendar='jalali')
    fig4.update_layout(title=stock)
    fig5.update_layout(title=stock)

    fig4.show()
    fig5.show()

    ################################################################################################

    breakpoint_test = 0

cursor.close()
conn.commit()
conn.close()
