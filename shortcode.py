
import sqlite3
import time
import polars as pl
import pandas as pd
# from OptionFor import *
from datetime import datetime
# import datetime as datetime
import math
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import streamlit as st




conn=sqlite3.connect("15july.db")


st.set_page_config(layout='wide')
st.title("MONEYFLOW")
placeholder=st.empty()
# ////////////STREAMLIT GRAPH DISPLAY//////

# print(pl.from_pandas(pd.read_sql("SELECT * FROM CStrike", conn)))

# p=pl.from_pandas(pd.read_sql("SELECT * FROM CStrike", conn)).drop('volume_right','oi_right')

pos=999
neg=-999
def joindf(c,p):

    k=c.join(p,on='timestamp',how="full",maintain_order="left_right")
        # pl.when((pl.col(k.columns[1])>pos)&(pl.col(k.columns[3])<neg)).then(pl.lit(k.columns[1])).alias('ress')

    k=k.with_columns(
        pl.when(pl.col('timestamp').is_null()==True).then(pl.col('timestamp_right'))
            .when(pl.col('timestamp').is_not_null()==True).then(pl.col('timestamp'))
            .alias('timestamp'),

        ).sort('timestamp').drop('timestamp_right')
    # k=k.drop( 'oi', 'strike', 'ratio', 'coi', 'Positions','Positions_right', 'oi_right', 'strike_right', 'ratio_right', 'coi_right',)
    k = k.with_columns(

        pl.col('PositionClosed').fill_null(0),
        pl.col('NewPosition').fill_null(0),

        pl.col('PositionClosed_right').fill_null(0),
        pl.col('NewPosition_right').fill_null(0),
    )
    k = k.with_columns(

        (pl.col('PositionClosed') + pl.col('PositionClosed_right')).alias('PositionClosed'),
        (pl.col('NewPosition') + pl.col('NewPosition_right')).alias('NewPosition'),
    )#.drop('NewPosition_right', 'PositionClosed_right')
    k = k.select(['timestamp', "NewPosition","PositionClosed"])

    return k


amt=75000

def OI(k):

    k = k.with_columns(
        pl.col("timestamp").str.to_datetime(format="%Y-%m-%d %H:%M:%S"),
        ((pl.col('oi').cast(int))/75).alias('oi'),
                pl.when((pl.col('volume').cast(int)) > amt).then(pl.col('volume')).alias('filter_vol'),

    ).drop('ltp_right', 'volume_right', 'oi_right')

    k=k.with_columns(
        ((pl.col('filter_vol').cast(int)) / 75).alias('filter_vol'),
    # pl.col('oi').diff().alias('coi').fill_null(0)
    )

    # ratio vol/NT
    k = k.with_columns(
        (pl.col('filter_vol').cast(int) * pl.col('ltp').cast(int)).alias('ratio')

    )

    k = k.group_by_dynamic("timestamp", every="3m").agg(
        [
            pl.col("oi").last().cast(int).alias("oi"),
            pl.col("strike").last().alias("strike"),
            pl.when(pl.col('volume').cast(int) > amt).then(pl.col('ratio').sum()).max().alias('ratio'),
            pl.when(pl.col('volume').cast(int) > amt).then(pl.col('filter_vol').sum()).max().alias('filter_vol'),

            # pl.col('index').count().alias('NT')

        ])

    k = k.with_columns(pl.col('oi').diff().alias('coi'))
    k = k.with_columns(

     pl.when(pl.col('coi')<-1000).then(pl.lit('PositionClosed'))
        .when(pl.col('coi')>1000).then(pl.lit('NewPosition'))
        .when((pl.col('coi')>-1000)&(pl.col('coi')<1000)).then(pl.lit('Reverse'))
        .alias('Positions'),
    )#.drop_nulls()
    k = k.with_columns(
     pl.when(pl.col('coi')<0).then(pl.col('ratio')*-1)
        .when(pl.col('coi')>0).then(pl.col('ratio'))
        .alias('ratio'),)


#add 50 of its moneyflow to ratio
    k=k.with_columns(

        pl.when(pl.col('Positions')=='Reverse').then(pl.col('ratio').abs()*-1*0.5)
        .when(pl.col('Positions') == 'PositionClosed').then(pl.col('ratio')).otherwise(0)
        .alias('PositionClosed'),
        pl.when(pl.col('Positions')=='Reverse').then(pl.col('ratio').abs()*0.5)
          .when(pl.col('Positions')=='NewPosition').then(pl.col('ratio')).otherwise(0).alias('NewPosition'),
        # pl.col('PositionClosed').alias('oldclosed'),
        # pl.col('NewPosition').alias('oldNewPosition'),
    )
    # k = k.with_columns([(pl.col("timestamp").dt.truncate("1m"))])

    return k

def listmake(list):
    l=[]
    for i in list:
        if i.is_empty()!=True:
            l=l+[i]

    kj=l[0]  #FIRST ITEM
    l.remove(l[0])
    return kj,l


def join13df(k,list):
    for df in list:
        k=joindf(k,df)
    return k



while True:

    time.sleep(10)
    nifty = pl.from_pandas(pd.read_sql("SELECT * FROM CStrike", conn)).drop('volume_right', 'oi_right')

    co1,co2,co3,co4,co5,co6,co7,co8,co9,co10,co11,co12,co13,po1,po2,po3,po4,po5,po6,po7,po8,po9,po10,po11,po12,po13=OI(pl.from_pandas(pd.read_sql("SELECT * FROM C6N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C5N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C4N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C3N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C2N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C1N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM CStrike", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C1", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C2", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C3", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C4", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C5", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM C6", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P6N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P5N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P4N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P3N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P2N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P1N", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM PStrike", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P1", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P2", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P3", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P4", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P5", conn))),\
                 OI(pl.from_pandas(pd.read_sql("SELECT * FROM P6", conn)))

    calllist=[co1,co2,co3,co4,co5,co6,co7,co8,co9,co10,co11,co12,co13]
    putlist=[po1,po2,po3,po4,po5,po6,po7,po8,po9,po10,po11,po12,po13]
    # l = []


    calllist=listmake(calllist)
    putlist=listmake(putlist)
    calldf=join13df(calllist[0],calllist[1])
    putdf=join13df(putlist[0],putlist[1])

    # calldf=calldf.with_columns(
    #     pl.when(pl.col('NewPosition')==0).then(None).otherwise(pl.col('NewPosition')).alias('NewPosition'),
    #     pl.when(pl.col('PositionClosed')==0).then(None).otherwise(pl.col('PositionClosed')).alias('PositionClosed'),
    # )
    # putdf=putdf.with_columns(
    #     pl.when(pl.col('NewPosition')==0).then(None).otherwise(pl.col('NewPosition')).alias('NewPosition'),
    #     pl.when(pl.col('PositionClosed')==0).then(None).otherwise(pl.col('PositionClosed')).alias('PositionClosed'),
    # )


    fig= make_subplots(specs=[[{"secondary_y": True}]]).update_yaxes(fixedrange=False)
    fig2= make_subplots(specs=[[{"secondary_y": True}]]).update_yaxes(fixedrange=False)

    fig1= make_subplots(specs=[[{"secondary_y": True}]]).update_yaxes(fixedrange=False)
    fig1.update_yaxes(showgrid=False,)
    fig1.update_xaxes(
        showgrid=True,
       # gridwidth=1,
       #  gridcolor='black'
    )
    # fig1.update_layout(grid=dict(columns=2))

    fig1.add_trace(go.Bar(x=calldf['timestamp'], y=calldf['NewPosition'], marker_color='lightgreen',text=(calldf['NewPosition']/100000).cast(float),name='NewPosition'))#.update_traces(textfont_size=20, textangle=0, textposition="outside", cliponaxis=True)
    fig1.add_trace(go.Bar(x=calldf['timestamp'], y=calldf['PositionClosed'], marker_color='lightgreen',name='PositionClosed',text=(calldf['PositionClosed']/100000)))
    fig1.add_trace(go.Line(x=calldf['timestamp'], y=calldf['PositionClosed'].cum_sum()+calldf['NewPosition'].cum_sum(), marker_color='lightgreen',name='CallTotal'))
    fig1.add_trace(go.Line(x=calldf['timestamp'], y=putdf['PositionClosed'].cum_sum()+putdf['NewPosition'].cum_sum(), marker_color='red',name='PutTotal'))
    fig1.add_trace(go.Bar(x=putdf['timestamp'], y=putdf['NewPosition'], marker_color='lightpink',name='NewPosition',text=(putdf['NewPosition']/100000)))
    fig1.add_trace(go.Bar(x=putdf['timestamp'], y=putdf['PositionClosed'],marker_color='lightpink',name='PositionClosed',text=(putdf['PositionClosed']/100000)))

    fig1.add_trace(go.Line(x=nifty['timestamp'], y=nifty['ltp_right'] ,marker_color='lightblue'),secondary_y=True)
    fig.add_trace(go.Line(x=nifty['timestamp'], y=nifty['ltp_right'] ,marker_color='lightblue'),secondary_y=True)
    fig.add_trace(
        go.Bar(x=co1['timestamp'], y=co1['ratio'], marker_color='green', text=co1['strike'] + co1['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co2['timestamp'], y=co2['ratio'], marker_color='green', text=co2['strike'] + co2['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co3['timestamp'], y=co3['ratio'], marker_color='green', text=co3['strike'] + co3['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co4['timestamp'], y=co4['ratio'], marker_color='green', text=co4['strike'] + co4['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co5['timestamp'], y=co5['ratio'], marker_color='green', text=co5['strike'] + co5['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co6['timestamp'], y=co6['ratio'], marker_color='green', text=co6['strike'] + co6['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co7['timestamp'], y=co7['ratio'], marker_color='green', text=co7['strike'] + co7['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co8['timestamp'], y=co8['ratio'], marker_color='green', text=co8['strike'] + co8['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co9['timestamp'], y=co9['ratio'], marker_color='green', text=co9['strike'] + co9['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co10['timestamp'], y=co10['ratio'], marker_color='green', text=co10['strike'] + co10['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co11['timestamp'], y=co11['ratio'], marker_color='green', text=co11['strike'] + co11['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co12['timestamp'], y=co12['ratio'], marker_color='green', text=co12['strike'] + co12['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=co13['timestamp'], y=co13['ratio'], marker_color='green', text=co13['strike'] + co13['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po1['timestamp'], y=po1['ratio'], marker_color='red', text=po1['strike'] + po1['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po2['timestamp'], y=po2['ratio'], marker_color='red', text=po2['strike'] + po2['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po3['timestamp'], y=po3['ratio'], marker_color='red', text=po3['strike'] + po3['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po4['timestamp'], y=po4['ratio'], marker_color='red', text=po4['strike'] + po4['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po5['timestamp'], y=po5['ratio'], marker_color='red', text=po5['strike'] + po5['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po6['timestamp'], y=po6['ratio'], marker_color='red', text=po6['strike'] + po6['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po7['timestamp'], y=po7['ratio'], marker_color='red', text=po7['strike'] + po7['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po8['timestamp'], y=po8['ratio'], marker_color='red', text=po8['strike'] + po8['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po9['timestamp'], y=po9['ratio'], marker_color='red', text=po9['strike'] + po9['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po10['timestamp'], y=po10['ratio'], marker_color='red', text=po10['strike'] + po10['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po11['timestamp'], y=po11['ratio'], marker_color='red', text=po11['strike'] + po11['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po12['timestamp'], y=po12['ratio'], marker_color='red', text=po12['strike'] + po12['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig.add_trace(
        go.Bar(x=po13['timestamp'], y=po13['ratio'], marker_color='red', text=po13['strike'] + po13['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(go.Line(x=nifty['timestamp'], y=nifty['ltp_right'], marker_color='lightblue'), secondary_y=True)
    fig2.add_trace(
        go.Bar(x=co1['timestamp'], y=co1['filter_vol'], marker_color='green', text=co1['strike'] + co1['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co2['timestamp'], y=co2['filter_vol'], marker_color='green', text=co2['strike'] + co2['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co3['timestamp'], y=co3['filter_vol'], marker_color='green', text=co3['strike'] + co3['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co4['timestamp'], y=co4['filter_vol'], marker_color='green', text=co4['strike'] + co4['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co5['timestamp'], y=co5['filter_vol'], marker_color='green', text=co5['strike'] + co5['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co6['timestamp'], y=co6['filter_vol'], marker_color='green', text=co6['strike'] + co6['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co7['timestamp'], y=co7['filter_vol'], marker_color='green', text=co7['strike'] + co7['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co8['timestamp'], y=co8['filter_vol'], marker_color='green', text=co8['strike'] + co8['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co9['timestamp'], y=co9['filter_vol'], marker_color='green', text=co9['strike'] + co9['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co10['timestamp'], y=co10['filter_vol'], marker_color='green', text=co10['strike'] + co10['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co11['timestamp'], y=co11['filter_vol'], marker_color='green', text=co11['strike'] + co11['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co12['timestamp'], y=co12['filter_vol'], marker_color='green', text=co12['strike'] + co12['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=co13['timestamp'], y=co13['filter_vol'], marker_color='green', text=co13['strike'] + co13['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po1['timestamp'], y=po1['filter_vol'], marker_color='red', text=po1['strike'] + po1['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po2['timestamp'], y=po2['filter_vol'], marker_color='red', text=po2['strike'] + po2['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po3['timestamp'], y=po3['filter_vol'], marker_color='red', text=po3['strike'] + po3['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po4['timestamp'], y=po4['filter_vol'], marker_color='red', text=po4['strike'] + po4['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po5['timestamp'], y=po5['filter_vol'], marker_color='red', text=po5['strike'] + po5['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po6['timestamp'], y=po6['filter_vol'], marker_color='red', text=po6['strike'] + po6['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po7['timestamp'], y=po7['filter_vol'], marker_color='red', text=po7['strike'] + po7['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po8['timestamp'], y=po8['filter_vol'], marker_color='red', text=po8['strike'] + po8['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po9['timestamp'], y=po9['filter_vol'], marker_color='red', text=po9['strike'] + po9['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po10['timestamp'], y=po10['filter_vol'], marker_color='red', text=po10['strike'] + po10['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po11['timestamp'], y=po11['filter_vol'], marker_color='red', text=po11['strike'] + po11['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po12['timestamp'], y=po12['filter_vol'], marker_color='red', text=po12['strike'] + po12['Positions'],
               insidetextfont=dict(color='white', size=30)))

    fig2.add_trace(
        go.Bar(x=po13['timestamp'], y=po13['filter_vol'], marker_color='red', text=po13['strike'] + po13['Positions'],
               insidetextfont=dict(color='white', size=30)))
    fig.update_layout(height=400)
    fig2.update_layout(height=400)
    with placeholder.container():

        st.plotly_chart(fig,height=400, use_container_width=True)
        st.plotly_chart(fig2,height=400, use_container_width=True)
        st.plotly_chart(fig1, use_container_width=True)
#




