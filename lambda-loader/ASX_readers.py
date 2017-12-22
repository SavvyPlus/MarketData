from petl import *
from datetime import date
import urllib2
import os
import pymssql
from numpy import inf
from boto3 import client
import time

conn = client('s3')

def format_OpenInterestReport(filename):
    """ Function to extract and transform a daily Settlement Futures file from ASX Energy
        Returns a petl class object if input filename exists
    """

    filename = urllib2.unquote(filename)
    filename = filename.replace("+"," ")
    obj = conn.get_object(Bucket="savvyloader", Key="savvy-queue/%s" % (filename))
    data = obj['Body'].read().decode('utf-8')
    datasource = MemorySource(data)

    t = fromcsv(datasource)

    # dateparser to parse from string to datetime
    i1 = dateparser('%Y/%m/%d',strict=False)
    i2 = dateparser('%d/%m/%Y',strict=False)

    try:
        filenameOnly = os.path.basename(filename)   
        t1 = setheader(t, ['Code','Open_Interest','Date_tmp'])      

        Code= map(lambda x: x if x is None or len(x.decode('utf-8')) > 8 else x.decode('utf-8')[:7].encode("utf-8"), t1['Code'])
        Date = map(i1, map(i2, t1['Date_tmp']))    
        t1 = cutout(t1, 'Date_tmp')
        t1 = addcolumn(t1, 'Date', Date)
        t1 = cutout(t1, 'Code')
        t1 = addcolumn(t1,'Code',Code)

        return t1
    except Exception as e:
        print(e)
        #print filename + " does not exist"
        pass

def format_FinalSnapShot(filename):

    filename = urllib2.unquote(filename)
    filename = filename.replace("+"," ")
    obj = conn.get_object(Bucket="savvyloader", Key="savvy-queue/%s" % (filename))
    data = obj['Body'].read().decode('utf-8')
    datasource = MemorySource(data)

    t = fromcsv(datasource)
    i1 = dateparser('%Y/%m/%d',strict=False)
    i2 = dateparser('%d/%m/%Y',strict=False)


    try:
        t1 = setheader(t, ('Code','Last_Trading_Date_tmp','Bid_Price','Bid_Size','Ask_Price','Ask_Size','Last_Price','Traded_Volume','Open_Price','High_Price','Low_Price','Settlement_Price','Settlement_Date_tmp','Implied_Volatility','Last_Trade_Time_tmp'))      
        t2 = replace(t1, header(t1), '', None)

        Last_Trading_Date = map(i1, map(i2, t2['Last_Trading_Date_tmp']))    
        Settlement_Date = map(i1, map(i2,t2['Settlement_Date_tmp']))

        # Lambda replace - with : in Last Trade Time
        Last_Trade_Time= map(lambda x: x if x is None else x.decode('utf-8').replace("-", ":").encode("utf-8"), t2['Last_Trade_Time_tmp'])
        Code= map(lambda x: x if x is None or len(x.decode('utf-8')) > 8 else x.decode('utf-8')[:7].encode("utf-8"), t2['Code'])
        Type= map(lambda x: 1 if x is None or len(x.decode('utf-8')) > 8 else 2, t2['Code'])

        t2 = cutout(t2, 'Last_Trading_Date_tmp')
        t2 = addcolumn(t2, 'Last_Trading_Date', Last_Trading_Date)
        t2 = cutout(t2,'Settlement_Date_tmp')
        t2 = addcolumn(t2,'Settlement_Date',Settlement_Date)
        t2 = cutout(t2,'Last_Trade_Time_tmp')
        t2 = addcolumn(t2,'Last_Trade_Time',Last_Trade_Time)
        t2 = cutout(t2,'Code')
        t2 = addcolumn(t2,'Code',Code)
        return t2
    except Exception as e:
        print('error here')
        print(e)
        #print filename + " does not exist"
        pass

def format_TradeLog(filename):
    filename = urllib2.unquote(filename)
    filename = filename.replace("+"," ")
    obj = conn.get_object(Bucket="savvyloader", Key="savvy-queue/%s" % (filename))
    data = obj['Body'].read().decode('utf-8')
    datasource = MemorySource(data)

    t = fromcsv(datasource)    
    i1 = dateparser('%Y/%m/%d',strict=False)
    i2 = dateparser('%d/%m/%Y',strict=False)
    try:
        filenameOnly = os.path.basename(filename)  
        t1 = setheader(t, ['Date','Time','trade_type','display_code','volume','_$_mwh']) 
        t2 = replace(t1, header(t1), '', None)
        format_date = map(i1,map(i2,t2['Date']))
        format_time = map(lambda x: x if x is None else x.decode('utf-8').replace("-", ":").encode("utf-8"), t2['Time'])
        format_datetime =map(lambda x,y : x if x is None else (str(x).decode('utf-8') +' '+y.decode('utf-8')).encode('utf-8'), format_date ,format_time )

        t2 = cutout(t2,'Time')
        t2 = cutout(t2,'Date')
        t2 = addcolumn(t2,'date_time',format_datetime)

        return t2
    except IOError:
        #print filename + " does not exist"
        pass
