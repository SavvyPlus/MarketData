# -*- coding: utf-8 -*-
"""
Created on Thu Jul 03 17:01:55 2014

@author: andrew.dodds
"""
import pymssql
import logging
from StringIO import StringIO
from numpy import all
from pandas import read_csv, ExcelFile, DataFrame
from re import match, sub
from math import isnan
from petl import addfield
from datetime import datetime
import os
from zipfile import ZipFile
import ASX_readers
import xml.etree.ElementTree as ET




logger = logging.getLogger(__name__)
tags = ['version:1', 'application:savvy data loader']


def sql_merge_statement(dest_table,all_fields,key_fields):
    
    data_fields = list(set(all_fields).difference(key_fields))        
    all_fields = map(lambda x: "[" + x + "]", all_fields)
    key_fields = map(lambda x: "[" + x + "]", key_fields)
    data_fields = map(lambda x: "[" + x + "]", data_fields)

    if len(key_fields) > 0:        
        s = "MERGE " + dest_table + "\nUSING (\n\tVALUES(" + ','.join(map(lambda x:'%d', all_fields)) + ")\n)"
        s = s + " AS src (" + ','.join(all_fields) + ")\n ON "
        s = s + ' AND '.join(map(lambda x: (dest_table+".{c} = src.{c}").format(c=x), key_fields))
        s = s + "\nWHEN MATCHED THEN \n\tUPDATE SET " + ','.join(map(lambda x: "{c} = src.{c}".format(c=x), data_fields))
        s = s + "\nWHEN NOT MATCHED THEN \n\tINSERT (" + ','.join(all_fields) + ")"
        s = s + "\n\tVALUES (" + ','.join(map(lambda x:'src.'+x, all_fields)) + ")\n;"
        
    else:
        s = "INSERT INTO " + dest_table + "(" + ','.join(all_fields) + ") VALUES (" + ','.join(map(lambda x:'?', all_fields)) + ")"
    return s

def asx_load(source_file_id,fname,conn,table=None,key_fields=None,output_type='db'):
    if table == 'ASX_OpenInterest_Options':
        d = ASX_readers.format_OpenInterestOptions(fname)
    elif table == 'ASX_OpenInterest_Futures':
        d = ASX_readers.format_OpenInterestFutures(fname)
    elif table == 'ASX_OpenInterest_Caps':
        d = ASX_readers.format_OpenInterestCaps(fname)
    elif table == 'ASX_Settlement_Caps':
        d = ASX_readers.format_SettlementCaps(fname)
    elif table == 'ASX_Settlement_Futures':
        d = ASX_readers.format_SettlementFutures(fname)
    elif table == 'ASX_ClosingSnapshot_Futures':
        d = ASX_readers.format_ClosingSnapshotFutures(fname)
    elif table == 'ASX_ClosingSnapshot_Options':
        d = ASX_readers.format_ClosingSnapshotOptions(fname)
    elif table == 'ASX_OpenInterest_Report':
        d = ASX_readers.format_OpenInterestReport(fname)
    elif table == 'ASX_FinalSnapShot':
        d = ASX_readers.format_FinalSnapShot(fname)
    elif table == 'ASX_TradeLog':
        d = ASX_readers.format_TradeLog(fname)
    else:
        return (False,0)
    
    if d is None:
        return (False,0)
        
    d = addfield(d, 'source_file_id', source_file_id)
    if output_type == 'petl':
        return d

    # determine key fields and data fields
    if key_fields is None:
        key_fields = []
    else:        
        if not set(key_fields).issubset(d[0]):
            return (False,0)

    # merge into database
    sql = sql_merge_statement(table,d[0],key_fields)
            
    sql_params = d[1:]#map(tuple, df.values)
    # convert nans to None so insert/update will work correctly    
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x,sp),sql_params)  

    #try:    
    # merge to database if records found
    if len(d)-1 > 0:
        try:
            index =0 
            curs = conn.cursor()
            for data in sql_params:
                index = index + 1
                if index % 1000 == 0:
                    print(index)
                
                curs.execute(sql, tuple(data))   
            conn.commit()
            curs.close()
            print("finish")    
        except Exception as e:
            print(e)
    #except:
    #    raise
    #    return (df, sql)
    
    return (True,len(d)-1)
        

