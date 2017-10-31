# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 18:09:25 2016

@author: james.cheong
"""

from petl import *
from datetime import date
import datetime
import pandas as pd
import os
import pyodbc
import matplotlib.mlab as ml
import logging
from numpy import inf

from re import sub
from cdecimal import Decimal

logging.basicConfig(filename='TestLog_ResolveConflicts.log',format='%(asctime)s - %(levelname)s - %(message)s', level=logging.ERROR, datefmt='%d/%m/%Y %I:%M:%S %p')    
logger = logging.getLogger(__name__)


def format_RateCard(filename):
    """ Function to extract and transform the weekly Hydro Tasmania Rate Card file from www.hydro.com.au/energy/tasmanian-contract-prices
        Returns a pandas data frame if input filename exists
    """
    headers = ['Period','FLAT_Swap','PEAK_Swap','NSLP_Swap','FLAT_Cap_$300','File_Creation_Date']
    tmp = empty()   # Create empty petl object with column headers
    tmp = setheader(tmp, headers)

    t = fromcsv(filename)

    try:
         if t[0][0]=='File creation time:':
            CreationDate = datetime.datetime.strptime(t[0][1], '%d/%m/%Y %H:%M')
        
            if t[2][1] == 'PEAK Swap' and t[2][2]=='FLAT Swap' and t[2][3]=='$300 Flat Cap*':
                t1 = skip(t,2)
                index1 = ml.find(map(lambda x: x[1] == 'Flat Swap', t1))   # Find first row of second table
                
                t2 = rowslice(t1, index1-1)       # Slice out the sub-table from the CSV file                            
                index2 = ml.find(map(lambda x: x[1] != '', t2))         # Find rows that contain data
                if index2[1] == 1:                
                    st1 = rowslice(t2, 0, index2[len(index2)-1])  # Slice out the useful rows from the subtable                
                else:
                    st1 = rowslice(t2, index2[1]-1, index2[len(index2)-1])  # Slice out the useful rows from the subtable                
                st1 = setheader(st1, ['Period','PEAK_Swap','FLAT_Swap','FLAT_Cap_$300','blank_col'])
                st1 = cutout(st1, 'blank_col')
                
                #index3 = ml.find(map(lambda x: x[1] == 'start date', t1))   # Find first row of third table in new format starting 15/6/2016
                index3 = ml.find(map(lambda x: x[0] == 'start date', t1))   # Find first row of third table in new format starting 27/9/2016
                if len(index3) == 0:
                    t3 = rowslice(t1, index1, len(t1))      # Old format
                else:
                    t3 = rowslice(t1, index1, index3-2)     # New format, ignore 3rd table
                index4 = ml.find(map(lambda x: x[3] != '', t3))         # Find rows that contain data
                if index4[1] == 1:                
                    st2 = rowslice(t3, 0, index4[len(index4)-1])  # Slice out the useful rows from the subtable
                else:
                    st2 = rowslice(t3, index4[1]-1, index4[len(index4)-1])  # Slice out the useful rows from the subtable
                st2 = setheader(st2, ['Period','FLAT_Swap','FLAT_Cap_$300','NSLP_Swap','blank_col'])
                st2 = cutout(st2, 'blank_col')
                
                tmp = cat(st1,st2)
                tmp = addfield(tmp, 'File_Creation_Date', CreationDate)
                
                # value = Decimal(sub(r'[^\d.]', '', tmp[1][1]))
                
                tmp = convert(tmp, 'PEAK_Swap', lambda x: Decimal(sub(r'[^\d.]', '', x)), where=lambda r: r.PEAK_Swap is not None)
                tmp = convert(tmp, 'FLAT_Swap', lambda x: Decimal(sub(r'[^\d.]', '', x)), where=lambda r: r.FLAT_Swap is not None)
                tmp = convert(tmp, 'NSLP_Swap', lambda x: Decimal(sub(r'[^\d.]', '', x)), where=lambda r: r.NSLP_Swap is not None)
                tmp = convert(tmp, 'FLAT_Cap_$300', lambda x: Decimal(sub(r'[^\d.]', '', x)), where=lambda r: r.FLAT_Swap is not None)
                
                return tmp
                
    except IOError:
        #print filename + " does not exist"
        pass


def processFiles(runDate = date.today(), dir = os.path.normpath("C:/Development/TasRateCard/")):    

    print "Processing Hydro Tasmania Rate Card files: "    
    # Establish database connections
    odbcconn = r"Driver={SQL Server Native Client 11.0};Server=MEL-LT-002\SQLEXPRESS;Database=ASXData;Trusted_Connection=yes;"
    conn = pyodbc.connect(odbcconn)

    datestr = runDate.strftime("%Y%m%d")
    filename = "rate_card_prices_" + datestr + ".csv"    
    tmp = format_RateCard(os.path.join(dir, filename))    
    if tmp is not None:
        try:
            appenddb(tmp, conn, 'TAS_HydroTasRateCard')
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)          
            
    conn.close()


if __name__ == "__main__":    
    processFiles()
