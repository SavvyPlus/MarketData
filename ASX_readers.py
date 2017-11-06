# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 10:02:03 2014

@author: james.cheong
"""



from petl import *
from datetime import date

import os
import pyodbc
import matplotlib.mlab as ml
import logging
from numpy import inf

logging.basicConfig(filename='TestLog_ResolveConflicts.log',format='%(asctime)s - %(levelname)s - %(message)s', level=logging.ERROR, datefmt='%d/%m/%Y %I:%M:%S %p')    
logger = logging.getLogger(__name__)


def format_OpenInterestOptions(filename):
    """ Function to extract and transform a daily Open Interest Options file from ASX Energy
        Returns a pandas data frame if input filename exists
    """
    isodate = dateparser('%d %b %Y')    
    headers = ['NSW_Call_OI','NSW_Strike','NSW_Put_OI','VIC_Call_OI','VIC_Strike','VIC_Put_OI','QLD_Call_OI','QLD_Strike','QLD_Put_OI','SA_Call_OI','SA_Strike','SA_Put_OI','NSW_Period','VIC_Period','QLD_Period','SA_Period','Settlement_Date']    
    tmp = empty()   # Create empty petl object with column headers
    tmp = setheader(tmp, headers)
    
    t = fromtsv(filename)    

    try:
        settlementdate =  isodate(t[0][2])      # Pull out date field from first row
        t1 = skip(t,2)
        t1 = setheader(t1, [headers[0],headers[1],headers[2],headers[3],headers[4],headers[5],headers[6],headers[7],headers[8],headers[9],headers[10],headers[11],'blank_col'])
        t2 = cutout(t1, 'blank_col')

        index = ml.find(map(lambda x: x[0] is None, t2))       # Find row numbers where the first column is NULL
        
        for i in range(0, len(index)):
            if i < len(index)-1 and index[i+1]-index[i] > 1:     # Find first row of a sub-table within the CSV file
                #print index[i]
                st = rowslice(t2, index[i], index[i+1]-1)       # Slice out the sub-table from the CSV file
        
                if st['NSW_Call_OI'][0] == 'NSW':            # Sub-table with useable headings has "NSW" in row 0, column 0
                    NSW_Period = st['NSW_Strike'][0]         # Extract the periods for each region
                    VIC_Period = st['VIC_Strike'][0]
                    QLD_Period = st['QLD_Strike'][0]
                    SA_Period = st['SA_Strike'][0]
                        
                    st1 = skip(st,3)                        # Skip 2 rows of sub-table headings
                    st1 = setheader(st1, headers[0:12])    
                    st1 = addfield(st1, headers[12], NSW_Period)
                    st1 = addfield(st1, headers[13], VIC_Period)
                    st1 = addfield(st1, headers[14], QLD_Period)
                    st1 = addfield(st1, headers[15], SA_Period)
                    st1 = addfield(st1, headers[16], settlementdate)
                    
                    tmp = cat(tmp, st1)
                
        if len(tmp) == 1:
            return None     # File exists but no useful headings. Applies to files from 5/12/2003 to 4/11/2004
        else:
            return tmp
        
    except IOError:
        #print filename + " does not exist"
        pass


def format_OpenInterestFutures(filename):
    """ Function to extract and transform a daily Open Interest Futures file from ASX Energy
        Returns a petl class object if input filename exists
    """
    isodate = dateparser('%d %b %Y')    
    t = fromtsv(filename)    
    
    try:
        settlementdate =  isodate(t[0][3])      # Pull out date field from first row
        t1 = skip(t,1)                          # Skip first row        
        t1 = setheader(t1, ['NSW_Period', 'NSW_Peak_OI', 'NSW_Base_OI', 'VIC_Period','VIC_Peak_OI', 'VIC_Base_OI', 'QLD_Period','QLD_Peak_OI', 'QLD_Base_OI','SA_Period','SA_Peak_OI', 'SA_Base_OI'])        
        t2 = addfield(t1, "Settlement_Date", settlementdate)

        return t2
        
    except IOError:
        #print filename + " does not exist"
        pass
    

def format_OpenInterestCaps(filename):
    """ Function to extract and transform a daily Open Interest Caps file from ASX Energy
        Returns a petl class object if input filename exists
    """
    isodate = dateparser('%d %b %Y')    
    t = fromtsv(filename)    
    
    try:
        settlementdate =  isodate(t[0][11])      # Pull out date field from first row
        t1 = skip(t,1)                          # Skip first row        
        t1 = setheader(t1, ['NSW_Period','NSW_Base_OI','VIC_Period','VIC_Base_OI', 'QLD_Period','QLD_Base_OI','SA_Period','SA_Base_OI'])        
        t2 = addfield(t1, "Settlement_Date", settlementdate)

        return t2
        
    except IOError:
        #print filename + " does not exist"
        pass


def format_SettlementCaps(filename):
    """ Function to extract and transform a daily Settlement Caps file from ASX Energy
        Returns a petl class object if input filename exists
    """
    isodate = dateparser('%d %b %Y')    
    t = fromtsv(filename)    
    
    try:
        settlementdate =  isodate(t[0][11])      # Pull out date field from first row
        t1 = skip(t,1)                          # Skip first row        
        t1 = setheader(t1, ['NSW_Period','NSW_Base_Price','VIC_Period','VIC_Base_Price', 'QLD_Period','QLD_Base_Price','SA_Period','SA_Base_Price'])        
        t2 = addfield(t1, "Settlement_Date", settlementdate)

        return t2
        
    except IOError:
        #print filename + " does not exist"
        pass


def format_SettlementFutures(filename):
    """ Function to extract and transform a daily Settlement Futures file from ASX Energy
        Returns a petl class object if input filename exists
    """
    isodate = dateparser('%d %b %Y')    
    t = fromtsv(filename)    
    
    try:
        settlementdate =  isodate(t[0][11])      # Pull out date field from first row
        t1 = skip(t,1)                          # Skip first row        
        t1 = setheader(t1, ['NSW_Period','NSW_Peak_Price','NSW_OffPeak_Price','NSW_Base_Price','VIC_Period','VIC_Peak_Price','VIC_OffPeak_Price','VIC_Base_Price', 'QLD_Period','QLD_Peak_Price','QLD_OffPeak_Price','QLD_Base_Price','SA_Period','SA_Peak_Price','SA_OffPeak_Price','SA_Base_Price'])        
        t2 = replace(t1, header(t1), '-', 0)    # Replace '-' with 0, mainly for Monthly PK & OP Settlement Prices
        t3 = addfield(t2, "Settlement_Date", settlementdate)
    
        return t3
        
    except IOError:
        #print filename + " does not exist"
        pass


def format_ClosingSnapshotFutures(filename):
    """ Function to extract and transform a daily Closing Snapshot Futures file from ASX Energy
        Returns a petl class object if input filename exists
    """
    i1 = dateparser('%Y/%m/%d',strict=False)
    i2 = dateparser('%d/%m/%Y',strict=False)
    t = fromcsv(filename)    
    
    try:   
        t1 = setheader(t, ['Code','Expiration_tmp','Bid_Price','Bid_Size','Ask_Price','Ask_Size','Last_Price','Volume','Last_Volume','Net_Change','Open_Price','High_Price','Low_Price','Settlement_Price','Settlement_Date_tmp','Trade_Date_tmp','Trade_Time'])
        t2 = replace(t1, header(t1), '', None)    # Replace blanks with NULL
        
        # Convert date strings to datetime.date objects
        Expiration = map(i1, map(i2, t2['Expiration_tmp']))     
        Settlement_Date = map(i1, map(i2, t2['Settlement_Date_tmp']))
        Trade_Date = map(i1, map(i2, t2['Trade_Date_tmp']))
        
        # Remove date string columns and replace with datetime.date
        t3 = cutout(t2, 'Expiration_tmp')
        t3 = cutout(t3, 'Settlement_Date_tmp')
        t3 = cutout(t3, 'Trade_Date_tmp')
        t3 = addcolumn(t3, 'Expiration', Expiration)
        t3 = addcolumn(t3, 'Settlement_Date', Settlement_Date)
        t3 = addcolumn(t3, 'Trade_Date', Trade_Date)

        # Check if Settlement Date field is empty or requires padding
        if t3['Settlement_Date'][0] is None:       # Entire column of Settlement Date field is empty
            isodate = dateparser('%Y%m%d')
            filenameOnly = os.path.basename(filename)
            settlementdate =  isodate(filenameOnly[filenameOnly.find('20'):filenameOnly.find('20')+8])      # Extract date from filenameOnly
            t3 = replace(t3, 'Settlement_Date', None, settlementdate)
        else:
            t3 = replace(t3, 'Settlement_Date', None, t3['Settlement_Date'][0])    # Replace any empty settlement date cells with the settlement date from first row of the csv file

        
        # Routine to handle duplicate rows for FY and CY strips from 2008-03-31 to 2012-10-09
        c = conflicts(t3, ['Code','Settlement_Date'])   # Identify conflicts: Select rows with the same Code and Settlement_Date but differing values in other fields. NULLS are not considered conflicts!
        d = duplicates(t3, ['Code','Settlement_Date'])   # Identify duplicates: Select rows with the same Code and Settlement_Date
        if len(c) == 1 and len(d) > 1:  # Duplicates detected, but no conflicts. 
            t4 = mergeduplicates(t3, ['Code','Settlement_Date'])        # Merge duplicate rows with the same Code and Settlement_Date
            return t4
        elif len(c) > 1:                # Conflicts detected, resolve conflicts!
            t4 = convertnumbers(t3)
            t4 = replace(t4, 'Trade_Date', None, date(1980, 7, 1))      # Replace NULL Trade_Date with a placeholder date becase max() can't compare datetime.date to NoneType
            t4 = replace(t4, 'High_Price', None, inf)                  # Replace NULL High_Price with a placeholder inf becase min(None, 44.7) returns None, when we expect 44.7            
            
            # Group rows by Code and Settlement_Date and then apply the appropriate aggregate functions (max & min in this case) to the other fields
            t5 = aggregate(t4, key=('Code','Settlement_Date'))
            t5['Expiration'] = 'Expiration', max
            t5['Bid_Price'] = 'Bid_Price', max
            t5['Bid_Size'] = 'Bid_Size', max
            t5['Ask_Price'] = 'Ask_Price', max
            t5['Ask_Size'] = 'Ask_Size', max
            t5['Last_Price'] = 'Last_Price', max
            t5['Volume'] = 'Volume', max                # See ClosingSnapshot_Futures_20090701: HNZ2010
            t5['Last_Volume'] = 'Last_Volume', max
            t5['Net_Change'] = 'Net_Change', max
            t5['Open_Price'] = 'Open_Price', max
            t5['High_Price'] = 'High_Price', min        # See ClosingSnapshot_Futures_20120229: HNZ2013, ClosingSnapshot_Futures_20121005: HNZ2014
            t5['Low_Price'] = 'Low_Price', max          # See ClosingSnapshot_Futures_20080625: HVM2010, ClosingSnapshot_Futures_20080428: HVZ2009
            t5['Settlement_Price'] = 'Settlement_Price', max
            t5['Trade_Date'] = 'Trade_Date', max
            t5['Trade_Time'] = 'Trade_Time', max
            
            t5 = replace(t5, 'Trade_Date', date(1980, 7, 1), None)      # Replace placeholder date with NULL
            t5 = replace(t5, 'High_Price', inf, None)                  # Replace -inf with NULL
            
            t6 = mergeduplicates(t5, ['Code','Settlement_Date'])        # Merge duplicate rows with the same Code and Settlement_Date
            
            return t6
        else :
            return t3
                

        
    except IOError:
        #print filename + " does not exist"
        pass


def format_ClosingSnapshotOptions(filename):
    """ Function to extract and transform a daily Closing Snapshot Options file from ASX Energy
        Returns a petl class object if input filename exists
    """
    i1 = dateparser('%Y/%m/%d',strict=False)
    i2 = dateparser('%d/%m/%Y',strict=False)
    t = fromcsv(filename)    
    
    try:   
        t1 = setheader(t, ['Code','Strike_Price','Expiration_tmp','Bid_Size','Bid_Price','Ask_Price','Ask_Size','Last_Price','Net_Change','Volume','Last_Volume','Settlement_Price','Settlement_Date_tmp','Open_Price','High_Price','Low_Price','Implied_Volatility','Trade_Date_tmp','Trade_Time'])
        t2 = replace(t1, header(t1), '', None)    # Replace blanks with NULL
        
        # Convert date strings to datetime.date objects
        Expiration = map(i1, map(i2, t2['Expiration_tmp']))     
        Settlement_Date = map(i1, map(i2, t2['Settlement_Date_tmp']))
        Trade_Date = map(i1, map(i2, t2['Trade_Date_tmp']))
        
        # Remove date string columns and replace with datetime.date
        t3 = cutout(t2, 'Expiration_tmp')
        t3 = cutout(t3, 'Settlement_Date_tmp')
        t3 = cutout(t3, 'Trade_Date_tmp')
        t3 = addcolumn(t3, 'Expiration', Expiration)
        t3 = addcolumn(t3, 'Settlement_Date', Settlement_Date)
        t3 = addcolumn(t3, 'Trade_Date', Trade_Date)
        
        # Check if Settlement Date field is empty
        if t3['Settlement_Date'][0] is None:       # Entire column of Settlement Date field is empty
            isodate = dateparser('%Y%m%d')
            filenameOnly = os.path.basename(filename)
            settlementdate =  isodate(filenameOnly[filenameOnly.find('20'):filenameOnly.find('20')+8])      # Extract date from filenameOnly
            t3 = replace(t3, 'Settlement_Date', None, settlementdate)
        else:
            t3 = replace(t3, 'Settlement_Date', None, t3['Settlement_Date'][0])    # Replace any empty settlement date cells with the settlement date from first row of the csv file
            
        return t3
        
    except IOError:
        #print filename + " does not exist"
        pass

def format_OpenInterestReport(filename):
    """ Function to extract and transform a daily Settlement Futures file from ASX Energy
        Returns a petl class object if input filename exists
    """
    # dateparser to parse from string to datetime
    i1 = dateparser('%Y/%m/%d',strict=False)
    i2 = dateparser('%d/%m/%Y',strict=False)
    t = fromcsv(filename) 

    try:
        filenameOnly = os.path.basename(filename)   
        t1 = setheader(t, ['Code','Open_Interest','Date_tmp'])      

        Code= map(lambda x: x if x is None else x.decode('utf-8')[:7].encode("utf-8"), t1['Code'])
        Date = map(i1, map(i2, t1['Date_tmp']))    
        t1 = cutout(t1, 'Date_tmp')
        t1 = addcolumn(t1, 'Date', Date)
        t1 = cutout(t1, 'Code')
        t1 = addcolumn(t1,'Code',Code)

        return t1
    except IOError:
        #print filename + " does not exist"
        pass

def format_FinalSnapShot(filename):
    t = fromcsv(filename)
    i1 = dateparser('%Y/%m/%d',strict=False)
    i2 = dateparser('%d/%m/%Y',strict=False)

    try:
        filenameOnly = os.path.basename(filename)   
        t1 = setheader(t, ['Code','Last_Trading_Date_tmp','Bid_Price','Bid_Size','Ask_Price','Ask_Size','Last_Price','Traded_Volume','Open_Price','High_Price','Low_Price','Settlement_Price','Settlement_Date_tmp','Implied_Volatility','Last_Trade_Time_tmp'])      
        t2 = replace(t1, header(t1), '', None)

        Last_Trading_Date = map(i1, map(i2, t2['Last_Trading_Date_tmp']))    
        Settlement_Date = map(i1, map(i2,t2['Settlement_Date_tmp']))

        # Lambda replace - with : in Last Trade Time
        Last_Trade_Time= map(lambda x: x if x is None else x.decode('utf-8').replace("-", ":").encode("utf-8"), t2['Last_Trade_Time_tmp'])
        Code= map(lambda x: x if x is None else x.decode('utf-8')[:7].encode("utf-8"), t2['Code'])

        t2 = cutout(t2, 'Last_Trading_Date_tmp')
        t2 = addcolumn(t2, 'Last_Trading_Date', Last_Trading_Date)
        t2 = cutout(t2,'Settlement_Date_tmp')
        t2 = addcolumn(t2,'Settlement_Date',Settlement_Date)
        t2 = cutout(t2,'Last_Trade_Time_tmp')
        t2 = addcolumn(t2,'Last_Trade_Time',Last_Trade_Time)
        t2 = cutout(t2,'Code')
        t2 = addcolumn(t2,'Code',Code)

        return t2
    except IOError:
        #print filename + " does not exist"
        pass

def format_TradeLog(filename):
    t = fromcsv(filename)
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

#def processFiles(runDate = date(2002, 9, 3), dir = os.path.normpath("C:/Development/Daily Files/test2/")):
#def processFiles(runDate = date.today(), dir = os.path.normpath("C:/MarketData/ASX Daily Files/")):
def processFiles(runDate = date.today(), dir = os.path.normpath("C:/Development/Daily Files/")):    

    print "Processing ASX Daily files: "    
    # Establish database connections
    odbcconn = r"Driver={SQL Server Native Client 11.0};Server=MEL-LT-002\SQLEXPRESS;Database=ASXData;Trusted_Connection=yes;"
    conn = pyodbc.connect(odbcconn)

    datestr = runDate.strftime("%Y%m%d")

    filename = "OpenInterest_Options_" + datestr + ".txt"    
    tmp = format_OpenInterestOptions(os.path.join(dir, filename))    
    if tmp is not None:
        try:
            appenddb(tmp, conn, 'ASX_OpenInterest_Options')
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)
        
    filename = "OpenInterest_Futures_" + datestr + ".txt"    
    tmp = format_OpenInterestFutures(os.path.join(dir, filename))    
    if tmp is not None:       
        try:
            appenddb(tmp, conn, 'ASX_OpenInterest_Futures')
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e       
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)
        
    filename = "OpenInterest_Caps_" + datestr + ".txt"    
    tmp = format_OpenInterestCaps(os.path.join(dir, filename))    
    if tmp is not None:       
        try:
            appenddb(tmp, conn, 'ASX_OpenInterest_Caps')
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e       
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)
        
    filename = "Settlement_Caps_" + datestr + ".txt"
    tmp = format_SettlementCaps(os.path.join(dir, filename))    
    if tmp is not None:   
        try:
            appenddb(tmp, conn, 'ASX_Settlement_Caps')        
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e    
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)
        
    filename = "Settlement_Futures_" + datestr + ".txt"
    tmp = format_SettlementFutures(os.path.join(dir, filename))   
    if tmp is not None:
        try:
            appenddb(tmp, conn, 'ASX_Settlement_Futures')       
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)
            
    filename = "ClosingSnapshot_Futures_" + datestr + ".csv"    
    tmp = format_ClosingSnapshotFutures(os.path.join(dir, filename))    
    if tmp is not None:
        try:
            appenddb(tmp, conn, 'ASX_ClosingSnapshot_Futures')    
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e     
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)                    
        
    filename = "ClosingSnapshot_Options_" + datestr + ".csv"    
    tmp = format_ClosingSnapshotOptions(os.path.join(dir, filename))    
    if tmp is not None:
        try:
            appenddb(tmp, conn, 'ASX_ClosingSnapshot_Options')
            print filename
        except Exception as e:
            print 'Error occured when processing %s' % filename               
            print e
            logger.error('Error occured when processing %s' % filename)
            logger.error(e)
            
    conn.close()


if __name__ == "__main__":    
    processFiles()