# -*- coding: utf-8 -*-
"""
Created on Tue Jun 24 11:50:58 2014

@author: james.cheong
"""

from datetime import date
import pandas as pd
import os
import ASX_ETL
#import ASX_DailyFiles



#startdate = date(2002, 9, 3)
#startdate = date(2003, 12, 5)          # Entire column of Settlement_Date field is empty,  ClosingSnapshot_Futures_
#startdate = date(2009, 6, 23)          # Some rows of Settlement_Date is empty, ClosingSnapshot_Futures_
#startdate = date(2009, 7, 8)           # 23/06/2009 to 29/06/2009 and 8/07/2009 to 21/07/2009 Dodgy data in raw file, NULL when 0 expected, OpenInterest_Cap
#startdate = date(2012, 11, 21)          # Date format is dd/mm/YYYY instead of YYYY/mm/dd , ClosingSnapshot_Futures_
#startdate = date(2012, 11, 22)
#startdate = date(2014, 3, 12)           # Missing Expiration date, ClosingSnapshot_Options_
#startdate = date(2014, 3, 13)
#startdate = date(2014, 4, 1)        # To 1st to 3rd April: Missing Expiration date and Date format is dd/mm/YYYY instead of YYYY/mm/dd, ClosingSnapshot_Options_
#startdate = date(2014, 4, 4)


startdate = date(2002, 9, 3)
#startdate = date(2008, 3, 31)
#startdate=date(2008, 5, 13)

#startdate = date(2014, 6, 13)
enddate = date(2014, 6, 16)
#enddate = date(2008, 5, 13)

dir = os.path.normpath("C:/Development/Daily Files/")


ts = pd.date_range(start=startdate, end=enddate, freq="D")


for day in ts:
    print day
 #   ASX_DailyFiles.download(day, dir)    
    ASX_ETL.processFiles(day, dir)