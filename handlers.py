# -*- coding: utf-8 -*-
"""
Created on Thu Jul 03 17:01:55 2014

@author: andrew.dodds
"""
import pyodbc
import logging
from StringIO import StringIO
from pandas import read_csv, ExcelFile, DataFrame
from re import match, sub
from math import isnan
from petl import addfield
from datetime import datetime
from cdecimal import Decimal
from numpy import all
import os
from zipfile import ZipFile
import ASX_readers
import TasHydro_Readers
import xml.etree.ElementTree as ET
from datadog import api as DataDogAPI

logger = logging.getLogger(__name__)
tags = ['version:1', 'application:savvy data loader']

MERCARI_FIELD = [
    'instrument',
    'tenor_type',
    'tenor',
    'settle',
    'bid',
    'offer',
    'mid',
    'bid_method',
    'offer_method',
    'mid_method'
]


# db_connection_string = 'Driver={SQL Server Native Client 11.0};Server=MEL-LT-001\SQLEXPRESS;Database=MarketData;Trusted_Connection=yes;'
# conn = pyodbc.connect(db_connection_string)

def format_column_heading(ch):
    # handle tupleized columns
    if ch.__class__ is tuple:
        tup = ch
        ch = ''
        for elem in tup:
            ch = ch + ('' if elem.startswith('Unnamed') else elem + ' ')

    # remove leading/trailing whitespace    
    ch = ch.strip()

    # remove [number] from rhs
    s = match(r"\][0-9]+\[", ch[::-1])  # apply reversed pattern to reversed string because it occurs on rhs
    ch = ch if s is None else ch[:-s.end()]

    # ensure all characters are alphanumeric or underscore
    ch = sub(r"[^$A-Za-z0-9_]+", '_', ch)

    # remove leading & trailing underscores
    ch = ch.strip("_")

    # ensure first character is valid else prepend underscore
    ch = ch if match(r"[$0-9]", ch) is None else "_" + ch

    # lower case
    ch = ch.lower()

    return ch


def sql_merge_statement(dest_table, all_fields, key_fields):
    data_fields = list(set(all_fields).difference(key_fields))
    all_fields = map(lambda x: "[" + x + "]", all_fields)
    key_fields = map(lambda x: "[" + x + "]", key_fields)
    data_fields = map(lambda x: "[" + x + "]", data_fields)

    if len(key_fields) > 0:
        s = "MERGE " + dest_table + "\nUSING (\n\tVALUES(" + ','.join(map(lambda x: '?', all_fields)) + ")\n)"
        s = s + " AS src (" + ','.join(all_fields) + ")\n ON "
        s = s + ' AND '.join(map(lambda x: (dest_table + ".{c} = src.{c}").format(c=x), key_fields))
        s = s + "\nWHEN MATCHED THEN \n\tUPDATE SET " + ','.join(
            map(lambda x: "{c} = src.{c}".format(c=x), data_fields))
        s = s + "\nWHEN NOT MATCHED THEN \n\tINSERT (" + ','.join(all_fields) + ")"
        s = s + "\n\tVALUES (" + ','.join(map(lambda x: 'src.' + x, all_fields)) + ")\n;"

    else:
        s = "INSERT INTO " + dest_table + "(" + ','.join(all_fields) + ") VALUES (" + ','.join(
            map(lambda x: '?', all_fields)) + ")"
    return s


def asx_load(source_file_id, fname, conn, table=None, key_fields=None, output_type='db'):
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
        logger.error('Invalid, unspecified or unsupported ASX table specified for loading')
        DataDogAPI.Event.create(title="Invalid:",
                                text='Invalid, unspecified or unsupported ASX table specified for loading', tags=tags,
                                alert_type="error")
        return (False, 0)

    if d is None:
        logger.error('No result returned from asx_reader function. Possible invalid file')
        DataDogAPI.Event.create(title="No result:",
                                text='No result returned from asx_reader function. Possible invalid file', tags=tags,
                                alert_type="error")
        return (False, 0)

    d = addfield(d, 'source_file_id', source_file_id)
    if output_type == 'petl':
        return d

    # determine key fields and data fields
    if key_fields is None:
        key_fields = []
    else:
        if not set(key_fields).issubset(d[0]):
            logger.error('key_fields must be a subset of csv fields')
            DataDogAPI.Event.create(title="Key_fields Error:", text='key_fields must be a subset of csv fields',
                                    tags=tags, alert_type="error")
            return (False, 0)

    # merge into database
    sql = sql_merge_statement(table, d[0], key_fields)

    sql_params = d[1:]  # map(tuple, df.values)
    # convert nans to None so insert/update will work correctly    
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)

    # try:
    # merge to database if records found
    if len(d) - 1 > 0:
        curs = conn.cursor()
        curs.executemany(sql, sql_params)
        conn.commit()
        curs.close()
    # except:
    #    raise
    #    return (df, sql)

    return (True, len(d) - 1)


def tasHydro_load(source_file_id, fname, conn, table=None, key_fields=None, output_type='db'):
    if table == 'TAS_HydroTasRateCard':
        print
        fname
        d = TasHydro_Readers.format_RateCard(fname)
    else:
        DataDogAPI.Event.create(title="Invalid:",
                                text='Invalid, unspecified or unsupported Tas Hydro table specified for loading',
                                tags=tags, alert_type="error")
        logger.error('Invalid, unspecified or unsupported Tas Hydro table specified for loading')
        return (False, 0)

    if d is None:
        DataDogAPI.Event.create(title="No result:",
                                text='No result returned from TasHydro_Readers function. Possible invalid file',
                                tags=tags, alert_type="error")
        logger.error('No result returned from TasHydro_Readers function. Possible invalid file')
        return (False, 0)

    d = addfield(d, 'source_file_id', source_file_id)
    if output_type == 'petl':
        return d

    # determine key fields and data fields
    if key_fields is None:
        key_fields = []
    else:
        if not set(key_fields).issubset(d[0]):
            DataDogAPI.Event.create(title="key_fields Error:", text='key_fields must be a subset of csv fields',
                                    tags=tags, alert_type="error")
            logger.error('key_fields must be a subset of csv fields')
            return (False, 0)

    # merge into database
    sql = sql_merge_statement(table, d[0], key_fields)

    sql_params = d[1:]  # map(tuple, df.values)
    # convert nans to None so insert/update will work correctly    
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)

    try:
        # merge to database if records found
        if len(d) - 1 > 0:
            curs = conn.cursor()
            curs.executemany(sql, sql_params)
            conn.commit()
            curs.close()
    except pyodbc.IntegrityError:
        print
        "IntegrityError: Duplicate key."
        #    raise
    #    return (df, sql)

    return (True, len(d) - 1)


def unzip_handler(source_file_id, fname, conn, dest_folder=None):
    (file_folder, file_name) = os.path.split(fname)
    dest_folder = file_folder if dest_folder is None else dest_folder

    with ZipFile(fname) as zf:
        zf.extractall(dest_folder)

    return (True, 0)


def xl_load(source_file_id, fname, conn, dest_table, key_fields=None, **kwargs):
    xl = ExcelFile(fname)
    df = xl.parse(**kwargs)
    df = df.rename(columns=format_column_heading)

    # add source file identifier
    df['source_file_id'] = source_file_id

    # determine key fields and data fields
    if key_fields is None:
        key_fields = []
    else:
        if not set(key_fields).issubset(df.keys()):
            logger.error('key_fields must be a subset of csv fields. key_fields: %s, csv fields: %s', str(key_fields),
                         str(df.keys()))
            error_text = 'key_fields must be a subset of csv fields. key_fields: %s, csv fields: %s' % str(
                key_fields), str(df.keys())
            DataDogAPI.Event.create(title="key_fields Error:", text=error_text, tags=tags, alert_type="error")

            return (False, 0)

    if dest_table is None:
        return df

    # merge into database
    sql = sql_merge_statement(dest_table, df.keys(), key_fields)

    sql_params = map(tuple, df.values)
    # convert nans to None so insert/update will work correctly    
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)

    # try:
    # merge to database if any records found
    if len(df) > 0:
        curs = conn.cursor()
        curs.executemany(sql, sql_params)
        conn.commit()
        curs.close()
    # except:
    #    raise
    #    return (df, sql)

    return (True, len(df))


def weather_forecast_load(source_file_id, fname, conn=None, dest_table='BOM_PrecisForecast2'):
    tree = ET.parse(fname)
    root = tree.getroot()

    df = DataFrame()
    for a in root.findall('.//area'):
        for fp in a.findall('forecast-period'):
            rec = a.attrib
            rec.update(fp.attrib)
            for c in fp.findall('*'):
                rec.update({c.attrib['type']: c.text})
            df = df.append(rec, ignore_index=True)
            # return df
    df = df.rename(columns=format_column_heading)
    df = df.convert_objects(convert_numeric=True)
    # add source file identifier
    df['source_file_id'] = source_file_id
    key_fields = ['description', 'start_time_utc']
    # return dataframe if destination table is not specified
    if conn is None:
        return df
    # merge into database
    sql = sql_merge_statement(dest_table, df.keys(), key_fields)

    sql_params = map(tuple, df.values)
    # convert nans to None so insert/update will work correctly    
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)
    # try:
    # merge to database if any records found
    if len(df) > 0:
        curs = conn.cursor()
        curs.executemany(sql, sql_params)
        conn.commit()
        curs.close()
    # except:
    #    raise
    #    return (df, sql)
    return (True, len(df))


def csv_load(source_file_id, fname, conn, dest_table, header_end_text=None, footer_start_text=None, key_fields=None,
             **kwargs):
    # read entire file into memory
    f = open(fname, 'rt')
    s = f.read()
    f.close()

    # identify payload, top & tail
    start_index = 0 if header_end_text is None else s.find(header_end_text) + len(header_end_text)
    if header_end_text is not None and start_index < len(header_end_text):  # note: find returns -1 if string not found
        logger.error("The text specified to indicate the end of the header is not found")
        DataDogAPI.Event.create(title="Header not found:",
                                text='The text specified to indicate the end of the header is not found', tags=tags,
                                alert_type="error")

        return (False, 0)
    else:
        end_index = len(s) if footer_start_text is None else start_index + s[start_index:].find(footer_start_text)
        if end_index < start_index:
            logger.error("The text specified to indicate the beginning of the footer is not found")
            DataDogAPI.Event.create(title="Footer not found:",
                                    text='The text specified to indicate the beginning of the footer is not found',
                                    tags=tags, alert_type="error")
            return (False, 0)

    csv_str = s[start_index:end_index]
    #    if csv_str[0] == ',':
    #        csv_scsv_str.replace("\n,","\n")[1:]
    str_buf = StringIO(csv_str)

    # read as csv
    df = read_csv(str_buf, **kwargs)

    # format headings
    df = df.rename(columns=format_column_heading)

    # parse csv headings and verify they match destination table
    df = df.rename(columns=format_column_heading)

    # add source file identifier
    df['source_file_id'] = source_file_id

    # determine key fields and data fields
    if key_fields is None:
        key_fields = []
    else:
        if not set(key_fields).issubset(df.keys()):
            error_text = 'key_fields must be a subset of csv fields. key_fields: %s, csv fields: %s' % str(
                key_fields), str(df.keys())
            logger.error(error_text)
            DataDogAPI.Event.create(title="CSV felds error:", text=error_text, tags=tags, alert_type="error")
            return (False, 0)

    #
    #    else:
    #        logger.warning('')

    # check for duplicates/conflicts

    # fields to compare

    # return dataframe if destination table is not specified
    if dest_table is None:
        return df

    # merge into database
    sql = sql_merge_statement(dest_table, df.keys(), key_fields)

    sql_params = map(tuple, df.values)
    # convert nans to None so insert/update will work correctly    
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)

    # try:
    # merge to database if any records found
    if len(df) > 0:
        curs = conn.cursor()
        curs.executemany(sql, sql_params)
        conn.commit()
        curs.close()
    # except:
    #    raise
    #    return (df, sql)

    return (True, len(df))


# SQLAlchemy Engine
# engine = sa.create_engine('mssql://MEL-LT-001\SQLEXPRESS/MarketData;trusted_connection=yes')

# BOM daily 
# df = handlers.csv_load(source_file_id=0,fname=r'C:\temp\bom daily\townsville_aero-201304.csv', conn=conn, dest_table='BOM_Daily', header=[9,10,11,12], tupleize_cols=True, skipfooter=1, engine='python', skipinitialspace=True, key_fields = ['station_name','date'],parse_dates=[1],dayFirst=True)
# {'dest_table':'BOM_Daily', 'header':[9,10,11,12], 'tupleize_cols':True, 'skipfooter':1, 'engine':'python', 'skipinitialspace':True, 'key_fields' : ['station_name','date'],'parse_dates':[1],'dayFirst':True}

# BOM 3100
# (df,sql) = handlers.csv_load(source_file_id=0,fname=r'C:\temp\bom files\success\IDY03100.201406300415.axf', conn=conn, dest_table='BOM_IntraDay_AWS', header_end_text="[metar2Data]\n", skipinitialspace=True, footer_start_text=r"[$]", na_values=[-9999,-9999.0], parse_dates=[[2,3]], keep_date_col=True, key_fields = ['id_num','date','time'])
# {'dest_table':'BOM_IntraDay_AWS', 'header_end_text':"[metar2Data]\n", 'skipinitialspace':True, 'footer_start_text':r"[$]", 'na_values':[-9999,-9999.0], 'parse_dates':[[2,3]], 'keep_date_col':True, 'key_fields' : ['id_name','date','time']}

# BOM 3000
# df = handlers.csv_load(source_file_id=0,fname=r'C:\temp\bom files\IDY03000.201407090915.axf', conn=conn, dest_table='BOM_IntraDay_SynOp', skipinitialspace=True, header_end_text="[synopData]\n", footer_start_text=r"[$]", na_values=[-9999,-9999.0], parse_dates=[[3,4]], keep_date_col=True, key_fields = ['wmo_id','date','time'])
# {'dest_table':'BOM_IntraDay_SynOp', 'skipinitialspace':True, 'header_end_text':"[synopData]\n", 'footer_start_text':r"[$]", 'na_values':[-9999,-9999.0], 'parse_dates':[[3,4]], 'keep_date_col':True, 'key_fields': ['wmo_id','date','time']}

# NSLP
# need to unzip first
# (df,sql) = handlers.csv_load(source_file_id=0,fname=r'C:\temp\mdmtm_ssequeirap_524220222.xml', conn=conn, dest_table='AEMO_NSLP', header_end_text=r'<CSVData>', footer_start_text=r'</CSVData>', parse_dates=[2,3], key_fields = ['profilename','profilearea','settlementdate'])
# {'dest_table':'AEMO_NSLP', 'header_end_text':r'<CSVData>', 'footer_start_text':r'</CSVData>', 'parse_dates':[2,3], 'key_fields' : ['profilename','profilearea','settlementdate']}

# BOM 60801
# df = handlers.csv_load(source_file_id=0,fname=r'IDV60801.94868.axf', conn=conn, dest_table=None, skipinitialspace=True, header_end_text="[data]\n", footer_start_text=r"[$]", na_values=[-9999,-9999.0], parse_dates=[5,6], keep_date_col=True, key_fields = ['wmo','aifstime_utc'])
# {'dest_table':None, 'skipinitialspace':True, 'header_end_text':"[synopData]\n", 'footer_start_text':r"[$]", 'na_values':[-9999,-9999.0], 'parse_dates':[[3,4]], 'keep_date_col':True, 'key_fields': ['wmo_id','date','time']}

# ASX Trade log
# df = handlers.csv_load(source_file_id=0, fname=r'c:\temp\asx_tradelog20140710.csv', conn=conn, dest_table='ASX_Tradelog', skiprows=1, parse_dates=[0], dayfirst=True, key_fields=[])
# {'dest_table':'ASX_Tradelog', 'skiprows':1, 'parse_dates':[0], 'dayfirst':True, 'key_fields':[]}


def sql_mdff_merge_statement(dest_table, fields, merge_keys):
    # data_fields = list(set(fields).difference(merge_keys))
    fields = map(lambda x: "[" + x + "]", fields)
    merge_keys = map(lambda x: "[" + x + "]", merge_keys)
    # data_fields = map(lambda x: "[" + x + "]", data_fields)

    if len(merge_keys) > 0:
        s = "MERGE " + dest_table + "\nUSING (\n\tVALUES(" + ','.join(map(lambda x: '?', fields)) + ")\n)"
        s = s + " AS src (" + ','.join(fields) + ")\n ON "
        s = s + ' AND '.join(map(lambda x: (
                "(" + dest_table + ".{c} = src.{c} OR (" + dest_table + ".{c} is null and src.{c} is null))").format(
            c=x), merge_keys))
        s = s + "\nWHEN MATCHED THEN \n\tUPDATE SET " + fields[0] + "=" + dest_table + "." + fields[
            0]  # + ','.join(map(lambda x: "{c} = src.{c}".format(c=x), data_fields))
        s = s + "\nWHEN NOT MATCHED THEN \n\tINSERT (" + ','.join(fields) + ")"
        s = s + "\n\tVALUES (" + ','.join(map(lambda x: 'src.' + x, fields)) + ")\n"
        s = s + "\nOUTPUT inserted.ID;"
    else:
        s = "INSERT INTO " + dest_table + "(" + ','.join(fields) + ") OUTPUT inserted.ID VALUES (" + ','.join(
            map(lambda x: '?', fields)) + ")"
    return s


def throws_a(func, *exceptions):
    try:
        func()
        return False
    except exceptions or Exception:
        return True


# checks that all tokens comply with MDFF requirements on length, data type, manadatory etc.
def mdf_length_type_check(toks, fields, data_types, lengths, mandatory):
    if not len(fields) == len(data_types) == len(lengths) == len(mandatory):
        raise Exception("Invalid configuration passed to mdf_length_type_check. Lengths are %d %d %d %d", len(fields),
                        len(data_types), len(lengths), len(mandatory))
    if not len(fields) == len(toks):
        error_text = 'Invalid token stream in meter data file. Too many or too few tokens. Expecting %d, found %d' % len(
            fields), len(toks[1:])
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid token stream:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    tok_length = map(len, toks)
    vals = []

    for i in range(0, len(fields)):  # check each field
        # warn if leading or trailing spaces, but remove and continue
        if toks[i].strip() <> toks[i]:
            logger.warn('Leading or trailing whitespace found in field %s. Found: "%s"', fields[i], toks[i])
            toks[i] = toks[i].strip()
            tok_length[i] = len(toks[i])

        if mandatory[i] and tok_length[i] == 0:  # for missing mandatory values
            error_text = 'Missing a mandatory value in field %s' % fields[i]
            logger.error(error_text)
            DataDogAPI.Event.create(title="Missing value:", text=error_text, tags=tags, alert_type="error")

            return (False, [])
        if data_types[i] == 'C' and tok_length[i] not in (0, lengths[i]):
            error_text = 'Fixed length string of incorrect length in field %s. Expecting %d characters, found "%s"' % \
                         fields[i], lengths[i], toks[i]
            logger.error(error_text)
            DataDogAPI.Event.create(title="Incorrect length:", text=error_text, tags=tags, alert_type="error")
            return (False, [])
        if data_types[i] == 'V' and tok_length[i] > lengths[i]:
            error_text = 'Value exceeds maximum allowed length in field %s. Expecting %d characters, found %d. Value is "%s"' % \
                         fields[i], lengths[i], tok_length[i], toks[i]
            logger.error(error_text)
            DataDogAPI.Event.create(title="Exceeds maximum length:", text=error_text, tags=tags, alert_type="error")
            return (False, [])
        if data_types[i] == 'D' and tok_length[i] not in (0, lengths[i]):
            error_text = 'Datetime value of incorrect length in field %s. Expecting %d characters, found "%s"' % fields[
                i], lengths[i], toks[i]
            logger.error(error_text)
            DataDogAPI.Event.create(title="Incorrect datetime:", text=error_text, tags=tags, alert_type="error")

            return (False, [])
        if data_types[i] == 'D' and tok_length[i] != 0:  # check that toks[i] contains a valid date
            try:
                if lengths[i] > 12:
                    s = int(toks[i][12:14])
                else:
                    s = 0
                if lengths[i] > 8:
                    h = int(toks[i][8:10])
                    m = int(toks[i][10:12])
                else:
                    h = m = 0
                datetime(int(toks[i][0:4]), int(toks[i][4:6]), int(toks[i][6:8]), h, m, s)
            except ValueError:
                error_text = 'Invalid date value. Expecting Datetime(%d), found "%s"' % lengths[i], toks[i]
                logger.error(error_text)
                DataDogAPI.Event.create(title="Incorrect date value:", text=error_text, tags=tags, alert_type="error")
                return (False, [])
        if data_types[i] == 'N' and int(lengths[i]) == lengths[i]:  # expecting integer
            if tok_length[i] > lengths[i]:
                error_text = 'Value exceeds maximum allowed length in field %s. Expecting max of %d characters, found %d. Value is "%s"' % \
                             fields[i], lengths[i], tok_length[i], toks[i]
                logger.error(error_text)
                DataDogAPI.Event.create(title="Exceeds maximum length:", text=error_text, tags=tags, alert_type="error")
                return (False, [])
            if len(toks[i]) > 0 and throws_a(lambda: int(toks[i]), ValueError):
                error_text = 'Invalid value encountered in field %s. Expecting integer, found "%s"' % fields[i], toks[i]
                logger.error(error_text)
                DataDogAPI.Event.create(title="Invalid field:", text=error_text, tags=tags, alert_type="error")
                return (False, [])
        if data_types[i] == 'N' and int(lengths[i]) != lengths[i]:  # expecting float
            (pre, post) = map(int, str(lengths[i]).split('.'))  # max digits expected before & after decimal place
            s = toks[i].split('.')
            if len(s[0]) > pre or (len(s) > 1 and len(s[1]) > post):
                error_text = 'Value exceeds maximum allowed length or precision in field %s. Expecting Numeric(%d.%d), found "%s"' % \
                             fields[i], pre, post, toks[i]
                logger.error(error_text)
                DataDogAPI.Event.create(title="Exceeds maximum length:", text=error_text, tags=tags, alert_type="error")
                return (False, [])
            if len(s) > 2 or (len(s[0]) > 0 and throws_a(lambda: int(s[0]), ValueError)) or (
                    len(s) == 2 and (throws_a(lambda: int(s[1]), ValueError) or int(s[1]) < 0)) or throws_a(
                lambda: float(toks[i]), ValueError):
                error_text = 'Invalid value encountered in field %s. Expecting Numeric(%d.%d), found "%s"' % fields[
                    i], pre, post, toks[i]
                logger.error(error_text)
                DataDogAPI.Event.create(title="Invalid field:", text=error_text, tags=tags, alert_type="error")
                return (False, [])

        if data_types[i] in ("C", "V"):
            val = toks[i]
        elif data_types[i] == "D" and len(toks[i]) == 0:  # null date
            val = None
        elif data_types[i] == "D" and len(toks[i]) > 0:
            val = datetime(int(toks[i][0:4]), int(toks[i][4:6]), int(toks[i][6:8]), h, m, s)
        elif data_types[i] == "N" and int(lengths[i]) != lengths[i] and len(toks[i]) > 0:
            val = Decimal(toks[i])
        elif data_types[i] == "N" and int(lengths[i]) == lengths[i] and len(toks[i]) > 0:
            val = int(toks[i])
        elif data_types[i] == "N" and len(toks[i]) == 0:
            val = None
        else:
            error_text = 'Unhandled data type encountered in field %s' % fields[i]
            logger.error(error_text)
            DataDogAPI.Event.create(title="Unhandled data type:", text=error_text, tags=tags, alert_type="error")
            return (False, [])

        vals.append(val)

    return (True, vals)


def process_MDF_100(conn, version_header, toks, last_rec_ind):
    valid_after = [None]
    table_name = 'MDF_FileDetails'
    fields = ['VersionHeader', 'DateTime', 'FromParticipant', 'ToParticipant']
    merge_keys = fields
    data_types = 'VDVV'
    lengths = [5, 12, 10, 10]
    mandatory = [True, True, True, True]

    if not last_rec_ind in valid_after:  # check for file blocking errors
        error_text = 'Meter data file blocking error'
        logger.error(error_text)
        DataDogAPI.Event.create(title="Unhandled data type:", text=error_text, tags=tags, alert_type="error")
        return (False, [])

    # allocate tokens, confirm data types and field lengths as required
    (status, vals) = mdf_length_type_check(toks, fields, data_types, lengths, mandatory)
    if not status:
        return (False, [])

    # specific checks    
    if (version_header is not None) and version_header != toks[0]:
        error_text = 'VersionHeader in filename and 100 record do not match. Filename has %s and 100-record has %s' % version_header, \
                     toks[0]
        logger.error(error_text)
        DataDogAPI.Event.create(title="'VersionHeader not match:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    if toks[0] not in ("NEM12", "NEM13"):
        error_text = 'VersionHeader in 100 record is invalid. Requires NEM12 or NEM13, found %s' % toks[0]
        logger.error(error_text)
        DataDogAPI.Event.create(title="VersionHeader not match:", text=error_text, tags=tags, alert_type="error")
        return (False, [])

    # merge record to database, returning id
    sql = sql_mdff_merge_statement(table_name, fields, merge_keys)
    curs = conn.cursor()
    curs.execute(sql, tuple(vals))
    thisid = curs.fetchone()[0]
    curs.close()
    vals.append(thisid)
    return (True, vals)


def process_MDF_200(conn, version_header, toks, last_rec_ind, last100):
    valid_after = ['100', '300', '400', '500']
    table_name = 'MDF_Interval_StreamDetails'
    fields = ['NMI', 'NMIConfiguration', 'RegisterID', 'NMISuffix', 'MDMDataStreamIdentifier', 'MeterSerialNumber',
              'UOM', 'IntervalLength', 'NextScheduledReadDate']
    merge_keys = fields
    data_types = 'CVVCCVVND'
    lengths = [10, 240, 10, 2, 2, 12, 5, 2, 8]
    mandatory = [True, True, False, True, False, False, True, True, False]

    if not last_rec_ind in valid_after:  # check for file blocking errors
        logger.error('Meter data file blocking error')
        DataDogAPI.Event.create(title="Blocking error:", text='Meter data file blocking error', tags=tags,
                                alert_type="error")
        return (False, [])

    # allocate tokens, confirm data types and field lengths as required
    (status, vals) = mdf_length_type_check(toks, fields, data_types, lengths, mandatory)
    if not status:
        return (False, [])

    # specific checks
    # valid NMIConfiguration
    if not all(map(lambda x: match(r'[A-HJ-NP-Z][1-9A-HJ-NP-Z]', x) is not None,
                   [vals[1][i:i + 2] for i in xrange(0, len(vals[1]), 2)])):
        error_text = 'Invalid NMI Configuration. Found %s' % vals[1]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid NMI Configuration:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # valid RegisterID?
    # valid NMISuffix
    if match(r'[A-HJ-NP-Z][1-9A-HJ-NP-Z]', vals[3]) is None:
        error_text = 'Invalid NMI Suffix. Found %s' % vals[3]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid NMI Suffix:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # valid MDMDataStreamIdentifier?
    # valid UOM
    if not vals[6].lower() in (
            'mwh', 'kwh', 'wh', 'mvarh', 'kvarh', 'varh', 'mvar', 'kvar', 'var', 'mw', 'kw', 'w', 'mvah', 'kvah', 'vah',
            'mva',
            'kva', 'va', 'kv', 'v', 'ka', 'a', 'pf'):
        error_text = 'Invalid UOM encountered. Found value %s' % vals[6]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid UOM:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
        # valid IntervalLength
    if not vals[7] in (1, 5, 10, 15, 30):
        error_text = 'Invalid IntervalLength value encountered. Found %d' % vals[7]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid IntervalLength:", text=error_text, tags=tags, alert_type="error")
        return (False, [])

    # merge record to database, returning id
    sql = sql_mdff_merge_statement(table_name, fields, merge_keys)
    curs = conn.cursor()
    curs.execute(sql, tuple(vals))
    thisid = curs.fetchone()[0]
    curs.close()
    vals.append(thisid)
    return (True, vals)


def process_MDF_300(conn, version_header, toks, last_rec_ind, last100, last200, source_file_id):
    valid_after = ['200', '300', '400', '500']
    table_name = 'MDF_Interval_Readings'
    # print last200
    n_readings = 1440 / last200[7]
    qm = 1 + n_readings
    fields = ['IntervalDate'] + ['IntervalValue' + str(i) for i in range(1, n_readings + 1)] + ['QualityMethod',
                                                                                                'ReasonCode',
                                                                                                'ReasonDescription',
                                                                                                'UpdateDateTime',
                                                                                                'MSATSLoadDateTime']
    db_fields = ['IntervalDate'] + ['IntervalValue' + str(i) for i in range(1, n_readings + 1)] + ['UpdateDateTime',
                                                                                                   'MSATSLoadDateTime']

    data_types = 'D' + 'N' * n_readings + 'VNVDD'
    if last200[6][0].upper() == 'M':
        reading_length = 15.6
    elif last200[6][0].upper() == 'K':
        reading_length = 15.3
    elif last200[6][0].upper() == 'p':
        reading_length = 15.2
    else:
        reading_length = 15

    lengths = [8] + [reading_length for i in range(1, n_readings + 1)] + [3, 3, 240, 14, 14]
    mandatory = [True] + [True for i in range(1, n_readings + 1)] + [True, False, False, False, False]

    if not last_rec_ind in valid_after:  # check for file blocking errors
        logger.error('Meter data file blocking error')
        DataDogAPI.Event.create(title="File blocking:", text='Meter data file blocking error', tags=tags,
                                alert_type="error")

        return (False, [])

    # allocate tokens, confirm data types and field lengths as required
    (status, vals) = mdf_length_type_check(toks, fields, data_types, lengths, mandatory)
    if not status:
        return (False, [])

    # specific checks

    # valid qualitymethod
    if toks[qm] not in ('A', 'N', 'V') and match(r"[AEFNSV][1567][1-9]",
                                                 toks[qm]) is None:  # note: detects most but not all illegal values
        error_text = 'Invalid QualityMethod value in 300 row. Found %s' % toks[qm]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid QualityMethod:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # reasoncode valid if provided
    if len(toks[qm + 1]) > 0 and (vals[qm + 1] < 0 or vals[qm + 1] > 94):
        error_text = 'Invalid ReasonCode supplied in 300 row. Found %s' % toks[qm + 1]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid ReasonCode:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # no reasoncode if qualityflag is V
    if (vals[qm + 1] is not None and toks[qm][0] == 'V') or (vals[qm + 1] is None and toks[qm][0] in ('F', 'S')):
        error_text = 'In 300 row, ReasonCode supplied with quality "V" or ReasonCode not supplied with Quality "F" or "S". Quality flag %s, ReasonCode %s' % \
                     toks[qm][0], toks[qm + 1]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid quality:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
        # reasondescription supplied if reasoncode = 0
    if len(vals[qm + 2]) < 1 and vals[qm + 1] == 0:
        error_text = 'Missing ReasonDescription where ReasonCode is 0 in 300 row'
        logger.error(error_text)
        DataDogAPI.Event.create(title="Missing ReasonDescription:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # updatedatetime provided unless qualitymethod is N
    if vals[qm + 3] is None and vals[qm][0] != 'N':
        error_text = 'Missing UpdateDateTime in 300 row where Quality is not "N"'
        logger.error(error_text)
        DataDogAPI.Event.create(title="Missing UpdateDateTime:", text=error_text, tags=tags, alert_type="error")
        return (False, [])

    # merge record to database, returning id    

    # sql = sql_mdff_merge_statement(table_name,db_fields,db_fields)
    sql = sql_mdff_merge_statement(table_name, db_fields, [])
    curs = conn.cursor()
    insert_vals = vals[0:n_readings + 1] + vals[n_readings + 4:]
    insert_vals[0] = str(insert_vals[0])
    for t in range(1, n_readings + 1):
        insert_vals[t] = float(insert_vals[t])

    curs.execute(sql, tuple(insert_vals))
    thisid = curs.fetchone()[0]
    curs.execute(
        'INSERT MDF_Interval_Key (FileDetailsID,StreamDetailsID,ReadingID,source_file_id) OUTPUT Inserted.ID VALUES (?,?,?,?)',
        (last100[-1], last200[-1], thisid, source_file_id))
    key_id = curs.fetchone()[0]
    curs.close()
    vals.append(key_id)
    vals.append(thisid)

    # insert dummy 400 record to hold quality information
    if toks[qm][0] != 'V':
        (tf, res) = process_MDF_400(conn, version_header, ['1', str(n_readings), toks[qm], toks[qm + 1], toks[qm + 2]],
                                    '300', last200, vals, None)
        if not tf:
            return (False, [])

    # return value
    return (True, vals)


def process_MDF_400(conn, version_header, toks, last_rec_ind, last200, last300, last400):
    valid_after = ['300', '400']
    table_name = 'MDF_Interval_Quality'
    fields = ['StartInterval', 'EndInterval', 'QualityMethod', 'ReasonCode', 'ReasonDescription']

    data_types = 'NNVNV'
    lengths = [4, 4, 3, 3, 240]
    mandatory = [True, True, True, False, False]
    qm = 2

    if not last_rec_ind in valid_after:  # check for file blocking errors
        error_text = 'Meter data file blocking error'
        logger.error(error_text)
        DataDogAPI.Event.create(title="File blocking:", text=error_text, tags=tags, alert_type="error")
        return (False, [])

    # allocate tokens, confirm data types and field lengths as required
    (status, vals) = mdf_length_type_check(toks, fields, data_types, lengths, mandatory)
    if not status:
        return (False, [])

    # specific checks    
    if vals[0] < 1 or vals[1] < vals[0] or vals[1] > 1440 / last200[7]:
        error_text = 'Illegal StartInterval/EndInterval values. StartInterval = %d, EndInterval = %d, IntervalLength = %d' % \
                     vals[0], vals[1], last200[7]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Illegal StartInterval/EndInterval values:", text=error_text, tags=tags,
                                alert_type="error")
        return (False, [])
    if (last_rec_ind != '400' and vals[0] != 1) or (last_rec_ind == '400' and vals[0] != last400[1] + 1):
        error_text = 'Mismatch between StartInterval and preceeding row in 400-record'
        logger.error(error_text)
        DataDogAPI.Event.create(title="Mismatch between StartInterval and preceeding:", text=error_text, tags=tags,
                                alert_type="error")
        return (False, [])
    # valid qualitymethod
    if toks[qm] not in ('A', 'N') and match(r"[AEFNS][1567][1-9]",
                                            toks[qm]) is None:  # note: detects most but not all illegal values
        error_text = 'Invalid QualityMethod value in 400 row. Found %s' % toks[qm]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid QualityMethod:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # reasoncode valid if provided
    if len(toks[qm + 1]) > 0 and (vals[qm + 1] < 0 or vals[qm + 1] > 94):
        error_text = 'Invalid ReasonCode supplied in 400 row. Found %s' % toks[qm + 1]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid ReasonCode:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # no reasoncode if qualityflag is V
    if (vals[qm + 1] is not None and toks[qm][0] == 'V') or (vals[qm + 1] is None and toks[qm][0] in ('F', 'S')):
        error_text = 'In 400 row, ReasonCode supplied with quality "V" or ReasonCode not supplied with Quality "F" or "S". Quality flag %s, ReasonCode %s' % \
                     toks[qm][0], toks[qm + 1]
        logger.error(error_text)
        DataDogAPI.Event.create(title="Invalid quality:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
        # reasondescription supplied if reasoncode = 0
    if len(vals[qm + 2]) < 1 and vals[qm + 1] == 0:
        error_text = 'Missing ReasonDescription where ReasonCode is 0 in 300 row'
        logger.error(error_text)
        DataDogAPI.Event.create(title="Missing ReasonDescription:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    # last300 had qualityflag V, or its a single-record 400 row
    if last300[-7][0] != "V" and (vals[0] != 1 or vals[1] != 1440 / last200[7]):
        error_text = '400-record found after 300-row with quality not V'
        logger.error('400-record found after 300-row with quality not V')
        DataDogAPI.Event.create(title="Invalid quality:", text=error_text, tags=tags, alert_type="error")
        return (False, [])

    # merge record to database, returning id
    sql = sql_mdff_merge_statement(table_name, fields + ['KeyID'], fields + ['KeyID'])
    curs = conn.cursor()
    curs.execute(sql, tuple(vals + [last300[-2]]))
    thisid = curs.fetchone()[0]
    curs.close()
    vals.append(thisid)
    return (True, vals)


def process_MDF_500(conn, version_header, toks, last_rec_ind, last300):
    valid_after = ['300', '400', '500']
    table_name = 'MDF_Interval_B2BDetails'
    fields = ['TransCode', 'RetServiceOrder', 'ReadDateTime', 'IndexRead']
    data_types = 'CVDV'
    lengths = [1, 15, 14, 15]
    mandatory = [True, False, False, False]

    if not last_rec_ind in valid_after:  # check for file blocking errors
        error_text = 'Meter data file blocking error'
        logger.error(error_text)
        DataDogAPI.Event.create(title="File block error:", text=error_text, tags=tags, alert_type="error")
        return (False, [])

    # allocate tokens, confirm data types and field lengths as required
    (status, vals) = mdf_length_type_check(toks, fields, data_types, lengths, mandatory)
    if not status:
        return (False, [])

    # specific checks    
    if vals[0] not in ("A", "C", "G", "D", "E", "N", "O", "S", "R"):
        logger.error('')
        return (False, [])

    # merge record to database, returning id
    sql = sql_mdff_merge_statement(table_name, fields + ['KeyID'], fields + ['KeyID'])
    curs = conn.cursor()
    curs.execute(sql, tuple(vals + [last300[-2]]))
    thisid = curs.fetchone()[0]
    curs.close()
    vals.append(thisid)
    return (True, vals)


def process_MDF_900(conn, version_header, toks, last_rec_ind):
    valid_after = ['300', '400', '500']
    if not last_rec_ind in valid_after:  # check for file blocking errors
        error_text = 'Meter data file blocking error'
        logger.error(error_text)
        DataDogAPI.Event.create(title="File block error:", text=error_text, tags=tags, alert_type="error")
        return (False, [])
    return (True, [])


def process_MDF_250(conn, version_header, toks, last_rec_ind, last100):
    return (True, [])


def process_MDF_550(conn, version_header, toks, last_rec_ind, last250):
    return (True, [])


def aemo_meter_data_handler(source_file_id, fname, conn):
    (fpath, filename) = os.path.split(fname)
    (filename, fileext) = os.path.splitext(filename)
    # is file zipped? if yes, unzip to temp folder and reset fname
    # if fileext == '.zip':

    # is file XML? extract csv data

    # determine if file has a valid filename for a NEM12 or NEM13 file
    s = filename.split('#')
    if len(s) == 4 and s[0] in ("NEM12", "NEM13") and len(s[1]) <= 36 and len(s[2]) <= 10 and len(s[3]) <= 10:
        (version_header, sender_id_val, from_participant, to_participant) = s
    else:
        logger.warning('Invalid file name encountered %s. Expecting NEMXX#IDENTIFIER_LEN36#FROMPARTIC#TOPARTICIP',
                       filename)
        version_header = sender_id_val = from_participant = to_participant = None

    # process file
    with open(fname, 'rt') as f:
        line_number = 0
        last_rec_ind = None
        last100 = last200 = last250 = last300 = last400 = None

        while last_rec_ind != '900':
            toks = f.readline().strip().split(',')  # read and split next line
            rec_ind = toks[0].strip()

            if rec_ind == '100':
                status, last100 = process_MDF_100(conn, version_header, toks[1:], last_rec_ind)
            elif rec_ind == '200':
                status, last200 = process_MDF_200(conn, version_header, toks[1:], last_rec_ind, last100)
            elif rec_ind == '300':
                status, last300 = process_MDF_300(conn, version_header, toks[1:], last_rec_ind, last100, last200,
                                                  source_file_id)
            elif rec_ind == '400':
                status, last400 = process_MDF_400(conn, version_header, toks[1:], last_rec_ind, last200, last300,
                                                  last400)
            elif rec_ind == '500':
                status, res = process_MDF_500(conn, version_header, toks[1:], last_rec_ind, last300)
            elif rec_ind == '900':
                status, res = process_MDF_900(conn, version_header, toks[1:], last_rec_ind)
            elif rec_ind == '250':
                status, last250 = process_MDF_250(conn, version_header, toks[1:], last_rec_ind, last100)
            elif rec_ind == '550':
                status, res = process_MDF_550(conn, version_header, toks[1:], last_rec_ind, last100, last250)
            else:
                error_text = 'Meter data file error. Invalid record indicator found in line %d: "%s"' % line_number, rec_ind
                logger.error(error_text)
                DataDogAPI.Event.create(title="Data block error:", text=error_text, tags=tags, alert_type="error")

                status = False

            if status == False:
                error_text = 'Error encountered processing file %s. The error occurred at line number %d', fname, line_number
                logger.error(error_text)
                DataDogAPI.Event.create(title="Processing block error:", text=error_text, tags=tags, alert_type="error")
                return (False, 0)

            # prepare for next iteration of loop
            last_rec_ind = rec_ind
            line_number = line_number + 1

        if len(f.read().strip()) > 0:
            error_text = 'Meter data file blocking error. File contents found following the end of a 100-900 block (line %d)' % line_number
            logger.error(error_text)
            DataDogAPI.Event.create(title="File block error:", text=error_text, tags=tags, alert_type="error")
            raise

    conn.commit()

    return (True, line_number)


def mercari_data_handler(source_file_id, fname, conn=None, dest_table='Environmental_Price_MercariClosingPrices', header_end_text=None, footer_start_text=None,**kwargs):
    # read entire file into memory
    f = open(fname, 'rt')
    s = f.read()
    f.close()
 
    # identify payload, top & tail
    start_index = 0 if header_end_text is None else s.find(header_end_text) + len(header_end_text)
    if header_end_text is not None and start_index < len(header_end_text):  # note: find returns -1 if string not found
        logger.error("The text specified to indicate the end of the header is not found")
        DataDogAPI.Event.create(title="Header not found:",
                                text='The text specified to indicate the end of the header is not found', tags=tags,
                                alert_type="error")

        return (False, 0)
    else:
        end_index = len(s) if footer_start_text is None else start_index + s[start_index:].find(footer_start_text)
        if end_index < start_index:
            logger.error("The text specified to indicate the beginning of the footer is not found")
            DataDogAPI.Event.create(title="Footer not found:",
                                    text='The text specified to indicate the beginning of the footer is not found',
                                    tags=tags, alert_type="error")
            return (False, 0)
  
    csv_str = s[start_index:end_index]
    #    if csv_str[0] == ',':
    #        csv_scsv_str.replace("\n,","\n")[1:]
    str_buf = StringIO(csv_str)
  
    # read as csv
   
    df = read_csv(str_buf)
    
    # parse csv headings and verify they match destination table
    df = df.rename(columns=format_column_heading_without_blank_header_key)

    #remove last column key error
    if 'unknown_10' in df.keys():
        del df.keys()['unknown_10']
     
    # add source file identifier
    df['source_file_id'] = source_file_id


    # determine key fields and data fields
    key_fields = MERCARI_FIELD
    if key_fields is None:
        key_fields = []
    else:
        if not set(key_fields).issubset(df.keys()):
            error_text = 'key_fields must be a subset of csv fields. key_fields: %s, csv fields: %s' % str(
                key_fields), str(df.keys())
            logger.error(error_text)
            DataDogAPI.Event.create(title="CSV felds error:", text=error_text, tags=tags, alert_type="error")
            return (False, 0)

    #
    #    else:
    #        logger.warning('')

    # check for duplicates/conflicts

    # fields to compare

    # return dataframe if destination table is not specified
    if dest_table is None:
        return df
    # merge into database
	
    sql = sql_mercari_merge_statement(dest_table, df.keys(), key_fields)


   
    sql_params = map(tuple, df.values)
        
    # convert nans to None so insert/update will work correctly    
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)

    intergrate_params = []
    for item in sql_params:
        if item[0] != item[1] and item[0] != item[2] and item[0] != item[3]:
            del item[len(item) - 2]
            if item[0] is None:
                item[0] = 'ESC'
            if item[1] is None:
                item[1] = 'Spot'
            intergrate_params.append(item)    

    # try:
    # merge to database if any records found
    if len(df) > 0:
        curs = conn.cursor()
        curs.executemany(sql, intergrate_params)
        conn.commit()
        curs.close()
    # except:
    #    raise
    #    return (df, sql)

    return (True, len(df))


def sql_mercari_merge_statement(dest_table, all_fields, key_fields):
  
    data_fields = list(set(all_fields).difference(key_fields))
    all_fields = map(lambda x: "[" + x + "]", all_fields)

    if '[unnamed_10]' in all_fields:
        all_fields.remove('[unnamed_10]')

    key_fields = map(lambda x: "[" + x + "]", key_fields)
    data_fields = map(lambda x: "[" + x + "]", data_fields)
 
    if '[unnamed_10]' in data_fields:
        data_fields.remove('[unnamed_10]')
    
    if len(key_fields) > 0:
        s = "MERGE " + dest_table + "\nUSING (\n\tVALUES(" + ','.join(map(lambda x: '?', all_fields)) + ")\n)"
        s = s + " AS src (" + ','.join(all_fields) + ")\n ON "
        s = s + ' AND '.join(map(lambda x: (dest_table + ".{c} = src.{c}").format(c=x), key_fields))
        s = s + "\nWHEN MATCHED THEN \n\tUPDATE SET " + ','.join(
            map(lambda x: "{c} = src.{c}".format(c=x), data_fields))
        s = s + "\nWHEN NOT MATCHED THEN \n\tINSERT (" + ','.join(all_fields) + ")"
        s = s + "\n\tVALUES (" + ','.join(map(lambda x: 'src.' + x, all_fields)) + ")\n;"

    else:
        s = "INSERT INTO " + dest_table + "(" + ','.join(all_fields) + ") VALUES (" + ','.join(
            map(lambda x: '?', all_fields)) + ")"

    return s

   


def format_column_heading_without_blank_header_key(ch):

    # handle tupleized columns
    if ch.__class__ is tuple:
        tup = ch
        ch = ''
        for elem in tup:
            ch = ch + ('' if elem.startswith('Unnamed') else elem + ' ')

    # remove leading/trailing whitespace    
    ch = ch.strip()

    # remove [number] from rhs
    s = match(r"\][0-9]+\[", ch[::-1])  # apply reversed pattern to reversed string because it occurs on rhs
    ch = ch if s is None else ch[:-s.end()]

    # ensure all characters are alphanumeric or underscore
    ch = sub(r"[^$A-Za-z0-9_]+", '_', ch)

    # remove leading & trailing underscores
    ch = ch.strip("_")

    # ensure first character is valid else prepend underscore
    ch = ch if match(r"[$0-9]", ch) is None else "_" + ch

    # lower case
    ch = ch.lower()

    return ch