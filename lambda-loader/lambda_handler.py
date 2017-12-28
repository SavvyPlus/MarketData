import sys

import pymssql
import sys
import time
import datetime
import ast              # literal_eval
import configparser
import os
import json
import logging
import logging.config
import fnmatch
import shutil
import handlers
from boto3 import client
import re
import urllib2


conn = client('s3')

def move_file(filename):
    """
    Moves a file from src to dst
    """
    print("moving file")
    print(filename)
    conn.copy({'Bucket': "savvyloader", 'Key': 'savvy-process/%s' % filename}, "savvyloader", 'savvy-archine/%s' % filename)
    conn.delete_object(Bucket="savvyloader", Key='savvy-process/%s' % filename)
    

def get_source_folder_list():
    try:
        connection = pymssql.connect(server='app-2-savvy-work.ciimkozo2x5b.ap-southeast-2.rds.amazonaws.com', user='djangounchained', password='jLddF5JQ%v!0', database='MarketData')
        print("Connected...\n")

        cursor = connection.cursor()
        cursor.execute("""
            SELECT ID,source_folder,success_folder,fail_folder,[priority]      
            ,filename_pattern,handler,handler_params,success_retention_days
            FROM [MarketData].[dbo].[SavvyLoaderJobsLambda]
            where active_flag = 1
            ORDER BY [priority] ASC
        """)
        print('query executed...\n')

        folder_tup = cursor.fetchall()
        
        print(folder_tup)
        # Close database connections        
        connection.close()
    
        return folder_tup   
    except:
        print("Unexpected error: ", sys.exc_info()[0] )

def get_source_file_list(folders,filename):
    files = []
    # note: folders are probably already sorted in priority order but don't rely on this
    for folder in folders:
        dirpath = folder[1].strip()
        priority = folder[4]
        jobid = folder[0]
        filename_pattern = folder[5].strip()
        if fnmatch.fnmatch(filename, filename_pattern):
            return folder
def process_file(file_name, folder_tup):
    print(folder_tup)
    # todo: if folder_tup not supplied, search folders for 1st match. For ad-hoc use/testing
    
    # unpack file & folder tuples
    (job_id,source_folder,success_folder,fail_folder,pr,fnp,handler,handler_params,pd) = folder_tup
#    job_id = -1
#    handler = ''
#    
#    source_folder = ''
#    handler_params = ''
#    success_folder = ''
#    fail_folder = ''
    
    # initially file_name may be full path or just file name    
    file_fullname = os.path.join(source_folder, file_name)
    (tmp,file_name) = os.path.split(file_fullname)

    # database connection
    try:
        connection = pymssql.connect(server='app-2-savvy-work.ciimkozo2x5b.ap-southeast-2.rds.amazonaws.com', user='djangounchained', password='jLddF5JQ%v!0', database='MarketData')
    except pyodbc.Error:
        error_text = "Error processing file: %s. Could not establish database connection using connection string %s" % (file_name, db_connection_string)        
        print(error_text)
        return False

    try:
        file_mod_time = datetime.datetime.fromtimestamp(time.time())
    except Exception as e:
        print(e)        
        return False                
    # write to loader file list in database
    try:
        with connection.cursor() as curs:
            # `file_name` field in [MarketData].[dbo].[SavvyLoaderFiles] is limit at 80 characters
            if len(file_name) > 80:
                file_name_saved_in_db = file_name[-80:]
            else:
                file_name_saved_in_db = file_name
            tmp = (job_id, file_name_saved_in_db, source_folder, 'STARTED', 0, file_mod_time)
            print('insert files')
            curs.execute("INSERT INTO MarketData.dbo.SavvyLoaderFiles (job_id,file_name,source_folder,process_status,records_processed,file_modified_dttm) OUTPUT Inserted.ID VALUES (%d,%d,%d,%d,%d,%d)", tmp)
            fileid = curs.fetchone()[0]     
            connection.commit()
    except Exception as e:
        error_text = "Could not log file to database: %s" % file_name
        print(e)
        return False
    
    # handler parameters
    try:
        hp = ast.literal_eval(handler_params)
        hp = dict(hp)
    except SyntaxError:
        hp = {}
    except TypeError:
        hp = {}            


    try:
        if handler == 'csv_handler':
            (success,recs_loaded) = handlers.csv_load(source_file_id=fileid,fname=file_fullname,conn=connection, **hp)
        elif handler == 'move_only':
            (success,recs_loaded) = (True,0) # no processing to do - simply return success
        elif handler == 'unzip':
            (success,recs_loaded) = handlers.unzip_handler(source_file_id=fileid,fname=file_fullname,conn=connection, **hp)
        elif handler == 'asx_handler':
            print('asx_handler')
            (success,recs_loaded) = handlers.asx_load(source_file_id=fileid,fname=file_name,conn=connection, **hp)
        elif handler == 'tasHydro_handler':
            (success,recs_loaded) = handlers.tasHydro_load(source_file_id=fileid,fname=file_fullname,conn=connection, **hp)            
        elif handler == 'nem12_handler':
            (success,recs_loaded) = handlers.aemo_meter_data_handler(source_file_id=fileid,fname=file_fullname,conn=connection, **hp)
        elif handler == 'precis_forecast_handler':
            (success,recs_loaded) = handlers.weather_forecast_load(source_file_id=fileid,fname=file_fullname,conn=connection)
        else:
            (success,recs_loaded) = (False,0)
            logger.error("Invalid or unknown loading handler specified: %s", handler)
    except Exception as e:
        (success,recs_loaded) = (False,0)
        error_text= "Unknown error in loading handler while processing file %s" % (file_name)
        print(e)
    connection.close()
        


def lambda_handler(event, context):
    filename = event['Records'][0]['s3']['object']['key'].split('/')[-1]
    filename = urllib2.unquote(filename)
    filename = filename.replace('+',' ')
    file_folder = get_source_file_list(get_source_folder_list(),filename)
    process_file(filename,file_folder)
    move_file(filename)

# main method for testing outside of lambda environment
if __name__ == '__main__':
    filename = 'From(ASX Energy (webmaster@asxenergy.com.au))_ID(90_2)_Electricity-Au-FinalSnapshot-20170501.csv'
    # get_source_file_list(get_source_folder_list())
    # files =get_source_file_list(get_source_folder_list())
    # this_file = files.pop()
    # print(this_file[0])    
    # print(this_file[1])
    move_file(filename)

