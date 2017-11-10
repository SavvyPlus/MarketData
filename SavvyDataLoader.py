# -*- coding: utf-8 -*-
"""
Created on Wed Jul 02 14:17:19 2014

@author: andrew.dodds
"""

import sys
import cdecimal
assert "sqlalchemy" not in sys.modules
#assert "decimal" not in sys.modules
#sys.modules["decimal"] = cdecimal

import time
import datetime
import ast              # literal_eval
import pyodbc
import configparser
import os
import json
import logging
import logging.config
import fnmatch
import shutil
import handlers
import traceback
from datadog import initialize
from datadog import statsd
from datadog import api as DataDogAPI


def setup_logging(
    default_path='logging_config.json', 
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:            
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
    config = configparser.ConfigParser()
    config.read('DataDog.cfg')
    api_key = str(config.get('Datadog Connection','api_key'))
    app_key = str(config.get('Datadog Connection','app_key'))

    initialize(api_key=api_key,app_key=app_key)  
    statsd.increment('page.views')

# Initialise logging
tags = ['version:1', 'application:loader']

logger = logging.getLogger(__name__)
setup_logging()
logger.info("Savvy Data Loader starting...")

# Configuration
logger.info("Loading configuration files")
config = configparser.RawConfigParser()
config.read('SavvyDataLoader.cfg')
db_connection_string = config.get('Database Connection','odbcconnectionstring')
db_connect_retry_seconds = config.getint('Database Connection','retry_time_seconds')

folders_refresh_seconds = config.getint('Refresh Times','source_locations_refresh_seconds')
files_refresh_seconds = config.getint('Refresh Times','source_files_refresh_seconds')
folders_purge_seconds = 60*config.getint('Refresh Times','archive_purge_delay_minutes')

def get_source_folder_list():
    # Database connection
    logger.info("Retrieving folder list from database.")
    try:
        conn = pyodbc.connect(db_connection_string)
    except:
        logger.error("FATAL ERROR. Could not establish database connection using connection string %s", db_connection_string, exc_info=1)
        time.sleep(db_connect_retry_seconds)
        return []        
        
    with conn.cursor() as curs:    
        curs.execute("""
            SELECT ID,source_folder,success_folder,fail_folder,[priority]      
            ,filename_pattern,handler,handler_params,success_retention_days
            FROM [MarketData].[dbo].[SavvyLoaderJobs]
            where active_flag = 1
            ORDER BY [priority] ASC
        """)
        folder_tup = curs.fetchall()
    
    # Close database connections        
    conn.close()
    
    return folder_tup    
    

def get_source_file_list(folders):
    logger.info("Refreshing file queue for all folders")
    files = []
    # note: folders are probably already sorted in priority order but don't rely on this
    for folder in folders:
        dirpath = folder[1].strip()
        priority = folder[4]
        jobid = folder[0]
        filename_pattern = folder[5].strip()
        
        full_filenames = [os.path.join(dirpath, fn) for fn in fnmatch.filter(os.listdir(dirpath),filename_pattern)]
        file_stats = [(path,os.stat(path)) for path in full_filenames]
        # (priority,modified date,size,filename,folder)
        entries = [(priority,item[1].st_mtime,item[1].st_size,item[0],folder) for item in file_stats]   

        files = files+entries
        
    # sort by priority,modified date,size,filename,folder
    files = sorted(files,key=lambda t: (t[0],t[1],t[2],t[3]),reverse=True)
    # (filename,folder)    
    files = [(item[3],item[4]) for item in files]     
    
    return files    
        
def purge_archives(folders):
    logger.info('Archive folder purging commenced')
    
    for folder in folders:
        (job_id,source_folder,archive_folder,fail_folder,pr,filename_pattern,handler,handler_params,purge_delay) = folder        
        try:
            if purge_delay >= 0:    # if purge days is negative, no purging
                full_filenames = [os.path.join(archive_folder, fn) for fn in fnmatch.filter(os.listdir(archive_folder),filename_pattern)]
                for fname in full_filenames:      
                    f_mod = datetime.datetime.fromtimestamp(os.path.getmtime(fname))
                    if datetime.datetime.now() - f_mod > datetime.timedelta(days=purge_delay):
                        os.remove(fname)
        except EnvironmentError:
            logger.warning('[JobID=%d] Error purging files from folder: %s', job_id, archive_folder, exc_info=1)
                            
        
    logger.info('Archive folder purging complete')
    
def process_file(file_name, folder_tup):
    
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

    logger.info('Commencing process for file: %s', file_name)

    # database connection
    try:
        conn = pyodbc.connect(db_connection_string)
    except pyodbc.Error:
        error_text = "Error processing file: %s. Could not establish database connection using connection string %s" % (file_name, db_connection_string)        
        logger.error(error_text, exc_info=1)
        DataDogAPI.Event.create(title="Error processing file:", text=error_text, alert_type="error",tags=tags) 

        time.sleep(db_connect_retry_seconds)
        return False
        
    # check that file is actually present in specified location, and is not opened exclusively
    try:
        file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_fullname))
        # rename file to ensure it's not locked by another process        
        os.rename(file_fullname, file_fullname+".renamed")
        os.rename(file_fullname+".renamed", file_fullname)

    except EnvironmentError:
        error_text= "File does not exist or is in use by another process: %s" % (file_fullname)        
        logger.error(error_text, file_fullname, exc_info=1)
        DataDogAPI.Event.create(title="EnvironmentError file:", text=error_text, alert_type="error") 
        return False

                    
    # write to loader file list in database
    try:
        with conn.cursor() as curs:
            # `file_name` field in [MarketData].[dbo].[SavvyLoaderFiles] is limit at 80 characters
            if len(file_name) > 80:
                file_name_saved_in_db = file_name[-80:]
            else:
                file_name_saved_in_db = file_name
            tmp = (job_id, file_name_saved_in_db, source_folder, 'STARTED', 0, file_mod_time)
            curs.execute("INSERT INTO MarketData.dbo.SavvyLoaderFiles (job_id,file_name,source_folder,process_status,records_processed,file_modified_dttm) OUTPUT Inserted.ID VALUES (?,?,?,?,?,?)", tmp)
            fileid = curs.fetchone()[0]            
            conn.commit()
    except pyodbc.Error:
        error_text = "Could not log file to database: %s" % file_name
        logger.error(error_text)
        DataDogAPI.Event.create(title="File Error:", text=error_text, alert_type="error",tags=tags) 

        return False
    
    # handler parameters
    try:
        hp = ast.literal_eval(handler_params)
        hp = dict(hp)
    except SyntaxError:
        logger.warn("Invalid handler parameter string encountered for file: %s. Illegal syntax for a Python literal: %s", file_name, handler_params)            
        hp = {}
    except TypeError:
        logger.warn("Invalid handler parameter string encountered for file: %s. Expecting a dict type", file_name)            
        hp = {}
    
        
    # process file
    try:
        if handler == 'csv_handler':
            (success,recs_loaded) = handlers.csv_load(source_file_id=fileid,fname=file_fullname,conn=conn, **hp)
        elif handler == 'move_only':
            (success,recs_loaded) = (True,0) # no processing to do - simply return success
        elif handler == 'unzip':
            (success,recs_loaded) = handlers.unzip_handler(source_file_id=fileid,fname=file_fullname,conn=conn, **hp)
        elif handler == 'asx_handler':
            (success,recs_loaded) = handlers.asx_load(source_file_id=fileid,fname=file_fullname,conn=conn, **hp)
        elif handler == 'tasHydro_handler':
            (success,recs_loaded) = handlers.tasHydro_load(source_file_id=fileid,fname=file_fullname,conn=conn, **hp)            
        elif handler == 'nem12_handler':
            (success,recs_loaded) = handlers.aemo_meter_data_handler(source_file_id=fileid,fname=file_fullname,conn=conn, **hp)
        elif handler == 'precis_forecast_handler':
            (success,recs_loaded) = handlers.weather_forecast_load(source_file_id=fileid,fname=file_fullname,conn=conn)
        else:
            (success,recs_loaded) = (False,0)
            logger.error("Invalid or unknown loading handler specified: %s", handler)
    except Exception as ex:
        (success,recs_loaded) = (False,0)
        error_text= "Unknown error in loading handler while processing file %s" % (file_name)  
        logger.error(error_text)
        DataDogAPI.Event.create(title="Unknown error: ", text=error_text+"\n" +str(ex) , alert_type="error",tags=tags,aggregation_key="initialize") 


        
    
    if success:
        logger.info('File loaded successfully: %s', file_name)
        dest_folder = os.path.join(success_folder,file_name)
    else:        
        logger.info('File failed to load: %s', file_name) 
        dest_folder = os.path.join(fail_folder,file_name)
    
    try:        
        shutil.move(file_fullname, dest_folder)    
    except EnvironmentError:
        error_text = "Unable to move file: %s to location %s"% (file_name, dest_folder)        
        logger.error(error_text, exc_info=1)
        DataDogAPI.Event.create(title="Move File error: ", text=error_text, alert_type="error",tags=tags) 

        
    # write to loader file list in database
    try:
        with conn.cursor() as curs:            
            tmp = ('SUCCESS' if success else 'ERROR', recs_loaded, fileid)
            curs.execute("UPDATE MarketData.dbo.SavvyLoaderFiles SET process_status = ?, records_processed = ? WHERE ID = ?", tmp)
            conn.commit()
    except pyodbc.Error:
        logger.warn("Could not log file processing status to database: %s", file_name)
    
        
    conn.close()
        
    return success
    

def main():    
    last_folders_refresh = last_files_refresh = last_purge = None
    try:
        while True:            
            # refresh folder list if required
            if last_folders_refresh is None or time.clock()-last_folders_refresh > folders_refresh_seconds:
                last_folders_refresh = time.clock()
                folders = get_source_folder_list()
                last_files_refresh = None       # make sure we refresh files if folders change

            # purge old archive files
            if last_purge is None or time.clock()-last_purge > folders_purge_seconds:
                last_purge = time.clock()
                purge_archives(folders)
                
            # retrieve or refresh file list
            if last_files_refresh is None or time.clock()-last_files_refresh > files_refresh_seconds:
                last_files_refresh = time.clock()
                files = get_source_file_list(folders)

            # process first file in queue or sleep if no files
            if len(files) == 0:
                time.sleep(1)
            else:
                this_file = files.pop()
                process_file(this_file[0], this_file[1])   # filename and folder
            
    except KeyboardInterrupt:        
        logger.info("Application terminated by user. Exiting")
    except:
        error_text = "Fatal error encountered. Exiting"
        logger.error(error_text, exc_info=1)
        DataDogAPI.Event.create(title="Fatal error", text=error_text, alert_type="error",tags=tags) 
        raise
        
    

if __name__ == "__main__":
    main()