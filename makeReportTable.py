# -*- coding: utf-8 -*-
"""
Created on Thu Jul 03 17:01:55 2014

@author: andrew.dodds
"""
import pyodbc
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

db_connection_string = 'Driver={SQL Server Native Client 11.0};Server=MEL-SVR-DB-002;Database=MarketData;Trusted_Connection=yes;'
#db_connection_string = 'Driver={SQL Server Native Client 11.0};Server=MEL-LT-001\SQLEXPRESS;Database=Reporting;Trusted_Connection=yes;'

# example
# f = get_view_info('MarketData','dbo','spotPriceView')
# print create_report_table_sql_command('Reporting','dbo','SpotPrice_Vers',f)
# print create_report_view_sql_command('SpotPrice','Reporting','dbo','SpotPrice_Vers',f)
# print create_migration_proc_sql_command('Reporting', 'dbo', 'SpotPrice_Vers', 'MarketData.dbo.spotPriceView', f, ['settlementdate','regionid'], False, False)

# example 2
# f = get_view_info('MarketData','dbo','Futures_Trades')
#print create_report_table_sql_command('Reporting','dbo','Futures_Trades_Vers',f) + \
#    create_report_view_sql_command('Futures_Trades','Reporting','dbo','Futures_Trades_Vers',f) + \
#    create_migration_proc_sql_command('Reporting', 'dbo', 'Futures_Trades_Vers', 'MarketData.dbo.Futures_Trades', f, ['Trade_Time', 'Trade_Type', 'Trade_Volume', 'InstrumentCode', 'Trade_Price'], True)



def get_view_info(catalog,schema,name):
    # Database connection
    logger.info("Retrieving view information from database.")
    try:
        conn = pyodbc.connect(db_connection_string)
    except:
        logger.error("FATAL ERROR. Could not establish database connection using connection string %s", db_connection_string, exc_info=1)        
        return []        
        
    with conn.cursor() as curs:
        curs.execute(""" 
        SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION
        FROM """ + catalog +""".INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '""" + name + "' AND TABLE_SCHEMA='" + schema + "'"
        )
        column_tup = curs.fetchall()
    
    # Close database connections        
    conn.close()
    
    return column_tup
    

def sql_column_string(column_tup,all_nullable_flag=False):
    name = "["+column_tup[3]+"]"
    datatype = column_tup[5]
    null = 'NOT NULL' if column_tup[4] == 'NO' else 'NULL'
    char_max = 'max' if column_tup[6] == 1 else column_tup[6]
    num_prec = column_tup[7]
    num_scale = column_tup[8]
    datetime_prec = column_tup[9]
    if all_nullable_flag:
        null = ''
    
    if datatype in ("bigint","int","smallint","tinyint","bit","money","smallmoney"):
        return " ".join((name,datatype,null))
        
    if datatype in ("decimal","numeric"):
        return " ".join((name,datatype+"("+str(num_prec)+","+str(num_scale)+")",null))

    if datatype in ("datetime","smalldatetime","date"):
        return " ".join((name,datatype,null))
        
    if datatype in ("time","datetimeoffset","datetime2"):
        return " ".join((name,datatype+"("+str(datetime_prec)+")",null))
        
    if datatype in ("varchar","nvarchar","char","nchar"):
        return " ".join((name,datatype+"("+str(char_max)+")",null))
        
    if datatype in ("binary","varbinary"):
        return " ".join((name,datatype+"("+str(char_max)+")",null))
        
    if datatype in ("float","real"):
        return " ".join((name,datatype+"("+str(num_prec)+")",null))

    if datatype in ("ntext","text","image"):
        return " ".join((name,datatype,null))
    
    logger.error("Unhandled data type %s", datatype)
    

def create_report_table_sql_command(catalog,schema,name,column_tup,index_key_fields=None):
    f = column_tup
    create_table_sql = """
USE [{0}]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [{1}].[{2}](
	[ID] [int] IDENTITY(1,1) NOT NULL,
""".format(catalog,schema,name) + "\n".join(map(lambda x: "\t"+sql_column_string(x)+",",f)) + """
	[ActiveFrom] [datetime2] NOT NULL,
	[ActiveTo] [datetime2] NOT NULL,
  CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO


""".format(name)
    
    if index_key_fields is None:
        return create_table_sql
        
    create_index_sql = """
CREATE NONCLUSTERED INDEX [KeyIndex_{2}] ON [{1}].[{2}]
(""".format(catalog,schema,name) + ','.join(map(lambda x: x+' ASC', index_key_fields)) + """
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
GO    
""".format(catalog,schema,name)

    return create_table_sql + create_index_sql
    
    
def create_report_view_sql_command(view_name,catalog,schema,name,column_tup):
    f = column_tup    
    create_view_sql = """
USE [{0}]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [{1}].[{3}]
AS
SELECT        ID, """.format(catalog,schema,name,view_name) + ",".join(map(lambda x: x[3],f)) + """,
	ActiveFrom AS Last_Modified_dttm
FROM [{1}].[{2}]
WHERE        (ActiveTo = CONVERT(DATETIME2, '9999-12-31 00:00:00', 102))
GO
""".format(catalog,schema,name)

    return create_view_sql
    
def create_migration_proc_sql_command(target_catalog, target_schema, target_table, source_view, column_tup, key_fields, point_in_time=True, remove_if_not_found=True, sp_name=None):
    all_fields = map(lambda x: x[3],column_tup)
    data_fields = list(set(all_fields).difference(key_fields))
    if set(data_fields).union(key_fields) <> set(all_fields):
        logger.error('key_fields are not valid fields. key_fields are case sensitive')
        return
    if len(key_fields) == 0:
        logger.error('no key fields specified')
        return
    
    if sp_name is None:
        sp_name = "sp_Synchronise_Reporting_Table_" + target_table
        
    if remove_if_not_found == False:
        optional_delete_clause = ""
    elif remove_if_not_found == True:
        optional_delete_clause = "WHEN NOT MATCHED BY SOURCE AND T.ActiveTo = @late_date THEN DELETE"
        
    if point_in_time==False:
        versioning_clause = ""
    elif point_in_time==True:
         versioning_clause = """
INSERT {0} ({1},ActiveFrom,ActiveTo)	--all fields, table name
SELECT {1},ActiveFrom,@now						--all fields
FROM @change_table
WHERE merge_action = 'UPDATE' OR merge_action = 'DELETE'
""".format(target_catalog+"."+target_schema+"."+target_table,
           ",".join(all_fields), 
)


    
            
    
    update_clause = "" if len(data_fields)==0 else """
	WHEN MATCHED AND T.ActiveTo = @late_date AND ({0}) THEN		--data fields
		UPDATE SET {1}, T.ActiveFrom = @now							--data fields
      """.format("\n OR " .join(map(lambda x: "(S.[{0}]<>T.[{0}] OR (S.[{0}] IS NULL AND T.[{0}] IS NOT NULL) OR (T.[{0}] is null and S.[{0}] is not null))".format(x),data_fields)), ",".join(map(lambda x: "T."+x+"=S."+x, data_fields)))


    sql = """
CREATE PROCEDURE {9}
AS
BEGIN

DECLARE @report_table_name varchar(50) = '{0}'
DECLARE @process_status varchar(20) = 'SUCCESS'

BEGIN TRANSACTION
DECLARE @log TABLE (
	entry_datetime datetime DEFAULT (getdate()),
	txt varchar(1000)
)
INSERT @log (txt) VALUES('Starting process for table ' + @report_table_name)

BEGIN TRY

DECLARE @now datetime = getdate()
DECLARE @late_date datetime = '9999-12-31'

DECLARE @rows_upd varchar(10) = '0'
DECLARE @rows_ins varchar(10) = '0'
DECLARE @rows_del varchar(10) = '0'

DECLARE @total_source_records int
SET @total_source_records = (SELECT COUNT(*) FROM {1})

-- Replace field names and data types with those of view/table
DECLARE @change_table TABLE (
	[merge_action] varchar(6),	
	{2},
	[ActiveFrom] [datetime],
	[ActiveTo] [datetime]
)

INSERT @log (txt) VALUES('Commencing merge')

MERGE {3} T USING {1} S
ON {8}			--key fields
{10}
WHEN NOT MATCHED BY TARGET THEN
	INSERT ({5},ActiveFrom,ActiveTo) VALUES ({6},@now,@late_date) --all fields
{4}
OUTPUT $action as [action],{7},deleted.ActiveFrom,deleted.ActiveTo INTO @change_table --all fields
;
--) as A;

SET @rows_ins = (SELECT CAST(COUNT(merge_action) AS varchar(10)) from @change_table where merge_action = 'INSERT')
SET @rows_upd = (SELECT CAST(COUNT(merge_action) AS varchar(10)) from @change_table where merge_action = 'UPDATE')
SET @rows_del = (SELECT CAST(COUNT(merge_action) AS varchar(10)) from @change_table where merge_action = 'DELETE')


--select @rows_ins,@rows_upd,@rows_del

--select * from {1}
--select * from @change_table

INSERT @log (txt) VALUES('Marking inactive records')

{11}

INSERT @log (txt) VALUES('Process complete. Inserted '+@rows_ins+', updated '+@rows_upd+', deleted '+@rows_del)
COMMIT TRANSACTION
END TRY
BEGIN CATCH
	ROLLBACK TRANSACTION	
	INSERT @log (txt) VALUES('Msg '+CAST(ERROR_NUMBER() AS varchar(10))+', '+ isnull(ERROR_PROCEDURE(),'Script') +' Line '+CAST(ERROR_LINE() AS varchar(10))+'. '+ERROR_MESSAGE())
	SET @process_status = 'FAILURE'
END CATCH

INSERT dbo.[Messages] (entry_datetime,log_entry)
select entry_datetime, txt from @log

INSERT dbo.[MergeProcess] (Process_Name,Target_Table,Process_Status,Records_Inserted,Records_Updated,Records_Deleted,Total_Source_Records,Start_Time,Finish_Time)
VALUES (@report_table_name,@report_table_name,@process_status,CAST(@rows_ins as int),CAST(@rows_upd as int),CAST(@rows_del as int),@total_source_records,@now,getdate())

END
GO
""".format(target_table, 
           source_view, 
           ',\n\t'.join(map(lambda x:sql_column_string(x,True),column_tup)),
            target_catalog+"."+target_schema+"."+target_table, 
            optional_delete_clause, 
            ",".join(all_fields), 
            ",".join(map(lambda x: "S."+x,all_fields)), 
            ",".join(map(lambda x: "deleted."+x,all_fields)), 
            " AND ".join(map(lambda x: "T."+x+" = S."+x,key_fields)), 
            sp_name,
            update_clause,
            versioning_clause
            )
    return sql
    
    #                                                                                                                                                                                                                                                                       (T.RRP <> S.RRP)
    
    
    