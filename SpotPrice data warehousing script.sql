-- Replace name of table
DECLARE @report_table_name varchar(50) = 'SpotPrice'
DECLARE @process_status varchar(20) = 'SUCCESS'

BEGIN TRANSACTION
DECLARE @log TABLE (
	entry_datetime datetime DEFAULT (getdate()),
	txt varchar(1000)
)
INSERT @log (txt) VALUES('Starting process for table ' + @report_table_name)

BEGIN TRY
USE Reporting

DECLARE @now datetime = getdate()
DECLARE @late_date datetime = '9999-12-31'

DECLARE @rows_upd varchar(10) = '0'
DECLARE @rows_ins varchar(10) = '0'
DECLARE @rows_del varchar(10) = '0'

DECLARE @total_source_records int
SET @total_source_records = (SELECT COUNT(*) FROM MarketData.dbo.SpotPriceView)

-- Replace field names and data types with those of view/table
DECLARE @change_table TABLE (
	[merge_action] varchar(6),	
	[SETTLEMENTDATE] [datetime],
	[REGIONID] [varchar](100),--[REGIONID] [varchar](10),
	[RRP] [float],
	--[RRP] [numeric](15, 5),
	[ActiveFrom] [datetime],
	[ActiveTo] [datetime]
)

INSERT @log (txt) VALUES('Commencing merge')

-- SEVERAL CHANGES REQUIRED
MERGE dbo.SpotPrice T USING MarketData.dbo.SpotPriceView S					--table & view names
ON T.SETTLEMENTDATE = S.SETTLEMENTDATE AND T.REGIONID = S.REGIONID			--key fields
WHEN MATCHED AND T.ActiveTo = @late_date AND ((T.RRP <> S.RRP)) THEN		--data fields
	UPDATE SET T.RRP = S.RRP, T.ActiveFrom = @now							--data fields
WHEN NOT MATCHED BY TARGET THEN
	INSERT (SETTLEMENTDATE,REGIONID,RRP,ActiveFrom,ActiveTo) VALUES (S.SETTLEMENTDATE,S.REGIONID,S.RRP,@now,@late_date) --all fields
WHEN NOT MATCHED BY SOURCE AND T.ActiveTo = @late_date THEN					--are these
	DELETE																	--required?
OUTPUT $action as [action],deleted.settlementdate,deleted.regionid,deleted.rrp,deleted.ActiveFrom,deleted.ActiveTo INTO @change_table --all fields
;
--) as A;

SET @rows_ins = (SELECT CAST(COUNT(merge_action) AS varchar(10)) from @change_table where merge_action = 'INSERT')
SET @rows_upd = (SELECT CAST(COUNT(merge_action) AS varchar(10)) from @change_table where merge_action = 'UPDATE')
SET @rows_del = (SELECT CAST(COUNT(merge_action) AS varchar(10)) from @change_table where merge_action = 'DELETE')


--select @rows_ins,@rows_upd,@rows_del

--select * from MarketData.dbo.SpotPriceView
--select * from @change_table

INSERT @log (txt) VALUES('Marking inactive records')

INSERT dbo.SpotPrice (SETTLEMENTDATE,REGIONID,RRP,ActiveFrom,ActiveTo)	--all fields, table name
SELECT SETTLEMENTDATE,REGIONID,RRP,ActiveFrom,@now						--all fields
FROM @change_table
WHERE merge_action = 'UPDATE' OR merge_action = 'DELETE'

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

INSERT dbo.[MergeProcess] (Process_Name,Target_Table,Process_Status,Records_Inserted,Records_Updated,Records_Deleted,Total_Source_Records)
VALUES (@report_table_name,@report_table_name,@process_status,CAST(@rows_ins as int),CAST(@rows_upd as int),CAST(@rows_del as int),@total_source_records)