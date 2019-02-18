## HM01X_Data Change Log
### 2019/02/18

#### Insert unzip_pattern record for HM01X files

```
USE [MarketData_TEST]
GO

INSERT INTO [dbo].[SavvyLoaderJobs]
           ([source_folder]
           ,[success_folder]
           ,[fail_folder]
           ,[priority]
           ,[active_flag]
           ,[filename_pattern]
           ,[handler]
           ,[handler_params]
           ,[success_retention_days]
           ,[job_description])
     VALUES
           ('D:\MarketData\Queue\BOM\Intraday-FTP'
           ,'D:\MarketData\Archive\BOM\Intraday-FTP'
           ,'D:\MarketData\Errors\BOM\Intraday-FTP'
           ,10
           ,1
           ,'DS050_hourly_*.zip'
           ,'unzip_pattern'
           ,'{''pattern'': ''HM01X_Data_.+\.txt''}'
           ,-1
           ,'Unzip BOM Intraday HM01X Files')
GO
```

```
USE [MarketData_TEST]
GO

INSERT INTO [dbo].[SavvyLoaderJobs]
           ([source_folder]
           ,[success_folder]
           ,[fail_folder]
           ,[priority]
           ,[active_flag]
           ,[filename_pattern]
           ,[handler]
           ,[handler_params]
           ,[success_retention_days]
           ,[job_description])
     VALUES
           ('D:\MarketData\Queue\BOM\Intraday-FTP'
           ,'D:\MarketData\Archive\BOM\Intraday-FTP'
           ,'D:\MarketData\Errors\BOM\Intraday-FTP'
           ,10
           ,1
           ,'HM01X_Data_*.txt'
           ,'hm01x_handler'
           ,'{}'
           ,-1
           ,'BOM Intraday HM01X Files Handler')
GO
```
