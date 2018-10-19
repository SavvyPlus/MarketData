## Change Log - Charlie
Created At: 18/10/2018

## Update Database


**Create new record in SavvyLoaderJob**

> *Warning: Must check source_folder, success_folder and fail_folder before run code*

```
USE [MarketData]
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
           ('D:\MarketData\Queue\Mercari'
           ,'D:\MarketData\Archive\Mercari'
           ,'D:\MarketData\Errors\Mercari'
           ,10
           ,1
           ,'ClosingRates*.csv'
           ,'mercari_handle'
           ,'{''table'':''Environmental_Price_MercariClosingPrices''}'
           ,-1
           ,'Emvironmental Price Mercari Closing Prices')
GO

```

**Create table Environmental_Price_MercariClosingPrices** 

```
USE [MarketData]
GO

/****** Object:  Table [dbo].[Environmental_Price_MercariClosingPrices]    Script Date: 10/18/2018 5:02:28 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Environmental_Price_MercariClosingPrices](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[instrument] [nchar](20) NULL,
	[tenor_type] [nchar](20) NULL,
	[tenor] [nchar](20) NULL,
	[settle] [nvarchar](30) NULL,
	[bid] [float] NULL,
	[offer] [float] NULL,
	[mid] [float] NULL,
	[bid_method] [int] NULL,
	[offer_method] [int] NULL,
	[mid_method] [nchar](10) NULL,
	[source_file_id] [int] NULL,
	[added_dttm] [datetime] NULL
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Environmental_Price_MercariClosingPrices] ADD  CONSTRAINT [DF_Environmental_Price_MercariClosingPrices_added_dttm]  DEFAULT (getdate()) FOR [added_dttm]
GO

```


**Update code on SavvyDataLoader.py 234:235**

234:235

```
... 
        elif (handler == 'mercari_handler'):
            (success, recs_loaded) = handlers.mercari_data_handler(source_file_id=fileid, fname=file_fullname,
                                                                      conn=conn, **hp)
...
```

**Update code on Handler.py**

27:38
```
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
```

992:1023

``` 
def mercari_data_handler(source_file_id, fname, conn=None, table='Environmental_Price_MercariClosingPrices', **kwargs):
    (fpath, filename) = os.path.split(fname)
    (filename, fileext) = os.path.splitext(filename)
    key_field = MERCARI_FIELD

    xl = ExcelFile(fname)
    data_field = xl.parse(**kwargs)
    data_field = data_field.rename(columns=format_column_heading)
    # add source file identifier
    data_field['source_file_id'] = source_file_id

    if table is None:
        return data_field

    # merge into database
    sql = sql_merge_statement(table, data_field.keys(), key_field)

    sql_params = map(tuple, data_field.values)

    # convert nans to None so insert/update will work correctly
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)
    # try:
    # merge to database if any records found
    if len(data_field) > 0:
        curs = conn.cursor()
        curs.executemany(sql, sql_params)
        conn.commit()
        curs.close()
    # except:
    #    raise
    #    return (df, sql)
    return True, len(data_field)
```

