# Change Log
 - Created At: 18/10/2018
 - Last Updated At: 22/10/2018

# Update Database

## 18/10/2018
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
           ('E:\MarketData\Queue\Mercari'
           ,'E:\MarketData\Archive\Mercari'
           ,'E:\MarketData\Errors\Mercari'
           ,10
           ,1
           ,'ClosingRates*.csv'
           ,'mercari_handler'
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

UPDATE 2 new function handling Mercari

```
...
def mercari_data_handler(source_file_id, fname, conn=None, dest_table='Environmental_Price_MercariClosingPrices',
                         header_end_text=None, footer_start_text=None, **kwargs):
...

def sql_mercari_merge_statement(dest_table, all_fields, key_fields):
...

```

---

## 22/10/2018

Change name table Environmental_Price_MercariClosingPrices to Environmental_Prices_MercariClosingPrices

> Using Procedures
``` 
EXEC sp_rename 'Environmental_Price_MercariClosingPrices', 'Environmental_Prices_MercariClosingPrices'
```

update code match a day in filename

``` 
...
    date_match = search(r'\d{4}-\d{2}-\d{2}', fname)
    if date_match.group():
        df['added_dttm'] = date_match.group();
...        
```
