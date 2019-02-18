
import logging
from StringIO import StringIO
import pandas as pd

import settings
import helpers


logger = logging.getLogger(__name__)


def hm01x_loader(source_file_id, file_path, **kwargs):
    header = kwargs.get('header', 0)
    columns = kwargs.get('columns', settings.BOM_IntraDay_Weather_Columns)

    df = pd.read_csv(file_path, header=header)

    # df = df.drop(columns=['#'])
    del df['#']
    df.insert(2, 'DateTime_Local', 0, allow_duplicates=True)
    df.insert(8, 'DateTime_Std', 0, allow_duplicates=True)

    df['DateModified'] = helpers.get_time_now()
    df['Source_File_ID'] = source_file_id

    df.columns = columns

    df['DateTime_Local'] = df['Year_Local'].map(str) + '-' + df['Month_Local'].map(str) + '-' + \
                           df['Day_Local'].map(str) + ' ' + df['Hour_Local'].map(str) + ':' + \
                           df['Minute_Local'].map(str) + ':00.000'
    # df['DateTime_Local'] = pd.to_datetime(df['DateTime_Local'], format='%Y-%m-%d %H:%M:%S.%f')

    df['DateTime_Std'] = df['Year_Std'].map(str) + '-' + df['Month_Std'].map(str) + '-' + \
                         df['Day_Std'].map(str) + ' ' + df['Hour_Std'].map(str) + ':' + \
                         df['Minute_Std'].map(str) + ':00.000'
    # df['DateTime_Std'] = pd.to_datetime(df['DateTime_Std'], format='%Y-%m-%d %H:%M:%S.%f')
    return df


def hm01x_handler(source_file_id, file_path, conn=None, **kwargs):
    try:
        df = hm01x_loader(source_file_id, file_path, **kwargs)
    except Exception as e:
        logger.error('hm01x_loader error: {}'.format(e))
        return (False, 0)

    dest_table = kwargs.get('dest_table', settings.BOM_IntraDay_Weather_Table)

    if dest_table is None:
        return (df, len(df.index))

    # merge into database
    key_fields = kwargs.get('key_fields', settings.BOM_IntraDay_Weather_Keys)
    sql = helpers.sql_merge_statement(dest_table, list(df.columns.values), key_fields)

    sql_params = map(tuple, df.values)
    # convert nans to None so insert/update will work correctly
    sql_params = map(lambda sp: map(lambda x: None if x.__class__ is float and isnan(x) else x, sp), sql_params)

    try:
        if len(df.index) > 0:
            curs = conn.cursor()
            curs.executemany(sql, sql_params)
            conn.commit()
            curs.close()
    except Exception as e:
        logger.error('hm01x_handler saving db error: {}'.format(e))

    return (True, len(df.index))
