
import logging
from StringIO import StringIO
import pandas as pd

import settings
import helpers


logger = logging.getLogger(__name__)


def hm01x_loader(file_path, source_file_id):
    df = pd.read_csv(file_path, header=0)
    df = df.drop(columns=['#'])
    df.insert(2, 'DateTime_Local', 0, allow_duplicates=True)
    df.insert(8, 'DateTime_Std', 0, allow_duplicates=True)
    df['DateModified'] = helpers.get_time_now()
    df['Source_File_ID'] = source_file_id
    df.columns = settings.BOM_IntraDay_Weather_Columns

    df['DateTime_Local'] = df['Year_Local'].map(str) + '-' + df['Month_Local'].map(str) + '-' + \
                           df['Day_Local'].map(str) + ' ' + df['Hour_Local'].map(str) + ':' + \
                           df['Minute_Local'].map(str) + ':00.000'
    df['DateTime_Local'] = pd.to_datetime(df['DateTime_Local'], format='%Y-%m-%d %H:%M:%S.%f')

    df['DateTime_Std'] = df['Year_Std'].map(str) + '-' + df['Month_Std'].map(str) + '-' + \
                         df['Day_Std'].map(str) + ' ' + df['Hour_Std'].map(str) + ':' + \
                         df['Minute_Std'].map(str) + ':00.000'
    df['DateTime_Std'] = pd.to_datetime(df['DateTime_Std'], format='%Y-%m-%d %H:%M:%S.%f')
    # df.to_csv('test_bom_hm01x.csv', index=False)
    return df
