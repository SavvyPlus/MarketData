import os

import helpers


def unzip_pattern_handler(file_path, pattern, dest_folder=None):
    # HM01X_Data_002064_5559568190
    # pattern = 'HM01X_Data_.+\.txt'
    file_folder, file_name = os.path.split(file_path)
    dest_folder = file_folder if dest_folder is None else dest_folder    
    helpers.unzip_matching_pattern(file_path, dest_folder, pattern)
    return (True, 0)
