import helpers


def unzip_pattern_handler(file_path, dest_folder, pattern):
    # HM01X_Data_002064_5559568190
    # pattern = 'HM01X_Data_.+\.txt'
    helpers.unzip_matching_pattern(file_path, dest_folder, pattern)
    return (True, 0)
